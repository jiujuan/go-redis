package migration

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// KeyFetcher 从节点批量扫描 key 的接口
type KeyFetcher interface {
	// ScanKeys 从 node 扫描 key，cursor=0 开始，返回 (nextCursor, keys, error)
	// nextCursor=0 表示扫描完毕
	ScanKeys(node string, cursor int, count int) (nextCursor int, keys []string, err error)
}

// KeyMover 执行单个 key 的搬运操作接口
type KeyMover interface {
	// MoveKey 将 key 从 srcNode 搬运到 dstNode
	// 需实现：读取 → 原子写入新节点 → 确认后删除旧节点
	MoveKey(key, srcNode, dstNode string) error
}

// Migrator 后台渐进式迁移器
type Migrator struct {
	cfg         *MigrationConfig
	ringMgr     *RingManager
	fetcher     KeyFetcher
	mover       KeyMover
	mctx        *migrationContext

	// 迁移进度（原子计数）
	migratedCount atomic.Int64
	failedCount   atomic.Int64
	skippedCount  atomic.Int64
	totalEstimate atomic.Int64

	onProgress func(Progress) // 进度回调
	onDone     func(task *MigrationTask)
	onError    func(key string, err error)
}

// NewMigrator 创建迁移器
func NewMigrator(
	cfg *MigrationConfig,
	ringMgr *RingManager,
	fetcher KeyFetcher,
	mover KeyMover,
) *Migrator {
	if cfg == nil {
		cfg = DefaultMigrationConfig()
	}
	return &Migrator{
		cfg:     cfg,
		ringMgr: ringMgr,
		fetcher: fetcher,
		mover:   mover,
		mctx:    newMigrationContext(cfg.BatchSize, cfg.Concurrency, cfg.RetryLimit),
	}
}

// OnProgress 设置进度回调（在迁移 goroutine 中调用，避免阻塞）
func (m *Migrator) OnProgress(fn func(Progress)) *Migrator {
	m.onProgress = fn
	return m
}

// OnDone 设置迁移完成回调
func (m *Migrator) OnDone(fn func(*MigrationTask)) *Migrator {
	m.onDone = fn
	return m
}

// OnError 设置单 key 迁移失败回调
func (m *Migrator) OnError(fn func(key string, err error)) *Migrator {
	m.onError = fn
	return m
}

// Start 启动后台迁移，非阻塞，立即返回
// addedNodes: 本次新加入的节点列表
func (m *Migrator) Start(taskID string, addedNodes []string) error {
	if !m.mctx.state.CompareAndSwap(int32(StateIdle), int32(StatePreparing)) {
		return fmt.Errorf("migration already in progress (state: %s)", m.mctx.getState())
	}

	task := &MigrationTask{
		ID:         taskID,
		AddedNodes: addedNodes,
		StartedAt:  time.Now(),
	}
	m.mctx.mu.Lock()
	m.mctx.task = task
	m.mctx.cancelCh = make(chan struct{})
	m.mctx.doneCh = make(chan struct{})
	m.mctx.mu.Unlock()

	// 重置计数器
	m.migratedCount.Store(0)
	m.failedCount.Store(0)
	m.skippedCount.Store(0)
	m.totalEstimate.Store(0)

	// 启动双环：保存旧环，引入新节点
	m.ringMgr.BeginMigration(addedNodes)
	m.mctx.setState(StateMigrating)

	log.Printf("[migrator] task %s started, adding nodes: %v", taskID, addedNodes)
	go m.runMigration(task)
	return nil
}

// Cancel 取消迁移（回滚到旧环）
func (m *Migrator) Cancel() {
	state := m.mctx.getState()
	if state == StateIdle || state == StateDone {
		return
	}
	close(m.mctx.cancelCh)
	<-m.mctx.doneCh
	m.ringMgr.AbortMigration()
	m.mctx.setState(StateIdle)
	log.Printf("[migrator] migration cancelled, rolled back to old ring")
}

// Wait 等待迁移完成（阻塞）
func (m *Migrator) Wait() {
	if m.mctx.getState() == StateIdle {
		return
	}
	<-m.mctx.doneCh
}

// Progress 获取当前迁移进度
func (m *Migrator) Progress() Progress {
	m.mctx.mu.RLock()
	task := m.mctx.task
	m.mctx.mu.RUnlock()

	state := m.mctx.getState()
	migrated := m.migratedCount.Load()
	total := m.totalEstimate.Load()

	var percent float64
	if total > 0 {
		percent = float64(migrated) / float64(total) * 100
	}

	return Progress{
		State:   state,
		Task:    task,
		Percent: percent,
	}
}

// runMigration 后台迁移主流程
func (m *Migrator) runMigration(task *MigrationTask) {
	defer func() {
		close(m.mctx.doneCh)
		task.FinishedAt = time.Now()
		if m.onDone != nil {
			m.onDone(task)
		}
		log.Printf("[migrator] task %s finished: migrated=%d failed=%d skipped=%d elapsed=%s",
			task.ID,
			m.migratedCount.Load(),
			m.failedCount.Load(),
			m.skippedCount.Load(),
			task.FinishedAt.Sub(task.StartedAt).Round(time.Millisecond),
		)
	}()

	// 获取所有旧节点，逐节点扫描
	oldNodes := m.ringMgr.OldNodes()
	if len(oldNodes) == 0 {
		log.Printf("[migrator] no old nodes found, nothing to migrate")
		m.finishMigration()
		return
	}

	// 并发扫描各旧节点
	var wg sync.WaitGroup
	sem := make(chan struct{}, m.cfg.Concurrency)

	for _, node := range oldNodes {
		if m.mctx.isCancelled() {
			return
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(srcNode string) {
			defer wg.Done()
			defer func() { <-sem }()
			m.migrateNode(srcNode)
		}(node)
	}
	wg.Wait()

	if m.mctx.isCancelled() {
		return
	}

	// 收尾：验证阶段（二次扫描确保无遗漏）
	m.mctx.setState(StateFinishing)
	log.Printf("[migrator] entering finishing phase, running verification scan...")
	m.verificationScan(oldNodes)

	m.finishMigration()
}

// migrateNode 扫描并迁移单个节点上的所有 key
func (m *Migrator) migrateNode(srcNode string) {
	log.Printf("[migrator] scanning node: %s", srcNode)
	cursor := 0
	batchNum := 0

	for {
		if m.mctx.isCancelled() {
			return
		}

		// 批量扫描 key
		nextCursor, keys, err := m.fetcher.ScanKeys(srcNode, cursor, m.cfg.BatchSize)
		if err != nil {
			log.Printf("[migrator] scan error on %s (cursor=%d): %v", srcNode, cursor, err)
			// 短暂等待后重试
			select {
			case <-m.mctx.cancelCh:
				return
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}

		batchNum++
		m.processBatch(keys, srcNode, batchNum)

		if nextCursor == 0 {
			break // 扫描完毕
		}
		cursor = nextCursor

		// 批次间隔：让出 CPU，降低对正常请求的影响
		if m.cfg.BatchInterval > 0 {
			select {
			case <-m.mctx.cancelCh:
				return
			case <-time.After(m.cfg.BatchInterval):
			}
		}
	}

	log.Printf("[migrator] node %s scan complete, processed %d batches", srcNode, batchNum)
}

// processBatch 处理一批 key 的迁移
func (m *Migrator) processBatch(keys []string, srcNode string, batchNum int) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, m.cfg.Concurrency)

	for _, key := range keys {
		if m.mctx.isCancelled() {
			return
		}

		// 检查该 key 是否需要迁移
		should, oldNode, newNode := m.ringMgr.ShouldMigrate(key)
		if !should || oldNode != srcNode {
			m.skippedCount.Add(1)
			continue
		}

		wg.Add(1)
		sem <- struct{}{}
		go func(k, src, dst string) {
			defer wg.Done()
			defer func() { <-sem }()
			m.migrateKey(k, src, dst)
		}(key, oldNode, newNode)
	}
	wg.Wait()

	// 报告进度
	m.reportProgress()
}

// migrateKey 迁移单个 key（含重试）
func (m *Migrator) migrateKey(key, srcNode, dstNode string) {
	var lastErr error
	for attempt := 0; attempt <= m.cfg.RetryLimit; attempt++ {
		if m.mctx.isCancelled() {
			return
		}
		if attempt > 0 {
			backoff := time.Duration(attempt*attempt) * 100 * time.Millisecond
			select {
			case <-m.mctx.cancelCh:
				return
			case <-time.After(backoff):
			}
			log.Printf("[migrator] retry %d for key %s: %v", attempt, key, lastErr)
		}

		if err := m.mover.MoveKey(key, srcNode, dstNode); err != nil {
			lastErr = err
			continue
		}

		// 成功
		m.migratedCount.Add(1)
		m.totalEstimate.Add(1)
		return
	}

	// 超过重试次数
	m.failedCount.Add(1)
	log.Printf("[migrator] failed to migrate key %s after %d retries: %v",
		key, m.cfg.RetryLimit, lastErr)
	if m.onError != nil {
		m.onError(key, lastErr)
	}
}

// verificationScan 二次扫描，确保所有需迁移的 key 都已搬运
func (m *Migrator) verificationScan(oldNodes []string) {
	missed := int64(0)
	for _, node := range oldNodes {
		if m.mctx.isCancelled() {
			return
		}
		cursor := 0
		for {
			nextCursor, keys, err := m.fetcher.ScanKeys(node, cursor, m.cfg.BatchSize*2)
			if err != nil {
				break
			}
			for _, key := range keys {
				should, oldNode, newNode := m.ringMgr.ShouldMigrate(key)
				if should && oldNode == node {
					// 仍有未迁移的 key，补充迁移
					missed++
					m.migrateKey(key, oldNode, newNode)
				}
			}
			if nextCursor == 0 {
				break
			}
			cursor = nextCursor
		}
	}
	if missed > 0 {
		log.Printf("[migrator] verification: found and migrated %d missed keys", missed)
	}
}

func (m *Migrator) finishMigration() {
	m.ringMgr.FinishMigration()
	m.mctx.setState(StateDone)
	// 迁移完成后重置为 idle，允许下次扩容
	defer m.mctx.state.Store(int32(StateIdle))
	log.Printf("[migrator] migration complete, old ring cleared, single ring mode restored")
}

func (m *Migrator) reportProgress() {
	if m.onProgress == nil {
		return
	}
	p := m.Progress()
	select {
	case m.mctx.progressCh <- p:
	default:
		// 进度通道满，丢弃（非阻塞）
	}
	m.onProgress(p)
}
