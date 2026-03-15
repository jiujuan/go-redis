// Package migration 实现 v0.4 的渐进式数据迁移。
//
// # 核心思路：双环路由 + 后台渐进迁移
//
// 扩容流程：
//  1. 调用 ClusterClient.AddNode(newAddr) 触发扩容
//  2. 系统保留旧哈希环快照（oldRing），同时建立含新节点的新哈希环（newRing）
//  3. 读请求：先走 newRing；若目标节点返回 KEY_NOT_FOUND，自动回源 oldRing（兜底读）
//  4. 写请求：直接走 newRing，保证新写入落到正确位置
//  5. 后台 Migrator 按批次扫描所有旧节点，找出在新环下应迁移到别处的 key，
//     DUMP + RESTORE 搬运，确认成功后 DEL 旧节点副本
//  6. 所有 key 迁移完毕 → 清除 oldRing，回归单环模式
//
// 迁移期间的并发安全：
//   - 写操作始终写入 newRing 目标节点，不写旧节点
//   - 读兜底（oldRing 回源）只读不写，保证幂等
//   - 迁移搬运使用 SETNX/原子写，避免覆盖已有新写入
//   - 每个 key 迁移后立刻标记，重启可续传（基于持久化迁移进度）
package migration

import (
	"sync"
	"sync/atomic"
	"time"
)

// MigrationState 迁移状态枚举
type MigrationState int32

const (
	StateIdle      MigrationState = iota // 无迁移任务
	StatePreparing                       // 准备中（构建新环、连接新节点）
	StateMigrating                       // 迁移进行中
	StateFinishing                       // 收尾（验证所有 key 已迁移）
	StateDone                            // 迁移完成
)

func (s MigrationState) String() string {
	switch s {
	case StateIdle:
		return "idle"
	case StatePreparing:
		return "preparing"
	case StateMigrating:
		return "migrating"
	case StateFinishing:
		return "finishing"
	case StateDone:
		return "done"
	default:
		return "unknown"
	}
}

// MigrationTask 一次扩容迁移任务的描述
type MigrationTask struct {
	ID          string    // 任务唯一 ID（时间戳）
	AddedNodes  []string  // 本次新加入的节点
	StartedAt   time.Time
	FinishedAt  time.Time
	TotalKeys   int64 // 需迁移的 key 总数（估算）
	MigratedKeys int64 // 已迁移 key 数
	FailedKeys  int64 // 迁移失败 key 数（会重试）
	SkippedKeys int64 // 跳过（新环与旧环节点相同，无需迁移）
}

// Progress 迁移进度快照（线程安全读）
type Progress struct {
	State        MigrationState
	Task         *MigrationTask
	Percent      float64 // 0~100
	EstimatedETA time.Duration
}

// NodeKeyRange 描述某个节点上需要迁移的 key 范围
type NodeKeyRange struct {
	SourceNode string // 旧节点
	TargetNode string // 新节点
	Keys       []string
}

// migrationContext 迁移运行时上下文（内部使用）
type migrationContext struct {
	mu          sync.RWMutex
	state       atomic.Int32  // MigrationState
	task        *MigrationTask
	cancelCh    chan struct{}
	doneCh      chan struct{}
	progressCh  chan Progress  // 进度事件广播
	batchSize   int
	concurrency int
	retryLimit  int
}

func newMigrationContext(batchSize, concurrency, retryLimit int) *migrationContext {
	mc := &migrationContext{
		cancelCh:    make(chan struct{}),
		doneCh:      make(chan struct{}),
		progressCh:  make(chan Progress, 64),
		batchSize:   batchSize,
		concurrency: concurrency,
		retryLimit:  retryLimit,
	}
	mc.state.Store(int32(StateIdle))
	return mc
}

func (mc *migrationContext) getState() MigrationState {
	return MigrationState(mc.state.Load())
}

func (mc *migrationContext) setState(s MigrationState) {
	mc.state.Store(int32(s))
}

func (mc *migrationContext) isCancelled() bool {
	select {
	case <-mc.cancelCh:
		return true
	default:
		return false
	}
}

// MigrationConfig 迁移配置
type MigrationConfig struct {
	// BatchSize 每批次扫描/迁移的 key 数量
	BatchSize int
	// Concurrency 并发迁移 goroutine 数
	Concurrency int
	// RetryLimit 单个 key 迁移失败最大重试次数
	RetryLimit int
	// BatchInterval 批次间隔，控制迁移速率，避免影响正常请求
	BatchInterval time.Duration
	// ReadFallbackTimeout 兜底读超时
	ReadFallbackTimeout time.Duration
}

// DefaultMigrationConfig 默认迁移配置（保守策略，对业务影响最小）
func DefaultMigrationConfig() *MigrationConfig {
	return &MigrationConfig{
		BatchSize:           100,
		Concurrency:         4,
		RetryLimit:          3,
		BatchInterval:       10 * time.Millisecond, // 批次间 10ms 让出 CPU
		ReadFallbackTimeout: 500 * time.Millisecond,
	}
}
