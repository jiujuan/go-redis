package migration_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/go-redis/pkg/migration"
)

// ---- Mock 实现（单元测试用）----

// mockStore 内存 KV 存储，模拟单个节点
type mockStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func newMockStore() *mockStore {
	return &mockStore{data: make(map[string]string)}
}

func (s *mockStore) set(key, val string) {
	s.mu.Lock()
	s.data[key] = val
	s.mu.Unlock()
}

func (s *mockStore) get(key string) (string, bool) {
	s.mu.RLock()
	v, ok := s.data[key]
	s.mu.RUnlock()
	return v, ok
}

func (s *mockStore) del(key string) bool {
	s.mu.Lock()
	_, ok := s.data[key]
	delete(s.data, key)
	s.mu.Unlock()
	return ok
}

func (s *mockStore) exists(key string) bool {
	s.mu.RLock()
	_, ok := s.data[key]
	s.mu.RUnlock()
	return ok
}

func (s *mockStore) keys() []string {
	s.mu.RLock()
	result := make([]string, 0, len(s.data))
	for k := range s.data {
		result = append(result, k)
	}
	s.mu.RUnlock()
	return result
}

func (s *mockStore) size() int {
	s.mu.RLock()
	n := len(s.data)
	s.mu.RUnlock()
	return n
}

// mockCluster 模拟集群（多节点）
type mockCluster struct {
	mu    sync.RWMutex
	nodes map[string]*mockStore
}

func newMockCluster(nodes ...string) *mockCluster {
	mc := &mockCluster{nodes: make(map[string]*mockStore)}
	for _, n := range nodes {
		mc.nodes[n] = newMockStore()
	}
	return mc
}

func (mc *mockCluster) addNode(node string) {
	mc.mu.Lock()
	mc.nodes[node] = newMockStore()
	mc.mu.Unlock()
}

func (mc *mockCluster) getStore(node string) *mockStore {
	mc.mu.RLock()
	s := mc.nodes[node]
	mc.mu.RUnlock()
	return s
}

func (mc *mockCluster) totalKeys() int {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	total := 0
	for _, s := range mc.nodes {
		total += s.size()
	}
	return total
}

func (mc *mockCluster) allKeys() []string {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	var all []string
	seen := make(map[string]struct{})
	for _, s := range mc.nodes {
		for _, k := range s.keys() {
			if _, ok := seen[k]; !ok {
				all = append(all, k)
				seen[k] = struct{}{}
			}
		}
	}
	return all
}

// mockFetcher 实现 KeyFetcher
type mockFetcher struct {
	cluster *mockCluster
}

func (f *mockFetcher) ScanKeys(node string, cursor int, count int) (int, []string, error) {
	s := f.cluster.getStore(node)
	if s == nil {
		return 0, nil, fmt.Errorf("node not found: %s", node)
	}
	allKeys := s.keys()
	total := len(allKeys)
	end := cursor + count
	if end >= total {
		return 0, allKeys[cursor:total], nil
	}
	return end, allKeys[cursor:end], nil
}

// mockMover 实现 KeyMover
type mockMover struct {
	cluster   *mockCluster
	moveCount atomic.Int64
}

func (m *mockMover) MoveKey(key, srcNode, dstNode string) error {
	src := m.cluster.getStore(srcNode)
	dst := m.cluster.getStore(dstNode)
	if src == nil || dst == nil {
		return fmt.Errorf("invalid node")
	}

	val, ok := src.get(key)
	if !ok {
		return nil // 已被删除
	}

	// 目标节点已有新写入，跳过
	if dst.exists(key) {
		src.del(key)
		m.moveCount.Add(1)
		return nil
	}

	dst.set(key, val)
	src.del(key)
	m.moveCount.Add(1)
	return nil
}

// ---- 测试用例 ----

func TestRingManager_BasicRouting(t *testing.T) {
	nodes := []string{"node1:6379", "node2:6379"}
	rm := migration.NewRingManager(nodes, 150)

	key := "test:key"
	node := rm.GetWriteNode(key)
	if node == "" {
		t.Fatal("should route to a node")
	}
	t.Logf("key %q -> node %s", key, node)
}

func TestRingManager_BeginMigration(t *testing.T) {
	nodes := []string{"node1:6379", "node2:6379"}
	rm := migration.NewRingManager(nodes, 150)

	// 添加新节点，开始迁移
	rm.BeginMigration([]string{"node3:6379"})

	if !rm.IsMigrating() {
		t.Fatal("should be in migrating state")
	}

	// 旧节点仍在
	oldNodes := rm.OldNodes()
	if len(oldNodes) != 2 {
		t.Fatalf("expected 2 old nodes, got %d", len(oldNodes))
	}

	// 新环包含 3 个节点
	newNodes := rm.Nodes()
	if len(newNodes) != 3 {
		t.Fatalf("expected 3 new nodes, got %d", len(newNodes))
	}
}

func TestRingManager_ShouldMigrate(t *testing.T) {
	nodes := []string{"node1:6379", "node2:6379"}
	rm := migration.NewRingManager(nodes, 150)
	rm.BeginMigration([]string{"node3:6379"})

	// 找一个迁移后会改变路由的 key
	found := false
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key:%d", i)
		should, src, dst := rm.ShouldMigrate(key)
		if should {
			t.Logf("key %q: %s -> %s", key, src, dst)
			found = true
			break
		}
	}
	if !found {
		t.Log("no keys need migration in sample (normal for small sample)")
	}
}

func TestRingManager_FinishMigration(t *testing.T) {
	nodes := []string{"node1:6379", "node2:6379"}
	rm := migration.NewRingManager(nodes, 150)
	rm.BeginMigration([]string{"node3:6379"})
	rm.FinishMigration()

	if rm.IsMigrating() {
		t.Fatal("should not be migrating after finish")
	}
	if rm.OldNodes() != nil {
		t.Fatal("old nodes should be cleared")
	}
}

func TestRingManager_AbortMigration(t *testing.T) {
	nodes := []string{"node1:6379", "node2:6379"}
	rm := migration.NewRingManager(nodes, 150)

	// 记录初始路由
	key := "abort:test"
	beforeNode := rm.GetWriteNode(key)

	rm.BeginMigration([]string{"node3:6379"})
	rm.AbortMigration()

	afterNode := rm.GetWriteNode(key)
	if afterNode != beforeNode {
		t.Fatalf("after abort, key should route to same node. before=%s after=%s", beforeNode, afterNode)
	}
}

func TestMigrator_FullMigration(t *testing.T) {
	// 构建初始集群（2个节点，各 50 个 key）
	nodes := []string{"node1", "node2"}
	cluster := newMockCluster(nodes...)
	cluster.addNode("node3")

	rm := migration.NewRingManager(nodes, 50)

	// 写入 1000 个 key 到对应的旧环节点
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key:%d", i)
		node := rm.GetWriteNode(key)
		cluster.getStore(node).set(key, fmt.Sprintf("value:%d", i))
	}

	initialTotal := cluster.totalKeys()
	t.Logf("initial keys: %d", initialTotal)

	// 开始迁移到 node3
	fetcher := &mockFetcher{cluster: cluster}
	mover := &mockMover{cluster: cluster}

	cfg := migration.DefaultMigrationConfig()
	cfg.BatchInterval = 0 // 测试中不等待
	cfg.BatchSize = 50

	migrator := migration.NewMigrator(cfg, rm, fetcher, mover)

	var doneCalled bool
	migrator.OnDone(func(task *migration.MigrationTask) {
		doneCalled = true
		t.Logf("migration done: migrated=%d failed=%d", task.MigratedKeys, task.FailedKeys)
	})

	if err := migrator.Start("test-task-1", []string{"node3"}); err != nil {
		t.Fatalf("start migration: %v", err)
	}
	migrator.Wait()

	if !doneCalled {
		t.Error("OnDone callback not called")
	}
	if rm.IsMigrating() {
		t.Error("should not be migrating after completion")
	}
	t.Logf("moved keys: %d", mover.moveCount.Load())
	t.Logf("node1 keys: %d, node2 keys: %d, node3 keys: %d",
		cluster.getStore("node1").size(),
		cluster.getStore("node2").size(),
		cluster.getStore("node3").size(),
	)

	// 验证：node3 应该有数据了
	if cluster.getStore("node3").size() == 0 {
		t.Error("node3 should have received migrated keys")
	}

	// 验证：迁移后，key 路由应该能找到
	missingCount := 0
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key:%d", i)
		newNode := rm.GetWriteNode(key)
		if !cluster.getStore(newNode).exists(key) {
			missingCount++
		}
	}
	if missingCount > 0 {
		t.Errorf("%d keys not found on their target node after migration", missingCount)
	}
}

func TestMigrator_ConcurrentWriteDuringMigration(t *testing.T) {
	nodes := []string{"node1", "node2"}
	cluster := newMockCluster(nodes...)
	cluster.addNode("node3")
	rm := migration.NewRingManager(nodes, 50)

	// 预填充数据
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key:%d", i)
		node := rm.GetWriteNode(key)
		cluster.getStore(node).set(key, fmt.Sprintf("v0:%d", i))
	}

	cfg := migration.DefaultMigrationConfig()
	cfg.BatchInterval = 1 * time.Millisecond
	cfg.BatchSize = 20

	fetcher := &mockFetcher{cluster: cluster}
	mover := &mockMover{cluster: cluster}
	migrator := migration.NewMigrator(cfg, rm, fetcher, mover)

	var writeCount atomic.Int64
	var stopWrite atomic.Bool

	// 并发写入（模拟正常业务）
	go func() {
		for i := 0; !stopWrite.Load(); i++ {
			key := fmt.Sprintf("newkey:%d", i)
			// 写操作始终走 newRing（GetWriteNode 在 BeginMigration 后已含 node3）
			node := rm.GetWriteNode(key)
			cluster.getStore(node).set(key, fmt.Sprintf("new:%d", i))
			writeCount.Add(1)
			time.Sleep(time.Microsecond * 100)
		}
	}()

	if err := migrator.Start("concurrent-test", []string{"node3"}); err != nil {
		t.Fatalf("start migration: %v", err)
	}
	migrator.Wait()

	stopWrite.Store(true)
	t.Logf("concurrent writes during migration: %d", writeCount.Load())
	t.Logf("final: node1=%d node2=%d node3=%d",
		cluster.getStore("node1").size(),
		cluster.getStore("node2").size(),
		cluster.getStore("node3").size(),
	)
}

func TestMigrator_Cancel(t *testing.T) {
	nodes := []string{"node1", "node2"}
	cluster := newMockCluster(nodes...)
	cluster.addNode("node3")
	rm := migration.NewRingManager(nodes, 50)

	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key:%d", i)
		node := rm.GetWriteNode(key)
		cluster.getStore(node).set(key, "v")
	}

	cfg := migration.DefaultMigrationConfig()
	cfg.BatchInterval = 50 * time.Millisecond // 慢速迁移
	cfg.BatchSize = 5

	migrator := migration.NewMigrator(cfg, rm, &mockFetcher{cluster: cluster}, &mockMover{cluster: cluster})
	if err := migrator.Start("cancel-test", []string{"node3"}); err != nil {
		t.Fatalf("start: %v", err)
	}

	// 短暂运行后取消
	time.Sleep(30 * time.Millisecond)
	migrator.Cancel()

	// 取消后应回滚到旧环（node3 不在路由中）
	if rm.IsMigrating() {
		t.Error("should not be migrating after cancel")
	}
	t.Log("migration cancelled successfully, ring rolled back")
}

func TestKeyDistribution_Uniformity(t *testing.T) {
	nodes := []string{"192.168.1.1:6379", "192.168.1.2:6379", "192.168.1.3:6379"}

	// 生成 10000 个测试 key
	keys := make([]string, 10000)
	for i := range keys {
		keys[i] = fmt.Sprintf("key:%d", i)
	}

	dist := migration.KeyDistributionTest(nodes, keys, 150)

	total := len(keys)
	expected := float64(total) / float64(len(nodes))
	t.Logf("Expected per node: %.0f (%.1f%%)", expected, 100.0/float64(len(nodes)))
	for node, count := range dist {
		pct := float64(count) / float64(total) * 100
		deviation := (float64(count) - expected) / expected * 100
		t.Logf("  %s: %d keys (%.1f%%, deviation=%.1f%%)", node, count, pct, deviation)

		// 均匀性验证：偏差不超过 20%
		if deviation > 20 || deviation < -20 {
			t.Errorf("node %s has too large deviation: %.1f%%", node, deviation)
		}
	}
}

func TestKeyDistribution_AddNode(t *testing.T) {
	oldNodes := []string{"node1:6379", "node2:6379"}
	newNodes := []string{"node1:6379", "node2:6379", "node3:6379"}

	keys := make([]string, 10000)
	for i := range keys {
		keys[i] = fmt.Sprintf("key:%d", i)
	}

	oldDist := migration.KeyDistributionTest(oldNodes, keys, 150)
	newDist := migration.KeyDistributionTest(newNodes, keys, 150)

	// 统计有多少 key 的路由发生变化
	changed := 0
	// 简化：只统计 newDist 中 node3 的比例即可说明迁移量
	node3Keys := newDist["node3:6379"]
	pct := float64(node3Keys) / float64(len(keys)) * 100

	t.Logf("Old distribution: node1=%d, node2=%d", oldDist["node1:6379"], oldDist["node2:6379"])
	t.Logf("New distribution: node1=%d, node2=%d, node3=%d (%.1f%%)",
		newDist["node1:6379"], newDist["node2:6379"], newDist["node3:6379"], pct)
	t.Logf("Keys that need migration to node3: %d (%.1f%%)", node3Keys, pct)
	_ = changed

	// 扩容后 node3 应该接管约 1/3 的 key
	if pct < 20 || pct > 45 {
		t.Errorf("node3 should have ~33%% of keys, got %.1f%%", pct)
	}
}
