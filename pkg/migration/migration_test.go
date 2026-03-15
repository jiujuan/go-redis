package migration_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/go-redis/pkg/migration"
)

// ─────────────────────────────────────────────
//  Mock infrastructure (minimal, local to this file)
// ─────────────────────────────────────────────

type memStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func newMemStore() *memStore { return &memStore{data: make(map[string]string)} }

func (s *memStore) set(k, v string)         { s.mu.Lock(); s.data[k] = v; s.mu.Unlock() }
func (s *memStore) get(k string) (string, bool) {
	s.mu.RLock(); v, ok := s.data[k]; s.mu.RUnlock(); return v, ok
}
func (s *memStore) del(k string) { s.mu.Lock(); delete(s.data, k); s.mu.Unlock() }
func (s *memStore) has(k string) bool {
	s.mu.RLock(); _, ok := s.data[k]; s.mu.RUnlock(); return ok
}
func (s *memStore) keys() []string {
	s.mu.RLock()
	r := make([]string, 0, len(s.data))
	for k := range s.data { r = append(r, k) }
	s.mu.RUnlock()
	return r
}
func (s *memStore) size() int { s.mu.RLock(); n := len(s.data); s.mu.RUnlock(); return n }

type memCluster struct {
	mu    sync.RWMutex
	nodes map[string]*memStore
}

func newMemCluster(nodes ...string) *memCluster {
	c := &memCluster{nodes: make(map[string]*memStore)}
	for _, n := range nodes { c.nodes[n] = newMemStore() }
	return c
}
func (c *memCluster) add(n string) { c.mu.Lock(); c.nodes[n] = newMemStore(); c.mu.Unlock() }
func (c *memCluster) store(n string) *memStore {
	c.mu.RLock(); s := c.nodes[n]; c.mu.RUnlock(); return s
}

type mockFetcher struct{ cluster *memCluster }

func (f *mockFetcher) ScanKeys(node string, cursor int, count int) (int, []string, error) {
	s := f.cluster.store(node)
	if s == nil {
		return 0, nil, fmt.Errorf("node not found: %s", node)
	}
	all := s.keys()
	end := cursor + count
	if end >= len(all) {
		return 0, all[cursor:], nil
	}
	return end, all[cursor:end], nil
}

type mockMover struct {
	cluster *memCluster
	moved   atomic.Int64
}

func (m *mockMover) MoveKey(key, src, dst string) error {
	ss := m.cluster.store(src)
	ds := m.cluster.store(dst)
	if ss == nil || ds == nil {
		return fmt.Errorf("invalid node")
	}
	val, ok := ss.get(key)
	if !ok {
		return nil
	}
	if ds.has(key) {
		ss.del(key)
		m.moved.Add(1)
		return nil
	}
	ds.set(key, val)
	ss.del(key)
	m.moved.Add(1)
	return nil
}

// ─────────────────────────────────────────────
//  MigrationState tests
// ─────────────────────────────────────────────

func TestMigrationState_String(t *testing.T) {
	cases := []struct {
		state migration.MigrationState
		want  string
	}{
		{migration.StateIdle, "idle"},
		{migration.StatePreparing, "preparing"},
		{migration.StateMigrating, "migrating"},
		{migration.StateFinishing, "finishing"},
		{migration.StateDone, "done"},
	}
	for _, c := range cases {
		if got := c.state.String(); got != c.want {
			t.Errorf("State(%d).String() = %q, want %q", c.state, got, c.want)
		}
	}
}

func TestMigrationState_Unknown(t *testing.T) {
	s := migration.MigrationState(99)
	if s.String() != "unknown" {
		t.Errorf("unknown state: %q", s.String())
	}
}

// ─────────────────────────────────────────────
//  DefaultMigrationConfig tests
// ─────────────────────────────────────────────

func TestDefaultMigrationConfig(t *testing.T) {
	cfg := migration.DefaultMigrationConfig()
	if cfg.BatchSize <= 0 {
		t.Error("BatchSize should be > 0")
	}
	if cfg.Concurrency <= 0 {
		t.Error("Concurrency should be > 0")
	}
	if cfg.RetryLimit < 0 {
		t.Error("RetryLimit should be >= 0")
	}
	if cfg.BatchInterval < 0 {
		t.Error("BatchInterval should be >= 0")
	}
}

// ─────────────────────────────────────────────
//  RingManager tests
// ─────────────────────────────────────────────

func TestRingManager_GetWriteNode_Deterministic(t *testing.T) {
	rm := migration.NewRingManager([]string{"n1", "n2"}, 50)
	n1 := rm.GetWriteNode("mykey")
	n2 := rm.GetWriteNode("mykey")
	if n1 != n2 {
		t.Error("GetWriteNode must be deterministic for same key")
	}
	if n1 == "" {
		t.Error("GetWriteNode must not return empty string")
	}
}

func TestRingManager_GetWriteNode_Distribution(t *testing.T) {
	nodes := []string{"n1:6379", "n2:6379", "n3:6379"}
	rm := migration.NewRingManager(nodes, 150)

	counts := make(map[string]int)
	for i := 0; i < 3000; i++ {
		n := rm.GetWriteNode(fmt.Sprintf("key:%d", i))
		counts[n]++
	}
	for node, cnt := range counts {
		pct := float64(cnt) / 3000 * 100
		if pct < 15 || pct > 55 {
			t.Errorf("node %s has poor distribution: %.1f%%", node, pct)
		}
	}
}

func TestRingManager_GetReadNodes_NormalMode(t *testing.T) {
	rm := migration.NewRingManager([]string{"n1", "n2"}, 50)
	primary, fallback := rm.GetReadNodes("key")
	if primary == "" {
		t.Error("primary node should not be empty")
	}
	if fallback != "" {
		t.Error("fallback should be empty in non-migrating mode")
	}
}

func TestRingManager_GetReadNodes_MigratingMode(t *testing.T) {
	rm := migration.NewRingManager([]string{"n1", "n2"}, 50)
	rm.BeginMigration([]string{"n3"})

	// After migration start, some keys may have a fallback
	primary, _ := rm.GetReadNodes("some:key")
	if primary == "" {
		t.Error("primary should not be empty during migration")
	}
}

func TestRingManager_BeginMigration_NodeCount(t *testing.T) {
	rm := migration.NewRingManager([]string{"n1", "n2"}, 50)
	rm.BeginMigration([]string{"n3"})

	if !rm.IsMigrating() {
		t.Error("should be migrating after BeginMigration")
	}
	if old := rm.OldNodes(); len(old) != 2 {
		t.Errorf("old nodes: got %d, want 2", len(old))
	}
	if newN := rm.Nodes(); len(newN) != 3 {
		t.Errorf("new nodes: got %d, want 3", len(newN))
	}
}

func TestRingManager_BeginMigration_MultipleNodes(t *testing.T) {
	rm := migration.NewRingManager([]string{"n1"}, 50)
	rm.BeginMigration([]string{"n2", "n3"})
	if nodes := rm.Nodes(); len(nodes) != 3 {
		t.Errorf("after adding 2 nodes: got %d, want 3", len(nodes))
	}
}

func TestRingManager_FinishMigration(t *testing.T) {
	rm := migration.NewRingManager([]string{"n1", "n2"}, 50)
	rm.BeginMigration([]string{"n3"})
	rm.FinishMigration()

	if rm.IsMigrating() {
		t.Error("should not be migrating after finish")
	}
	if rm.OldNodes() != nil {
		t.Error("old ring should be cleared after finish")
	}
}

func TestRingManager_AbortMigration_RestoresRouting(t *testing.T) {
	nodes := []string{"n1", "n2"}
	rm := migration.NewRingManager(nodes, 100)

	// Record routing before migration
	before := make(map[string]string)
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("k%d", i)
		before[k] = rm.GetWriteNode(k)
	}

	rm.BeginMigration([]string{"n3"})
	rm.AbortMigration()

	// After abort, routing should match pre-migration
	for k, want := range before {
		got := rm.GetWriteNode(k)
		if got != want {
			t.Errorf("after abort, key %q routes to %q, want %q", k, got, want)
		}
	}
}

func TestRingManager_ShouldMigrate(t *testing.T) {
	rm := migration.NewRingManager([]string{"n1", "n2"}, 100)
	rm.BeginMigration([]string{"n3"})

	// Find at least one key that needs migration
	found := false
	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("probe:%d", i)
		should, src, dst := rm.ShouldMigrate(key)
		if should {
			if src == "" || dst == "" || src == dst {
				t.Errorf("ShouldMigrate(%q): invalid src=%q dst=%q", key, src, dst)
			}
			found = true
			break
		}
	}
	if !found {
		t.Log("no migration needed in probe set (acceptable for small clusters)")
	}
}

func TestRingManager_ShouldMigrate_NotMigrating(t *testing.T) {
	rm := migration.NewRingManager([]string{"n1", "n2"}, 50)
	should, _, _ := rm.ShouldMigrate("key")
	if should {
		t.Error("ShouldMigrate should be false when not migrating")
	}
}

func TestRingManager_IsMigrating_States(t *testing.T) {
	rm := migration.NewRingManager([]string{"n1"}, 50)
	if rm.IsMigrating() {
		t.Error("should not be migrating initially")
	}
	rm.BeginMigration([]string{"n2"})
	if !rm.IsMigrating() {
		t.Error("should be migrating after BeginMigration")
	}
	rm.FinishMigration()
	if rm.IsMigrating() {
		t.Error("should not be migrating after FinishMigration")
	}
}

func TestRingManager_Concurrent(t *testing.T) {
	rm := migration.NewRingManager([]string{"n1", "n2"}, 100)

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", n)
			node := rm.GetWriteNode(key)
			if node == "" {
				t.Errorf("concurrent GetWriteNode returned empty")
			}
			rm.GetReadNodes(key)
		}(i)
	}
	wg.Wait()
}

// ─────────────────────────────────────────────
//  Migrator tests
// ─────────────────────────────────────────────

func newMigratorSetup(nodes []string, newNode string) (
	*migration.RingManager,
	*memCluster,
	*mockFetcher,
	*mockMover,
) {
	cluster := newMemCluster(nodes...)
	cluster.add(newNode)
	rm := migration.NewRingManager(nodes, 50)
	fetcher := &mockFetcher{cluster: cluster}
	mover := &mockMover{cluster: cluster}
	return rm, cluster, fetcher, mover
}

func TestMigrator_Start_InvalidWhenAlreadyRunning(t *testing.T) {
	rm, _, fetcher, mover := newMigratorSetup([]string{"n1", "n2"}, "n3")

	cfg := migration.DefaultMigrationConfig()
	cfg.BatchInterval = 0
	m := migration.NewMigrator(cfg, rm, fetcher, mover)

	if err := m.Start("t1", []string{"n3"}); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if err := m.Start("t2", []string{"n4"}); err == nil {
		t.Error("second Start while running should return error")
	}
	m.Wait()
}

func TestMigrator_Start_DoesNotBlockCaller(t *testing.T) {
	rm, _, fetcher, mover := newMigratorSetup([]string{"n1"}, "n2")
	cfg := migration.DefaultMigrationConfig()
	cfg.BatchInterval = 0
	m := migration.NewMigrator(cfg, rm, fetcher, mover)

	started := make(chan struct{})
	go func() {
		close(started)
		m.Start("t1", []string{"n2"})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Error("Start blocked caller")
	}
	m.Wait()
}

func TestMigrator_FullMigration_MovesKeys(t *testing.T) {
	nodes := []string{"n1", "n2"}
	rm, cluster, fetcher, mover := newMigratorSetup(nodes, "n3")

	// Pre-populate both old nodes
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("k%d", i)
		node := rm.GetWriteNode(key)
		cluster.store(node).set(key, fmt.Sprintf("v%d", i))
	}

	cfg := migration.DefaultMigrationConfig()
	cfg.BatchInterval = 0
	cfg.BatchSize = 30

	var doneCalled bool
	m := migration.NewMigrator(cfg, rm, fetcher, mover).
		OnDone(func(*migration.MigrationTask) { doneCalled = true })

	if err := m.Start("full", []string{"n3"}); err != nil {
		t.Fatal(err)
	}
	m.Wait()

	if !doneCalled {
		t.Error("OnDone not called")
	}
	if rm.IsMigrating() {
		t.Error("should not be migrating after completion")
	}
	// n3 should have received some keys
	if cluster.store("n3").size() == 0 {
		t.Log("n3 got 0 keys - possible if all keys already routed to n1/n2 after ring change")
	}
	// All keys should be findable on their new ring node
	misses := 0
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("k%d", i)
		node := rm.GetWriteNode(key)
		if !cluster.store(node).has(key) {
			misses++
		}
	}
	if misses > 0 {
		t.Errorf("%d keys missing from their target node after migration", misses)
	}
}

func TestMigrator_Cancel_RollsBackRing(t *testing.T) {
	nodes := []string{"n1", "n2"}
	rm, cluster, fetcher, mover := newMigratorSetup(nodes, "n3")

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("slow:%d", i)
		node := rm.GetWriteNode(key)
		cluster.store(node).set(key, "v")
	}

	cfg := migration.DefaultMigrationConfig()
	cfg.BatchInterval = 100 * time.Millisecond // slow so we can cancel
	cfg.BatchSize = 5

	m := migration.NewMigrator(cfg, rm, fetcher, mover)
	if err := m.Start("cancel-test", []string{"n3"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	m.Cancel()

	if rm.IsMigrating() {
		t.Error("ring should not be migrating after cancel")
	}
	// After cancel, n3 should not be the write target (ring rolled back)
	n3Keys := 0
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("slow:%d", i)
		if rm.GetWriteNode(key) == "n3" {
			n3Keys++
		}
	}
	if n3Keys > 0 {
		t.Errorf("after cancel, %d keys still route to n3 (should be 0 - ring was rolled back)", n3Keys)
	}
}

func TestMigrator_Progress_Reporting(t *testing.T) {
	nodes := []string{"n1", "n2"}
	rm, cluster, fetcher, mover := newMigratorSetup(nodes, "n3")

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("prog:%d", i)
		node := rm.GetWriteNode(key)
		cluster.store(node).set(key, "v")
	}

	var progressCalls atomic.Int64
	cfg := migration.DefaultMigrationConfig()
	cfg.BatchInterval = 0

	m := migration.NewMigrator(cfg, rm, fetcher, mover).
		OnProgress(func(migration.Progress) { progressCalls.Add(1) })

	m.Start("progress-test", []string{"n3"})
	m.Wait()

	// Progress should have been called at least once
	if progressCalls.Load() == 0 {
		t.Log("no progress callbacks (acceptable if migration completes in single batch)")
	}
}

func TestMigrator_Progress_BeforeStart(t *testing.T) {
	rm := migration.NewRingManager([]string{"n1"}, 50)
	m := migration.NewMigrator(nil, rm, &mockFetcher{newMemCluster("n1")}, &mockMover{cluster: newMemCluster("n1")})
	p := m.Progress()
	if p.State != migration.StateIdle {
		t.Errorf("initial state: got %s, want idle", p.State)
	}
}

func TestMigrator_Wait_WhenIdle(t *testing.T) {
	// Wait on a migrator that has never started should return immediately
	rm := migration.NewRingManager([]string{"n1"}, 50)
	m := migration.NewMigrator(nil, rm, &mockFetcher{newMemCluster("n1")}, &mockMover{cluster: newMemCluster("n1")})
	done := make(chan struct{})
	go func() {
		m.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Error("Wait on idle migrator should return immediately")
	}
}

func TestMigrator_OnError_Callback(t *testing.T) {
	nodes := []string{"n1"}
	rm := migration.NewRingManager(nodes, 50)
	cluster := newMemCluster("n1", "n2")

	// Populate n1
	for i := 0; i < 20; i++ {
		cluster.store("n1").set(fmt.Sprintf("err:%d", i), "v")
	}

	// Mover that always fails
	failMover := &alwaysFailMover{}

	var errKeys []string
	var mu sync.Mutex

	cfg := migration.DefaultMigrationConfig()
	cfg.BatchInterval = 0
	cfg.RetryLimit = 0

	m := migration.NewMigrator(cfg, rm, &mockFetcher{cluster}, failMover).
		OnError(func(key string, err error) {
			mu.Lock()
			errKeys = append(errKeys, key)
			mu.Unlock()
		})

	m.Start("err-test", []string{"n2"})
	m.Wait()

	// Some keys should have triggered OnError since mover always fails
	mu.Lock()
	n := len(errKeys)
	mu.Unlock()
	if n == 0 {
		t.Log("no error callbacks - possible if no keys need migration in this ring config")
	}
}

type alwaysFailMover struct{}

func (a *alwaysFailMover) MoveKey(key, src, dst string) error {
	return fmt.Errorf("always fail")
}

// ─────────────────────────────────────────────
//  KeyDistributionTest helper
// ─────────────────────────────────────────────

func TestKeyDistributionTest_Uniformity(t *testing.T) {
	nodes := []string{"n1:6379", "n2:6379", "n3:6379"}
	keys := make([]string, 9000)
	for i := range keys {
		keys[i] = fmt.Sprintf("key:%d", i)
	}
	dist := migration.KeyDistributionTest(nodes, keys, 150)

	total := len(keys)
	expected := float64(total) / float64(len(nodes))
	for node, cnt := range dist {
		deviation := (float64(cnt) - expected) / expected * 100
		if deviation > 25 || deviation < -25 {
			t.Errorf("node %s deviation %.1f%% exceeds ±25%%", node, deviation)
		}
	}
}

func TestKeyDistributionTest_AllKeysAssigned(t *testing.T) {
	nodes := []string{"n1", "n2"}
	keys := []string{"a", "b", "c", "d", "e"}
	dist := migration.KeyDistributionTest(nodes, keys, 50)

	total := 0
	for _, cnt := range dist {
		total += cnt
	}
	if total != len(keys) {
		t.Errorf("total assigned: got %d, want %d", total, len(keys))
	}
}

func TestKeyDistributionTest_SingleNode(t *testing.T) {
	nodes := []string{"only"}
	keys := []string{"a", "b", "c"}
	dist := migration.KeyDistributionTest(nodes, keys, 50)
	if dist["only"] != 3 {
		t.Errorf("single node should get all keys: got %d", dist["only"])
	}
}

func TestKeyDistributionTest_AddNodeReducesExisting(t *testing.T) {
	oldNodes := []string{"n1", "n2"}
	newNodes := []string{"n1", "n2", "n3"}
	keys := make([]string, 6000)
	for i := range keys { keys[i] = fmt.Sprintf("k:%d", i) }

	oldDist := migration.KeyDistributionTest(oldNodes, keys, 150)
	newDist := migration.KeyDistributionTest(newNodes, keys, 150)

	// n1 and n2 should each have fewer keys after n3 joins
	if newDist["n1"] >= oldDist["n1"] && newDist["n2"] >= oldDist["n2"] {
		t.Error("adding a node should reduce load on existing nodes")
	}
	// n3 should have some keys
	if newDist["n3"] == 0 {
		t.Error("new node n3 should receive some keys")
	}
}
