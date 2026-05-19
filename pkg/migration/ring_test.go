package migration

import (
	"fmt"
	"testing"
)

func TestBuildRingEmpty(t *testing.T) {
	hr := buildRing(nil, 10)
	if hr == nil {
		t.Fatal("buildRing returned nil")
	}
	if got := hr.lookup("k"); got != "" {
		t.Fatalf("lookup on empty ring = %q, want empty", got)
	}
}

func TestBuildRingAndLookup(t *testing.T) {
	hr := buildRing([]string{"n1", "n2"}, 32)
	if len(hr.nodes) != 2 || len(hr.ring) != 64 {
		t.Fatalf("ring sizes = nodes:%d ring:%d", len(hr.nodes), len(hr.ring))
	}
	if got := hr.lookup("key"); got == "" {
		t.Fatal("lookup should not return empty node")
	}
}

func TestRingManagerReadWriteAndMigrationLifecycle(t *testing.T) {
	rm := NewRingManager([]string{"n1", "n2"}, 16)
	if got := rm.GetWriteNode("k"); got == "" {
		t.Fatal("GetWriteNode returned empty")
	}
	if p, f := rm.GetReadNodes("k"); p == "" || f != "" {
		t.Fatalf("GetReadNodes in normal mode = (%q,%q), want fallback empty", p, f)
	}

	changes := rm.BeginMigration([]string{"n3"})
	if !rm.IsMigrating() {
		t.Fatal("expected migrating state after BeginMigration")
	}
	if len(rm.OldNodes()) != 2 || len(rm.Nodes()) != 3 {
		t.Fatalf("nodes mismatch: old=%v new=%v", rm.OldNodes(), rm.Nodes())
	}
	if changes == nil {
		t.Fatal("BeginMigration should return a map")
	}

	rm.FinishMigration()
	if rm.IsMigrating() {
		t.Fatal("expected migration to finish")
	}
	if rm.OldNodes() != nil {
		t.Fatal("old ring should be cleared after FinishMigration")
	}
}

func TestRingManagerAbortRestoresOldRing(t *testing.T) {
	rm := NewRingManager([]string{"n1", "n2"}, 32)
	before := make(map[string]string)
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("k:%d", i)
		before[k] = rm.GetWriteNode(k)
	}

	rm.BeginMigration([]string{"n3"})
	rm.AbortMigration()

	for k, want := range before {
		if got := rm.GetWriteNode(k); got != want {
			t.Fatalf("after abort %q -> %q, want %q", k, got, want)
		}
	}
}

func TestRingManagerShouldMigrate(t *testing.T) {
	rm := NewRingManager([]string{"n1", "n2"}, 32)
	if should, src, dst := rm.ShouldMigrate("k"); should || src != "" || dst != "" {
		t.Fatalf("ShouldMigrate before migration = (%v,%q,%q)", should, src, dst)
	}

	rm.BeginMigration([]string{"n3"})
	found := false
	for i := 0; i < 5000; i++ {
		should, src, dst := rm.ShouldMigrate(fmt.Sprintf("p:%d", i))
		if should {
			if src == "" || dst == "" || src == dst {
				t.Fatalf("invalid migration route: src=%q dst=%q", src, dst)
			}
			found = true
			break
		}
	}
	if !found {
		t.Log("no migration key found in probe set")
	}
}

func TestKeyDistributionTest(t *testing.T) {
	nodes := []string{"n1", "n2", "n3"}
	keys := []string{"a", "b", "c", "d", "e", "f"}
	dist := KeyDistributionTest(nodes, keys, 16)
	if len(dist) != len(nodes) {
		t.Fatalf("distribution map size = %d, want %d", len(dist), len(nodes))
	}
	total := 0
	for _, n := range dist {
		total += n
	}
	if total != len(keys) {
		t.Fatalf("distributed total = %d, want %d", total, len(keys))
	}
}
