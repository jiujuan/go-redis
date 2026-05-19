package engine

import (
	"sync/atomic"
	"testing"
)

func TestNewShardedMapValidShardMask(t *testing.T) {
	m := newShardedMap(32)
	if len(m.shards) != 32 {
		t.Fatalf("len(shards) = %d, want 32", len(m.shards))
	}
	if m.shardMask != 31 {
		t.Fatalf("shardMask = %d, want 31", m.shardMask)
	}
}

func TestGetShardStable(t *testing.T) {
	m := newShardedMap(16)
	s1 := m.getShard("same-key")
	s2 := m.getShard("same-key")
	if s1 != s2 {
		t.Fatal("same key should always resolve to the same shard")
	}
}

func TestWithWriteLockMutatesUnderlyingData(t *testing.T) {
	m := newShardedMap(8)
	m.withWriteLock("k", func(data map[string]interface{}) {
		data["k"] = "v"
	})

	val, ok := m.get("k")
	if !ok || val != "v" {
		t.Fatalf("get after withWriteLock = (%v, %v), want (v, true)", val, ok)
	}
}

func TestWithReadLockReadsUnderlyingData(t *testing.T) {
	m := newShardedMap(8)
	m.set("k", "v")

	var seen atomic.Bool
	m.withReadLock("k", func(data map[string]interface{}) {
		if data["k"] == "v" {
			seen.Store(true)
		}
	})

	if !seen.Load() {
		t.Fatal("withReadLock did not observe expected value")
	}
}

func TestGetOrSetNilValue(t *testing.T) {
	m := newShardedMap(4)
	var calls atomic.Int32

	val := m.getOrSet("nil-key", func() interface{} {
		calls.Add(1)
		return nil
	})
	if val != nil {
		t.Fatalf("first getOrSet returned %v, want nil", val)
	}

	val = m.getOrSet("nil-key", func() interface{} {
		calls.Add(1)
		return "other"
	})
	if val != nil {
		t.Fatalf("second getOrSet returned %v, want nil", val)
	}
	if calls.Load() != 1 {
		t.Fatalf("factory calls = %d, want 1", calls.Load())
	}
}
