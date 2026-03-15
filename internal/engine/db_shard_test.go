package engine

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// ─────────────────────────────────────────────
//  shardedMap tests
// ─────────────────────────────────────────────

func TestShardedMap_SetGet(t *testing.T) {
	m := newShardedMap(16)
	m.set("k", "v")
	val, ok := m.get("k")
	if !ok || val != "v" {
		t.Fatalf("get: got (%v, %v), want (v, true)", val, ok)
	}
}

func TestShardedMap_GetMissing(t *testing.T) {
	m := newShardedMap(16)
	_, ok := m.get("missing")
	if ok {
		t.Error("expected false for missing key")
	}
}

func TestShardedMap_Del(t *testing.T) {
	m := newShardedMap(16)
	m.set("k", "v")
	if !m.del("k") {
		t.Error("del existing key should return true")
	}
	if m.del("k") {
		t.Error("del missing key should return false")
	}
	_, ok := m.get("k")
	if ok {
		t.Error("key should not exist after del")
	}
}

func TestShardedMap_Exists(t *testing.T) {
	m := newShardedMap(16)
	if m.exists("k") {
		t.Error("should not exist before set")
	}
	m.set("k", "v")
	if !m.exists("k") {
		t.Error("should exist after set")
	}
}

func TestShardedMap_Count(t *testing.T) {
	m := newShardedMap(16)
	for i := 0; i < 100; i++ {
		m.set(fmt.Sprintf("k%d", i), i)
	}
	if m.count() != 100 {
		t.Errorf("count: got %d, want 100", m.count())
	}
}

func TestShardedMap_Keys(t *testing.T) {
	m := newShardedMap(16)
	m.set("a", 1)
	m.set("b", 2)
	m.set("c", 3)
	keys := m.keys()
	if len(keys) != 3 {
		t.Errorf("keys: got %d, want 3", len(keys))
	}
}

func TestShardedMap_Flush(t *testing.T) {
	m := newShardedMap(16)
	for i := 0; i < 50; i++ {
		m.set(fmt.Sprintf("k%d", i), i)
	}
	m.flush()
	if m.count() != 0 {
		t.Errorf("count after flush: got %d, want 0", m.count())
	}
}

func TestShardedMap_GetOrSet(t *testing.T) {
	m := newShardedMap(16)
	calls := 0
	val := m.getOrSet("k", func() interface{} {
		calls++
		return "created"
	})
	if val != "created" || calls != 1 {
		t.Error("first getOrSet should call factory")
	}
	// second call should not invoke factory
	val = m.getOrSet("k", func() interface{} {
		calls++
		return "overwrite"
	})
	if val != "created" || calls != 1 {
		t.Error("second getOrSet should reuse existing value")
	}
}

func TestShardedMap_InvalidShardCount_Fallback(t *testing.T) {
	// non-power-of-2 should fall back to defaultShardCount
	m := newShardedMap(7)
	if len(m.shards) != defaultShardCount {
		t.Errorf("expected fallback to %d shards, got %d", defaultShardCount, len(m.shards))
	}
	m2 := newShardedMap(0)
	if len(m2.shards) != defaultShardCount {
		t.Errorf("expected fallback to %d shards, got %d", defaultShardCount, len(m2.shards))
	}
}

func TestShardedMap_ConcurrentReadWrite(t *testing.T) {
	m := newShardedMap(64)
	var wg sync.WaitGroup
	var errCount atomic.Int64

	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := fmt.Sprintf("key:%d", n%20)
			m.set(key, n)
			val, ok := m.get(key)
			if !ok {
				errCount.Add(1)
			}
			_ = val
		}(i)
	}
	wg.Wait()
	if errCount.Load() > 0 {
		t.Errorf("%d concurrent get failures", errCount.Load())
	}
}

// ─────────────────────────────────────────────
//  fnv32 hash distribution test
// ─────────────────────────────────────────────

func TestFnv32_Deterministic(t *testing.T) {
	h1 := fnv32("hello")
	h2 := fnv32("hello")
	if h1 != h2 {
		t.Error("fnv32 must be deterministic")
	}
}

func TestFnv32_Different(t *testing.T) {
	if fnv32("a") == fnv32("b") {
		t.Error("fnv32(a) == fnv32(b) collision (unlikely but possible)")
	}
}

func TestFnv32_EmptyString(t *testing.T) {
	h := fnv32("")
	if h == 0 {
		t.Error("fnv32 of empty string should not be 0 (FNV offset basis)")
	}
}

// ─────────────────────────────────────────────
//  GoRedis (db.go) common key operations
// ─────────────────────────────────────────────

func TestDB_DelMultiple(t *testing.T) {
	db := NewGoRedis()
	db.Set("a", "1")
	db.Set("b", "2")
	db.Set("c", "3")
	n := db.Del("a", "b", "z") // z does not exist
	if n != 2 {
		t.Errorf("Del: got %d, want 2", n)
	}
	if db.Exists("a") != 0 {
		t.Error("a should be deleted")
	}
}

func TestDB_Exists_Multiple(t *testing.T) {
	db := NewGoRedis()
	db.Set("x", "1")
	db.Set("y", "2")
	n := db.Exists("x", "y", "z")
	if n != 2 {
		t.Errorf("Exists: got %d, want 2", n)
	}
}

func TestDB_Rename(t *testing.T) {
	db := NewGoRedis()
	db.Set("old", "value")
	if err := db.Rename("old", "new"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	val, err := db.Get("new")
	if err != nil || val != "value" {
		t.Error("renamed key should be accessible under new name")
	}
	if db.Exists("old") != 0 {
		t.Error("old key should not exist after rename")
	}
}

func TestDB_Rename_NotExist(t *testing.T) {
	db := NewGoRedis()
	if err := db.Rename("ghost", "new"); err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestDB_RenameNX(t *testing.T) {
	db := NewGoRedis()
	db.Set("src", "v")
	ok, err := db.RenameNX("src", "dst")
	if err != nil || !ok {
		t.Error("RenameNX to new key should succeed")
	}

	// dst now exists – should fail
	db.Set("src2", "v2")
	ok, err = db.RenameNX("src2", "dst")
	if err != nil || ok {
		t.Error("RenameNX when dst exists should return false")
	}
}

func TestDB_Type(t *testing.T) {
	db := NewGoRedis()
	db.Set("s", "v")
	db.HSet("h", "f", "v")
	db.RPush("l", "v")
	db.SAdd("st", "v")
	db.ZAdd("z", 1, "v")

	cases := []struct{ key, want string }{
		{"s", "string"},
		{"h", "hash"},
		{"l", "list"},
		{"st", "set"},
		{"z", "zset"},
		{"none", "none"},
	}
	for _, c := range cases {
		if got := db.Type(c.key); got != c.want {
			t.Errorf("Type(%q) = %q, want %q", c.key, got, c.want)
		}
	}
}

func TestDB_Keys_Pattern(t *testing.T) {
	db := NewGoRedis()
	db.Set("user:1", "a")
	db.Set("user:2", "b")
	db.Set("post:1", "c")

	all := db.Keys("*")
	if len(all) != 3 {
		t.Errorf("Keys(*): got %d keys, want 3", len(all))
	}

	users := db.Keys("user:*")
	if len(users) != 2 {
		t.Errorf("Keys(user:*): got %d, want 2", len(users))
	}
}

func TestDB_DBSize(t *testing.T) {
	db := NewGoRedis()
	if db.DBSize() != 0 {
		t.Error("initial DBSize should be 0")
	}
	db.Set("a", "1")
	db.Set("b", "2")
	if db.DBSize() != 2 {
		t.Errorf("DBSize: got %d, want 2", db.DBSize())
	}
}

func TestDB_FlushDB(t *testing.T) {
	db := NewGoRedis()
	for i := 0; i < 10; i++ {
		db.Set(fmt.Sprintf("k%d", i), "v")
	}
	db.FlushDB()
	if db.DBSize() != 0 {
		t.Errorf("after FlushDB, size = %d, want 0", db.DBSize())
	}
}

func TestDB_WithShardCount(t *testing.T) {
	db := NewGoRedis(WithShardCount(64))
	if db.shardCount != 64 {
		t.Errorf("shardCount: got %d, want 64", db.shardCount)
	}
	// invalid shard count should be ignored
	db2 := NewGoRedis(WithShardCount(7))
	if db2.shardCount != defaultShardCount {
		t.Errorf("invalid shardCount should fallback to default, got %d", db2.shardCount)
	}
}

// ─────────────────────────────────────────────
//  matchPattern tests
// ─────────────────────────────────────────────

func TestMatchPattern(t *testing.T) {
	cases := []struct {
		pattern, str string
		want         bool
	}{
		{"*", "anything", true},
		{"*", "", true},
		{"user:*", "user:1", true},
		{"user:*", "post:1", false},
		{"user:?", "user:1", true},
		{"user:?", "user:12", false},
		{"abc", "abc", true},
		{"abc", "abcd", false},
		{"a*c", "ac", true},
		{"a*c", "abc", true},
		{"a*c", "abbc", true},
		{"a*c", "abd", false},
	}
	for _, c := range cases {
		got := matchPattern(c.pattern, c.str)
		if got != c.want {
			t.Errorf("matchPattern(%q, %q) = %v, want %v", c.pattern, c.str, got, c.want)
		}
	}
}
