package engine

import (
	"math"
	"testing"
)

// ─────────────────────────────────────────────
//  skiplist internal tests
// ─────────────────────────────────────────────

func TestSkiplist_InsertAndLength(t *testing.T) {
	sl := newSkiplist()
	sl.insert(1.0, "a")
	sl.insert(2.0, "b")
	sl.insert(3.0, "c")
	if sl.length != 3 {
		t.Errorf("length: got %d, want 3", sl.length)
	}
}

func TestSkiplist_OrderedRange(t *testing.T) {
	sl := newSkiplist()
	sl.insert(3.0, "c")
	sl.insert(1.0, "a")
	sl.insert(2.0, "b")
	result := sl.rangeByIndex(0, -1, false)
	if len(result) != 3 || result[0] != "a" || result[1] != "b" || result[2] != "c" {
		t.Errorf("rangeByIndex: got %v", result)
	}
}

func TestSkiplist_RangeWithScore(t *testing.T) {
	sl := newSkiplist()
	sl.insert(10.0, "x")
	sl.insert(20.0, "y")
	result := sl.rangeByIndex(0, -1, true)
	// [x, 10, y, 20]
	if len(result) != 4 || result[0] != "x" || result[2] != "y" {
		t.Errorf("rangeByIndex withScore: got %v", result)
	}
}

func TestSkiplist_RangeByScore(t *testing.T) {
	sl := newSkiplist()
	sl.insert(10.0, "a")
	sl.insert(20.0, "b")
	sl.insert(30.0, "c")
	result := sl.rangeByScore(15.0, 25.0, false)
	if len(result) != 1 || result[0] != "b" {
		t.Errorf("rangeByScore: got %v", result)
	}
}

func TestSkiplist_RangeByScore_Inf(t *testing.T) {
	sl := newSkiplist()
	sl.insert(10.0, "a")
	sl.insert(20.0, "b")
	result := sl.rangeByScore(math.Inf(-1), math.Inf(1), false)
	if len(result) != 2 {
		t.Errorf("rangeByScore +/-inf: got %v", result)
	}
}

func TestSkiplist_Delete(t *testing.T) {
	sl := newSkiplist()
	sl.insert(1.0, "a")
	sl.insert(2.0, "b")
	if !sl.delete(1.0, "a") {
		t.Error("delete existing should return true")
	}
	if sl.length != 1 {
		t.Errorf("length after delete: got %d, want 1", sl.length)
	}
	if sl.delete(1.0, "a") {
		t.Error("delete non-existing should return false")
	}
}

func TestSkiplist_Rank(t *testing.T) {
	sl := newSkiplist()
	sl.insert(1.0, "a")
	sl.insert(2.0, "b")
	sl.insert(3.0, "c")
	if r := sl.rank(2.0, "b"); r != 1 {
		t.Errorf("rank(b): got %d, want 1", r)
	}
	if r := sl.rank(1.0, "a"); r != 0 {
		t.Errorf("rank(a): got %d, want 0", r)
	}
	if r := sl.rank(99.0, "z"); r != -1 {
		t.Errorf("rank missing: got %d, want -1", r)
	}
}

func TestSkiplist_RangeByIndex_NegativeIndex(t *testing.T) {
	sl := newSkiplist()
	sl.insert(1.0, "a")
	sl.insert(2.0, "b")
	sl.insert(3.0, "c")
	result := sl.rangeByIndex(-2, -1, false)
	if len(result) != 2 || result[0] != "b" || result[1] != "c" {
		t.Errorf("rangeByIndex negative: got %v", result)
	}
}

func TestSkiplist_RangeByIndex_Empty(t *testing.T) {
	sl := newSkiplist()
	result := sl.rangeByIndex(0, -1, false)
	if result != nil {
		t.Errorf("empty skiplist range: got %v", result)
	}
}

func TestSkiplist_SameMemberDifferentScore(t *testing.T) {
	// ZSet uses delete+insert for updates; skiplist itself handles distinct (score,member) pairs
	sl := newSkiplist()
	sl.insert(1.0, "a")
	sl.delete(1.0, "a")
	sl.insert(5.0, "a")
	if sl.length != 1 {
		t.Errorf("after update: length=%d", sl.length)
	}
	if r := sl.rank(5.0, "a"); r != 0 {
		t.Errorf("updated rank: got %d, want 0", r)
	}
}

// ─────────────────────────────────────────────
//  ZSet (GoRedis API) tests
// ─────────────────────────────────────────────

func TestZSet_ZAdd_ZScore(t *testing.T) {
	db := NewGoRedis()
	n, err := db.ZAdd("z", 100.0, "alice")
	if err != nil || n != 1 {
		t.Fatalf("ZAdd: got (%d, %v)", n, err)
	}
	score, ok, err := db.ZScore("z", "alice")
	if err != nil || !ok || score != 100.0 {
		t.Errorf("ZScore: got (%v, %v, %v)", score, ok, err)
	}
}

func TestZSet_ZAdd_Update(t *testing.T) {
	db := NewGoRedis()
	db.ZAdd("z", 1.0, "m")
	n, _ := db.ZAdd("z", 2.0, "m") // update
	if n != 0 {
		t.Error("ZAdd existing member should return 0 added")
	}
	score, _, _ := db.ZScore("z", "m")
	if score != 2.0 {
		t.Errorf("ZAdd update: score should be 2.0, got %v", score)
	}
}

func TestZSet_ZScore_Missing(t *testing.T) {
	db := NewGoRedis()
	_, ok, err := db.ZScore("ghost", "m")
	if err != nil || ok {
		t.Errorf("ZScore missing: got (ok=%v, err=%v)", ok, err)
	}
}

func TestZSet_ZRem(t *testing.T) {
	db := NewGoRedis()
	db.ZAdd("z", 1.0, "a")
	db.ZAdd("z", 2.0, "b")
	n, err := db.ZRem("z", "a", "z")
	if err != nil || n != 1 {
		t.Errorf("ZRem: got (%d, %v)", n, err)
	}
	_, ok, _ := db.ZScore("z", "a")
	if ok {
		t.Error("a should be removed")
	}
}

func TestZSet_ZRem_AutoDeletesEmptyKey(t *testing.T) {
	db := NewGoRedis()
	db.ZAdd("z", 1.0, "only")
	db.ZRem("z", "only")
	if db.Exists("z") != 0 {
		t.Error("empty zset should be auto-deleted")
	}
}

func TestZSet_ZRange(t *testing.T) {
	db := NewGoRedis()
	db.ZAdd("z", 3.0, "c")
	db.ZAdd("z", 1.0, "a")
	db.ZAdd("z", 2.0, "b")
	items, err := db.ZRange("z", 0, -1, false)
	if err != nil || len(items) != 3 {
		t.Fatalf("ZRange: got (%v, %v)", items, err)
	}
	if items[0] != "a" || items[1] != "b" || items[2] != "c" {
		t.Errorf("ZRange order: got %v", items)
	}
}

func TestZSet_ZRange_WithScore(t *testing.T) {
	db := NewGoRedis()
	db.ZAdd("z", 10.0, "x")
	db.ZAdd("z", 20.0, "y")
	items, _ := db.ZRange("z", 0, -1, true)
	// [x, 10, y, 20]
	if len(items) != 4 {
		t.Fatalf("ZRange WITHSCORES: got len=%d", len(items))
	}
	if items[0] != "x" || items[1] != "10" || items[2] != "y" || items[3] != "20" {
		t.Errorf("ZRange WITHSCORES: got %v", items)
	}
}

func TestZSet_ZRevRange(t *testing.T) {
	db := NewGoRedis()
	db.ZAdd("z", 1.0, "a")
	db.ZAdd("z", 2.0, "b")
	db.ZAdd("z", 3.0, "c")
	items, err := db.ZRevRange("z", 0, -1, false)
	if err != nil || len(items) != 3 {
		t.Fatalf("ZRevRange: got (%v, %v)", items, err)
	}
	if items[0] != "c" || items[2] != "a" {
		t.Errorf("ZRevRange order: got %v", items)
	}
}

func TestZSet_ZRangeByScore(t *testing.T) {
	db := NewGoRedis()
	db.ZAdd("z", 10.0, "a")
	db.ZAdd("z", 20.0, "b")
	db.ZAdd("z", 30.0, "c")
	items, err := db.ZRangeByScore("z", 15.0, 25.0, false)
	if err != nil || len(items) != 1 || items[0] != "b" {
		t.Errorf("ZRangeByScore: got (%v, %v)", items, err)
	}
}

func TestZSet_ZRangeByScore_Inf(t *testing.T) {
	db := NewGoRedis()
	db.ZAdd("z", 5.0, "x")
	db.ZAdd("z", 10.0, "y")
	items, _ := db.ZRangeByScore("z", math.Inf(-1), math.Inf(1), false)
	if len(items) != 2 {
		t.Errorf("ZRangeByScore +/-inf: got %v", items)
	}
}

func TestZSet_ZCard(t *testing.T) {
	db := NewGoRedis()
	n, _ := db.ZCard("ghost")
	if n != 0 {
		t.Error("ZCard missing key should be 0")
	}
	db.ZAdd("z", 1.0, "a")
	db.ZAdd("z", 2.0, "b")
	n, _ = db.ZCard("z")
	if n != 2 {
		t.Errorf("ZCard: got %d, want 2", n)
	}
}

func TestZSet_ZRank(t *testing.T) {
	db := NewGoRedis()
	db.ZAdd("z", 1.0, "a")
	db.ZAdd("z", 2.0, "b")
	db.ZAdd("z", 3.0, "c")
	r, err := db.ZRank("z", "b")
	if err != nil || r != 1 {
		t.Errorf("ZRank b: got (%d, %v), want (1, nil)", r, err)
	}
	r, _ = db.ZRank("z", "notexist")
	if r != -1 {
		t.Errorf("ZRank missing: got %d, want -1", r)
	}
}

func TestZSet_ZIncrBy(t *testing.T) {
	db := NewGoRedis()
	db.ZAdd("z", 10.0, "m")
	score, err := db.ZIncrBy("z", 5.0, "m")
	if err != nil || score != 15.0 {
		t.Errorf("ZIncrBy: got (%v, %v)", score, err)
	}
}

func TestZSet_ZIncrBy_NewMember(t *testing.T) {
	db := NewGoRedis()
	score, err := db.ZIncrBy("z", 7.0, "new")
	if err != nil || score != 7.0 {
		t.Errorf("ZIncrBy new member: got (%v, %v)", score, err)
	}
}

func TestZSet_ZCount(t *testing.T) {
	db := NewGoRedis()
	db.ZAdd("z", 10.0, "a")
	db.ZAdd("z", 20.0, "b")
	db.ZAdd("z", 30.0, "c")
	n, err := db.ZCount("z", 10.0, 20.0)
	if err != nil || n != 2 {
		t.Errorf("ZCount: got (%d, %v), want (2, nil)", n, err)
	}
}

func TestZSet_WrongType(t *testing.T) {
	db := NewGoRedis()
	db.Set("s", "string")
	_, err := db.ZAdd("s", 1.0, "m")
	if err != ErrWrongType {
		t.Errorf("expected ErrWrongType, got %v", err)
	}
}

func TestZSet_LargeDataset(t *testing.T) {
	db := NewGoRedis()
	const N = 1000
	for i := 0; i < N; i++ {
		db.ZAdd("rank", float64(i), formatInt64(int64(i)))
	}
	n, _ := db.ZCard("rank")
	if n != N {
		t.Errorf("ZCard after %d inserts: got %d", N, n)
	}
	items, _ := db.ZRange("rank", 0, 9, false)
	if len(items) != 10 || items[0] != "0" {
		t.Errorf("ZRange first 10: got %v", items)
	}
}
