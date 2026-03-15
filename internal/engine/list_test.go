package engine

import (
	"testing"
)

func TestList_LPush_LRange(t *testing.T) {
	db := NewGoRedis()
	n, err := db.LPush("l", "c", "b", "a")
	if err != nil || n != 3 {
		t.Fatalf("LPush: got (%d, %v)", n, err)
	}
	// LPush inserts at head each call: result = [a, b, c]
	items, err := db.LRange("l", 0, -1)
	if err != nil || len(items) != 3 {
		t.Fatalf("LRange: got %v, %v", items, err)
	}
	if items[0] != "a" || items[1] != "b" || items[2] != "c" {
		t.Errorf("LRange order: got %v", items)
	}
}

func TestList_RPush_LRange(t *testing.T) {
	db := NewGoRedis()
	db.RPush("l", "a", "b", "c")
	items, _ := db.LRange("l", 0, -1)
	if len(items) != 3 || items[0] != "a" || items[2] != "c" {
		t.Errorf("RPush order: got %v", items)
	}
}

func TestList_LPop(t *testing.T) {
	db := NewGoRedis()
	db.RPush("l", "x", "y", "z")
	val, err := db.LPop("l")
	if err != nil || val != "x" {
		t.Errorf("LPop: got (%q, %v), want (x, nil)", val, err)
	}
	n, _ := db.LLen("l")
	if n != 2 {
		t.Errorf("LLen after LPop: got %d, want 2", n)
	}
}

func TestList_RPop(t *testing.T) {
	db := NewGoRedis()
	db.RPush("l", "x", "y", "z")
	val, err := db.RPop("l")
	if err != nil || val != "z" {
		t.Errorf("RPop: got (%q, %v), want (z, nil)", val, err)
	}
}

func TestList_LPop_Empty(t *testing.T) {
	db := NewGoRedis()
	_, err := db.LPop("ghost")
	if err != ErrKeyNotFound {
		t.Errorf("LPop empty list: expected ErrKeyNotFound, got %v", err)
	}
}

func TestList_Pop_AutoDeletesEmptyKey(t *testing.T) {
	db := NewGoRedis()
	db.RPush("l", "only")
	db.LPop("l")
	if db.Exists("l") != 0 {
		t.Error("empty list should be auto-deleted after pop")
	}
}

func TestList_LRange_NegativeIndex(t *testing.T) {
	db := NewGoRedis()
	db.RPush("l", "a", "b", "c", "d", "e")
	items, err := db.LRange("l", -3, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 || items[0] != "c" || items[2] != "e" {
		t.Errorf("LRange negative: got %v", items)
	}
}

func TestList_LRange_OutOfBounds(t *testing.T) {
	db := NewGoRedis()
	db.RPush("l", "a", "b")
	items, err := db.LRange("l", 0, 100)
	if err != nil || len(items) != 2 {
		t.Errorf("LRange beyond end: got %v, %v", items, err)
	}
}

func TestList_LRange_Empty(t *testing.T) {
	db := NewGoRedis()
	items, err := db.LRange("ghost", 0, -1)
	if err != nil || len(items) != 0 {
		t.Errorf("LRange missing key: got (%v, %v)", items, err)
	}
}

func TestList_LRange_StartAfterStop(t *testing.T) {
	db := NewGoRedis()
	db.RPush("l", "a", "b", "c")
	items, err := db.LRange("l", 5, 3)
	if err != nil || len(items) != 0 {
		t.Errorf("LRange start>stop: got (%v, %v)", items, err)
	}
}

func TestList_LLen(t *testing.T) {
	db := NewGoRedis()
	n, _ := db.LLen("ghost")
	if n != 0 {
		t.Error("LLen missing key should be 0")
	}
	db.RPush("l", "a", "b", "c")
	n, _ = db.LLen("l")
	if n != 3 {
		t.Errorf("LLen: got %d, want 3", n)
	}
}

func TestList_LIndex(t *testing.T) {
	db := NewGoRedis()
	db.RPush("l", "a", "b", "c")
	val, err := db.LIndex("l", 1)
	if err != nil || val != "b" {
		t.Errorf("LIndex(1): got (%q, %v)", val, err)
	}
	val, err = db.LIndex("l", -1)
	if err != nil || val != "c" {
		t.Errorf("LIndex(-1): got (%q, %v)", val, err)
	}
}

func TestList_LIndex_OutOfRange(t *testing.T) {
	db := NewGoRedis()
	db.RPush("l", "a")
	_, err := db.LIndex("l", 5)
	if err != ErrOutOfRange {
		t.Errorf("expected ErrOutOfRange, got %v", err)
	}
}

func TestList_LSet(t *testing.T) {
	db := NewGoRedis()
	db.RPush("l", "a", "b", "c")
	if err := db.LSet("l", 1, "B"); err != nil {
		t.Fatal(err)
	}
	val, _ := db.LIndex("l", 1)
	if val != "B" {
		t.Errorf("LSet: got %q, want B", val)
	}
}

func TestList_LSet_OutOfRange(t *testing.T) {
	db := NewGoRedis()
	db.RPush("l", "a")
	err := db.LSet("l", 10, "x")
	if err != ErrOutOfRange {
		t.Errorf("expected ErrOutOfRange, got %v", err)
	}
}

func TestList_LRem(t *testing.T) {
	db := NewGoRedis()
	db.RPush("l", "a", "b", "a", "c", "a")
	// count=2 removes first 2 'a' from head
	n, err := db.LRem("l", 2, "a")
	if err != nil || n != 2 {
		t.Errorf("LRem count=2: got (%d, %v), want (2, nil)", n, err)
	}
	items, _ := db.LRange("l", 0, -1)
	// remaining: b, c, a
	if len(items) != 3 {
		t.Errorf("after LRem: got %v", items)
	}
}

func TestList_LRem_All(t *testing.T) {
	db := NewGoRedis()
	db.RPush("l", "x", "x", "x")
	n, _ := db.LRem("l", 0, "x") // 0 = remove all
	if n != 3 {
		t.Errorf("LRem all: got %d, want 3", n)
	}
}

func TestList_LRem_Negative(t *testing.T) {
	db := NewGoRedis()
	db.RPush("l", "a", "b", "a", "a")
	// count=-1 removes 1 from tail
	n, _ := db.LRem("l", -1, "a")
	if n != 1 {
		t.Errorf("LRem negative: got %d, want 1", n)
	}
}

func TestList_LPushX(t *testing.T) {
	db := NewGoRedis()
	// key does not exist – should not create
	n, _ := db.LPushX("ghost", "v")
	if n != 0 || db.Exists("ghost") != 0 {
		t.Error("LPushX on missing key should not create list")
	}
	db.RPush("l", "a")
	n, _ = db.LPushX("l", "b")
	if n != 2 {
		t.Errorf("LPushX on existing list: got %d, want 2", n)
	}
}

func TestList_RPushX(t *testing.T) {
	db := NewGoRedis()
	n, _ := db.RPushX("ghost", "v")
	if n != 0 {
		t.Error("RPushX on missing key should return 0")
	}
}

func TestList_WrongType(t *testing.T) {
	db := NewGoRedis()
	db.Set("s", "string")
	_, err := db.LPush("s", "v")
	if err != ErrWrongType {
		t.Errorf("expected ErrWrongType, got %v", err)
	}
}
