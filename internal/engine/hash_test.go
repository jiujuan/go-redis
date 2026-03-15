package engine

import (
	"sort"
	"testing"
)

func TestHash_HSet_HGet(t *testing.T) {
	db := NewGoRedis()
	n, err := db.HSet("h", "f1", "v1", "f2", "v2")
	if err != nil || n != 2 {
		t.Fatalf("HSet: got (%d, %v), want (2, nil)", n, err)
	}
	val, err := db.HGet("h", "f1")
	if err != nil || val != "v1" {
		t.Errorf("HGet f1: got (%q, %v)", val, err)
	}
	val, err = db.HGet("h", "f2")
	if err != nil || val != "v2" {
		t.Errorf("HGet f2: got (%q, %v)", val, err)
	}
}

func TestHash_HGet_MissingField(t *testing.T) {
	db := NewGoRedis()
	db.HSet("h", "f", "v")
	_, err := db.HGet("h", "nofield")
	if err != ErrMemberNotFound {
		t.Errorf("expected ErrMemberNotFound, got %v", err)
	}
}

func TestHash_HGet_MissingKey(t *testing.T) {
	db := NewGoRedis()
	_, err := db.HGet("ghost", "f")
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestHash_HSet_WrongType(t *testing.T) {
	db := NewGoRedis()
	db.Set("s", "string")
	_, err := db.HSet("s", "f", "v")
	if err != ErrWrongType {
		t.Errorf("expected ErrWrongType, got %v", err)
	}
}

func TestHash_HSet_Update(t *testing.T) {
	db := NewGoRedis()
	db.HSet("h", "f", "v1")
	n, _ := db.HSet("h", "f", "v2") // update existing field
	if n != 0 {
		t.Errorf("updating existing field should return 0 added, got %d", n)
	}
	val, _ := db.HGet("h", "f")
	if val != "v2" {
		t.Errorf("updated field value: got %q, want v2", val)
	}
}

func TestHash_HDel(t *testing.T) {
	db := NewGoRedis()
	db.HSet("h", "f1", "v1", "f2", "v2", "f3", "v3")
	n, err := db.HDel("h", "f1", "f2", "nofield")
	if err != nil || n != 2 {
		t.Errorf("HDel: got (%d, %v), want (2, nil)", n, err)
	}
	_, err = db.HGet("h", "f1")
	if err != ErrMemberNotFound {
		t.Error("f1 should be deleted")
	}
}

func TestHash_HDel_AllFields_DeletesKey(t *testing.T) {
	db := NewGoRedis()
	db.HSet("h", "f", "v")
	db.HDel("h", "f")
	if db.Exists("h") != 0 {
		t.Error("empty hash should be auto-deleted")
	}
}

func TestHash_HGetAll(t *testing.T) {
	db := NewGoRedis()
	db.HSet("h", "a", "1", "b", "2")
	all, err := db.HGetAll("h")
	if err != nil || len(all) != 4 {
		t.Errorf("HGetAll: got len=%d, want 4", len(all))
	}
}

func TestHash_HGetAll_Missing(t *testing.T) {
	db := NewGoRedis()
	all, err := db.HGetAll("ghost")
	if err != nil || len(all) != 0 {
		t.Errorf("HGetAll missing key: got (%v, %v)", all, err)
	}
}

func TestHash_HExists(t *testing.T) {
	db := NewGoRedis()
	db.HSet("h", "f", "v")
	ok, _ := db.HExists("h", "f")
	if !ok {
		t.Error("HExists existing field should be true")
	}
	ok, _ = db.HExists("h", "nofield")
	if ok {
		t.Error("HExists missing field should be false")
	}
}

func TestHash_HLen(t *testing.T) {
	db := NewGoRedis()
	db.HSet("h", "a", "1", "b", "2", "c", "3")
	n, err := db.HLen("h")
	if err != nil || n != 3 {
		t.Errorf("HLen: got (%d, %v), want (3, nil)", n, err)
	}
}

func TestHash_HKeys(t *testing.T) {
	db := NewGoRedis()
	db.HSet("h", "x", "1", "y", "2")
	keys, err := db.HKeys("h")
	if err != nil || len(keys) != 2 {
		t.Errorf("HKeys: got (%v, %v)", keys, err)
	}
	sort.Strings(keys)
	if keys[0] != "x" || keys[1] != "y" {
		t.Errorf("HKeys: got %v", keys)
	}
}

func TestHash_HVals(t *testing.T) {
	db := NewGoRedis()
	db.HSet("h", "f1", "v1", "f2", "v2")
	vals, err := db.HVals("h")
	if err != nil || len(vals) != 2 {
		t.Errorf("HVals: got (%v, %v)", vals, err)
	}
}

func TestHash_HMGet(t *testing.T) {
	db := NewGoRedis()
	db.HSet("h", "a", "1", "b", "2")
	vals, err := db.HMGet("h", "a", "b", "missing")
	if err != nil {
		t.Fatal(err)
	}
	if vals[0] != "1" || vals[1] != "2" || vals[2] != "" {
		t.Errorf("HMGet: got %v", vals)
	}
}

func TestHash_HSetNX(t *testing.T) {
	db := NewGoRedis()
	ok, err := db.HSetNX("h", "f", "v1")
	if err != nil || !ok {
		t.Error("first HSetNX should succeed")
	}
	ok, _ = db.HSetNX("h", "f", "v2")
	if ok {
		t.Error("second HSetNX should fail")
	}
	val, _ := db.HGet("h", "f")
	if val != "v1" {
		t.Errorf("HSetNX: value should remain v1, got %q", val)
	}
}

func TestHash_HIncrBy(t *testing.T) {
	db := NewGoRedis()
	db.HSet("h", "score", "10")
	n, err := db.HIncrBy("h", "score", 5)
	if err != nil || n != 15 {
		t.Errorf("HIncrBy: got (%d, %v), want (15, nil)", n, err)
	}
	n, _ = db.HIncrBy("h", "score", -3)
	if n != 12 {
		t.Errorf("HIncrBy negative: got %d, want 12", n)
	}
}

func TestHash_HIncrBy_NewField(t *testing.T) {
	db := NewGoRedis()
	n, err := db.HIncrBy("h", "new", 7)
	if err != nil || n != 7 {
		t.Errorf("HIncrBy new field: got (%d, %v), want (7, nil)", n, err)
	}
}
