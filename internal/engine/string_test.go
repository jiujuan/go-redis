package engine

import (
	"fmt"
	"sync"
	"testing"
)

func TestString_SetGet(t *testing.T) {
	db := NewGoRedis()
	if err := db.Set("name", "alice"); err != nil {
		t.Fatal(err)
	}
	val, err := db.Get("name")
	if err != nil || val != "alice" {
		t.Errorf("Get: got (%q, %v), want (alice, nil)", val, err)
	}
}

func TestString_GetNotFound(t *testing.T) {
	db := NewGoRedis()
	_, err := db.Get("missing")
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestString_GetWrongType(t *testing.T) {
	db := NewGoRedis()
	db.HSet("h", "f", "v")
	_, err := db.Get("h")
	if err != ErrWrongType {
		t.Errorf("expected ErrWrongType, got %v", err)
	}
}

func TestString_Overwrite(t *testing.T) {
	db := NewGoRedis()
	db.Set("k", "v1")
	db.Set("k", "v2")
	val, _ := db.Get("k")
	if val != "v2" {
		t.Errorf("overwrite: got %q, want v2", val)
	}
}

func TestString_GetSet(t *testing.T) {
	db := NewGoRedis()
	db.Set("k", "old")
	old, err := db.GetSet("k", "new")
	if err != nil || old != "old" {
		t.Errorf("GetSet: old=%q err=%v", old, err)
	}
	val, _ := db.Get("k")
	if val != "new" {
		t.Errorf("GetSet: new value should be 'new', got %q", val)
	}
}

func TestString_GetSet_NoExist(t *testing.T) {
	db := NewGoRedis()
	_, err := db.GetSet("ghost", "v")
	if err != ErrKeyNotFound {
		t.Errorf("GetSet on missing key: expected ErrKeyNotFound, got %v", err)
	}
	// value should still be written
	val, _ := db.Get("ghost")
	if val != "v" {
		t.Errorf("GetSet should write value even if key was absent, got %q", val)
	}
}

func TestString_SetNX(t *testing.T) {
	db := NewGoRedis()
	if !db.SetNX("k", "first") {
		t.Error("first SetNX should succeed")
	}
	if db.SetNX("k", "second") {
		t.Error("second SetNX should fail")
	}
	val, _ := db.Get("k")
	if val != "first" {
		t.Errorf("SetNX: value should remain 'first', got %q", val)
	}
}

func TestString_MSetMGet(t *testing.T) {
	db := NewGoRedis()
	if err := db.MSet("a", "1", "b", "2", "c", "3"); err != nil {
		t.Fatal(err)
	}
	vals := db.MGet("a", "b", "c", "d")
	expected := []string{"1", "2", "3", ""}
	for i, want := range expected {
		if vals[i] != want {
			t.Errorf("MGet[%d]: got %q, want %q", i, vals[i], want)
		}
	}
}

func TestString_MSet_OddArgs(t *testing.T) {
	db := NewGoRedis()
	if err := db.MSet("a", "1", "b"); err == nil {
		t.Error("MSet with odd args should return error")
	}
}

func TestString_Incr(t *testing.T) {
	db := NewGoRedis()
	n, err := db.Incr("counter")
	if err != nil || n != 1 {
		t.Errorf("first Incr: got (%d, %v), want (1, nil)", n, err)
	}
	n, _ = db.Incr("counter")
	if n != 2 {
		t.Errorf("second Incr: got %d, want 2", n)
	}
}

func TestString_Decr(t *testing.T) {
	db := NewGoRedis()
	db.Set("n", "10")
	n, err := db.Decr("n")
	if err != nil || n != 9 {
		t.Errorf("Decr: got (%d, %v), want (9, nil)", n, err)
	}
}

func TestString_IncrBy(t *testing.T) {
	db := NewGoRedis()
	n, _ := db.IncrBy("n", 5)
	if n != 5 {
		t.Errorf("IncrBy 5 from 0: got %d", n)
	}
	n, _ = db.IncrBy("n", -3)
	if n != 2 {
		t.Errorf("IncrBy -3: got %d", n)
	}
}

func TestString_IncrByWrongType(t *testing.T) {
	db := NewGoRedis()
	db.Set("k", "notanumber")
	_, err := db.Incr("k")
	if err == nil {
		t.Error("Incr on non-integer string should return error")
	}
}

func TestString_Append(t *testing.T) {
	db := NewGoRedis()
	db.Set("k", "Hello")
	n, err := db.Append("k", " World")
	if err != nil || n != 11 {
		t.Errorf("Append: got (%d, %v), want (11, nil)", n, err)
	}
	val, _ := db.Get("k")
	if val != "Hello World" {
		t.Errorf("Append result: got %q", val)
	}
}

func TestString_Append_NewKey(t *testing.T) {
	db := NewGoRedis()
	n, err := db.Append("new", "abc")
	if err != nil || n != 3 {
		t.Errorf("Append to new key: got (%d, %v)", n, err)
	}
}

func TestString_StrLen(t *testing.T) {
	db := NewGoRedis()
	db.Set("k", "hello")
	n, err := db.StrLen("k")
	if err != nil || n != 5 {
		t.Errorf("StrLen: got (%d, %v), want (5, nil)", n, err)
	}
}

func TestString_StrLen_Missing(t *testing.T) {
	db := NewGoRedis()
	n, err := db.StrLen("ghost")
	if err != nil || n != 0 {
		t.Errorf("StrLen missing: got (%d, %v), want (0, nil)", n, err)
	}
}

func TestString_ConcurrentIncr(t *testing.T) {
	db := NewGoRedis()
	db.Set("cnt", "0")
	var wg sync.WaitGroup
	const goroutines = 100
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db.Incr("cnt")
		}()
	}
	wg.Wait()
	val, _ := db.Get("cnt")
	if val != fmt.Sprintf("%d", goroutines) {
		t.Errorf("concurrent Incr: got %s, want %d", val, goroutines)
	}
}
