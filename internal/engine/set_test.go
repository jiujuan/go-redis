package engine

import (
	"sort"
	"testing"
)

func sortedStrings(s []string) []string {
	cp := make([]string, len(s))
	copy(cp, s)
	sort.Strings(cp)
	return cp
}

func TestSet_SAdd_SMembers(t *testing.T) {
	db := NewGoRedis()
	n, err := db.SAdd("s", "a", "b", "c")
	if err != nil || n != 3 {
		t.Fatalf("SAdd: got (%d, %v)", n, err)
	}
	members, err := db.SMembers("s")
	if err != nil || len(members) != 3 {
		t.Fatalf("SMembers: got (%v, %v)", members, err)
	}
}

func TestSet_SAdd_Duplicate(t *testing.T) {
	db := NewGoRedis()
	db.SAdd("s", "a", "b")
	n, _ := db.SAdd("s", "b", "c") // b is duplicate
	if n != 1 {
		t.Errorf("SAdd duplicate: got %d added, want 1", n)
	}
}

func TestSet_SRem(t *testing.T) {
	db := NewGoRedis()
	db.SAdd("s", "a", "b", "c")
	n, err := db.SRem("s", "a", "z")
	if err != nil || n != 1 {
		t.Errorf("SRem: got (%d, %v), want (1, nil)", n, err)
	}
	ok, _ := db.SIsMember("s", "a")
	if ok {
		t.Error("a should be removed")
	}
}

func TestSet_SRem_AutoDeletesEmptyKey(t *testing.T) {
	db := NewGoRedis()
	db.SAdd("s", "only")
	db.SRem("s", "only")
	if db.Exists("s") != 0 {
		t.Error("empty set should be auto-deleted")
	}
}

func TestSet_SIsMember(t *testing.T) {
	db := NewGoRedis()
	db.SAdd("s", "x")
	ok, _ := db.SIsMember("s", "x")
	if !ok {
		t.Error("x should be member")
	}
	ok, _ = db.SIsMember("s", "y")
	if ok {
		t.Error("y should not be member")
	}
}

func TestSet_SIsMember_Missing(t *testing.T) {
	db := NewGoRedis()
	ok, err := db.SIsMember("ghost", "x")
	if err != nil || ok {
		t.Errorf("SIsMember missing key: got (%v, %v)", ok, err)
	}
}

func TestSet_SCard(t *testing.T) {
	db := NewGoRedis()
	n, _ := db.SCard("ghost")
	if n != 0 {
		t.Error("SCard missing key should be 0")
	}
	db.SAdd("s", "a", "b", "c")
	n, _ = db.SCard("s")
	if n != 3 {
		t.Errorf("SCard: got %d, want 3", n)
	}
}

func TestSet_SInter(t *testing.T) {
	db := NewGoRedis()
	db.SAdd("s1", "a", "b", "c")
	db.SAdd("s2", "b", "c", "d")
	inter, err := db.SInter("s1", "s2")
	if err != nil {
		t.Fatal(err)
	}
	got := sortedStrings(inter)
	if len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Errorf("SInter: got %v, want [b c]", got)
	}
}

func TestSet_SInter_EmptyResult(t *testing.T) {
	db := NewGoRedis()
	db.SAdd("s1", "a")
	db.SAdd("s2", "b")
	inter, _ := db.SInter("s1", "s2")
	if len(inter) != 0 {
		t.Errorf("SInter disjoint sets: got %v, want []", inter)
	}
}

func TestSet_SInter_MissingKey(t *testing.T) {
	db := NewGoRedis()
	db.SAdd("s1", "a")
	inter, err := db.SInter("s1", "ghost")
	if err != nil || len(inter) != 0 {
		t.Errorf("SInter with missing key: got (%v, %v)", inter, err)
	}
}

func TestSet_SUnion(t *testing.T) {
	db := NewGoRedis()
	db.SAdd("s1", "a", "b")
	db.SAdd("s2", "b", "c")
	union, err := db.SUnion("s1", "s2")
	if err != nil {
		t.Fatal(err)
	}
	if len(union) != 3 {
		t.Errorf("SUnion: got %d elements, want 3", len(union))
	}
}

func TestSet_SDiff(t *testing.T) {
	db := NewGoRedis()
	db.SAdd("s1", "a", "b", "c")
	db.SAdd("s2", "b", "d")
	diff, err := db.SDiff("s1", "s2")
	if err != nil {
		t.Fatal(err)
	}
	got := sortedStrings(diff)
	if len(got) != 2 || got[0] != "a" || got[1] != "c" {
		t.Errorf("SDiff: got %v, want [a c]", got)
	}
}

func TestSet_SDiff_MissingFirst(t *testing.T) {
	db := NewGoRedis()
	diff, err := db.SDiff("ghost", "s2")
	if err != nil || len(diff) != 0 {
		t.Errorf("SDiff missing first key: got (%v, %v)", diff, err)
	}
}

func TestSet_SMove(t *testing.T) {
	db := NewGoRedis()
	db.SAdd("src", "a", "b")
	db.SAdd("dst", "c")
	ok, err := db.SMove("src", "dst", "a")
	if err != nil || !ok {
		t.Errorf("SMove: got (%v, %v)", ok, err)
	}
	ok, _ = db.SIsMember("src", "a")
	if ok {
		t.Error("a should be removed from src")
	}
	ok, _ = db.SIsMember("dst", "a")
	if !ok {
		t.Error("a should be in dst")
	}
}

func TestSet_SMove_NotMember(t *testing.T) {
	db := NewGoRedis()
	db.SAdd("src", "b")
	ok, err := db.SMove("src", "dst", "notexist")
	if err != nil || ok {
		t.Errorf("SMove non-member: got (%v, %v), want (false, nil)", ok, err)
	}
}

func TestSet_WrongType(t *testing.T) {
	db := NewGoRedis()
	db.Set("s", "string")
	_, err := db.SAdd("s", "v")
	if err != ErrWrongType {
		t.Errorf("expected ErrWrongType, got %v", err)
	}
}

func TestSet_SMembers_Missing(t *testing.T) {
	db := NewGoRedis()
	members, err := db.SMembers("ghost")
	if err != nil || len(members) != 0 {
		t.Errorf("SMembers missing: got (%v, %v)", members, err)
	}
}
