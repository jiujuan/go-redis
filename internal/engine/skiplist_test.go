package engine

import (
	"math"
	"testing"
)

func TestNewSkiplistInitialState(t *testing.T) {
	sl := newSkiplist()
	if sl.length != 0 {
		t.Fatalf("length = %d, want 0", sl.length)
	}
	if sl.level != 1 {
		t.Fatalf("level = %d, want 1", sl.level)
	}
	if sl.tail != nil {
		t.Fatal("tail should be nil for empty skiplist")
	}
	if !math.IsInf(sl.header.score, -1) {
		t.Fatal("header score should be negative infinity")
	}
	if len(sl.header.forward) != skiplistMaxLevel {
		t.Fatalf("header forward levels = %d, want %d", len(sl.header.forward), skiplistMaxLevel)
	}
}

func TestNewSkiplistNodeLevel(t *testing.T) {
	node := newSkiplistNode(3, 1.5, "member")
	if len(node.forward) != 3 {
		t.Fatalf("len(forward) = %d, want 3", len(node.forward))
	}
	if len(node.span) != 3 {
		t.Fatalf("len(span) = %d, want 3", len(node.span))
	}
	if node.score != 1.5 || node.member != "member" {
		t.Fatalf("node = (%v, %q), want (1.5, member)", node.score, node.member)
	}
}

func TestSkiplistLexicographicalOrderOnSameScore(t *testing.T) {
	sl := newSkiplist()
	sl.insert(10, "charlie")
	sl.insert(10, "alice")
	sl.insert(10, "bob")

	got := sl.rangeByIndex(0, -1, false)
	want := []string{"alice", "bob", "charlie"}
	if len(got) != len(want) {
		t.Fatalf("len(rangeByIndex) = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("rangeByIndex()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestSkiplistDeleteMissingFromEmpty(t *testing.T) {
	sl := newSkiplist()
	if sl.delete(1, "ghost") {
		t.Fatal("delete on empty skiplist should return false")
	}
}

func TestSkiplistDeleteUpdatesTail(t *testing.T) {
	sl := newSkiplist()
	sl.insert(1, "a")
	sl.insert(2, "b")
	sl.insert(3, "c")

	if !sl.delete(3, "c") {
		t.Fatal("expected delete of tail node to succeed")
	}
	if sl.tail == nil || sl.tail.member != "b" {
		t.Fatalf("tail = %#v, want member b", sl.tail)
	}
}

func TestSkiplistRangeByIndexOutOfBounds(t *testing.T) {
	sl := newSkiplist()
	sl.insert(1, "a")
	sl.insert(2, "b")

	if got := sl.rangeByIndex(5, 6, false); got != nil {
		t.Fatalf("rangeByIndex out of bounds = %v, want nil", got)
	}
	if got := sl.rangeByIndex(1, 0, false); got != nil {
		t.Fatalf("rangeByIndex start > stop = %v, want nil", got)
	}
}

func TestSkiplistRangeByScoreWithScores(t *testing.T) {
	sl := newSkiplist()
	sl.insert(1.25, "a")
	sl.insert(2, "b")
	sl.insert(3.5, "c")

	got := sl.rangeByScore(1, 3, true)
	want := []string{"a", "1.25", "b", "2"}
	if len(got) != len(want) {
		t.Fatalf("len(rangeByScore) = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("rangeByScore()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestSkiplistGetScoreAlwaysMissing(t *testing.T) {
	sl := newSkiplist()
	sl.insert(1, "a")
	score, ok := sl.getScore("a")
	if ok {
		t.Fatalf("getScore() ok = true with score %v, want false", score)
	}
	if score != 0 {
		t.Fatalf("getScore() score = %v, want 0", score)
	}
}
