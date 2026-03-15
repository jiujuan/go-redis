package engine_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/jiujuan/go-redis/internal/engine"
)

// ---- 测试辅助 ----

func newDB() *engine.GoRedis {
	return engine.NewGoRedis()
}

func assertNoErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func assertEqual(t *testing.T, got, want interface{}) {
	t.Helper()
	if fmt.Sprintf("%v", got) != fmt.Sprintf("%v", want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// ---- String 测试 ----

func TestString_SetGet(t *testing.T) {
	db := newDB()
	assertNoErr(t, db.Set("name", "tom"))

	val, err := db.Get("name")
	assertNoErr(t, err)
	assertEqual(t, val, "tom")
}

func TestString_GetNotFound(t *testing.T) {
	db := newDB()
	_, err := db.Get("notexist")
	if err != engine.ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestString_Del(t *testing.T) {
	db := newDB()
	db.Set("k", "v")
	assertEqual(t, db.Del("k"), 1)
	assertEqual(t, db.Del("k"), 0)
}

func TestString_SetNX(t *testing.T) {
	db := newDB()
	assertEqual(t, db.SetNX("k", "v1"), true)
	assertEqual(t, db.SetNX("k", "v2"), false)
	val, _ := db.Get("k")
	assertEqual(t, val, "v1")
}

func TestString_IncrDecr(t *testing.T) {
	db := newDB()
	n, err := db.Incr("counter")
	assertNoErr(t, err)
	assertEqual(t, n, int64(1))

	n, err = db.IncrBy("counter", 9)
	assertNoErr(t, err)
	assertEqual(t, n, int64(10))

	n, err = db.Decr("counter")
	assertNoErr(t, err)
	assertEqual(t, n, int64(9))
}

func TestString_Append(t *testing.T) {
	db := newDB()
	db.Set("k", "hello")
	n, err := db.Append("k", " world")
	assertNoErr(t, err)
	assertEqual(t, n, 11)
	val, _ := db.Get("k")
	assertEqual(t, val, "hello world")
}

func TestString_MSetMGet(t *testing.T) {
	db := newDB()
	assertNoErr(t, db.MSet("a", "1", "b", "2", "c", "3"))
	vals := db.MGet("a", "b", "c", "d")
	assertEqual(t, vals[0], "1")
	assertEqual(t, vals[1], "2")
	assertEqual(t, vals[2], "3")
	assertEqual(t, vals[3], "")
}

// ---- Hash 测试 ----

func TestHash_HSetHGet(t *testing.T) {
	db := newDB()
	n, err := db.HSet("user:1", "name", "alice", "age", "30")
	assertNoErr(t, err)
	assertEqual(t, n, 2)

	val, err := db.HGet("user:1", "name")
	assertNoErr(t, err)
	assertEqual(t, val, "alice")
}

func TestHash_HDel(t *testing.T) {
	db := newDB()
	db.HSet("h", "f1", "v1", "f2", "v2")
	n, err := db.HDel("h", "f1", "f3")
	assertNoErr(t, err)
	assertEqual(t, n, 1)

	_, err = db.HGet("h", "f1")
	if err != engine.ErrMemberNotFound {
		t.Fatalf("expected ErrMemberNotFound, got %v", err)
	}
}

func TestHash_HGetAll(t *testing.T) {
	db := newDB()
	db.HSet("h", "a", "1", "b", "2")
	all, err := db.HGetAll("h")
	assertNoErr(t, err)
	assertEqual(t, len(all), 4) // 2 field-value pairs
}

func TestHash_HIncrBy(t *testing.T) {
	db := newDB()
	db.HSet("h", "count", "10")
	n, err := db.HIncrBy("h", "count", 5)
	assertNoErr(t, err)
	assertEqual(t, n, int64(15))
}

// ---- List 测试 ----

func TestList_PushPop(t *testing.T) {
	db := newDB()
	db.RPush("q", "a", "b", "c")

	val, err := db.LPop("q")
	assertNoErr(t, err)
	assertEqual(t, val, "a")

	val, err = db.RPop("q")
	assertNoErr(t, err)
	assertEqual(t, val, "c")

	n, err := db.LLen("q")
	assertNoErr(t, err)
	assertEqual(t, n, 1)
}

func TestList_LRange(t *testing.T) {
	db := newDB()
	db.RPush("list", "a", "b", "c", "d", "e")

	items, err := db.LRange("list", 0, -1)
	assertNoErr(t, err)
	assertEqual(t, len(items), 5)
	assertEqual(t, items[0], "a")
	assertEqual(t, items[4], "e")

	items, err = db.LRange("list", 1, 3)
	assertNoErr(t, err)
	assertEqual(t, items, []string{"b", "c", "d"})
}

func TestList_LIndex(t *testing.T) {
	db := newDB()
	db.RPush("list", "x", "y", "z")
	val, err := db.LIndex("list", 1)
	assertNoErr(t, err)
	assertEqual(t, val, "y")

	val, err = db.LIndex("list", -1)
	assertNoErr(t, err)
	assertEqual(t, val, "z")
}

func TestList_LPush(t *testing.T) {
	db := newDB()
	db.LPush("list", "c", "b", "a") // LPush 每次插入头部
	items, _ := db.LRange("list", 0, -1)
	// LPush("c"), LPush("b"), LPush("a") => [a, b, c]
	assertEqual(t, items[0], "a")
	assertEqual(t, items[2], "c")
}

// ---- Set 测试 ----

func TestSet_AddMembers(t *testing.T) {
	db := newDB()
	n, err := db.SAdd("tags", "go", "redis", "db")
	assertNoErr(t, err)
	assertEqual(t, n, 3)

	ok, err := db.SIsMember("tags", "go")
	assertNoErr(t, err)
	assertEqual(t, ok, true)

	ok, err = db.SIsMember("tags", "python")
	assertNoErr(t, err)
	assertEqual(t, ok, false)
}

func TestSet_SRem(t *testing.T) {
	db := newDB()
	db.SAdd("s", "a", "b", "c")
	n, err := db.SRem("s", "b", "x")
	assertNoErr(t, err)
	assertEqual(t, n, 1)

	n, _ = db.SCard("s")
	assertEqual(t, n, 2)
}

func TestSet_SetOperations(t *testing.T) {
	db := newDB()
	db.SAdd("s1", "a", "b", "c")
	db.SAdd("s2", "b", "c", "d")

	inter, err := db.SInter("s1", "s2")
	assertNoErr(t, err)
	assertEqual(t, len(inter), 2) // b, c

	union, err := db.SUnion("s1", "s2")
	assertNoErr(t, err)
	assertEqual(t, len(union), 4) // a, b, c, d

	diff, err := db.SDiff("s1", "s2")
	assertNoErr(t, err)
	assertEqual(t, len(diff), 1) // a
}

// ---- ZSet 测试 ----

func TestZSet_AddAndScore(t *testing.T) {
	db := newDB()
	n, err := db.ZAdd("ranking", 100, "alice")
	assertNoErr(t, err)
	assertEqual(t, n, 1)

	db.ZAdd("ranking", 200, "bob")
	db.ZAdd("ranking", 150, "carol")

	score, ok, err := db.ZScore("ranking", "bob")
	assertNoErr(t, err)
	assertEqual(t, ok, true)
	assertEqual(t, score, float64(200))
}

func TestZSet_ZRange(t *testing.T) {
	db := newDB()
	db.ZAdd("z", 3, "c")
	db.ZAdd("z", 1, "a")
	db.ZAdd("z", 2, "b")

	items, err := db.ZRange("z", 0, -1, false)
	assertNoErr(t, err)
	assertEqual(t, items, []string{"a", "b", "c"})
}

func TestZSet_ZRangeByScore(t *testing.T) {
	db := newDB()
	db.ZAdd("z", 10, "x")
	db.ZAdd("z", 20, "y")
	db.ZAdd("z", 30, "z")

	items, err := db.ZRangeByScore("z", 15, 35, false)
	assertNoErr(t, err)
	assertEqual(t, items, []string{"y", "z"})
}

func TestZSet_ZRank(t *testing.T) {
	db := newDB()
	db.ZAdd("z", 1, "a")
	db.ZAdd("z", 2, "b")
	db.ZAdd("z", 3, "c")

	rank, err := db.ZRank("z", "b")
	assertNoErr(t, err)
	assertEqual(t, rank, 1) // 0-based
}

func TestZSet_ZIncrBy(t *testing.T) {
	db := newDB()
	db.ZAdd("z", 10, "member")
	newScore, err := db.ZIncrBy("z", 5, "member")
	assertNoErr(t, err)
	assertEqual(t, newScore, float64(15))
}

func TestZSet_ZRem(t *testing.T) {
	db := newDB()
	db.ZAdd("z", 1, "a")
	db.ZAdd("z", 2, "b")
	n, err := db.ZRem("z", "a", "c")
	assertNoErr(t, err)
	assertEqual(t, n, 1)
}

// ---- 通用命令测试 ----

func TestCommon_TypeCheck(t *testing.T) {
	db := newDB()
	db.Set("s", "v")
	db.HSet("h", "f", "v")
	db.RPush("l", "v")
	db.SAdd("set", "v")
	db.ZAdd("z", 1, "v")

	assertEqual(t, db.Type("s"), "string")
	assertEqual(t, db.Type("h"), "hash")
	assertEqual(t, db.Type("l"), "list")
	assertEqual(t, db.Type("set"), "set")
	assertEqual(t, db.Type("z"), "zset")
	assertEqual(t, db.Type("none"), "none")
}

func TestCommon_WrongType(t *testing.T) {
	db := newDB()
	db.RPush("list", "item")
	_, err := db.Get("list")
	if err != engine.ErrWrongType {
		t.Fatalf("expected ErrWrongType, got %v", err)
	}
}

func TestCommon_DBSizeFlush(t *testing.T) {
	db := newDB()
	db.Set("a", "1")
	db.Set("b", "2")
	db.Set("c", "3")
	assertEqual(t, db.DBSize(), 3)
	db.FlushDB()
	assertEqual(t, db.DBSize(), 0)
}

// ---- 并发测试 ----

func TestConcurrent_StringReadWrite(t *testing.T) {
	db := newDB()
	var wg sync.WaitGroup
	const goroutines = 100
	const ops = 100

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key:%d", id%10)
			for j := 0; j < ops; j++ {
				db.Set(key, fmt.Sprintf("value:%d:%d", id, j))
				db.Get(key)
			}
		}(i)
	}
	wg.Wait()
}

func TestConcurrent_HashReadWrite(t *testing.T) {
	db := newDB()
	var wg sync.WaitGroup
	const goroutines = 50

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := "shared:hash"
			field := fmt.Sprintf("field:%d", id)
			db.HSet(key, field, fmt.Sprintf("val:%d", id))
			db.HGet(key, field)
			db.HIncrBy(key, "counter", 1)
		}(i)
	}
	wg.Wait()

	n, _ := db.HIncrBy("shared:hash", "counter", 0) // 读取最终值
	t.Logf("counter after %d concurrent increments: %d", goroutines, n)
}

func TestConcurrent_ZSetOperations(t *testing.T) {
	db := newDB()
	var wg sync.WaitGroup
	const goroutines = 50

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			member := fmt.Sprintf("member:%d", id)
			db.ZAdd("leaderboard", float64(id*10), member)
			db.ZScore("leaderboard", member)
			db.ZRank("leaderboard", member)
		}(i)
	}
	wg.Wait()

	n, _ := db.ZCard("leaderboard")
	assertEqual(t, n, goroutines)
}

// ---- 基准测试 ----

func BenchmarkString_Set(b *testing.B) {
	db := newDB()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			db.Set(fmt.Sprintf("key:%d", i%1000), "value")
			i++
		}
	})
}

func BenchmarkString_Get(b *testing.B) {
	db := newDB()
	for i := 0; i < 1000; i++ {
		db.Set(fmt.Sprintf("key:%d", i), fmt.Sprintf("value:%d", i))
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			db.Get(fmt.Sprintf("key:%d", i%1000))
			i++
		}
	})
}

func BenchmarkHash_HSet(b *testing.B) {
	db := newDB()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			db.HSet("hash", fmt.Sprintf("field:%d", i%100), "value")
			i++
		}
	})
}

func BenchmarkZSet_ZAdd(b *testing.B) {
	db := newDB()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			db.ZAdd("zset", float64(i), fmt.Sprintf("member:%d", i%1000))
			i++
		}
	})
}
