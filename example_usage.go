// Package example 展示 go-redis 各版本的使用方式
package example

import (
	"fmt"
	"log"

	"github.com/jiujuan/go-redis/internal/engine"
)

// ExampleV01 演示 v0.1 单机内存模式的使用
func ExampleV01() {
	// 创建实例（默认 256 个分片）
	db := engine.NewGoRedis()

	// ---- String ----
	db.Set("name", "tom")
	val, _ := db.Get("name")
	fmt.Println("name:", val) // name: tom

	db.Set("counter", "0")
	n, _ := db.Incr("counter")
	fmt.Println("counter:", n) // counter: 1

	// ---- Hash ----
	db.HSet("user:1", "name", "alice", "age", "30", "city", "beijing")
	name, _ := db.HGet("user:1", "name")
	fmt.Println("user:1 name:", name) // user:1 name: alice

	all, _ := db.HGetAll("user:1")
	fmt.Printf("user:1 fields: %d\n", len(all)/2) // user:1 fields: 3

	// ---- List ----
	db.RPush("queue", "task1", "task2", "task3")
	task, _ := db.LPop("queue")
	fmt.Println("dequeued:", task) // dequeued: task1

	items, _ := db.LRange("queue", 0, -1)
	fmt.Println("queue remaining:", items) // queue remaining: [task2 task3]

	// ---- Set ----
	db.SAdd("tags", "go", "redis", "nosql", "cache")
	ok, _ := db.SIsMember("tags", "go")
	fmt.Println("has go tag:", ok) // has go tag: true

	db.SAdd("tags2", "go", "python", "database")
	inter, _ := db.SInter("tags", "tags2")
	fmt.Println("intersection:", inter) // intersection: [go]

	// ---- ZSet（跳表实现）----
	db.ZAdd("ranking", 100, "alice")
	db.ZAdd("ranking", 200, "bob")
	db.ZAdd("ranking", 150, "carol")
	db.ZAdd("ranking", 50, "dave")

	// 按 score 升序返回所有成员
	members, _ := db.ZRange("ranking", 0, -1, true)
	fmt.Println("ranking (with scores):", members)
	// ranking (with scores): [dave 50 alice 100 carol 150 bob 200]

	// 按 score 范围查询
	top, _ := db.ZRangeByScore("ranking", 100, 200, false)
	fmt.Println("score 100-200:", top) // score 100-200: [alice carol bob]

	// 查询排名
	rank, _ := db.ZRank("ranking", "carol")
	fmt.Println("carol rank:", rank) // carol rank: 2

	// ---- 通用操作 ----
	fmt.Println("keys count:", db.DBSize())
	fmt.Println("type of 'name':", db.Type("name"))
	fmt.Println("type of 'ranking':", db.Type("ranking"))

	db.Rename("name", "username")
	username, _ := db.Get("username")
	fmt.Println("username:", username) // username: tom
}

// ExampleV01Custom 演示自定义配置
func ExampleV01Custom() {
	// 使用自定义分片数（必须是 2 的幂次）
	db := engine.NewGoRedis(engine.WithShardCount(512))
	db.Set("key", "value")
	val, err := db.Get("key")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(val)
}
