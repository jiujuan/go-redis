// v0.4 渐进式数据迁移使用示例
package example

import (
	"fmt"
	"log"
	"time"

	"github.com/jiujuan/go-redis/pkg/client"
	"github.com/jiujuan/go-redis/pkg/migration"
)

// ExampleV04_ScaleOut 演示扩容时的自动数据迁移
//
// 场景：现有 2 个节点，加入 1 个新节点，触发自动迁移
// 整个迁移过程对业务读写完全透明
func ExampleV04_ScaleOut() {
	// ---- 初始化集群（2个节点）----
	nodes := []string{"127.0.0.1:6379", "127.0.0.1:6380"}
	c := client.NewClusterClient(
		nodes,
		client.WithMigrationConfig(&migration.MigrationConfig{
			BatchSize:           200,  // 每批迁移 200 个 key
			Concurrency:         4,    // 4 个并发迁移 goroutine
			RetryLimit:          3,    // 失败重试 3 次
			BatchInterval:       5 * time.Millisecond, // 批次间隔 5ms，降低影响
			ReadFallbackTimeout: 500 * time.Millisecond,
		}),
	)
	defer c.Close()

	// ---- 预写入数据（扩容前）----
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("user:%d", i)
		c.Set(key, fmt.Sprintf("data:%d", i))
	}
	fmt.Println("pre-loaded 1000 keys")

	// ---- 扩容：加入新节点，触发自动迁移 ----
	// 注意：此调用立即返回，迁移在后台进行
	taskID, err := c.AddNode("127.0.0.1:6381")
	if err != nil {
		log.Printf("add node: %v", err)
		return
	}
	fmt.Printf("migration task started: %s\n", taskID)

	// ---- 迁移期间正常读写（完全透明）----
	go func() {
		for i := 0; i < 100; i++ {
			// 写操作：直接写新环目标节点
			c.Set(fmt.Sprintf("live:%d", i), "live-value")

			// 读操作：新环 miss 时自动兜底旧环
			val, _ := c.Get(fmt.Sprintf("user:%d", i%1000))
			_ = val

			time.Sleep(10 * time.Millisecond)
		}
		fmt.Println("concurrent read/write done")
	}()

	// ---- 监控迁移进度 ----
	ticker := time.NewTicker(500 * time.Millisecond)
	go func() {
		for range ticker.C {
			p := c.MigrationProgress()
			if p == nil {
				return
			}
			fmt.Printf("migration: state=%s progress=%.1f%%\n", p.State, p.Percent)
			if p.State == migration.StateDone {
				return
			}
		}
	}()

	// ---- 等待迁移完成 ----
	c.WaitMigration()
	ticker.Stop()

	fmt.Println("migration complete!")
	fmt.Printf("is migrating: %v\n", c.IsMigrating()) // false

	// ---- 验证：所有 key 仍然可读 ----
	missing := 0
	for i := 0; i < 1000; i++ {
		val, err := c.Get(fmt.Sprintf("user:%d", i))
		if err != nil || val == "" {
			missing++
		}
	}
	fmt.Printf("missing keys after migration: %d\n", missing) // 0
}

// ExampleV04_CancelMigration 演示取消迁移并回滚
func ExampleV04_CancelMigration() {
	nodes := []string{"127.0.0.1:6379", "127.0.0.1:6380"}
	c := client.NewClusterClient(nodes)
	defer c.Close()

	taskID, err := c.AddNode("127.0.0.1:6381")
	if err != nil {
		return
	}
	fmt.Printf("started: %s\n", taskID)

	// 迁移进行中，决定回滚（例如新节点出现问题）
	time.Sleep(100 * time.Millisecond)
	c.CancelMigration()
	fmt.Println("migration cancelled, ring rolled back to original state")
	fmt.Printf("is migrating: %v\n", c.IsMigrating()) // false
}

// ExampleV04_MultiNodeScaleOut 演示一次性加入多个新节点
func ExampleV04_MultiNodeScaleOut() {
	nodes := []string{"127.0.0.1:6379"}
	c := client.NewClusterClient(nodes)
	defer c.Close()

	// 一次性扩容到 3 个节点
	taskID, err := c.AddNodes([]string{"127.0.0.1:6380", "127.0.0.1:6381"})
	if err != nil {
		log.Printf("add nodes: %v", err)
		return
	}

	fmt.Printf("expanding cluster, task: %s\n", taskID)
	fmt.Printf("cluster nodes: %v\n", c.Nodes())

	c.WaitMigration()
	fmt.Println("cluster expanded successfully")
}
