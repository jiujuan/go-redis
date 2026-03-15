// go-redis server 入口（v0.2+）
//
// 用法：
//
//	go run cmd/server/main.go [--port 6379] [--aof yes] [--rdb yes]
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jiujuan/go-redis/config"
	"github.com/jiujuan/go-redis/internal/engine"
	"github.com/jiujuan/go-redis/internal/server"
)

func main() {
	cfg := config.ParseFlags()

	log.Printf("[go-redis] starting server v0.2")
	log.Printf("[go-redis] config: addr=%s shards=%d aof=%v rdb=%v",
		cfg.Addr(), cfg.ShardCount, cfg.AOFEnabled, cfg.RDBEnabled)

	// 初始化存储引擎
	db := engine.NewGoRedis(engine.WithShardCount(cfg.ShardCount))

	// 启动 TCP 服务端
	srv := server.NewServer(cfg, db)

	ctx, cancel := context.WithCancel(context.Background())

	// 信号处理
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("[go-redis] received signal: %v", sig)
		cancel()
	}()

	if err := srv.StartWithContext(ctx); err != nil {
		log.Printf("[go-redis] server error: %v", err)
		os.Exit(1)
	}
}
