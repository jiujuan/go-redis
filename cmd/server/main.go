// go-redis server entrypoint.
//
// Usage:
//
//	go run cmd/server/main.go [--port 6379] [--aof=true] [--rdb=true]
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

var parseFlags = config.ParseFlags

var newGoRedis = engine.NewGoRedis

var newServer = server.NewServer

var startServerWithContext = func(srv *server.Server, ctx context.Context) error {
	return srv.StartWithContext(ctx)
}

func runWithContext(ctx context.Context, cfg *config.Config) error {
	db := newGoRedis(engine.WithShardCount(cfg.ShardCount))
	srv := newServer(cfg, db)
	return startServerWithContext(srv, ctx)
}

func main() {
	cfg := parseFlags()

	log.Printf("[go-redis] starting server v0.2")
	log.Printf("[go-redis] config: addr=%s shards=%d aof=%v rdb=%v",
		cfg.Addr(), cfg.ShardCount, cfg.AOFEnabled, cfg.RDBEnabled)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	go func() {
		sig := <-sigCh
		log.Printf("[go-redis] received signal: %v", sig)
		cancel()
	}()

	if err := runWithContext(ctx, cfg); err != nil {
		log.Printf("[go-redis] server error: %v", err)
		os.Exit(1)
	}
}
