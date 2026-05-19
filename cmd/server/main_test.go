package main

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/jiujuan/go-redis/config"
	"github.com/jiujuan/go-redis/internal/engine"
	"github.com/jiujuan/go-redis/internal/server"
)

func TestRunWithContext_PassesShardCount(t *testing.T) {
	origNewGoRedis := newGoRedis
	origNewServer := newServer
	origStart := startServerWithContext
	t.Cleanup(func() {
		newGoRedis = origNewGoRedis
		newServer = origNewServer
		startServerWithContext = origStart
	})

	var gotShardCount int
	newGoRedis = func(opts ...engine.Option) *engine.GoRedis {
		db := engine.NewGoRedis(opts...)
		gotShardCount = int(reflect.ValueOf(db).Elem().FieldByName("shardCount").Int())
		return db
	}
	newServer = func(cfg *config.Config, db *engine.GoRedis) *server.Server {
		return server.NewServer(cfg, db)
	}
	startServerWithContext = func(_ *server.Server, _ context.Context) error {
		return nil
	}

	cfg := config.DefaultConfig()
	cfg.ShardCount = 64
	if err := runWithContext(context.Background(), cfg); err != nil {
		t.Fatalf("runWithContext: %v", err)
	}
	if gotShardCount != 64 {
		t.Fatalf("shardCount = %d, want 64", gotShardCount)
	}
}

func TestRunWithContext_PropagatesStartError(t *testing.T) {
	origNewGoRedis := newGoRedis
	origNewServer := newServer
	origStart := startServerWithContext
	t.Cleanup(func() {
		newGoRedis = origNewGoRedis
		newServer = origNewServer
		startServerWithContext = origStart
	})

	newGoRedis = func(opts ...engine.Option) *engine.GoRedis {
		return engine.NewGoRedis(opts...)
	}
	newServer = func(cfg *config.Config, db *engine.GoRedis) *server.Server {
		return server.NewServer(cfg, db)
	}
	startServerWithContext = func(_ *server.Server, _ context.Context) error {
		return errors.New("boom")
	}

	err := runWithContext(context.Background(), config.DefaultConfig())
	if err == nil || err.Error() != "boom" {
		t.Fatalf("runWithContext error = %v, want boom", err)
	}
}
