package main

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jiujuan/go-redis/config"
	"github.com/jiujuan/go-redis/internal/engine"
	"github.com/jiujuan/go-redis/internal/resp"
	"github.com/jiujuan/go-redis/internal/server"
)

func freeTCPPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func waitForServer(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("server at %s did not start in time", addr)
}

func doRESP(t *testing.T, addr string, args ...string) *resp.Value {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	wr := resp.NewWriter(conn)
	if err := wr.WriteArrayHeader(len(args)); err != nil {
		t.Fatal(err)
	}
	for _, a := range args {
		if err := wr.WriteBulkString(a); err != nil {
			t.Fatal(err)
		}
	}
	if err := wr.Flush(); err != nil {
		t.Fatal(err)
	}
	r := resp.NewReader(conn)
	v, err := r.Read()
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func startIntegrationServer(t *testing.T, shardCount int) (string, context.CancelFunc, <-chan error) {
	t.Helper()
	port := freeTCPPort(t)
	cfg := config.DefaultConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = port
	cfg.ShardCount = shardCount

	db := engine.NewGoRedis(engine.WithShardCount(cfg.ShardCount))
	srv := server.NewServer(cfg, db)

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.StartWithContext(ctx)
	}()

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	waitForServer(t, addr)
	return addr, cancel, errCh
}

func shutdownIntegrationServer(t *testing.T, cancel context.CancelFunc, errCh <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("server returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server did not shut down")
	}
}

func assertSimpleString(t *testing.T, v *resp.Value, want string) {
	t.Helper()
	if v.Type != '+' || v.Str != want {
		t.Fatalf("response = %+v, want simple string %q", v, want)
	}
}

func assertInteger(t *testing.T, v *resp.Value, want int64) {
	t.Helper()
	if v.Type != ':' || v.Integer != want {
		t.Fatalf("response = %+v, want integer %d", v, want)
	}
}

func assertBulkString(t *testing.T, v *resp.Value, want string) {
	t.Helper()
	if v.Type != '$' || v.Str != want || v.IsNil {
		t.Fatalf("response = %+v, want bulk string %q", v, want)
	}
}

func assertNilBulk(t *testing.T, v *resp.Value) {
	t.Helper()
	if v.Type != '$' || !v.IsNil {
		t.Fatalf("response = %+v, want nil bulk string", v)
	}
}

func assertArrayStrings(t *testing.T, v *resp.Value, want []string) {
	t.Helper()
	if v.Type != '*' || len(v.Array) != len(want) {
		t.Fatalf("response = %+v, want array len %d", v, len(want))
	}
	got := make([]string, len(v.Array))
	for i, item := range v.Array {
		got[i] = item.Str
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("array[%d] = %q, want %q (full=%v)", i, got[i], want[i], got)
		}
	}
}

func assertArrayUnordered(t *testing.T, v *resp.Value, want []string) {
	t.Helper()
	if v.Type != '*' || len(v.Array) != len(want) {
		t.Fatalf("response = %+v, want array len %d", v, len(want))
	}
	got := make([]string, len(v.Array))
	for i, item := range v.Array {
		got[i] = item.Str
	}
	sort.Strings(got)
	expected := append([]string(nil), want...)
	sort.Strings(expected)
	for i := range expected {
		if got[i] != expected[i] {
			t.Fatalf("unordered array[%d] = %q, want %q (full=%v)", i, got[i], expected[i], got)
		}
	}
}

func assertErrorContains(t *testing.T, v *resp.Value, want string) {
	t.Helper()
	if v.Type != '-' || !strings.Contains(strings.ToLower(v.Str), strings.ToLower(want)) {
		t.Fatalf("response = %+v, want error containing %q", v, want)
	}
}

func TestServerIntegration_BasicCommands(t *testing.T) {
	addr, cancel, errCh := startIntegrationServer(t, 64)
	defer shutdownIntegrationServer(t, cancel, errCh)

	assertSimpleString(t, doRESP(t, addr, "PING"), "PONG")
	assertSimpleString(t, doRESP(t, addr, "SET", "k", "v"), "OK")
	assertBulkString(t, doRESP(t, addr, "GET", "k"), "v")
	assertSimpleString(t, doRESP(t, addr, "MSET", "a", "1", "b", "2"), "OK")
	assertArrayStrings(t, doRESP(t, addr, "MGET", "a", "b"), []string{"1", "2"})
}

func TestServerIntegration_HashCommands_Detailed(t *testing.T) {
	addr, cancel, errCh := startIntegrationServer(t, 32)
	defer shutdownIntegrationServer(t, cancel, errCh)

	assertInteger(t, doRESP(t, addr, "HSET", "h", "f1", "v1", "f2", "v2"), 2)
	assertBulkString(t, doRESP(t, addr, "HGET", "h", "f1"), "v1")
	assertNilBulk(t, doRESP(t, addr, "HGET", "h", "missing"))
	assertArrayStrings(t, doRESP(t, addr, "HGETALL", "h"), []string{"f1", "v1", "f2", "v2"})
	assertInteger(t, doRESP(t, addr, "HEXISTS", "h", "f1"), 1)
	assertInteger(t, doRESP(t, addr, "HEXISTS", "h", "missing"), 0)
	assertInteger(t, doRESP(t, addr, "HLEN", "h"), 2)
	assertInteger(t, doRESP(t, addr, "HDEL", "h", "f1"), 1)
	assertInteger(t, doRESP(t, addr, "HDEL", "h", "f1"), 0)
	assertInteger(t, doRESP(t, addr, "HMSET", "hm", "a", "1", "b", "2"), 2)
	assertArrayStrings(t, doRESP(t, addr, "HMGET", "hm", "a", "b", "none"), []string{"1", "2", ""})
	assertInteger(t, doRESP(t, addr, "HSETNX", "hm", "a", "9"), 0)
	assertInteger(t, doRESP(t, addr, "HSETNX", "hm", "c", "3"), 1)
	assertInteger(t, doRESP(t, addr, "HINCRBY", "hm", "score", "5"), 5)
	assertErrorContains(t, doRESP(t, addr, "HSET", "strkey"), "wrong number")
}

func TestServerIntegration_ListCommands_Detailed(t *testing.T) {
	addr, cancel, errCh := startIntegrationServer(t, 16)
	defer shutdownIntegrationServer(t, cancel, errCh)

	assertInteger(t, doRESP(t, addr, "RPUSH", "l", "a", "b", "c"), 3)
	assertInteger(t, doRESP(t, addr, "LPUSH", "l", "z"), 4)
	assertInteger(t, doRESP(t, addr, "LLEN", "l"), 4)
	assertArrayStrings(t, doRESP(t, addr, "LRANGE", "l", "0", "-1"), []string{"z", "a", "b", "c"})
	assertArrayStrings(t, doRESP(t, addr, "LRANGE", "l", "-2", "-1"), []string{"b", "c"})
	assertArrayStrings(t, doRESP(t, addr, "LRANGE", "l", "5", "8"), []string{})
	assertBulkString(t, doRESP(t, addr, "LINDEX", "l", "1"), "a")
	assertNilBulk(t, doRESP(t, addr, "LINDEX", "l", "9"))
	assertSimpleString(t, doRESP(t, addr, "LSET", "l", "0", "head"), "OK")
	assertErrorContains(t, doRESP(t, addr, "LSET", "l", "20", "x"), "range")
	assertBulkString(t, doRESP(t, addr, "LPOP", "l"), "head")
	assertBulkString(t, doRESP(t, addr, "RPOP", "l"), "c")
	assertInteger(t, doRESP(t, addr, "RPUSH", "l2", "x", "x", "y", "x"), 4)
	assertInteger(t, doRESP(t, addr, "LREM", "l2", "2", "x"), 2)
	assertInteger(t, doRESP(t, addr, "LPUSHX", "missing-list", "v"), 0)
	assertInteger(t, doRESP(t, addr, "RPUSHX", "missing-list", "v"), 0)
	assertErrorContains(t, doRESP(t, addr, "LREM", "l2", "bad", "x"), "integer")
}

func TestServerIntegration_SetAndShardBehavior_Detailed(t *testing.T) {
	addr, cancel, errCh := startIntegrationServer(t, 8)
	defer shutdownIntegrationServer(t, cancel, errCh)

	assertInteger(t, doRESP(t, addr, "SADD", "s1", "a", "b", "c"), 3)
	assertInteger(t, doRESP(t, addr, "SADD", "s2", "b", "c", "d"), 3)
	assertInteger(t, doRESP(t, addr, "SCARD", "s1"), 3)
	assertInteger(t, doRESP(t, addr, "SISMEMBER", "s1", "a"), 1)
	assertInteger(t, doRESP(t, addr, "SISMEMBER", "s1", "z"), 0)
	assertInteger(t, doRESP(t, addr, "SREM", "s1", "a"), 1)
	assertArrayUnordered(t, doRESP(t, addr, "SINTER", "s1", "s2"), []string{"b", "c"})
	assertArrayUnordered(t, doRESP(t, addr, "SUNION", "s1", "s2"), []string{"b", "c", "d"})
	assertArrayUnordered(t, doRESP(t, addr, "SDIFF", "s2", "s1"), []string{"d"})
	assertInteger(t, doRESP(t, addr, "SMOVE", "s2", "s1", "d"), 1)
	assertInteger(t, doRESP(t, addr, "DBSIZE"), 2)

	// Cross-shard behavior: many distinct keys should still be reachable and counted correctly.
	for i := 0; i < 20; i++ {
		assertSimpleString(t, doRESP(t, addr, "SET", fmt.Sprintf("shard:key:%d", i), fmt.Sprintf("v%d", i)), "OK")
	}
	assertInteger(t, doRESP(t, addr, "DBSIZE"), 22)
	for i := 0; i < 20; i++ {
		assertBulkString(t, doRESP(t, addr, "GET", fmt.Sprintf("shard:key:%d", i)), fmt.Sprintf("v%d", i))
	}
}

func TestServerIntegration_ZSetSkiplistCommands_Detailed(t *testing.T) {
	addr, cancel, errCh := startIntegrationServer(t, 32)
	defer shutdownIntegrationServer(t, cancel, errCh)

	assertInteger(t, doRESP(t, addr, "ZADD", "z", "10", "alice"), 1)
	assertInteger(t, doRESP(t, addr, "ZADD", "z", "20", "bob"), 1)
	assertInteger(t, doRESP(t, addr, "ZADD", "z", "5", "charlie"), 1)
	assertInteger(t, doRESP(t, addr, "ZCARD", "z"), 3)
	assertBulkString(t, doRESP(t, addr, "ZSCORE", "z", "alice"), "10")
	assertNilBulk(t, doRESP(t, addr, "ZSCORE", "z", "missing"))
	assertInteger(t, doRESP(t, addr, "ZRANK", "z", "charlie"), 0)
	assertInteger(t, doRESP(t, addr, "ZRANK", "z", "alice"), 1)
	assertNilBulk(t, doRESP(t, addr, "ZRANK", "z", "missing"))
	assertArrayStrings(t, doRESP(t, addr, "ZRANGE", "z", "0", "-1"), []string{"charlie", "alice", "bob"})
	assertArrayStrings(t, doRESP(t, addr, "ZRANGE", "z", "0", "-1", "WITHSCORES"), []string{"charlie", "5", "alice", "10", "bob", "20"})
	assertArrayStrings(t, doRESP(t, addr, "ZREVRANGE", "z", "0", "1"), []string{"alice", "charlie"})
	assertArrayStrings(t, doRESP(t, addr, "ZRANGEBYSCORE", "z", "8", "20"), []string{"alice", "bob"})
	assertBulkString(t, doRESP(t, addr, "ZINCRBY", "z", "5", "alice"), "15")
	assertInteger(t, doRESP(t, addr, "ZCOUNT", "z", "5", "20"), 3)
	assertInteger(t, doRESP(t, addr, "ZREM", "z", "bob"), 1)
	assertArrayStrings(t, doRESP(t, addr, "ZRANGE", "z", "0", "-1"), []string{"charlie", "alice"})

	// Same-score ordering validates skiplist member tie-break behavior.
	assertInteger(t, doRESP(t, addr, "ZADD", "z2", "1", "charlie"), 1)
	assertInteger(t, doRESP(t, addr, "ZADD", "z2", "1", "alice"), 1)
	assertInteger(t, doRESP(t, addr, "ZADD", "z2", "1", "bob"), 1)
	assertArrayStrings(t, doRESP(t, addr, "ZRANGE", "z2", "0", "-1"), []string{"alice", "bob", "charlie"})
}

func TestServerIntegration_WrongTypeAndArgumentErrors(t *testing.T) {
	addr, cancel, errCh := startIntegrationServer(t, 4)
	defer shutdownIntegrationServer(t, cancel, errCh)

	assertSimpleString(t, doRESP(t, addr, "SET", "s", "string"), "OK")
	assertErrorContains(t, doRESP(t, addr, "HSET", "s", "f", "v"), "wrongtype")
	assertErrorContains(t, doRESP(t, addr, "LRANGE", "s", "0", "-1"), "wrongtype")
	assertErrorContains(t, doRESP(t, addr, "ZADD", "s", "1", "m"), "wrongtype")
	assertErrorContains(t, doRESP(t, addr, "MSET", "a", "1", "b"), "wrong number")
	assertErrorContains(t, doRESP(t, addr, "NOTACOMMAND"), "unknown")
}
