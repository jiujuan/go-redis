package server_test

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jiujuan/go-redis/config"
	"github.com/jiujuan/go-redis/internal/engine"
	"github.com/jiujuan/go-redis/internal/server"
)

// ─────────────────────────────────────────────
//  Test harness
// ─────────────────────────────────────────────

type testServer struct {
	addr string
	srv  *server.Server
}

func startTestServer(t *testing.T) *testServer {
	t.Helper()
	port := freePort(t)
	cfg := config.DefaultConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = port

	db := engine.NewGoRedis()
	srv := server.NewServer(cfg, db)

	go srv.Start()

	// Wait until server is ready
	addr := cfg.Addr()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Cleanup(func() { srv.Stop() })
	return &testServer{addr: addr, srv: srv}
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

// client wraps a TCP connection with simple RESP helpers
type client struct {
	conn   net.Conn
	reader *bufio.Reader
}

func newClient(t *testing.T, addr string) *client {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	return &client{conn: conn, reader: bufio.NewReader(conn)}
}

func (c *client) close() { c.conn.Close() }

func (c *client) send(args ...string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(a), a)
	}
	c.conn.Write([]byte(sb.String()))
	return c.readOne()
}

func (c *client) readOne() string {
	c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	defer c.conn.SetReadDeadline(time.Time{})

	line, err := c.reader.ReadString('\n')
	if err != nil {
		return ""
	}
	line = strings.TrimRight(line, "\r\n")

	switch {
	case len(line) == 0:
		return ""
	case line[0] == '+' || line[0] == '-' || line[0] == ':':
		return line
	case line[0] == '$':
		var n int
		fmt.Sscanf(line[1:], "%d", &n)
		if n < 0 {
			return line
		}
		buf := make([]byte, n+2)
		c.reader.Read(buf)
		return line + "\r\n" + strings.TrimRight(string(buf), "\r\n")
	case line[0] == '*':
		var count int
		fmt.Sscanf(line[1:], "%d", &count)
		if count < 0 {
			return line
		}
		parts := []string{line}
		for i := 0; i < count; i++ {
			parts = append(parts, c.readOne())
		}
		return strings.Join(parts, "\n")
	}
	return line
}

// ─────────────────────────────────────────────
//  Server lifecycle
// ─────────────────────────────────────────────

func TestServer_StartStop(t *testing.T) {
	ts := startTestServer(t)
	conn, err := net.Dial("tcp", ts.addr)
	if err != nil {
		t.Fatalf("connect after start: %v", err)
	}
	conn.Close()
}

func TestServer_PING(t *testing.T) {
	ts := startTestServer(t)
	c := newClient(t, ts.addr)
	defer c.close()
	r := c.send("PING")
	if !strings.Contains(r, "PONG") {
		t.Errorf("PING: got %q", r)
	}
}

func TestServer_PING_WithMessage(t *testing.T) {
	ts := startTestServer(t)
	c := newClient(t, ts.addr)
	defer c.close()
	r := c.send("PING", "hello")
	if !strings.Contains(r, "hello") {
		t.Errorf("PING with message: got %q", r)
	}
}

func TestServer_ECHO(t *testing.T) {
	ts := startTestServer(t)
	c := newClient(t, ts.addr)
	defer c.close()
	r := c.send("ECHO", "world")
	if !strings.Contains(r, "world") {
		t.Errorf("ECHO: got %q", r)
	}
}

// ─────────────────────────────────────────────
//  String commands
// ─────────────────────────────────────────────

func TestServer_SET_GET(t *testing.T) {
	ts := startTestServer(t)
	c := newClient(t, ts.addr)
	defer c.close()

	r := c.send("SET", "k", "v")
	if !strings.HasPrefix(r, "+OK") {
		t.Errorf("SET: %q", r)
	}
	r = c.send("GET", "k")
	if !strings.Contains(r, "v") {
		t.Errorf("GET: %q", r)
	}
}

func TestServer_GET_Missing(t *testing.T) {
	ts := startTestServer(t)
	c := newClient(t, ts.addr)
	defer c.close()
	r := c.send("GET", "no_such_key_xyz")
	if !strings.HasPrefix(r, "$-1") {
		t.Errorf("GET missing: %q", r)
	}
}

func TestServer_SETNX(t *testing.T) {
	ts := startTestServer(t)
	c := newClient(t, ts.addr)
	defer c.close()

	r := c.send("SETNX", "nx", "first")
	if !strings.HasPrefix(r, ":1") {
		t.Errorf("SETNX new: %q", r)
	}
	r = c.send("SETNX", "nx", "second")
	if !strings.HasPrefix(r, ":0") {
		t.Errorf("SETNX existing: %q", r)
	}
}

func TestServer_GETSET(t *testing.T) {
	ts := startTestServer(t)
	c := newClient(t, ts.addr)
	defer c.close()

	c.send("SET", "gs", "old")
	r := c.send("GETSET", "gs", "new")
	if !strings.Contains(r, "old") {
		t.Errorf("GETSET old value: %q", r)
	}
	r = c.send("GET", "gs")
	if !strings.Contains(r, "new") {
		t.Errorf("GETSET new value: %q", r)
	}
}

func TestServer_INCR_INCRBY_DECR(t *testing.T) {
	ts := startTestServer(t)
	c := newClient(t, ts.addr)
	defer c.close()

	c.send("SET", "n", "10")
	r := c.send("INCR", "n")
	if !strings.HasPrefix(r, ":11") {
		t.Errorf("INCR: %q", r)
	}
	r = c.send("INCRBY", "n", "5")
	if !strings.HasPrefix(r, ":16") {
		t.Errorf("INCRBY: %q", r)
	}
	r = c.send("DECR", "n")
	if !strings.HasPrefix(r, ":15") {
		t.Errorf("DECR: %q", r)
	}
	r = c.send("DECRBY", "n", "5")
	if !strings.HasPrefix(r, ":10") {
		t.Errorf("DECRBY: %q", r)
	}
}

func TestServer_MSET_MGET(t *testing.T) {
	ts := startTestServer(t)
	c := newClient(t, ts.addr)
	defer c.close()

	r := c.send("MSET", "a", "1", "b", "2", "c", "3")
	if !strings.HasPrefix(r, "+OK") {
		t.Errorf("MSET: %q", r)
	}
	r = c.send("MGET", "a", "b", "c")
	if !strings.Contains(r, "1") || !strings.Contains(r, "2") || !strings.Contains(r, "3") {
		t.Errorf("MGET: %q", r)
	}
}

func TestServer_APPEND_STRLEN(t *testing.T) {
	ts := startTestServer(t)
	c := newClient(t, ts.addr)
	defer c.close()

	c.send("SET", "k", "Hello")
	r := c.send("APPEND", "k", " World")
	if !strings.HasPrefix(r, ":11") {
		t.Errorf("APPEND len: %q", r)
	}
	r = c.send("STRLEN", "k")
	if !strings.HasPrefix(r, ":11") {
		t.Errorf("STRLEN: %q", r)
	}
}

// ─────────────────────────────────────────────
//  Key commands
// ─────────────────────────────────────────────

func TestServer_DEL_EXISTS(t *testing.T) {
	ts := startTestServer(t)
	c := newClient(t, ts.addr)
	defer c.close()

	c.send("SET", "x", "1")
	c.send("SET", "y", "2")
	r := c.send("EXISTS", "x", "y", "z")
	if !strings.HasPrefix(r, ":2") {
		t.Errorf("EXISTS: %q", r)
	}
	r = c.send("DEL", "x", "y", "z")
	if !strings.HasPrefix(r, ":2") {
		t.Errorf("DEL: %q", r)
	}
}

func TestServer_TYPE(t *testing.T) {
	ts := startTestServer(t)
	c := newClient(t, ts.addr)
	defer c.close()

	c.send("SET", "s", "v")
	if r := c.send("TYPE", "s"); !strings.Contains(r, "string") {
		t.Errorf("TYPE string: %q", r)
	}
	c.send("HSET", "h", "f", "v")
	if r := c.send("TYPE", "h"); !strings.Contains(r, "hash") {
		t.Errorf("TYPE hash: %q", r)
	}
	c.send("RPUSH", "l", "v")
	if r := c.send("TYPE", "l"); !strings.Contains(r, "list") {
		t.Errorf("TYPE list: %q", r)
	}
	c.send("SADD", "st", "v")
	if r := c.send("TYPE", "st"); !strings.Contains(r, "set") {
		t.Errorf("TYPE set: %q", r)
	}
	c.send("ZADD", "z", "1", "v")
	if r := c.send("TYPE", "z"); !strings.Contains(r, "zset") {
		t.Errorf("TYPE zset: %q", r)
	}
}

func TestServer_RENAME(t *testing.T) {
	ts := startTestServer(t)
	c := newClient(t, ts.addr)
	defer c.close()

	c.send("SET", "old", "val")
	r := c.send("RENAME", "old", "new")
	if !strings.HasPrefix(r, "+OK") {
		t.Errorf("RENAME: %q", r)
	}
	r = c.send("GET", "new")
	if !strings.Contains(r, "val") {
		t.Errorf("GET renamed: %q", r)
	}
	r = c.send("EXISTS", "old")
	if !strings.HasPrefix(r, ":0") {
		t.Errorf("old key after rename: %q", r)
	}
}

func TestServer_FLUSHDB_DBSIZE(t *testing.T) {
	ts := startTestServer(t)
	c := newClient(t, ts.addr)
	defer c.close()

	c.send("SET", "a", "1")
	c.send("SET", "b", "2")
	r := c.send("DBSIZE")
	if !strings.HasPrefix(r, ":2") {
		t.Errorf("DBSIZE: %q", r)
	}
	c.send("FLUSHDB")
	r = c.send("DBSIZE")
	if !strings.HasPrefix(r, ":0") {
		t.Errorf("DBSIZE after FLUSHDB: %q", r)
	}
}

func TestServer_SELECT(t *testing.T) {
	ts := startTestServer(t)
	c := newClient(t, ts.addr)
	defer c.close()
	r := c.send("SELECT", "0")
	if !strings.HasPrefix(r, "+OK") {
		t.Errorf("SELECT: %q", r)
	}
}

func TestServer_KEYS(t *testing.T) {
	ts := startTestServer(t)
	c := newClient(t, ts.addr)
	defer c.close()

	c.send("FLUSHDB")
	c.send("SET", "user:1", "a")
	c.send("SET", "user:2", "b")
	r := c.send("KEYS", "user:*")
	if !strings.Contains(r, "user:") {
		t.Errorf("KEYS: %q", r)
	}
}

func TestServer_SCAN(t *testing.T) {
	ts := startTestServer(t)
	c := newClient(t, ts.addr)
	defer c.close()

	c.send("FLUSHDB")
	for i := 0; i < 5; i++ {
		c.send("SET", fmt.Sprintf("sk:%d", i), "v")
	}
	r := c.send("SCAN", "0", "COUNT", "10")
	if !strings.Contains(r, "sk:") {
		t.Errorf("SCAN: %q", r)
	}
}

// ─────────────────────────────────────────────
//  Hash commands
// ─────────────────────────────────────────────

func TestServer_Hash_Commands(t *testing.T) {
	ts := startTestServer(t)
	c := newClient(t, ts.addr)
	defer c.close()

	// HSET
	r := c.send("HSET", "h", "f1", "v1", "f2", "v2")
	if !strings.HasPrefix(r, ":2") {
		t.Errorf("HSET: %q", r)
	}
	// HGET
	r = c.send("HGET", "h", "f1")
	if !strings.Contains(r, "v1") {
		t.Errorf("HGET: %q", r)
	}
	// HGETALL
	r = c.send("HGETALL", "h")
	if !strings.Contains(r, "f1") || !strings.Contains(r, "v1") {
		t.Errorf("HGETALL: %q", r)
	}
	// HEXISTS
	r = c.send("HEXISTS", "h", "f1")
	if !strings.HasPrefix(r, ":1") {
		t.Errorf("HEXISTS: %q", r)
	}
	// HLEN
	r = c.send("HLEN", "h")
	if !strings.HasPrefix(r, ":2") {
		t.Errorf("HLEN: %q", r)
	}
	// HKEYS
	r = c.send("HKEYS", "h")
	if !strings.Contains(r, "f1") {
		t.Errorf("HKEYS: %q", r)
	}
	// HVALS
	r = c.send("HVALS", "h")
	if !strings.Contains(r, "v1") {
		t.Errorf("HVALS: %q", r)
	}
	// HDEL
	r = c.send("HDEL", "h", "f1")
	if !strings.HasPrefix(r, ":1") {
		t.Errorf("HDEL: %q", r)
	}
	// HMSET / HMGET
	c.send("HMSET", "h2", "a", "1", "b", "2")
	r = c.send("HMGET", "h2", "a", "b", "missing")
	if !strings.Contains(r, "1") {
		t.Errorf("HMGET: %q", r)
	}
	// HSETNX
	r = c.send("HSETNX", "h2", "a", "overwrite")
	if !strings.HasPrefix(r, ":0") {
		t.Errorf("HSETNX existing: %q", r)
	}
	// HINCRBY
	c.send("HSET", "h2", "score", "10")
	r = c.send("HINCRBY", "h2", "score", "5")
	if !strings.HasPrefix(r, ":15") {
		t.Errorf("HINCRBY: %q", r)
	}
}

// ─────────────────────────────────────────────
//  List commands
// ─────────────────────────────────────────────

func TestServer_List_Commands(t *testing.T) {
	ts := startTestServer(t)
	c := newClient(t, ts.addr)
	defer c.close()

	// RPUSH / LPUSH
	c.send("RPUSH", "l", "a", "b", "c")
	// LLEN
	r := c.send("LLEN", "l")
	if !strings.HasPrefix(r, ":3") {
		t.Errorf("LLEN: %q", r)
	}
	// LRANGE
	r = c.send("LRANGE", "l", "0", "-1")
	if !strings.Contains(r, "a") || !strings.Contains(r, "c") {
		t.Errorf("LRANGE: %q", r)
	}
	// LINDEX
	r = c.send("LINDEX", "l", "1")
	if !strings.Contains(r, "b") {
		t.Errorf("LINDEX: %q", r)
	}
	// LSET
	r = c.send("LSET", "l", "0", "A")
	if !strings.HasPrefix(r, "+OK") {
		t.Errorf("LSET: %q", r)
	}
	// LPOP
	r = c.send("LPOP", "l")
	if !strings.Contains(r, "A") {
		t.Errorf("LPOP: %q", r)
	}
	// RPOP
	r = c.send("RPOP", "l")
	if !strings.Contains(r, "c") {
		t.Errorf("RPOP: %q", r)
	}
	// LREM
	c.send("RPUSH", "l2", "x", "x", "y", "x")
	r = c.send("LREM", "l2", "2", "x")
	if !strings.HasPrefix(r, ":2") {
		t.Errorf("LREM: %q", r)
	}
	// LPUSHX / RPUSHX
	r = c.send("LPUSHX", "no_list", "v")
	if !strings.HasPrefix(r, ":0") {
		t.Errorf("LPUSHX missing: %q", r)
	}
}

// ─────────────────────────────────────────────
//  Set commands
// ─────────────────────────────────────────────

func TestServer_Set_Commands(t *testing.T) {
	ts := startTestServer(t)
	c := newClient(t, ts.addr)
	defer c.close()

	c.send("SADD", "s1", "a", "b", "c")
	c.send("SADD", "s2", "b", "c", "d")

	// SCARD
	r := c.send("SCARD", "s1")
	if !strings.HasPrefix(r, ":3") {
		t.Errorf("SCARD: %q", r)
	}
	// SISMEMBER
	r = c.send("SISMEMBER", "s1", "a")
	if !strings.HasPrefix(r, ":1") {
		t.Errorf("SISMEMBER: %q", r)
	}
	// SREM
	r = c.send("SREM", "s1", "a")
	if !strings.HasPrefix(r, ":1") {
		t.Errorf("SREM: %q", r)
	}
	// SINTER
	r = c.send("SINTER", "s1", "s2")
	if !strings.Contains(r, "b") && !strings.Contains(r, "c") {
		t.Errorf("SINTER: %q", r)
	}
	// SUNION
	r = c.send("SUNION", "s1", "s2")
	if !strings.Contains(r, "d") {
		t.Errorf("SUNION: %q", r)
	}
	// SDIFF
	r = c.send("SDIFF", "s2", "s1")
	if !strings.Contains(r, "d") {
		t.Errorf("SDIFF: %q", r)
	}
	// SMOVE
	r = c.send("SMOVE", "s2", "s1", "d")
	if !strings.HasPrefix(r, ":1") {
		t.Errorf("SMOVE: %q", r)
	}
}

// ─────────────────────────────────────────────
//  ZSet commands
// ─────────────────────────────────────────────

func TestServer_ZSet_Commands(t *testing.T) {
	ts := startTestServer(t)
	c := newClient(t, ts.addr)
	defer c.close()

	c.send("ZADD", "z", "10", "alice")
	c.send("ZADD", "z", "20", "bob")
	c.send("ZADD", "z", "5", "charlie")

	// ZCARD
	r := c.send("ZCARD", "z")
	if !strings.HasPrefix(r, ":3") {
		t.Errorf("ZCARD: %q", r)
	}
	// ZSCORE
	r = c.send("ZSCORE", "z", "alice")
	if !strings.Contains(r, "10") {
		t.Errorf("ZSCORE: %q", r)
	}
	// ZRANK
	r = c.send("ZRANK", "z", "charlie")
	if !strings.HasPrefix(r, ":0") {
		t.Errorf("ZRANK: %q", r)
	}
	// ZRANGE
	r = c.send("ZRANGE", "z", "0", "-1")
	if !strings.Contains(r, "charlie") {
		t.Errorf("ZRANGE: %q", r)
	}
	// ZREVRANGE
	r = c.send("ZREVRANGE", "z", "0", "0")
	if !strings.Contains(r, "bob") {
		t.Errorf("ZREVRANGE: %q", r)
	}
	// ZRANGEBYSCORE
	r = c.send("ZRANGEBYSCORE", "z", "8", "15")
	if !strings.Contains(r, "alice") {
		t.Errorf("ZRANGEBYSCORE: %q", r)
	}
	// ZINCRBY
	r = c.send("ZINCRBY", "z", "5", "alice")
	if !strings.Contains(r, "15") {
		t.Errorf("ZINCRBY: %q", r)
	}
	// ZCOUNT
	r = c.send("ZCOUNT", "z", "5", "20")
	if !strings.HasPrefix(r, ":3") {
		t.Errorf("ZCOUNT: %q", r)
	}
	// ZREM
	r = c.send("ZREM", "z", "bob")
	if !strings.HasPrefix(r, ":1") {
		t.Errorf("ZREM: %q", r)
	}
}

// ─────────────────────────────────────────────
//  Error cases
// ─────────────────────────────────────────────

func TestServer_UnknownCommand(t *testing.T) {
	ts := startTestServer(t)
	c := newClient(t, ts.addr)
	defer c.close()
	r := c.send("NOTACOMMAND")
	if !strings.HasPrefix(r, "-ERR") {
		t.Errorf("unknown command: %q", r)
	}
}

func TestServer_WrongType(t *testing.T) {
	ts := startTestServer(t)
	c := newClient(t, ts.addr)
	defer c.close()
	c.send("SET", "s", "string")
	r := c.send("HSET", "s", "f", "v")
	if !strings.HasPrefix(r, "-") {
		t.Errorf("WRONGTYPE: %q", r)
	}
}

func TestServer_MultipleClients_Concurrent(t *testing.T) {
	ts := startTestServer(t)
	results := make(chan bool, 20)
	for i := 0; i < 20; i++ {
		go func(id int) {
			c := newClient(t, ts.addr)
			defer c.close()
			key := fmt.Sprintf("cc:%d", id)
			c.send("SET", key, fmt.Sprintf("%d", id))
			r := c.send("GET", key)
			results <- strings.Contains(r, fmt.Sprintf("%d", id))
		}(i)
	}
	for i := 0; i < 20; i++ {
		if ok := <-results; !ok {
			t.Error("concurrent client read mismatch")
		}
	}
}
