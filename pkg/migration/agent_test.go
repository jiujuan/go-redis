package migration

import (
	"bufio"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jiujuan/go-redis/internal/resp"
	"github.com/jiujuan/go-redis/pkg/pool"
)

type fakeNode struct {
	mu    sync.Mutex
	store map[string]fakeEntry
}

type fakeEntry struct {
	typ   string
	value  string
	hash   []string
	list   []string
	set    []string
	zset   []string
}

type fakeRedisServer struct {
	t        *testing.T
	ln       net.Listener
	node     *fakeNode
	shutdown chan struct{}
}

func newFakeRedisServer(t *testing.T, node *fakeNode) *fakeRedisServer {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeRedisServer{t: t, ln: ln, node: node, shutdown: make(chan struct{})}
	go s.serve()
	return s
}

func (s *fakeRedisServer) addr() string { return s.ln.Addr().String() }

func (s *fakeRedisServer) close() {
	close(s.shutdown)
	_ = s.ln.Close()
}

func (s *fakeRedisServer) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.shutdown:
				return
			default:
				continue
			}
		}
		go s.handle(conn)
	}
}

func (s *fakeRedisServer) handle(conn net.Conn) {
	defer conn.Close()
	rd := resp.NewReader(conn)
	wr := resp.NewWriter(bufio.NewWriter(conn))
	for {
		v, err := rd.Read()
		if err != nil {
			return
		}
		args, err := v.ToArgs()
		if err != nil || len(args) == 0 {
			_ = wr.WriteError("bad args")
			_ = wr.Flush()
			continue
		}
		s.dispatch(wr, args)
		_ = wr.Flush()
	}
}

func (s *fakeRedisServer) dispatch(wr *resp.Writer, args []string) {
	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "SCAN":
		_ = wr.WriteArrayHeader(2)
		_ = wr.WriteBulkString("0")
		_ = wr.WriteArrayHeader(0)
	case "TYPE":
		s.node.mu.Lock()
		e, ok := s.node.store[args[1]]
		s.node.mu.Unlock()
		if !ok {
			_ = wr.WriteSimpleString("none")
			return
		}
		_ = wr.WriteSimpleString(e.typ)
	case "EXISTS":
		s.node.mu.Lock()
		_, ok := s.node.store[args[1]]
		s.node.mu.Unlock()
		_ = wr.WriteInteger(boolToInt(ok))
	case "GET":
		s.writeBulkForKey(wr, args[1], "string")
	case "HGETALL":
		s.writeArrayForKey(wr, args[1], "hash")
	case "LRANGE":
		s.writeArrayForKey(wr, args[1], "list")
	case "SMEMBERS":
		s.writeArrayForKey(wr, args[1], "set")
	case "ZRANGE":
		s.writeZSetArray(wr, args[1])
	case "SET", "HSET", "RPUSH", "SADD", "ZADD":
		s.node.mu.Lock()
		s.node.store[args[1]] = fakeEntry{typ: inferType(cmd), value: strings.Join(args[2:], ",")}
		s.node.mu.Unlock()
		_ = wr.WriteOK()
	case "DEL":
		s.node.mu.Lock()
		delete(s.node.store, args[1])
		s.node.mu.Unlock()
		_ = wr.WriteInteger(1)
	default:
		_ = wr.WriteError("unsupported")
	}
}

func (s *fakeRedisServer) writeBulkForKey(wr *resp.Writer, key, typ string) {
	s.node.mu.Lock()
	e, ok := s.node.store[key]
	s.node.mu.Unlock()
	if !ok || e.typ != typ {
		_ = wr.WriteNilBulk()
		return
	}
	_ = wr.WriteBulkString(e.value)
}

func (s *fakeRedisServer) writeArrayForKey(wr *resp.Writer, key, typ string) {
	s.node.mu.Lock()
	e, ok := s.node.store[key]
	s.node.mu.Unlock()
	if !ok || e.typ != typ {
		_ = wr.WriteNilArray()
		return
	}
	var arr []string
	switch typ {
	case "hash":
		arr = e.hash
	case "list":
		arr = e.list
	case "set":
		arr = e.set
	}
	_ = wr.WriteStringArray(arr)
}

func (s *fakeRedisServer) writeZSetArray(wr *resp.Writer, key string) {
	s.node.mu.Lock()
	e, ok := s.node.store[key]
	s.node.mu.Unlock()
	if !ok || e.typ != "zset" {
		_ = wr.WriteNilArray()
		return
	}
	_ = wr.WriteStringArray(e.zset)
}

func inferType(cmd string) string {
	switch cmd {
	case "SET":
		return "string"
	case "HSET":
		return "hash"
	case "RPUSH":
		return "list"
	case "SADD":
		return "set"
	case "ZADD":
		return "zset"
	default:
		return "string"
	}
}

func boolToInt(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

func TestFormatScanArgs(t *testing.T) {
	if got := FormatScanArgs(0, "*", 10); len(got) != 4 || got[0] != "SCAN" || got[1] != "0" || got[2] != "COUNT" || got[3] != "10" {
		t.Fatalf("FormatScanArgs wildcard = %v", got)
	}
	got := FormatScanArgs(3, "user:*", 20)
	if len(got) != 6 || got[2] != "MATCH" || got[3] != "user:*" || got[4] != "COUNT" || got[5] != "20" {
		t.Fatalf("FormatScanArgs match = %v", got)
	}
}

func TestNodeAgentLifecycleAndScanKeys(t *testing.T) {
	node := &fakeNode{store: map[string]fakeEntry{}}
	srv := newFakeRedisServer(t, node)
	defer srv.close()

	na := NewNodeAgent([]string{srv.addr()}, nil)
	defer na.Close()
	na.AddNode(srv.addr())
	na.AddNode(srv.addr())

	if len(na.pools) != 1 {
		t.Fatalf("expected one pool, got %d", len(na.pools))
	}

	next, keys, err := na.ScanKeys(srv.addr(), 0, 5)
	if err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}
	if next != 0 || len(keys) != 0 {
		t.Fatalf("ScanKeys = (%d,%v), want empty result", next, keys)
	}
}

func TestSendRESPRoundTrip(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		rd := resp.NewReader(c2)
		v, err := rd.Read()
		if err != nil {
			t.Errorf("server read: %v", err)
			return
		}
		args, _ := v.ToArgs()
		if len(args) != 2 || args[0] != "PING" || args[1] != "hello" {
			t.Errorf("unexpected args: %v", args)
		}
		wr := resp.NewWriter(c2)
		_ = wr.WriteOK()
		_ = wr.Flush()
	}()

	v, err := sendRESP(c1, "PING", "hello")
	if err != nil {
		t.Fatalf("sendRESP: %v", err)
	}
	if v.Type != '+' || v.Str != "OK" {
		t.Fatalf("sendRESP response = %+v", v)
	}
	<-done
}

func TestNodeAgentMoveKeyStringAndUnsupportedType(t *testing.T) {
	src := &fakeNode{store: map[string]fakeEntry{
		"alpha": {typ: "string", value: "1"},
		"bad":   {typ: "stream"},
	}}
	dst := &fakeNode{store: map[string]fakeEntry{}}
	srcSrv := newFakeRedisServer(t, src)
	dstSrv := newFakeRedisServer(t, dst)
	defer srcSrv.close()
	defer dstSrv.close()

	na := NewNodeAgent([]string{srcSrv.addr(), dstSrv.addr()}, &pool.Config{DialTimeout: time.Second, ReadTimeout: time.Second, IdleTimeout: time.Second})
	defer na.Close()

	if err := na.MoveKey("alpha", srcSrv.addr(), dstSrv.addr()); err != nil {
		t.Fatalf("MoveKey string: %v", err)
	}
	if _, ok := dst.store["alpha"]; !ok {
		t.Fatal("destination missing moved string key")
	}
	if _, ok := src.store["alpha"]; ok {
		t.Fatal("source still has moved string key")
	}

	if err := na.MoveKey("bad", srcSrv.addr(), dstSrv.addr()); err == nil {
		t.Fatal("MoveKey should reject unsupported type")
	}
}

func TestNodeAgentMoveKeyCollectionTypes(t *testing.T) {
	src := &fakeNode{store: map[string]fakeEntry{
		"h": {typ: "hash", hash: []string{"f1", "v1", "f2", "v2"}},
		"l": {typ: "list", list: []string{"a", "b"}},
		"s": {typ: "set", set: []string{"m1", "m2"}},
		"z": {typ: "zset", zset: []string{"m1", "1", "m2", "2"}},
	}}
	dst := &fakeNode{store: map[string]fakeEntry{}}
	srcSrv := newFakeRedisServer(t, src)
	dstSrv := newFakeRedisServer(t, dst)
	defer srcSrv.close()
	defer dstSrv.close()

	na := NewNodeAgent([]string{srcSrv.addr(), dstSrv.addr()}, nil)
	defer na.Close()

	for _, key := range []string{"h", "l", "s", "z"} {
		if err := na.MoveKey(key, srcSrv.addr(), dstSrv.addr()); err != nil {
			t.Fatalf("MoveKey %s: %v", key, err)
		}
		if _, ok := dst.store[key]; !ok {
			t.Fatalf("destination missing %s", key)
		}
	}
}
