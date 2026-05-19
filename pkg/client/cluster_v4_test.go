package client

import (
	"bufio"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jiujuan/go-redis/internal/resp"
	"github.com/jiujuan/go-redis/pkg/migration"
	"github.com/jiujuan/go-redis/pkg/pool"
)

type fakeClusterEntry struct {
	typ   string
	str   string
	hash  map[string]string
	list  []string
	set   map[string]struct{}
	zset  map[string]float64
}

type fakeClusterNode struct {
	mu    sync.Mutex
	data  map[string]*fakeClusterEntry
	failPing bool
}

type fakeClusterServer struct {
	t        *testing.T
	ln       net.Listener
	node     *fakeClusterNode
	shutdown chan struct{}
}

func newFakeClusterNode() *fakeClusterNode {
	return &fakeClusterNode{data: make(map[string]*fakeClusterEntry)}
}

func newFakeClusterServer(t *testing.T, node *fakeClusterNode) *fakeClusterServer {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeClusterServer{
		t:        t,
		ln:       ln,
		node:     node,
		shutdown: make(chan struct{}),
	}
	go s.serve()
	return s
}

func (s *fakeClusterServer) addr() string {
	return s.ln.Addr().String()
}

func (s *fakeClusterServer) close() {
	close(s.shutdown)
	_ = s.ln.Close()
}

func (s *fakeClusterServer) serve() {
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

func (s *fakeClusterServer) handle(conn net.Conn) {
	defer conn.Close()
	rd := resp.NewReader(conn)
	bw := bufio.NewWriter(conn)
	wr := resp.NewWriter(bw)
	for {
		v, err := rd.Read()
		if err != nil {
			return
		}
		args, err := v.ToArgs()
		if err != nil || len(args) == 0 {
			_ = wr.WriteError("bad args")
			_ = wr.Flush()
			_ = bw.Flush()
			continue
		}
		if err := s.dispatch(wr, args); err != nil {
			_ = wr.WriteErrorRaw(err.Error())
		}
		_ = wr.Flush()
		_ = bw.Flush()
	}
}

func (s *fakeClusterServer) dispatch(wr *resp.Writer, args []string) error {
	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "PING":
		if s.node.failPing {
			return fmt.Errorf("ERR ping failed")
		}
		return wr.WriteSimpleString("PONG")
	case "SET":
		s.setString(args[1], args[2])
		return wr.WriteOK()
	case "GET":
		e := s.get(args[1])
		if e == nil || e.typ != "string" {
			return wr.WriteNilBulk()
		}
		return wr.WriteBulkString(e.str)
	case "DEL":
		var n int64
		for _, key := range args[1:] {
			if s.del(key) {
				n++
			}
		}
		return wr.WriteInteger(n)
	case "EXISTS":
		if s.get(args[1]) != nil {
			return wr.WriteInteger(1)
		}
		return wr.WriteInteger(0)
	case "INCR", "DECR":
		delta := int64(1)
		if cmd == "DECR" {
			delta = -1
		}
		e := s.get(args[1])
		var cur int64
		if e != nil && e.typ == "string" && e.str != "" {
			n, err := strconv.ParseInt(e.str, 10, 64)
			if err != nil {
				return fmt.Errorf("ERR value is not an integer")
			}
			cur = n
		}
		cur += delta
		s.setString(args[1], strconv.FormatInt(cur, 10))
		return wr.WriteInteger(cur)
	case "HSET":
		if len(args[2:])%2 != 0 {
			return fmt.Errorf("ERR wrong number of arguments")
		}
		e := s.ensureHash(args[1])
		var added int64
		for i := 2; i < len(args); i += 2 {
			if _, ok := e.hash[args[i]]; !ok {
				added++
			}
			e.hash[args[i]] = args[i+1]
		}
		return wr.WriteInteger(added)
	case "HGET":
		e := s.get(args[1])
		if e == nil || e.typ != "hash" {
			return wr.WriteNilBulk()
		}
		v, ok := e.hash[args[2]]
		if !ok {
			return wr.WriteNilBulk()
		}
		return wr.WriteBulkString(v)
	case "HGETALL":
		e := s.get(args[1])
		if e == nil || e.typ != "hash" {
			return wr.WriteArrayHeader(0)
		}
		keys := make([]string, 0, len(e.hash))
		for k := range e.hash {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		if err := wr.WriteArrayHeader(len(keys) * 2); err != nil {
			return err
		}
		for _, k := range keys {
			if err := wr.WriteBulkString(k); err != nil {
				return err
			}
			if err := wr.WriteBulkString(e.hash[k]); err != nil {
				return err
			}
		}
		return nil
	case "HDEL":
		e := s.get(args[1])
		if e == nil || e.typ != "hash" {
			return wr.WriteInteger(0)
		}
		var removed int64
		for _, f := range args[2:] {
			if _, ok := e.hash[f]; ok {
				delete(e.hash, f)
				removed++
			}
		}
		return wr.WriteInteger(removed)
	case "LPUSH", "RPUSH":
		e := s.ensureList(args[1])
		if cmd == "LPUSH" {
			for _, v := range args[2:] {
				e.list = append([]string{v}, e.list...)
			}
		} else {
			e.list = append(e.list, args[2:]...)
		}
		return wr.WriteInteger(int64(len(e.list)))
	case "LPOP", "RPOP":
		e := s.get(args[1])
		if e == nil || e.typ != "list" || len(e.list) == 0 {
			return wr.WriteNilBulk()
		}
		var v string
		if cmd == "LPOP" {
			v = e.list[0]
			e.list = e.list[1:]
		} else {
			v = e.list[len(e.list)-1]
			e.list = e.list[:len(e.list)-1]
		}
		return wr.WriteBulkString(v)
	case "LRANGE":
		e := s.get(args[1])
		if e == nil || e.typ != "list" {
			return wr.WriteArrayHeader(0)
		}
		start, _ := strconv.Atoi(args[2])
		stop, _ := strconv.Atoi(args[3])
		items := sliceRange(e.list, start, stop)
		return wr.WriteStringArray(items)
	case "SADD":
		e := s.ensureSet(args[1])
		var added int64
		for _, m := range args[2:] {
			if _, ok := e.set[m]; !ok {
				e.set[m] = struct{}{}
				added++
			}
		}
		return wr.WriteInteger(added)
	case "SMEMBERS":
		e := s.get(args[1])
		if e == nil || e.typ != "set" {
			return wr.WriteArrayHeader(0)
		}
		items := make([]string, 0, len(e.set))
		for m := range e.set {
			items = append(items, m)
		}
		sort.Strings(items)
		return wr.WriteStringArray(items)
	case "SREM":
		e := s.get(args[1])
		if e == nil || e.typ != "set" {
			return wr.WriteInteger(0)
		}
		var removed int64
		for _, m := range args[2:] {
			if _, ok := e.set[m]; ok {
				delete(e.set, m)
				removed++
			}
		}
		return wr.WriteInteger(removed)
	case "SISMEMBER":
		e := s.get(args[1])
		if e == nil || e.typ != "set" {
			return wr.WriteInteger(0)
		}
		if _, ok := e.set[args[2]]; ok {
			return wr.WriteInteger(1)
		}
		return wr.WriteInteger(0)
	case "ZADD":
		e := s.ensureZSet(args[1])
		var added int64
		for i := 2; i < len(args); i += 2 {
			score, _ := strconv.ParseFloat(args[i], 64)
			member := args[i+1]
			if _, ok := e.zset[member]; !ok {
				added++
			}
			e.zset[member] = score
		}
		return wr.WriteInteger(added)
	case "ZSCORE":
		e := s.get(args[1])
		if e == nil || e.typ != "zset" {
			return wr.WriteNilBulk()
		}
		score, ok := e.zset[args[2]]
		if !ok {
			return wr.WriteNilBulk()
		}
		return wr.WriteBulkString(strconv.FormatFloat(score, 'f', -1, 64))
	case "ZRANGE":
		e := s.get(args[1])
		if e == nil || e.typ != "zset" {
			return wr.WriteArrayHeader(0)
		}
		start, _ := strconv.Atoi(args[2])
		stop, _ := strconv.Atoi(args[3])
		withScores := len(args) > 4 && strings.ToUpper(args[4]) == "WITHSCORES"
		items := sortedZSet(e.zset, withScores)
		if withScores {
			pairs := make([]string, 0, len(items))
			for _, item := range items {
				pairs = append(pairs, item.member, item.scoreStr)
			}
			return wr.WriteStringArray(sliceRange(pairs, start*2, stop*2+1))
		}
		members := make([]string, 0, len(items))
		for _, item := range items {
			members = append(members, item.member)
		}
		return wr.WriteStringArray(sliceRange(members, start, stop))
	case "ZREM":
		e := s.get(args[1])
		if e == nil || e.typ != "zset" {
			return wr.WriteInteger(0)
		}
		var removed int64
		for _, m := range args[2:] {
			if _, ok := e.zset[m]; ok {
				delete(e.zset, m)
				removed++
			}
		}
		return wr.WriteInteger(removed)
	case "ZRANK":
		e := s.get(args[1])
		if e == nil || e.typ != "zset" {
			return wr.WriteNilBulk()
		}
		items := sortedZSet(e.zset, false)
		for i, item := range items {
			if item.member == args[2] {
				return wr.WriteInteger(int64(i))
			}
		}
		return wr.WriteNilBulk()
	case "TYPE":
		e := s.get(args[1])
		if e == nil {
			return wr.WriteSimpleString("none")
		}
		return wr.WriteSimpleString(e.typ)
	case "SCAN":
		keys := s.keys()
		cursor, _ := strconv.Atoi(args[1])
		count := 10
		for i := 2; i < len(args)-1; i++ {
			if strings.ToUpper(args[i]) == "COUNT" {
				count, _ = strconv.Atoi(args[i+1])
			}
		}
		next := cursor + count
		if next > len(keys) {
			next = len(keys)
		}
		batch := keys[cursor:next]
		nextStr := "0"
		if next < len(keys) {
			nextStr = strconv.Itoa(next)
		}
		if err := wr.WriteArrayHeader(2); err != nil {
			return err
		}
		if err := wr.WriteBulkString(nextStr); err != nil {
			return err
		}
		return wr.WriteStringArray(batch)
	default:
		return fmt.Errorf("ERR unsupported command %s", cmd)
	}
}

func (s *fakeClusterServer) get(key string) *fakeClusterEntry {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()
	return s.node.data[key]
}

func (s *fakeClusterServer) keys() []string {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()
	keys := make([]string, 0, len(s.node.data))
	for k := range s.node.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (s *fakeClusterServer) del(key string) bool {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()
	_, ok := s.node.data[key]
	delete(s.node.data, key)
	return ok
}

func (s *fakeClusterServer) setString(key, value string) {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()
	s.node.data[key] = &fakeClusterEntry{typ: "string", str: value}
}

func (s *fakeClusterServer) ensureHash(key string) *fakeClusterEntry {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()
	e := s.node.data[key]
	if e == nil || e.typ != "hash" {
		e = &fakeClusterEntry{typ: "hash", hash: map[string]string{}}
		s.node.data[key] = e
	}
	return e
}

func (s *fakeClusterServer) ensureList(key string) *fakeClusterEntry {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()
	e := s.node.data[key]
	if e == nil || e.typ != "list" {
		e = &fakeClusterEntry{typ: "list"}
		s.node.data[key] = e
	}
	return e
}

func (s *fakeClusterServer) ensureSet(key string) *fakeClusterEntry {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()
	e := s.node.data[key]
	if e == nil || e.typ != "set" {
		e = &fakeClusterEntry{typ: "set", set: map[string]struct{}{}}
		s.node.data[key] = e
	}
	return e
}

func (s *fakeClusterServer) ensureZSet(key string) *fakeClusterEntry {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()
	e := s.node.data[key]
	if e == nil || e.typ != "zset" {
		e = &fakeClusterEntry{typ: "zset", zset: map[string]float64{}}
		s.node.data[key] = e
	}
	return e
}

type zitem struct {
	member   string
	score    float64
	scoreStr string
}

func sortedZSet(z map[string]float64, withScore bool) []zitem {
	items := make([]zitem, 0, len(z))
	for m, s := range z {
		items = append(items, zitem{
			member:   m,
			score:    s,
			scoreStr: strconv.FormatFloat(s, 'f', -1, 64),
		})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].score == items[j].score {
			return items[i].member < items[j].member
		}
		return items[i].score < items[j].score
	})
	return items
}

func sliceRange(items []string, start, stop int) []string {
	n := len(items)
	if n == 0 {
		return []string{}
	}
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || start >= n {
		return []string{}
	}
	out := make([]string, stop-start+1)
	copy(out, items[start:stop+1])
	return out
}

func newTestClusterClient(t *testing.T, addrs []string) *ClusterClient {
	t.Helper()
	cfg := &pool.Config{
		MaxIdle:     4,
		MaxActive:   8,
		IdleTimeout: time.Second,
		DialTimeout: time.Second,
		ReadTimeout: time.Second,
	}
	return NewClusterClient(addrs, WithPoolConfig(cfg), WithVirtualReplicas(32), WithMigrationConfig(&migration.MigrationConfig{
		BatchSize:           10,
		Concurrency:         2,
		RetryLimit:          1,
		BatchInterval:       0,
		ReadFallbackTimeout: 50 * time.Millisecond,
	}))
}

func TestOptionsAndConstructor(t *testing.T) {
	customPool := &pool.Config{MaxIdle: 1}
	customMig := &migration.MigrationConfig{BatchSize: 7, Concurrency: 2}
	c := NewClusterClient([]string{"n1"}, WithVirtualReplicas(8), WithPoolConfig(customPool), WithMigrationConfig(customMig))
	defer c.Close()

	if c.virtualReplicas != 8 {
		t.Fatalf("virtualReplicas = %d, want 8", c.virtualReplicas)
	}
	if c.poolCfg != customPool {
		t.Fatal("pool config option not applied")
	}
	if c.migCfg != customMig {
		t.Fatal("migration config option not applied")
	}

	c2 := NewClusterClient(nil, WithVirtualReplicas(0))
	defer c2.Close()
	if c2.virtualReplicas != defaultVirtualReplicas {
		t.Fatalf("default virtual replicas = %d, want %d", c2.virtualReplicas, defaultVirtualReplicas)
	}
}

func TestGetWriteNodeAndHelpersOnEmptyCluster(t *testing.T) {
	c := NewClusterClient(nil)
	defer c.Close()

	if _, err := c.getWriteNode("k"); err == nil {
		t.Fatal("expected getWriteNode to fail on empty cluster")
	}
	if _, err := c.NodeForKey("k"); err == nil {
		t.Fatal("expected NodeForKey to fail on empty cluster")
	}
	if _, err := c.sendToNode("missing", "PING"); err == nil {
		t.Fatal("expected sendToNode to fail for missing pool")
	}
}

func TestCheckOKReply(t *testing.T) {
	if err := checkOKReply(nil, fmt.Errorf("x")); err == nil {
		t.Fatal("expected original error")
	}
	if err := checkOKReply(&resp.Value{Type: '-', Str: "ERR bad"}, nil); err == nil {
		t.Fatal("expected error reply to become error")
	}
	if err := checkOKReply(&resp.Value{Type: '+', Str: "OK"}, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSendCommandRoundTrip(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		rd := resp.NewReader(c2)
		v, err := rd.Read()
		if err != nil {
			t.Errorf("read request: %v", err)
			return
		}
		args, _ := v.ToArgs()
		if len(args) != 2 || args[0] != "PING" || args[1] != "x" {
			t.Errorf("unexpected args: %v", args)
		}
		wr := resp.NewWriter(c2)
		_ = wr.WriteSimpleString("PONG")
		_ = wr.Flush()
	}()

	client := NewClusterClient(nil)
	defer client.Close()
	v, err := client.sendCommand(c1, "PING", "x")
	if err != nil {
		t.Fatalf("sendCommand: %v", err)
	}
	if v.Str != "PONG" {
		t.Fatalf("response = %q, want PONG", v.Str)
	}
	<-done
}

func TestStringHashListSetZSetAPIs(t *testing.T) {
	n1 := newFakeClusterNode()
	n2 := newFakeClusterNode()
	s1 := newFakeClusterServer(t, n1)
	s2 := newFakeClusterServer(t, n2)
	defer s1.close()
	defer s2.close()

	c := newTestClusterClient(t, []string{s1.addr(), s2.addr()})
	defer c.Close()

	if err := c.Set("name", "alice"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got, err := c.Get("name"); err != nil || got != "alice" {
		t.Fatalf("Get = (%q,%v), want (alice,nil)", got, err)
	}
	if exists, err := c.Exists("name"); err != nil || !exists {
		t.Fatalf("Exists = (%v,%v), want (true,nil)", exists, err)
	}
	if got, err := c.Incr("cnt"); err != nil || got != 1 {
		t.Fatalf("Incr = (%d,%v), want (1,nil)", got, err)
	}
	if got, err := c.Decr("cnt"); err != nil || got != 0 {
		t.Fatalf("Decr = (%d,%v), want (0,nil)", got, err)
	}
	if err := c.MSet("a", "1", "b", "2"); err != nil {
		t.Fatalf("MSet: %v", err)
	}
	if vals, err := c.MGet("a", "b", "missing"); err != nil || len(vals) != 3 || vals[0] != "1" || vals[2] != "" {
		t.Fatalf("MGet = (%v,%v)", vals, err)
	}
	if _, err := c.Del("name", "a", "b"); err != nil {
		t.Fatalf("Del: %v", err)
	}

	if added, err := c.HSet("h", "f1", "v1", "f2", "v2"); err != nil || added != 2 {
		t.Fatalf("HSet = (%d,%v)", added, err)
	}
	if got, err := c.HGet("h", "f1"); err != nil || got != "v1" {
		t.Fatalf("HGet = (%q,%v)", got, err)
	}
	if all, err := c.HGetAll("h"); err != nil || len(all) != 4 {
		t.Fatalf("HGetAll = (%v,%v)", all, err)
	}
	if removed, err := c.HDel("h", "f1"); err != nil || removed != 1 {
		t.Fatalf("HDel = (%d,%v)", removed, err)
	}

	if n, err := c.LPush("l", "b", "a"); err != nil || n != 2 {
		t.Fatalf("LPush = (%d,%v)", n, err)
	}
	if n, err := c.RPush("l", "c"); err != nil || n != 3 {
		t.Fatalf("RPush = (%d,%v)", n, err)
	}
	if vals, err := c.LRange("l", 0, -1); err != nil || len(vals) != 3 {
		t.Fatalf("LRange = (%v,%v)", vals, err)
	}
	if v, err := c.LPop("l"); err != nil || v != "a" {
		t.Fatalf("LPop = (%q,%v)", v, err)
	}
	if v, err := c.RPop("l"); err != nil || v != "c" {
		t.Fatalf("RPop = (%q,%v)", v, err)
	}

	if n, err := c.SAdd("s", "m1", "m2"); err != nil || n != 2 {
		t.Fatalf("SAdd = (%d,%v)", n, err)
	}
	if members, err := c.SMembers("s"); err != nil || len(members) != 2 {
		t.Fatalf("SMembers = (%v,%v)", members, err)
	}
	if ok, err := c.SIsMember("s", "m1"); err != nil || !ok {
		t.Fatalf("SIsMember = (%v,%v)", ok, err)
	}
	if n, err := c.SRem("s", "m1"); err != nil || n != 1 {
		t.Fatalf("SRem = (%d,%v)", n, err)
	}

	if n, err := c.ZAdd("z", 1.5, "m1"); err != nil || n != 1 {
		t.Fatalf("ZAdd = (%d,%v)", n, err)
	}
	if _, err := c.ZAdd("z", 2, "m2"); err != nil {
		t.Fatalf("second ZAdd: %v", err)
	}
	if score, err := c.ZScore("z", "m1"); err != nil || score != "1.5" {
		t.Fatalf("ZScore = (%q,%v)", score, err)
	}
	if vals, err := c.ZRange("z", 0, -1, true); err != nil || len(vals) != 4 {
		t.Fatalf("ZRange = (%v,%v)", vals, err)
	}
	if rank, err := c.ZRank("z", "m2"); err != nil || rank != 1 {
		t.Fatalf("ZRank = (%d,%v)", rank, err)
	}
	if n, err := c.ZRem("z", "m1"); err != nil || n != 1 {
		t.Fatalf("ZRem = (%d,%v)", n, err)
	}
}

func TestParameterValidationAndMissingValues(t *testing.T) {
	n1 := newFakeClusterNode()
	s1 := newFakeClusterServer(t, n1)
	defer s1.close()

	c := newTestClusterClient(t, []string{s1.addr()})
	defer c.Close()

	if err := c.MSet("odd"); err == nil {
		t.Fatal("expected MSet odd args to fail")
	}
	if got, err := c.Get("missing"); err != nil || got != "" {
		t.Fatalf("Get missing = (%q,%v)", got, err)
	}
	if got, err := c.HGet("missing", "field"); err != nil || got != "" {
		t.Fatalf("HGet missing = (%q,%v)", got, err)
	}
	if got, err := c.LPop("missing"); err != nil || got != "" {
		t.Fatalf("LPop missing = (%q,%v)", got, err)
	}
	if got, err := c.RPop("missing"); err != nil || got != "" {
		t.Fatalf("RPop missing = (%q,%v)", got, err)
	}
	if got, err := c.ZScore("missing", "m"); err != nil || got != "" {
		t.Fatalf("ZScore missing = (%q,%v)", got, err)
	}
	if got, err := c.ZRank("missing", "m"); err != nil || got != -1 {
		t.Fatalf("ZRank missing = (%d,%v)", got, err)
	}
}

func TestAddNodesRemoveNodeAndMigrationHelpers(t *testing.T) {
	n1 := newFakeClusterNode()
	n2 := newFakeClusterNode()
	n3 := newFakeClusterNode()
	s1 := newFakeClusterServer(t, n1)
	s2 := newFakeClusterServer(t, n2)
	s3 := newFakeClusterServer(t, n3)
	defer s1.close()
	defer s2.close()
	defer s3.close()

	c := newTestClusterClient(t, []string{s1.addr(), s2.addr()})
	defer c.Close()

	if c.MigrationProgress() != nil {
		t.Fatal("MigrationProgress should be nil before any migration")
	}
	if _, err := c.AddNodes([]string{s1.addr()}); err == nil {
		t.Fatal("expected duplicate AddNodes to fail")
	}
	taskID, err := c.AddNode(s3.addr())
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if !strings.HasPrefix(taskID, "mig-") {
		t.Fatalf("unexpected task id: %q", taskID)
	}
	c.WaitMigration()
	if c.IsMigrating() {
		t.Fatal("migration should be complete after WaitMigration")
	}
	if p := c.MigrationProgress(); p == nil {
		t.Fatal("MigrationProgress should be available after AddNode")
	}

	before := len(c.Nodes())
	c.RemoveNode(s2.addr())
	if len(c.Nodes()) != before-1 {
		t.Fatalf("node count after RemoveNode = %d, want %d", len(c.Nodes()), before-1)
	}
	c.CancelMigration()
}

func TestExecuteReadFallsBackDuringMigration(t *testing.T) {
	oldNode := newFakeClusterNode()
	newNode := newFakeClusterNode()
	s1 := newFakeClusterServer(t, oldNode)
	s2 := newFakeClusterServer(t, newNode)
	defer s1.close()
	defer s2.close()

	c := newTestClusterClient(t, []string{s1.addr()})
	defer c.Close()

	c.mu.Lock()
	c.pools[s2.addr()] = pool.NewPool(s2.addr(), c.poolCfg)
	c.nodes = append(c.nodes, s2.addr())
	c.agent.AddNode(s2.addr())
	c.mu.Unlock()

	var chosenKey string
	c.ringMgr.BeginMigration([]string{s2.addr()})
	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("mig:%d", i)
		should, src, dst := c.ringMgr.ShouldMigrate(key)
		if should && src == s1.addr() && dst == s2.addr() {
			chosenKey = key
			break
		}
	}
	if chosenKey == "" {
		t.Fatal("failed to find a key that migrates to new node")
	}
	oldNode.data[chosenKey] = &fakeClusterEntry{typ: "string", str: "fallback-value"}

	got, err := c.Get(chosenKey)
	if err != nil {
		t.Fatalf("Get with fallback: %v", err)
	}
	if got != "fallback-value" {
		t.Fatalf("Get with fallback = %q, want fallback-value", got)
	}
}

func TestHealthCheckAndBackgroundTicker(t *testing.T) {
	healthy := newFakeClusterNode()
	s1 := newFakeClusterServer(t, healthy)
	defer s1.close()

	c := newTestClusterClient(t, []string{s1.addr(), "127.0.0.1:1"})
	defer c.Close()

	c.HealthCheck()
	c.mu.RLock()
	_, healthyDead := c.dead[s1.addr()]
	_, unhealthyDead := c.dead["127.0.0.1:1"]
	c.mu.RUnlock()
	if healthyDead {
		t.Fatal("healthy node should not be marked dead")
	}
	if !unhealthyDead {
		t.Fatal("unhealthy node should be marked dead")
	}

	stop := c.StartHealthCheck(20 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	stop()
}

func TestKeyDistribution(t *testing.T) {
	nodes := []string{"n1", "n2", "n3"}
	keys := []string{"a", "b", "c", "d", "e", "f"}
	dist := KeyDistribution(nodes, keys, 32)
	if len(dist) != len(nodes) {
		t.Fatalf("distribution size = %d, want %d", len(dist), len(nodes))
	}
	total := 0
	for _, n := range dist {
		total += n
	}
	if total != len(keys) {
		t.Fatalf("distribution total = %d, want %d", total, len(keys))
	}
}
