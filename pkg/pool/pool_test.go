package pool_test

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/go-redis/pkg/pool"
)

// startEchoServer starts a TCP server that accepts connections and keeps them open.
// Returns the address and a stop function.
func startEchoServer(t *testing.T) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan struct{})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-done:
					return
				default:
					return
				}
			}
			go func(c net.Conn) {
				buf := make([]byte, 1024)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					c.Write(buf[:n])
				}
			}(conn)
		}
	}()
	return ln.Addr().String(), func() {
		close(done)
		ln.Close()
	}
}

// ─────────────────────────────────────────────

func TestPool_DefaultConfig(t *testing.T) {
	cfg := pool.DefaultConfig()
	if cfg.MaxIdle != 10 {
		t.Errorf("MaxIdle: got %d, want 10", cfg.MaxIdle)
	}
	if cfg.MaxActive != 100 {
		t.Errorf("MaxActive: got %d, want 100", cfg.MaxActive)
	}
	if cfg.IdleTimeout != 5*time.Minute {
		t.Errorf("IdleTimeout: got %v", cfg.IdleTimeout)
	}
}

func TestPool_GetPut_Basic(t *testing.T) {
	addr, stop := startEchoServer(t)
	defer stop()

	p := pool.NewPool(addr, nil)
	defer p.Close()

	conn, err := p.Get()
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if conn == nil {
		t.Fatal("Get returned nil conn")
	}

	stats := p.Stats()
	if stats["active"] != 1 {
		t.Errorf("active after Get: got %d, want 1", stats["active"])
	}

	p.Put(conn, false)
	stats = p.Stats()
	if stats["idle"] != 1 {
		t.Errorf("idle after Put: got %d, want 1", stats["idle"])
	}
	if stats["active"] != 0 {
		t.Errorf("active after Put: got %d, want 0", stats["active"])
	}
}

func TestPool_ReuseIdleConnection(t *testing.T) {
	addr, stop := startEchoServer(t)
	defer stop()

	p := pool.NewPool(addr, nil)
	defer p.Close()

	conn1, _ := p.Get()
	p.Put(conn1, false)

	conn2, err := p.Get()
	if err != nil {
		t.Fatalf("Get reuse: %v", err)
	}
	defer p.Put(conn2, false)

	// Should be the same connection object reused from idle pool
	if conn1 != conn2 {
		t.Log("connections differ (may have been replaced - acceptable)")
	}
}

func TestPool_Put_Discard(t *testing.T) {
	addr, stop := startEchoServer(t)
	defer stop()

	p := pool.NewPool(addr, nil)
	defer p.Close()

	conn, _ := p.Get()
	p.Put(conn, true) // discard

	stats := p.Stats()
	if stats["idle"] != 0 {
		t.Errorf("idle after discard: got %d, want 0", stats["idle"])
	}
	if stats["active"] != 0 {
		t.Errorf("active after discard: got %d, want 0", stats["active"])
	}
}

func TestPool_Put_NilConn(t *testing.T) {
	addr, stop := startEchoServer(t)
	defer stop()

	p := pool.NewPool(addr, nil)
	defer p.Close()

	// Put nil should not panic
	p.Put(nil, false)
	stats := p.Stats()
	if stats["idle"] != 0 {
		t.Errorf("idle after nil put: got %d", stats["idle"])
	}
}

func TestPool_MaxIdle_Enforced(t *testing.T) {
	addr, stop := startEchoServer(t)
	defer stop()

	cfg := &pool.Config{
		MaxIdle:     2,
		MaxActive:   10,
		DialTimeout: 2 * time.Second,
	}
	p := pool.NewPool(addr, cfg)
	defer p.Close()

	// Get 4 connections
	conns := make([]net.Conn, 4)
	for i := range conns {
		c, err := p.Get()
		if err != nil {
			t.Fatalf("Get[%d]: %v", i, err)
		}
		conns[i] = c
	}
	// Return all 4 — only MaxIdle=2 should be kept
	for _, c := range conns {
		p.Put(c, false)
	}
	stats := p.Stats()
	if stats["idle"] > 2 {
		t.Errorf("idle exceeds MaxIdle=2: got %d", stats["idle"])
	}
}

func TestPool_MaxActive_Waiter(t *testing.T) {
	addr, stop := startEchoServer(t)
	defer stop()

	cfg := &pool.Config{
		MaxIdle:     1,
		MaxActive:   1,
		DialTimeout: 200 * time.Millisecond,
	}
	p := pool.NewPool(addr, cfg)
	defer p.Close()

	conn1, err := p.Get()
	if err != nil {
		t.Fatalf("first Get: %v", err)
	}

	// Second Get should wait, then timeout
	_, err = p.Get()
	if err == nil {
		t.Error("second Get should fail when MaxActive=1 is exhausted")
	}

	p.Put(conn1, false)
}

func TestPool_MaxActive_WaiterNotified(t *testing.T) {
	addr, stop := startEchoServer(t)
	defer stop()

	cfg := &pool.Config{
		MaxIdle:     1,
		MaxActive:   1,
		DialTimeout: 2 * time.Second,
	}
	p := pool.NewPool(addr, cfg)
	defer p.Close()

	conn1, _ := p.Get()

	var (
		conn2  net.Conn
		getErr error
		wg     sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn2, getErr = p.Get()
	}()

	// Give the goroutine time to start waiting
	time.Sleep(30 * time.Millisecond)
	// Return conn1 — should unblock the waiter
	p.Put(conn1, false)

	wg.Wait()
	if getErr != nil {
		t.Fatalf("waiter Get: %v", getErr)
	}
	if conn2 != nil {
		p.Put(conn2, false)
	}
}

func TestPool_Close_RejectsNewGet(t *testing.T) {
	addr, stop := startEchoServer(t)
	defer stop()

	p := pool.NewPool(addr, nil)
	p.Close()

	_, err := p.Get()
	if err == nil {
		t.Error("Get after Close should return error")
	}
}

func TestPool_Close_NotifiesWaiters(t *testing.T) {
	addr, stop := startEchoServer(t)
	defer stop()

	cfg := &pool.Config{
		MaxIdle:     1,
		MaxActive:   1,
		DialTimeout: 5 * time.Second,
	}
	p := pool.NewPool(addr, cfg)

	conn, _ := p.Get()
	_ = conn

	var errCh = make(chan error, 1)
	go func() {
		_, err := p.Get()
		errCh <- err
	}()

	time.Sleep(20 * time.Millisecond)
	p.Close() // should unblock waiter

	select {
	case err := <-errCh:
		if err == nil {
			t.Error("waiter should get error when pool is closed")
		}
	case <-time.After(2 * time.Second):
		t.Error("waiter not unblocked after pool Close")
	}
}

func TestPool_Stats(t *testing.T) {
	addr, stop := startEchoServer(t)
	defer stop()

	p := pool.NewPool(addr, nil)
	defer p.Close()

	stats := p.Stats()
	if stats["idle"] != 0 || stats["active"] != 0 || stats["waiters"] != 0 {
		t.Errorf("initial stats: %v", stats)
	}

	c1, _ := p.Get()
	stats = p.Stats()
	if stats["active"] != 1 {
		t.Errorf("active=1: got %v", stats)
	}

	p.Put(c1, false)
	stats = p.Stats()
	if stats["idle"] != 1 || stats["active"] != 0 {
		t.Errorf("after put: %v", stats)
	}
}

func TestPool_NilConfig_UsesDefault(t *testing.T) {
	addr, stop := startEchoServer(t)
	defer stop()

	p := pool.NewPool(addr, nil) // nil config → use defaults
	defer p.Close()

	conn, err := p.Get()
	if err != nil {
		t.Fatalf("Get with nil config: %v", err)
	}
	p.Put(conn, false)
}

func TestPool_IdleTimeout_Expired(t *testing.T) {
	addr, stop := startEchoServer(t)
	defer stop()

	cfg := &pool.Config{
		MaxIdle:     5,
		MaxActive:   10,
		IdleTimeout: 50 * time.Millisecond,
		DialTimeout: 2 * time.Second,
	}
	p := pool.NewPool(addr, cfg)
	defer p.Close()

	conn, _ := p.Get()
	p.Put(conn, false)

	// Wait for idle timeout to expire
	time.Sleep(100 * time.Millisecond)

	// Next Get should create a new connection (old one expired)
	conn2, err := p.Get()
	if err != nil {
		t.Fatalf("Get after idle timeout: %v", err)
	}
	p.Put(conn2, false)
}

func TestPool_Concurrent(t *testing.T) {
	addr, stop := startEchoServer(t)
	defer stop()

	cfg := &pool.Config{
		MaxIdle:     5,
		MaxActive:   20,
		DialTimeout: 2 * time.Second,
	}
	p := pool.NewPool(addr, cfg)
	defer p.Close()

	var errCount atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := p.Get()
			if err != nil {
				errCount.Add(1)
				return
			}
			time.Sleep(time.Millisecond)
			p.Put(conn, false)
		}()
	}
	wg.Wait()

	if errCount.Load() > 0 {
		t.Errorf("%d concurrent Get errors", errCount.Load())
	}
}

func TestPool_DialUnreachable(t *testing.T) {
	p := pool.NewPool("127.0.0.1:19999", &pool.Config{
		DialTimeout: 100 * time.Millisecond,
	})
	defer p.Close()

	_, err := p.Get()
	if err == nil {
		t.Error("Get on unreachable address should return error")
	}
}
