// Package pool 实现 TCP 连接池（v0.3）。
package pool

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// Config 连接池配置
type Config struct {
	MaxIdle     int           // 最大空闲连接数
	MaxActive   int           // 最大活跃连接数（0 = 不限）
	IdleTimeout time.Duration // 空闲连接超时
	DialTimeout time.Duration // 建连超时
	ReadTimeout time.Duration // 读超时
}

// DefaultConfig 返回默认连接池配置
func DefaultConfig() *Config {
	return &Config{
		MaxIdle:     10,
		MaxActive:   100,
		IdleTimeout: 5 * time.Minute,
		DialTimeout: 3 * time.Second,
		ReadTimeout: 5 * time.Second,
	}
}

// idleConn 带时间戳的空闲连接
type idleConn struct {
	conn      net.Conn
	createdAt time.Time
	lastUsed  time.Time
}

// Pool TCP 连接池
type Pool struct {
	mu      sync.Mutex
	addr    string
	cfg     *Config
	idle    []*idleConn
	active  int
	closed  bool
	waiters []chan net.Conn
}

// NewPool 创建连接池
func NewPool(addr string, cfg *Config) *Pool {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	p := &Pool{
		addr: addr,
		cfg:  cfg,
	}
	return p
}

// Get 从连接池获取一个连接
func (p *Pool) Get() (net.Conn, error) {
	p.mu.Lock()

	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("pool is closed")
	}

	// 取出有效的空闲连接
	for len(p.idle) > 0 {
		ic := p.idle[len(p.idle)-1]
		p.idle = p.idle[:len(p.idle)-1]

		// 检查空闲超时
		if p.cfg.IdleTimeout > 0 && time.Since(ic.lastUsed) > p.cfg.IdleTimeout {
			ic.conn.Close()
			continue
		}
		p.active++
		p.mu.Unlock()
		return ic.conn, nil
	}

	// 检查是否达到最大活跃连接数
	if p.cfg.MaxActive > 0 && p.active >= p.cfg.MaxActive {
		// 等待归还
		ch := make(chan net.Conn, 1)
		p.waiters = append(p.waiters, ch)
		p.mu.Unlock()

		select {
		case conn := <-ch:
			if conn == nil {
				return nil, fmt.Errorf("pool closed while waiting")
			}
			return conn, nil
		case <-time.After(p.cfg.DialTimeout):
			return nil, fmt.Errorf("connection pool exhausted, timeout waiting")
		}
	}

	p.active++
	p.mu.Unlock()

	// 建立新连接
	conn, err := p.dial()
	if err != nil {
		p.mu.Lock()
		p.active--
		p.mu.Unlock()
		return nil, err
	}
	return conn, nil
}

// Put 归还连接到连接池
func (p *Pool) Put(conn net.Conn, discard bool) {
	if discard || conn == nil {
		if conn != nil {
			conn.Close()
		}
		p.mu.Lock()
		p.active--
		p.notifyWaiter(nil)
		p.mu.Unlock()
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.active--

	// 有等待者，直接转交
	if len(p.waiters) > 0 {
		ch := p.waiters[0]
		p.waiters = p.waiters[1:]
		p.active++ // 转交给等待者，active 不变
		ch <- conn
		return
	}

	// 空闲连接超限
	if p.cfg.MaxIdle > 0 && len(p.idle) >= p.cfg.MaxIdle {
		conn.Close()
		return
	}

	p.idle = append(p.idle, &idleConn{
		conn:      conn,
		createdAt: time.Now(),
		lastUsed:  time.Now(),
	})
}

func (p *Pool) notifyWaiter(conn net.Conn) {
	if len(p.waiters) > 0 {
		ch := p.waiters[0]
		p.waiters = p.waiters[1:]
		ch <- conn
	}
}

// Close 关闭连接池，断开所有空闲连接
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true
	for _, ic := range p.idle {
		ic.conn.Close()
	}
	p.idle = nil

	// 通知等待者
	for _, ch := range p.waiters {
		ch <- nil
	}
	p.waiters = nil
}

// Stats 返回连接池统计信息
func (p *Pool) Stats() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return map[string]int{
		"idle":    len(p.idle),
		"active":  p.active,
		"waiters": len(p.waiters),
	}
}

func (p *Pool) dial() (net.Conn, error) {
	timeout := p.cfg.DialTimeout
	if timeout == 0 {
		timeout = 3 * time.Second
	}
	conn, err := net.DialTimeout("tcp", p.addr, timeout)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", p.addr, err)
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(30 * time.Second)
	}
	return conn, nil
}
