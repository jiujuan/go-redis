// Package server 实现 go-redis 的 TCP 网络服务层（v0.2）。
package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jiujuan/go-redis/config"
	"github.com/jiujuan/go-redis/internal/engine"
)

// Server TCP 服务端
type Server struct {
	cfg        *config.Config
	db         *engine.GoRedis
	listener   net.Listener
	activeConn sync.Map    // map[net.Conn]struct{}
	connCount  atomic.Int64
	closed     atomic.Bool
	wg         sync.WaitGroup
	quit       chan struct{}
}

// NewServer 创建一个新的 TCP 服务端
func NewServer(cfg *config.Config, db *engine.GoRedis) *Server {
	return &Server{
		cfg:  cfg,
		db:   db,
		quit: make(chan struct{}),
	}
}

// Start 启动服务端，阻塞直到 Stop 被调用
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.cfg.Addr())
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.cfg.Addr(), err)
	}
	s.listener = ln
	log.Printf("[go-redis] listening on %s", s.cfg.Addr())

	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.closed.Load() {
				return nil // 正常关闭
			}
			log.Printf("[go-redis] accept error: %v", err)
			continue
		}

		// 连接数限制
		if s.cfg.MaxConnections > 0 && int(s.connCount.Load()) >= s.cfg.MaxConnections {
			log.Printf("[go-redis] max connections reached, rejecting %s", conn.RemoteAddr())
			conn.Close()
			continue
		}

		s.connCount.Add(1)
		s.activeConn.Store(conn, struct{}{})

		s.wg.Add(1)
		go func(c net.Conn) {
			defer func() {
				s.connCount.Add(-1)
				s.activeConn.Delete(c)
				s.wg.Done()
			}()
			s.handleConn(c)
		}(conn)
	}
}

// StartWithContext 带 context 的启动，context 取消时优雅关闭
func (s *Server) StartWithContext(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		s.Stop()
	}()
	return s.Start()
}

// Stop 优雅关闭服务端
func (s *Server) Stop() {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	log.Println("[go-redis] shutting down...")
	if s.listener != nil {
		s.listener.Close()
	}
	// 关闭所有活跃连接
	s.activeConn.Range(func(key, _ interface{}) bool {
		key.(net.Conn).Close()
		return true
	})

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("[go-redis] server stopped gracefully")
	case <-time.After(5 * time.Second):
		log.Println("[go-redis] server force stopped after timeout")
	}
}

// handleConn 处理单个客户端连接
func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	if s.cfg.ReadTimeout > 0 {
		conn.(*net.TCPConn).SetKeepAlive(true)
	}

	h := newHandler(conn, s.db)
	h.serve()
}

// Stats 返回服务端统计信息
func (s *Server) Stats() map[string]interface{} {
	return map[string]interface{}{
		"active_connections": s.connCount.Load(),
		"db_size":            s.db.DBSize(),
		"addr":               s.cfg.Addr(),
	}
}
