package server

import (
	"io"
	"log"
	"net"

	"github.com/jiujuan/go-redis/internal/engine"
	"github.com/jiujuan/go-redis/internal/resp"
)

// handler 处理单个连接的请求-响应循环
type handler struct {
	conn   net.Conn
	db     *engine.GoRedis
	reader *resp.Reader
	writer *resp.Writer
}

func newHandler(conn net.Conn, db *engine.GoRedis) *handler {
	return &handler{
		conn:   conn,
		db:     db,
		reader: resp.NewReader(conn),
		writer: resp.NewWriter(conn),
	}
}

// serve 请求-响应主循环
func (h *handler) serve() {
	addr := h.conn.RemoteAddr().String()
	log.Printf("[go-redis] client connected: %s", addr)
	defer log.Printf("[go-redis] client disconnected: %s", addr)

	for {
		val, err := h.reader.Read()
		if err != nil {
			if err != io.EOF {
				log.Printf("[go-redis] read from %s error: %v", addr, err)
			}
			return
		}

		args, err := val.ToArgs()
		if err != nil {
			h.writer.WriteError("protocol error: " + err.Error())
			h.writer.Flush()
			continue
		}

		if err := dispatch(h.db, args, h.writer); err != nil {
			log.Printf("[go-redis] dispatch error from %s: %v", addr, err)
			return
		}

		if err := h.writer.Flush(); err != nil {
			log.Printf("[go-redis] flush to %s error: %v", addr, err)
			return
		}
	}
}
