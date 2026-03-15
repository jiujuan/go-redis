package persistence

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"time"
)

// RDBEntry RDB 快照中的一条记录
type RDBEntry struct {
	Type  uint8  // 1=string 2=hash 3=list 4=set 5=zset
	Key   string
	Value interface{} // 根据 Type 不同
}

// RDBStringVal String 值
type RDBStringVal struct {
	Val string
}

// RDBHashVal Hash 值
type RDBHashVal struct {
	Fields map[string]string
}

// RDBListVal List 值
type RDBListVal struct {
	Items []string
}

// RDBSetVal Set 值
type RDBSetVal struct {
		Members []string
}

// RDBZSetVal ZSet 值
type RDBZSetVal struct {
	Members []string
	Scores  []float64
}

// RDBSnapshot 完整快照
type RDBSnapshot struct {
	CreatedAt time.Time
	Entries   []*RDBEntry
}

// SaveRDB 将快照数据写入文件
func SaveRDB(filename string, snapshot *RDBSnapshot) error {
	tmp := filename + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create rdb tmp: %w", err)
	}

	w := bufio.NewWriterSize(f, 1<<20) // 1MB 缓冲
	enc := gob.NewEncoder(w)

	if err := enc.Encode(snapshot); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("encode rdb: %w", err)
	}

	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("flush rdb: %w", err)
	}
	f.Sync()
	f.Close()

	// 原子替换
	if err := os.Rename(tmp, filename); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename rdb: %w", err)
	}

	log.Printf("[rdb] saved %d entries to %s", len(snapshot.Entries), filename)
	return nil
}

// LoadRDB 从文件加载快照
func LoadRDB(filename string) (*RDBSnapshot, error) {
	f, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("open rdb: %w", err)
	}
	defer f.Close()

	dec := gob.NewDecoder(bufio.NewReader(f))
	var snapshot RDBSnapshot
	if err := dec.Decode(&snapshot); err != nil {
		return nil, fmt.Errorf("decode rdb: %w", err)
	}

	log.Printf("[rdb] loaded %d entries from %s (created at %s)",
		len(snapshot.Entries), filename, snapshot.CreatedAt.Format(time.RFC3339))
	return &snapshot, nil
}

// init 注册 gob 类型
func init() {
	gob.Register(&RDBStringVal{})
	gob.Register(&RDBHashVal{})
	gob.Register(&RDBListVal{})
	gob.Register(&RDBSetVal{})
	gob.Register(&RDBZSetVal{})
}
