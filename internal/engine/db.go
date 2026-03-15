// Package engine 实现 go-redis 的核心存储引擎（v0.1）。
//
// 特性：
//   - 支持 String / Hash / List / Set / ZSet 五种数据结构
//   - 分片锁（256 个分片）减少并发锁竞争
//   - ZSet 使用跳表（skiplist）实现有序结构
//   - 纯内存存储，可直接作为库嵌入程序使用
package engine

import (
	"fmt"
	"strconv"
	"strings"
)

// GoRedis 是对外暴露的主结构体，持有分片存储
type GoRedis struct {
	store      *shardedMap
	shardCount int
}

// Option 是 GoRedis 的选项函数
type Option func(*GoRedis)

// WithShardCount 设置分片数量（必须是 2 的幂次）
func WithShardCount(n int) Option {
	return func(db *GoRedis) {
		if n > 0 && (n&(n-1)) == 0 {
			db.shardCount = n
		}
	}
}

// NewGoRedis 创建一个新的 GoRedis 实例
func NewGoRedis(opts ...Option) *GoRedis {
	db := &GoRedis{
		shardCount: defaultShardCount,
	}
	for _, opt := range opts {
		opt(db)
	}
	db.store = newShardedMap(db.shardCount)
	return db
}

// ========== 通用 Key 操作 ==========

// Del 删除一个或多个 key，返回实际删除数量
func (db *GoRedis) Del(keys ...string) int {
	deleted := 0
	for _, k := range keys {
		if db.store.del(k) {
			deleted++
		}
	}
	return deleted
}

// Exists 检查 key 是否存在
func (db *GoRedis) Exists(keys ...string) int {
	count := 0
	for _, k := range keys {
		if db.store.exists(k) {
			count++
		}
	}
	return count
}

// Type 返回 key 存储的值类型
func (db *GoRedis) Type(key string) string {
	var result string
	db.store.withReadLock(key, func(data map[string]interface{}) {
		e, ok := data[key]
		if !ok {
			result = "none"
			return
		}
		result = e.(*entry).typ.String()
	})
	return result
}

// Keys 返回所有匹配 pattern 的 key（支持 * 通配符）
// 注意：生产环境慎用，会遍历所有分片
func (db *GoRedis) Keys(pattern string) []string {
	all := db.store.keys()
	if pattern == "*" {
		return all
	}
	var matched []string
	for _, k := range all {
		if matchPattern(pattern, k) {
			matched = append(matched, k)
		}
	}
	return matched
}

// DBSize 返回数据库中 key 的总数量
func (db *GoRedis) DBSize() int {
	return db.store.count()
}

// FlushDB 清空所有数据
func (db *GoRedis) FlushDB() {
	db.store.flush()
}

// Rename 重命名 key
func (db *GoRedis) Rename(oldKey, newKey string) error {
	val, ok := db.store.get(oldKey)
	if !ok {
		return ErrKeyNotFound
	}
	db.store.set(newKey, val)
	db.store.del(oldKey)
	return nil
}

// RenameNX 仅当 newKey 不存在时重命名，返回是否成功
func (db *GoRedis) RenameNX(oldKey, newKey string) (bool, error) {
	val, ok := db.store.get(oldKey)
	if !ok {
		return false, ErrKeyNotFound
	}
	if db.store.exists(newKey) {
		return false, nil
	}
	db.store.set(newKey, val)
	db.store.del(oldKey)
	return true, nil
}

// ========== 工具函数 ==========

func errorf(format string, args ...interface{}) error {
	return fmt.Errorf(format, args...)
}

func parseInt64(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	return strconv.ParseInt(s, 10, 64)
}

func formatInt64(n int64) string {
	return strconv.FormatInt(n, 10)
}

// matchPattern 简单的 glob 模式匹配，支持 * 和 ?
func matchPattern(pattern, str string) bool {
	return matchHelper(pattern, str)
}

func matchHelper(p, s string) bool {
	if p == "" {
		return s == ""
	}
	if p == "*" {
		return true
	}

	// 找第一个 * 的位置
	starIdx := strings.IndexByte(p, '*')
	if starIdx == -1 {
		// 没有 *，逐字符对比（支持 ?）
		if len(p) != len(s) {
			return false
		}
		for i := range p {
			if p[i] != '?' && p[i] != s[i] {
				return false
			}
		}
		return true
	}

	// 处理 * 之前的部分
	prefix := p[:starIdx]
	for i, c := range prefix {
		if i >= len(s) || (c != '?' && byte(c) != s[i]) {
			return false
		}
	}

	// 递归匹配 * 之后的部分
	rest := p[starIdx+1:]
	for i := starIdx; i <= len(s); i++ {
		if matchHelper(rest, s[i:]) {
			return true
		}
	}
	return false
}
