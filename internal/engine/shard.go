package engine

import (
	"sync"
)

// shardCount 分片数量，必须是 2 的幂次，便于位运算取模
const defaultShardCount = 256

// shard 单个分片：一把读写锁 + 一个 map
type shard struct {
	mu   sync.RWMutex
	data map[string]interface{}
}

// shardedMap 分片哈希表，减少锁竞争
type shardedMap struct {
	shards    []*shard
	shardMask uint32 // shardCount - 1，用于位运算取模
}

// newShardedMap 创建分片哈希表
func newShardedMap(shardCount int) *shardedMap {
	if shardCount <= 0 || (shardCount&(shardCount-1)) != 0 {
		shardCount = defaultShardCount
	}
	shards := make([]*shard, shardCount)
	for i := range shards {
		shards[i] = &shard{
			data: make(map[string]interface{}),
		}
	}
	return &shardedMap{
		shards:    shards,
		shardMask: uint32(shardCount - 1),
	}
}

// getShard 根据 key 获取对应分片
func (m *shardedMap) getShard(key string) *shard {
	hash := fnv32(key)
	return m.shards[hash&m.shardMask]
}

// get 读取 key 对应的值
func (m *shardedMap) get(key string) (interface{}, bool) {
	s := m.getShard(key)
	s.mu.RLock()
	val, ok := s.data[key]
	s.mu.RUnlock()
	return val, ok
}

// set 写入 key-value
func (m *shardedMap) set(key string, val interface{}) {
	s := m.getShard(key)
	s.mu.Lock()
	s.data[key] = val
	s.mu.Unlock()
}

// del 删除 key，返回是否存在
func (m *shardedMap) del(key string) bool {
	s := m.getShard(key)
	s.mu.Lock()
	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
	}
	s.mu.Unlock()
	return ok
}

// exists 判断 key 是否存在
func (m *shardedMap) exists(key string) bool {
	s := m.getShard(key)
	s.mu.RLock()
	_, ok := s.data[key]
	s.mu.RUnlock()
	return ok
}

// getOrSet 原子读取或写入：若 key 不存在则调用 fn 创建并写入
// 返回最终值
func (m *shardedMap) getOrSet(key string, fn func() interface{}) interface{} {
	s := m.getShard(key)

	// 先尝试读
	s.mu.RLock()
	val, ok := s.data[key]
	s.mu.RUnlock()
	if ok {
		return val
	}

	// 写锁创建
	s.mu.Lock()
	defer s.mu.Unlock()
	// double-check
	if val, ok = s.data[key]; ok {
		return val
	}
	val = fn()
	s.data[key] = val
	return val
}

// withWriteLock 在写锁保护下执行 fn
func (m *shardedMap) withWriteLock(key string, fn func(data map[string]interface{})) {
	s := m.getShard(key)
	s.mu.Lock()
	fn(s.data)
	s.mu.Unlock()
}

// withReadLock 在读锁保护下执行 fn
func (m *shardedMap) withReadLock(key string, fn func(data map[string]interface{})) {
	s := m.getShard(key)
	s.mu.RLock()
	fn(s.data)
	s.mu.RUnlock()
}

// keys 返回所有 key（遍历所有分片，加读锁）
func (m *shardedMap) keys() []string {
	var result []string
	for _, s := range m.shards {
		s.mu.RLock()
		for k := range s.data {
			result = append(result, k)
		}
		s.mu.RUnlock()
	}
	return result
}

// count 返回总 key 数量
func (m *shardedMap) count() int {
	total := 0
	for _, s := range m.shards {
		s.mu.RLock()
		total += len(s.data)
		s.mu.RUnlock()
	}
	return total
}

// flush 清空所有数据
func (m *shardedMap) flush() {
	for _, s := range m.shards {
		s.mu.Lock()
		s.data = make(map[string]interface{})
		s.mu.Unlock()
	}
}

// fnv32 FNV-1a 哈希，速度快，分布均匀
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= prime32
	}
	return hash
}
