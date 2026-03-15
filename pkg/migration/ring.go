package migration

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

// hashRing 一致性哈希环（不可变快照，读多写少）
type hashRing struct {
	ring      []uint32
	ring2node map[uint32]string
	nodes     []string
	replicas  int
}

func buildRing(nodes []string, virtualReplicas int) *hashRing {
	hr := &hashRing{
		ring2node: make(map[uint32]string),
		nodes:     make([]string, len(nodes)),
		replicas:  virtualReplicas,
	}
	copy(hr.nodes, nodes)
	for _, node := range nodes {
		for i := 0; i < virtualReplicas; i++ {
			vkey := fmt.Sprintf("%s#%d", node, i)
			h := crc32.ChecksumIEEE([]byte(vkey))
			hr.ring = append(hr.ring, h)
			hr.ring2node[h] = node
		}
	}
	sort.Slice(hr.ring, func(i, j int) bool { return hr.ring[i] < hr.ring[j] })
	return hr
}

// lookup 根据 key 找到对应节点
func (hr *hashRing) lookup(key string) string {
	if len(hr.ring) == 0 {
		return ""
	}
	h := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(hr.ring), func(i int) bool { return hr.ring[i] >= h })
	if idx == len(hr.ring) {
		idx = 0
	}
	return hr.ring2node[hr.ring[idx]]
}

// RingManager 管理双环路由（正常态单环，迁移中双环）
//
// 读策略：newRing 优先，miss 时兜底 oldRing
// 写策略：始终写 newRing
type RingManager struct {
	mu              sync.RWMutex
	newRing         *hashRing // 当前有效环（含新节点）
	oldRing         *hashRing // 迁移期间的旧环快照，正常态为 nil
	virtualReplicas int
	migrating       bool
}

// NewRingManager 创建环管理器
func NewRingManager(nodes []string, virtualReplicas int) *RingManager {
	return &RingManager{
		newRing:         buildRing(nodes, virtualReplicas),
		virtualReplicas: virtualReplicas,
	}
}

// GetWriteNode 写操作：始终走 newRing
func (rm *RingManager) GetWriteNode(key string) string {
	rm.mu.RLock()
	node := rm.newRing.lookup(key)
	rm.mu.RUnlock()
	return node
}

// GetReadNodes 读操作：返回 (primaryNode, fallbackNode)
// primaryNode 是 newRing 对应节点
// fallbackNode 是 oldRing 对应节点（迁移期间 key 可能还在旧节点，非迁移期间为空字符串）
func (rm *RingManager) GetReadNodes(key string) (primary, fallback string) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	primary = rm.newRing.lookup(key)
	if rm.oldRing != nil && rm.migrating {
		fallback = rm.oldRing.lookup(key)
		if fallback == primary {
			fallback = "" // 同一节点，无需兜底
		}
	}
	return
}

// BeginMigration 开始迁移：保存旧环快照，更新新环（含新节点）
// 返回需要迁移的 key 映射：sourceNode -> targetNode（只返回路由变化的节点对）
func (rm *RingManager) BeginMigration(newNodes []string) map[string]string {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// 保存旧环
	rm.oldRing = rm.newRing

	// 构建新环（旧节点 + 新节点）
	allNodes := make([]string, len(rm.oldRing.nodes))
	copy(allNodes, rm.oldRing.nodes)
	allNodes = append(allNodes, newNodes...)
	rm.newRing = buildRing(allNodes, rm.virtualReplicas)
	rm.migrating = true

	// 计算路由变化：对旧环上每个节点，找出在新环中会路由到不同节点的 key 区间
	// 这里返回可能受影响的 source->target 节点对（用于 migrator 扫描）
	routeChanges := make(map[string]string) // sourceNode -> targetNode
	for _, vhash := range rm.oldRing.ring {
		oldNode := rm.oldRing.ring2node[vhash]
		// 找出在新环中，原来属于 oldNode 的那段弧上，新路由到哪个节点
		// 实际迁移时，migrator 会逐 key 检查 newRing.lookup(key) != oldRing.lookup(key)
		newNode := rm.newRing.lookup(fmt.Sprintf("__probe_%d__", vhash))
		if newNode != oldNode {
			routeChanges[oldNode] = newNode
		}
	}
	return routeChanges
}

// FinishMigration 迁移完成：清除旧环，切换为纯单环模式
func (rm *RingManager) FinishMigration() {
	rm.mu.Lock()
	rm.oldRing = nil
	rm.migrating = false
	rm.mu.Unlock()
}

// AbortMigration 终止迁移：回滚到旧环
func (rm *RingManager) AbortMigration() {
	rm.mu.Lock()
	if rm.oldRing != nil {
		rm.newRing = rm.oldRing
		rm.oldRing = nil
	}
	rm.migrating = false
	rm.mu.Unlock()
}

// IsMigrating 是否正在迁移
func (rm *RingManager) IsMigrating() bool {
	rm.mu.RLock()
	v := rm.migrating
	rm.mu.RUnlock()
	return v
}

// Nodes 返回当前新环的节点列表
func (rm *RingManager) Nodes() []string {
	rm.mu.RLock()
	result := make([]string, len(rm.newRing.nodes))
	copy(result, rm.newRing.nodes)
	rm.mu.RUnlock()
	return result
}

// OldNodes 返回旧环节点列表（迁移期间）
func (rm *RingManager) OldNodes() []string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	if rm.oldRing == nil {
		return nil
	}
	result := make([]string, len(rm.oldRing.nodes))
	copy(result, rm.oldRing.nodes)
	return result
}

// ShouldMigrate 判断某个 key 是否需要从 sourceNode 迁移到 targetNode
// 即：在旧环中路由到 sourceNode，在新环中路由到不同的 targetNode
func (rm *RingManager) ShouldMigrate(key string) (should bool, sourceNode, targetNode string) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	if rm.oldRing == nil {
		return false, "", ""
	}
	oldNode := rm.oldRing.lookup(key)
	newNode := rm.newRing.lookup(key)
	if oldNode == newNode {
		return false, oldNode, newNode
	}
	return true, oldNode, newNode
}
