package migration

import (
	"fmt"
	"hash/crc32"
	"sort"
)

// KeyDistributionTest 统计各节点 key 分布（供测试使用）
func KeyDistributionTest(nodes []string, keys []string, virtualReplicas int) map[string]int {
	ring := make([]uint32, 0)
	ring2node := make(map[uint32]string)

	for _, node := range nodes {
		for i := 0; i < virtualReplicas; i++ {
			vkey := fmt.Sprintf("%s#%d", node, i)
			h := crc32.ChecksumIEEE([]byte(vkey))
			ring = append(ring, h)
			ring2node[h] = node
		}
	}
	sort.Slice(ring, func(i, j int) bool { return ring[i] < ring[j] })

	dist := make(map[string]int)
	for _, node := range nodes {
		dist[node] = 0
	}
	for _, key := range keys {
		h := crc32.ChecksumIEEE([]byte(key))
		idx := sort.Search(len(ring), func(i int) bool { return ring[i] >= h })
		if idx == len(ring) {
			idx = 0
		}
		dist[ring2node[ring[idx]]]++
	}
	return dist
}
