package engine

import (
	"math"
	"math/rand"
)

const (
	skiplistMaxLevel = 32
	skiplistP        = 0.25
)

// skiplistNode 跳表节点
type skiplistNode struct {
	member  string
	score   float64
	forward []*skiplistNode // 每层的前进指针
	span    []int           // 每层跨越的节点数（用于 rank 计算）
	backward *skiplistNode  // 后退指针（最底层）
}

// skiplistLevel 返回一个随机层高
func skiplistLevel() int {
	level := 1
	for level < skiplistMaxLevel && rand.Float64() < skiplistP {
		level++
	}
	return level
}

// skiplist 跳表
type skiplist struct {
	header *skiplistNode
	tail   *skiplistNode
	length int
	level  int
}

// newSkiplist 创建一个新的跳表
func newSkiplist() *skiplist {
	header := &skiplistNode{
		score:   math.Inf(-1),
		forward: make([]*skiplistNode, skiplistMaxLevel),
		span:    make([]int, skiplistMaxLevel),
	}
	return &skiplist{
		header: header,
		level:  1,
	}
}

// newNode 创建新节点
func newSkiplistNode(level int, score float64, member string) *skiplistNode {
	return &skiplistNode{
		member:  member,
		score:   score,
		forward: make([]*skiplistNode, level),
		span:    make([]int, level),
	}
}

// insert 插入元素，若 member 已存在则更新 score
// 返回是否是新元素
func (sl *skiplist) insert(score float64, member string) bool {
	update := make([]*skiplistNode, skiplistMaxLevel)
	rank := make([]int, skiplistMaxLevel)

	cur := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		if i == sl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for cur.forward[i] != nil &&
			(cur.forward[i].score < score ||
				(cur.forward[i].score == score && cur.forward[i].member < member)) {
			rank[i] += cur.span[i]
			cur = cur.forward[i]
		}
		update[i] = cur
	}

	// 检查是否已存在（相同 member 不同 score）
	// 外部调用者负责先删后插

	lvl := skiplistLevel()
	if lvl > sl.level {
		for i := sl.level; i < lvl; i++ {
			rank[i] = 0
			update[i] = sl.header
			update[i].span[i] = sl.length
		}
		sl.level = lvl
	}

	node := newSkiplistNode(lvl, score, member)
	for i := 0; i < lvl; i++ {
		node.forward[i] = update[i].forward[i]
		update[i].forward[i] = node

		// span = (rank[0] - rank[i]) + 1
		node.span[i] = update[i].span[i] - (rank[0] - rank[i])
		update[i].span[i] = (rank[0] - rank[i]) + 1
	}

	// 高层 span +1
	for i := lvl; i < sl.level; i++ {
		update[i].span[i]++
	}

	// backward 指针
	if update[0] == sl.header {
		node.backward = nil
	} else {
		node.backward = update[0]
	}
	if node.forward[0] != nil {
		node.forward[0].backward = node
	} else {
		sl.tail = node
	}

	sl.length++
	return true
}

// delete 删除 member，返回是否存在
func (sl *skiplist) delete(score float64, member string) bool {
	update := make([]*skiplistNode, skiplistMaxLevel)
	cur := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for cur.forward[i] != nil &&
			(cur.forward[i].score < score ||
				(cur.forward[i].score == score && cur.forward[i].member < member)) {
			cur = cur.forward[i]
		}
		update[i] = cur
	}

	target := cur.forward[0]
	if target == nil || target.score != score || target.member != member {
		return false
	}

	sl.deleteNode(target, update)
	return true
}

func (sl *skiplist) deleteNode(node *skiplistNode, update []*skiplistNode) {
	for i := 0; i < sl.level; i++ {
		if update[i].forward[i] == node {
			update[i].span[i] += node.span[i] - 1
			update[i].forward[i] = node.forward[i]
		} else {
			update[i].span[i]--
		}
	}

	if node.forward[0] != nil {
		node.forward[0].backward = node.backward
	} else {
		sl.tail = node.backward
	}

	for sl.level > 1 && sl.header.forward[sl.level-1] == nil {
		sl.level--
	}
	sl.length--
}

// getScore 查找 member 的 score，未找到返回 (0, false)
func (sl *skiplist) getScore(member string) (float64, bool) {
	// 跳表本身按 score 排序，查 member 需要 O(n)，生产中应配合 hashmap
	// 此处由 ZSetValue 的 map 提供 O(1) 查找，skiplist 只负责有序遍历
	return 0, false
}

// rangeByIndex 按 [start, stop] 索引（0-based）返回 member 列表
// withScore=true 时交替返回 member, score
func (sl *skiplist) rangeByIndex(start, stop int, withScore bool) []string {
	length := sl.length
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop {
		return nil
	}

	// 定位到 start 节点
	cur := sl.header.forward[0]
	for i := 0; i < start; i++ {
		cur = cur.forward[0]
	}

	var result []string
	for i := start; i <= stop && cur != nil; i++ {
		result = append(result, cur.member)
		if withScore {
			result = append(result, formatScore(cur.score))
		}
		cur = cur.forward[0]
	}
	return result
}

// rangeByScore 按 [minScore, maxScore] 返回 member 列表
func (sl *skiplist) rangeByScore(minScore, maxScore float64, withScore bool) []string {
	cur := sl.header.forward[0]
	var result []string
	for cur != nil {
		if cur.score < minScore {
			cur = cur.forward[0]
			continue
		}
		if cur.score > maxScore {
			break
		}
		result = append(result, cur.member)
		if withScore {
			result = append(result, formatScore(cur.score))
		}
		cur = cur.forward[0]
	}
	return result
}

// rank 返回 member 的排名（0-based），未找到返回 -1
func (sl *skiplist) rank(score float64, member string) int {
	rank := 0
	cur := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for cur.forward[i] != nil &&
			(cur.forward[i].score < score ||
				(cur.forward[i].score == score && cur.forward[i].member < member)) {
			rank += cur.span[i]
			cur = cur.forward[i]
		}
	}
	cur = cur.forward[0]
	if cur != nil && cur.score == score && cur.member == member {
		return rank
	}
	return -1
}
