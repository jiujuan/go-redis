package engine

import "sync"

// ZSetValue 有序集合数据结构的内部值
// 使用 map 提供 O(1) 的 score 查询，skiplist 提供有序遍历
type ZSetValue struct {
	mu      sync.RWMutex
	scores  map[string]float64 // member -> score，O(1) 查询
	sl      *skiplist          // 有序结构，支持范围查询
}

func newZSetValue() *ZSetValue {
	return &ZSetValue{
		scores: make(map[string]float64),
		sl:     newSkiplist(),
	}
}

// ========== ZSet ==========

func (db *GoRedis) getOrCreateZSet(key string) (*ZSetValue, error) {
	var zv *ZSetValue
	var err error
	db.store.withWriteLock(key, func(data map[string]interface{}) {
		if e, ok := data[key]; ok {
			en := e.(*entry)
			if en.typ != TypeZSet {
				err = ErrWrongType
				return
			}
			zv = en.val.(*ZSetValue)
		} else {
			zv = newZSetValue()
			data[key] = &entry{typ: TypeZSet, val: zv}
		}
	})
	return zv, err
}

func (db *GoRedis) getZSet(key string) (*ZSetValue, error) {
	var zv *ZSetValue
	var err error
	db.store.withReadLock(key, func(data map[string]interface{}) {
		if e, ok := data[key]; ok {
			en := e.(*entry)
			if en.typ != TypeZSet {
				err = ErrWrongType
				return
			}
			zv = en.val.(*ZSetValue)
		}
	})
	return zv, err
}

// ZAdd 向有序集合添加成员，若已存在则更新 score，返回新增数量
func (db *GoRedis) ZAdd(key string, score float64, member string) (int, error) {
	zv, err := db.getOrCreateZSet(key)
	if err != nil {
		return 0, err
	}

	added := 0
	zv.mu.Lock()
	if oldScore, exists := zv.scores[member]; exists {
		// 更新：先从跳表删除旧节点，再插入新节点
		zv.sl.delete(oldScore, member)
	} else {
		added = 1
	}
	zv.scores[member] = score
	zv.sl.insert(score, member)
	zv.mu.Unlock()
	return added, nil
}

// ZScore 返回有序集合中成员的 score
func (db *GoRedis) ZScore(key, member string) (float64, bool, error) {
	zv, err := db.getZSet(key)
	if err != nil {
		return 0, false, err
	}
	if zv == nil {
		return 0, false, nil
	}

	zv.mu.RLock()
	score, ok := zv.scores[member]
	zv.mu.RUnlock()
	return score, ok, nil
}

// ZRem 从有序集合中删除一个或多个成员，返回删除数量
func (db *GoRedis) ZRem(key string, members ...string) (int, error) {
	zv, err := db.getZSet(key)
	if err != nil {
		return 0, err
	}
	if zv == nil {
		return 0, nil
	}

	removed := 0
	zv.mu.Lock()
	for _, m := range members {
		if score, ok := zv.scores[m]; ok {
			zv.sl.delete(score, m)
			delete(zv.scores, m)
			removed++
		}
	}
	empty := len(zv.scores) == 0
	zv.mu.Unlock()

	if empty && removed > 0 {
		db.Del(key)
	}
	return removed, nil
}

// ZRange 返回有序集合中按索引 [start, stop] 范围的成员（升序）
// withScore=true 时结果中交替包含 member 和 score
func (db *GoRedis) ZRange(key string, start, stop int, withScore bool) ([]string, error) {
	zv, err := db.getZSet(key)
	if err != nil {
		return nil, err
	}
	if zv == nil {
		return []string{}, nil
	}

	zv.mu.RLock()
	result := zv.sl.rangeByIndex(start, stop, withScore)
	zv.mu.RUnlock()
	return result, nil
}

// ZRevRange 返回有序集合中按索引 [start, stop] 范围的成员（降序）
func (db *GoRedis) ZRevRange(key string, start, stop int, withScore bool) ([]string, error) {
	result, err := db.ZRange(key, start, stop, withScore)
	if err != nil {
		return nil, err
	}
	// 反转结果
	reverseStrings(result, withScore)
	return result, nil
}

// ZRangeByScore 返回 score 在 [minScore, maxScore] 范围内的成员
func (db *GoRedis) ZRangeByScore(key string, minScore, maxScore float64, withScore bool) ([]string, error) {
	zv, err := db.getZSet(key)
	if err != nil {
		return nil, err
	}
	if zv == nil {
		return []string{}, nil
	}

	zv.mu.RLock()
	result := zv.sl.rangeByScore(minScore, maxScore, withScore)
	zv.mu.RUnlock()
	return result, nil
}

// ZCard 返回有序集合的成员数量
func (db *GoRedis) ZCard(key string) (int, error) {
	zv, err := db.getZSet(key)
	if err != nil {
		return 0, err
	}
	if zv == nil {
		return 0, nil
	}

	zv.mu.RLock()
	count := len(zv.scores)
	zv.mu.RUnlock()
	return count, nil
}

// ZRank 返回成员在有序集合中的排名（升序，0-based），不存在返回 -1
func (db *GoRedis) ZRank(key, member string) (int, error) {
	zv, err := db.getZSet(key)
	if err != nil {
		return -1, err
	}
	if zv == nil {
		return -1, nil
	}

	zv.mu.RLock()
	score, ok := zv.scores[member]
	var rank int
	if ok {
		rank = zv.sl.rank(score, member)
	} else {
		rank = -1
	}
	zv.mu.RUnlock()
	return rank, nil
}

// ZIncrBy 将有序集合中成员的 score 增加 delta，返回新 score
func (db *GoRedis) ZIncrBy(key string, delta float64, member string) (float64, error) {
	zv, err := db.getOrCreateZSet(key)
	if err != nil {
		return 0, err
	}

	var newScore float64
	zv.mu.Lock()
	if oldScore, ok := zv.scores[member]; ok {
		zv.sl.delete(oldScore, member)
		newScore = oldScore + delta
	} else {
		newScore = delta
	}
	zv.scores[member] = newScore
	zv.sl.insert(newScore, member)
	zv.mu.Unlock()
	return newScore, nil
}

// ZCount 返回 score 在 [minScore, maxScore] 范围内的成员数量
func (db *GoRedis) ZCount(key string, minScore, maxScore float64) (int, error) {
	result, err := db.ZRangeByScore(key, minScore, maxScore, false)
	if err != nil {
		return 0, err
	}
	return len(result), nil
}

// reverseStrings 原地反转字符串切片
// 若 withScore，每两个元素为一组进行反转
func reverseStrings(s []string, withScore bool) {
	if len(s) == 0 {
		return
	}
	if !withScore {
		for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
			s[i], s[j] = s[j], s[i]
		}
		return
	}
	// 每两个元素一组反转
	pairs := len(s) / 2
	for i, j := 0, pairs-1; i < j; i, j = i+1, j-1 {
		s[i*2], s[j*2] = s[j*2], s[i*2]
		s[i*2+1], s[j*2+1] = s[j*2+1], s[i*2+1]
	}
}
