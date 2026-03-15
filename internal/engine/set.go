package engine

import "sync"

// SetValue Set 数据结构的内部值
type SetValue struct {
	mu   sync.RWMutex
	data map[string]struct{}
}

func newSetValue() *SetValue {
	return &SetValue{data: make(map[string]struct{})}
}

// ========== Set ==========

func (db *GoRedis) getOrCreateSet(key string) (*SetValue, error) {
	var sv *SetValue
	var err error
	db.store.withWriteLock(key, func(data map[string]interface{}) {
		if e, ok := data[key]; ok {
			en := e.(*entry)
			if en.typ != TypeSet {
				err = ErrWrongType
				return
			}
			sv = en.val.(*SetValue)
		} else {
			sv = newSetValue()
			data[key] = &entry{typ: TypeSet, val: sv}
		}
	})
	return sv, err
}

func (db *GoRedis) getSet(key string) (*SetValue, error) {
	var sv *SetValue
	var err error
	db.store.withReadLock(key, func(data map[string]interface{}) {
		if e, ok := data[key]; ok {
			en := e.(*entry)
			if en.typ != TypeSet {
				err = ErrWrongType
				return
			}
			sv = en.val.(*SetValue)
		}
	})
	return sv, err
}

// SAdd 向集合添加一个或多个成员，返回新增数量
func (db *GoRedis) SAdd(key string, members ...string) (int, error) {
	sv, err := db.getOrCreateSet(key)
	if err != nil {
		return 0, err
	}

	added := 0
	sv.mu.Lock()
	for _, m := range members {
		if _, ok := sv.data[m]; !ok {
			sv.data[m] = struct{}{}
			added++
		}
	}
	sv.mu.Unlock()
	return added, nil
}

// SRem 从集合中删除一个或多个成员，返回删除数量
func (db *GoRedis) SRem(key string, members ...string) (int, error) {
	sv, err := db.getSet(key)
	if err != nil {
		return 0, err
	}
	if sv == nil {
		return 0, nil
	}

	removed := 0
	sv.mu.Lock()
	for _, m := range members {
		if _, ok := sv.data[m]; ok {
			delete(sv.data, m)
			removed++
		}
	}
	empty := len(sv.data) == 0
	sv.mu.Unlock()

	if empty && removed > 0 {
		db.Del(key)
	}
	return removed, nil
}

// SMembers 返回集合中的所有成员
func (db *GoRedis) SMembers(key string) ([]string, error) {
	sv, err := db.getSet(key)
	if err != nil {
		return nil, err
	}
	if sv == nil {
		return []string{}, nil
	}

	sv.mu.RLock()
	result := make([]string, 0, len(sv.data))
	for m := range sv.data {
		result = append(result, m)
	}
	sv.mu.RUnlock()
	return result, nil
}

// SIsMember 判断成员是否在集合中
func (db *GoRedis) SIsMember(key, member string) (bool, error) {
	sv, err := db.getSet(key)
	if err != nil {
		return false, err
	}
	if sv == nil {
		return false, nil
	}

	sv.mu.RLock()
	_, ok := sv.data[member]
	sv.mu.RUnlock()
	return ok, nil
}

// SCard 返回集合成员数量
func (db *GoRedis) SCard(key string) (int, error) {
	sv, err := db.getSet(key)
	if err != nil {
		return 0, err
	}
	if sv == nil {
		return 0, nil
	}

	sv.mu.RLock()
	count := len(sv.data)
	sv.mu.RUnlock()
	return count, nil
}

// SInter 返回多个集合的交集
func (db *GoRedis) SInter(keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return []string{}, nil
	}

	sets := make([]*SetValue, 0, len(keys))
	for _, k := range keys {
		sv, err := db.getSet(k)
		if err != nil {
			return nil, err
		}
		if sv == nil {
			return []string{}, nil // 有一个空集合，交集为空
		}
		sets = append(sets, sv)
	}

	// 以第一个集合为基础，逐一过滤
	sets[0].mu.RLock()
	candidates := make(map[string]struct{}, len(sets[0].data))
	for m := range sets[0].data {
		candidates[m] = struct{}{}
	}
	sets[0].mu.RUnlock()

	for _, sv := range sets[1:] {
		sv.mu.RLock()
		for m := range candidates {
			if _, ok := sv.data[m]; !ok {
				delete(candidates, m)
			}
		}
		sv.mu.RUnlock()
	}

	result := make([]string, 0, len(candidates))
	for m := range candidates {
		result = append(result, m)
	}
	return result, nil
}

// SUnion 返回多个集合的并集
func (db *GoRedis) SUnion(keys ...string) ([]string, error) {
	union := make(map[string]struct{})
	for _, k := range keys {
		sv, err := db.getSet(k)
		if err != nil {
			return nil, err
		}
		if sv == nil {
			continue
		}
		sv.mu.RLock()
		for m := range sv.data {
			union[m] = struct{}{}
		}
		sv.mu.RUnlock()
	}

	result := make([]string, 0, len(union))
	for m := range union {
		result = append(result, m)
	}
	return result, nil
}

// SDiff 返回第一个集合相对于其余集合的差集
func (db *GoRedis) SDiff(keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return []string{}, nil
	}

	first, err := db.getSet(keys[0])
	if err != nil {
		return nil, err
	}
	if first == nil {
		return []string{}, nil
	}

	first.mu.RLock()
	candidates := make(map[string]struct{}, len(first.data))
	for m := range first.data {
		candidates[m] = struct{}{}
	}
	first.mu.RUnlock()

	for _, k := range keys[1:] {
		sv, err := db.getSet(k)
		if err != nil {
			return nil, err
		}
		if sv == nil {
			continue
		}
		sv.mu.RLock()
		for m := range sv.data {
			delete(candidates, m)
		}
		sv.mu.RUnlock()
	}

	result := make([]string, 0, len(candidates))
	for m := range candidates {
		result = append(result, m)
	}
	return result, nil
}

// SMove 将成员从一个集合移动到另一个集合
func (db *GoRedis) SMove(src, dst, member string) (bool, error) {
	srcSv, err := db.getSet(src)
	if err != nil {
		return false, err
	}
	if srcSv == nil {
		return false, nil
	}

	srcSv.mu.Lock()
	_, ok := srcSv.data[member]
	if ok {
		delete(srcSv.data, member)
	}
	srcSv.mu.Unlock()

	if !ok {
		return false, nil
	}

	dstSv, err := db.getOrCreateSet(dst)
	if err != nil {
		// 回滚
		srcSv.mu.Lock()
		srcSv.data[member] = struct{}{}
		srcSv.mu.Unlock()
		return false, err
	}

	dstSv.mu.Lock()
	dstSv.data[member] = struct{}{}
	dstSv.mu.Unlock()
	return true, nil
}
