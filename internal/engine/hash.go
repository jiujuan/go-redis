package engine

import "sync"

// HashValue Hash 数据结构的内部值
type HashValue struct {
	mu   sync.RWMutex
	data map[string]string
}

func newHashValue() *HashValue {
	return &HashValue{data: make(map[string]string)}
}

// ========== Hash ==========

// getOrCreateHash 获取或创建 Hash 对象（带写锁）
func (db *GoRedis) getOrCreateHash(key string) (*HashValue, error) {
	var hv *HashValue
	var err error
	db.store.withWriteLock(key, func(data map[string]interface{}) {
		if e, ok := data[key]; ok {
			en := e.(*entry)
			if en.typ != TypeHash {
				err = ErrWrongType
				return
			}
			hv = en.val.(*HashValue)
		} else {
			hv = newHashValue()
			data[key] = &entry{typ: TypeHash, val: hv}
		}
	})
	return hv, err
}

// getHash 获取 Hash 对象（带读锁），不存在返回 nil
func (db *GoRedis) getHash(key string) (*HashValue, error) {
	var hv *HashValue
	var err error
	db.store.withReadLock(key, func(data map[string]interface{}) {
		if e, ok := data[key]; ok {
			en := e.(*entry)
			if en.typ != TypeHash {
				err = ErrWrongType
				return
			}
			hv = en.val.(*HashValue)
		}
	})
	return hv, err
}

// HSet 设置 hash 中的字段值，返回新增字段数
func (db *GoRedis) HSet(key string, fieldValues ...string) (int, error) {
	if len(fieldValues)%2 != 0 {
		return 0, errorf("HSet requires even number of field-value pairs")
	}
	hv, err := db.getOrCreateHash(key)
	if err != nil {
		return 0, err
	}

	added := 0
	hv.mu.Lock()
	for i := 0; i < len(fieldValues); i += 2 {
		field, value := fieldValues[i], fieldValues[i+1]
		if _, exists := hv.data[field]; !exists {
			added++
		}
		hv.data[field] = value
	}
	hv.mu.Unlock()
	return added, nil
}

// HGet 获取 hash 中字段的值
func (db *GoRedis) HGet(key, field string) (string, error) {
	hv, err := db.getHash(key)
	if err != nil {
		return "", err
	}
	if hv == nil {
		return "", ErrKeyNotFound
	}

	hv.mu.RLock()
	val, ok := hv.data[field]
	hv.mu.RUnlock()

	if !ok {
		return "", ErrMemberNotFound
	}
	return val, nil
}

// HDel 删除 hash 中的字段，返回删除数量
func (db *GoRedis) HDel(key string, fields ...string) (int, error) {
	hv, err := db.getHash(key)
	if err != nil {
		return 0, err
	}
	if hv == nil {
		return 0, nil
	}

	deleted := 0
	hv.mu.Lock()
	for _, f := range fields {
		if _, ok := hv.data[f]; ok {
			delete(hv.data, f)
			deleted++
		}
	}
	hv.mu.Unlock()

	// 若 hash 为空，删除 key
	if deleted > 0 {
		hv.mu.RLock()
		empty := len(hv.data) == 0
		hv.mu.RUnlock()
		if empty {
			db.Del(key)
		}
	}
	return deleted, nil
}

// HGetAll 返回 hash 中所有字段和值，交替排列 field, value, field, value...
func (db *GoRedis) HGetAll(key string) ([]string, error) {
	hv, err := db.getHash(key)
	if err != nil {
		return nil, err
	}
	if hv == nil {
		return []string{}, nil
	}

	hv.mu.RLock()
	result := make([]string, 0, len(hv.data)*2)
	for f, v := range hv.data {
		result = append(result, f, v)
	}
	hv.mu.RUnlock()
	return result, nil
}

// HExists 判断 hash 中字段是否存在
func (db *GoRedis) HExists(key, field string) (bool, error) {
	hv, err := db.getHash(key)
	if err != nil {
		return false, err
	}
	if hv == nil {
		return false, nil
	}

	hv.mu.RLock()
	_, ok := hv.data[field]
	hv.mu.RUnlock()
	return ok, nil
}

// HLen 返回 hash 中字段数量
func (db *GoRedis) HLen(key string) (int, error) {
	hv, err := db.getHash(key)
	if err != nil {
		return 0, err
	}
	if hv == nil {
		return 0, nil
	}

	hv.mu.RLock()
	length := len(hv.data)
	hv.mu.RUnlock()
	return length, nil
}

// HKeys 返回 hash 中所有字段名
func (db *GoRedis) HKeys(key string) ([]string, error) {
	hv, err := db.getHash(key)
	if err != nil {
		return nil, err
	}
	if hv == nil {
		return []string{}, nil
	}

	hv.mu.RLock()
	keys := make([]string, 0, len(hv.data))
	for f := range hv.data {
		keys = append(keys, f)
	}
	hv.mu.RUnlock()
	return keys, nil
}

// HVals 返回 hash 中所有值
func (db *GoRedis) HVals(key string) ([]string, error) {
	hv, err := db.getHash(key)
	if err != nil {
		return nil, err
	}
	if hv == nil {
		return []string{}, nil
	}

	hv.mu.RLock()
	vals := make([]string, 0, len(hv.data))
	for _, v := range hv.data {
		vals = append(vals, v)
	}
	hv.mu.RUnlock()
	return vals, nil
}

// HMGet 批量获取字段值
func (db *GoRedis) HMGet(key string, fields ...string) ([]string, error) {
	hv, err := db.getHash(key)
	if err != nil {
		return nil, err
	}

	result := make([]string, len(fields))
	if hv == nil {
		return result, nil
	}

	hv.mu.RLock()
	for i, f := range fields {
		result[i] = hv.data[f] // 不存在时返回空字符串
	}
	hv.mu.RUnlock()
	return result, nil
}

// HSetNX 仅在字段不存在时设置，返回是否设置成功
func (db *GoRedis) HSetNX(key, field, value string) (bool, error) {
	hv, err := db.getOrCreateHash(key)
	if err != nil {
		return false, err
	}

	hv.mu.Lock()
	_, exists := hv.data[field]
	if !exists {
		hv.data[field] = value
	}
	hv.mu.Unlock()
	return !exists, nil
}

// HIncrBy 将 hash 字段的整数值增加 delta
func (db *GoRedis) HIncrBy(key, field string, delta int64) (int64, error) {
	hv, err := db.getOrCreateHash(key)
	if err != nil {
		return 0, err
	}

	var result int64
	hv.mu.Lock()
	current, _ := parseInt64(hv.data[field])
	current += delta
	hv.data[field] = formatInt64(current)
	result = current
	hv.mu.Unlock()
	return result, nil
}
