package engine

// ========== String ==========

// Set 设置字符串键值
func (db *GoRedis) Set(key, value string) error {
	db.store.withWriteLock(key, func(data map[string]interface{}) {
		data[key] = &entry{typ: TypeString, val: value}
	})
	return nil
}

// Get 获取字符串值
func (db *GoRedis) Get(key string) (string, error) {
	var result string
	var err error
	db.store.withReadLock(key, func(data map[string]interface{}) {
		e, ok := data[key]
		if !ok {
			err = ErrKeyNotFound
			return
		}
		en := e.(*entry)
		if en.typ != TypeString {
			err = ErrWrongType
			return
		}
		result = en.val.(string)
	})
	return result, err
}

// GetSet 设置新值并返回旧值
func (db *GoRedis) GetSet(key, value string) (string, error) {
	var old string
	var err error
	db.store.withWriteLock(key, func(data map[string]interface{}) {
		if e, ok := data[key]; ok {
			en := e.(*entry)
			if en.typ != TypeString {
				err = ErrWrongType
				return
			}
			old = en.val.(string)
		} else {
			err = ErrKeyNotFound
		}
		data[key] = &entry{typ: TypeString, val: value}
	})
	return old, err
}

// SetNX 仅在 key 不存在时设置，返回是否设置成功
func (db *GoRedis) SetNX(key, value string) bool {
	set := false
	db.store.withWriteLock(key, func(data map[string]interface{}) {
		if _, ok := data[key]; !ok {
			data[key] = &entry{typ: TypeString, val: value}
			set = true
		}
	})
	return set
}

// MSet 批量设置字符串键值
func (db *GoRedis) MSet(pairs ...string) error {
	if len(pairs)%2 != 0 {
		return errorf("MSet requires even number of arguments")
	}
	for i := 0; i < len(pairs); i += 2 {
		db.Set(pairs[i], pairs[i+1])
	}
	return nil
}

// MGet 批量获取字符串值（不存在的 key 返回空字符串，err 为 ErrKeyNotFound）
func (db *GoRedis) MGet(keys ...string) []string {
	result := make([]string, len(keys))
	for i, k := range keys {
		val, _ := db.Get(k)
		result[i] = val
	}
	return result
}

// Incr 将 key 的整数值自增 1
func (db *GoRedis) Incr(key string) (int64, error) {
	return db.IncrBy(key, 1)
}

// Decr 将 key 的整数值自减 1
func (db *GoRedis) Decr(key string) (int64, error) {
	return db.IncrBy(key, -1)
}

// IncrBy 将 key 的整数值加上 delta
func (db *GoRedis) IncrBy(key string, delta int64) (int64, error) {
	var result int64
	var err error
	db.store.withWriteLock(key, func(data map[string]interface{}) {
		var current int64
		if e, ok := data[key]; ok {
			en := e.(*entry)
			if en.typ != TypeString {
				err = ErrWrongType
				return
			}
			current, err = parseInt64(en.val.(string))
			if err != nil {
				return
			}
		}
		current += delta
		result = current
		data[key] = &entry{typ: TypeString, val: formatInt64(current)}
	})
	return result, err
}

// Append 向字符串追加内容，返回追加后的长度
func (db *GoRedis) Append(key, value string) (int, error) {
	var length int
	var err error
	db.store.withWriteLock(key, func(data map[string]interface{}) {
		if e, ok := data[key]; ok {
			en := e.(*entry)
			if en.typ != TypeString {
				err = ErrWrongType
				return
			}
			newVal := en.val.(string) + value
			en.val = newVal
			length = len(newVal)
		} else {
			data[key] = &entry{typ: TypeString, val: value}
			length = len(value)
		}
	})
	return length, err
}

// StrLen 返回字符串长度
func (db *GoRedis) StrLen(key string) (int, error) {
	val, err := db.Get(key)
	if err != nil {
		if err == ErrKeyNotFound {
			return 0, nil
		}
		return 0, err
	}
	return len(val), nil
}
