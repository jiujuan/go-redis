package engine

import (
	"container/list"
	"sync"
)

// ListValue List 数据结构的内部值，使用双向链表
type ListValue struct {
	mu   sync.RWMutex
	data *list.List
}

func newListValue() *ListValue {
	return &ListValue{data: list.New()}
}

// ========== List ==========

func (db *GoRedis) getOrCreateList(key string) (*ListValue, error) {
	var lv *ListValue
	var err error
	db.store.withWriteLock(key, func(data map[string]interface{}) {
		if e, ok := data[key]; ok {
			en := e.(*entry)
			if en.typ != TypeList {
				err = ErrWrongType
				return
			}
			lv = en.val.(*ListValue)
		} else {
			lv = newListValue()
			data[key] = &entry{typ: TypeList, val: lv}
		}
	})
	return lv, err
}

func (db *GoRedis) getList(key string) (*ListValue, error) {
	var lv *ListValue
	var err error
	db.store.withReadLock(key, func(data map[string]interface{}) {
		if e, ok := data[key]; ok {
			en := e.(*entry)
			if en.typ != TypeList {
				err = ErrWrongType
				return
			}
			lv = en.val.(*ListValue)
		}
	})
	return lv, err
}

// LPush 从列表头部插入一个或多个元素，返回列表长度
func (db *GoRedis) LPush(key string, values ...string) (int, error) {
	lv, err := db.getOrCreateList(key)
	if err != nil {
		return 0, err
	}

	lv.mu.Lock()
	for _, v := range values {
		lv.data.PushFront(v)
	}
	length := lv.data.Len()
	lv.mu.Unlock()
	return length, nil
}

// RPush 从列表尾部插入一个或多个元素，返回列表长度
func (db *GoRedis) RPush(key string, values ...string) (int, error) {
	lv, err := db.getOrCreateList(key)
	if err != nil {
		return 0, err
	}

	lv.mu.Lock()
	for _, v := range values {
		lv.data.PushBack(v)
	}
	length := lv.data.Len()
	lv.mu.Unlock()
	return length, nil
}

// LPop 从列表头部弹出元素
func (db *GoRedis) LPop(key string) (string, error) {
	lv, err := db.getList(key)
	if err != nil {
		return "", err
	}
	if lv == nil {
		return "", ErrKeyNotFound
	}

	lv.mu.Lock()
	front := lv.data.Front()
	if front == nil {
		lv.mu.Unlock()
		return "", ErrEmptyList
	}
	lv.data.Remove(front)
	length := lv.data.Len()
	lv.mu.Unlock()

	// 列表为空时删除 key
	if length == 0 {
		db.Del(key)
	}
	return front.Value.(string), nil
}

// RPop 从列表尾部弹出元素
func (db *GoRedis) RPop(key string) (string, error) {
	lv, err := db.getList(key)
	if err != nil {
		return "", err
	}
	if lv == nil {
		return "", ErrKeyNotFound
	}

	lv.mu.Lock()
	back := lv.data.Back()
	if back == nil {
		lv.mu.Unlock()
		return "", ErrEmptyList
	}
	lv.data.Remove(back)
	length := lv.data.Len()
	lv.mu.Unlock()

	if length == 0 {
		db.Del(key)
	}
	return back.Value.(string), nil
}

// LRange 返回列表中 [start, stop] 范围的元素（0-based，支持负索引）
func (db *GoRedis) LRange(key string, start, stop int) ([]string, error) {
	lv, err := db.getList(key)
	if err != nil {
		return nil, err
	}
	if lv == nil {
		return []string{}, nil
	}

	lv.mu.RLock()
	defer lv.mu.RUnlock()

	length := lv.data.Len()
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
		return []string{}, nil
	}

	result := make([]string, 0, stop-start+1)
	idx := 0
	for e := lv.data.Front(); e != nil; e = e.Next() {
		if idx > stop {
			break
		}
		if idx >= start {
			result = append(result, e.Value.(string))
		}
		idx++
	}
	return result, nil
}

// LLen 返回列表长度
func (db *GoRedis) LLen(key string) (int, error) {
	lv, err := db.getList(key)
	if err != nil {
		return 0, err
	}
	if lv == nil {
		return 0, nil
	}

	lv.mu.RLock()
	length := lv.data.Len()
	lv.mu.RUnlock()
	return length, nil
}

// LIndex 返回列表中指定索引的元素（0-based，支持负索引）
func (db *GoRedis) LIndex(key string, index int) (string, error) {
	lv, err := db.getList(key)
	if err != nil {
		return "", err
	}
	if lv == nil {
		return "", ErrKeyNotFound
	}

	lv.mu.RLock()
	defer lv.mu.RUnlock()

	length := lv.data.Len()
	if index < 0 {
		index = length + index
	}
	if index < 0 || index >= length {
		return "", ErrOutOfRange
	}

	idx := 0
	for e := lv.data.Front(); e != nil; e = e.Next() {
		if idx == index {
			return e.Value.(string), nil
		}
		idx++
	}
	return "", ErrOutOfRange
}

// LSet 设置列表中指定索引的元素
func (db *GoRedis) LSet(key string, index int, value string) error {
	lv, err := db.getList(key)
	if err != nil {
		return err
	}
	if lv == nil {
		return ErrKeyNotFound
	}

	lv.mu.Lock()
	defer lv.mu.Unlock()

	length := lv.data.Len()
	if index < 0 {
		index = length + index
	}
	if index < 0 || index >= length {
		return ErrOutOfRange
	}

	idx := 0
	for e := lv.data.Front(); e != nil; e = e.Next() {
		if idx == index {
			e.Value = value
			return nil
		}
		idx++
	}
	return ErrOutOfRange
}

// LRem 从列表中删除 count 个等于 value 的元素，返回删除数量
// count > 0: 从头到尾；count < 0: 从尾到头；count = 0: 全部删除
func (db *GoRedis) LRem(key string, count int, value string) (int, error) {
	lv, err := db.getList(key)
	if err != nil {
		return 0, err
	}
	if lv == nil {
		return 0, nil
	}

	lv.mu.Lock()
	defer lv.mu.Unlock()

	removed := 0
	abs := count
	if abs < 0 {
		abs = -abs
	}

	var toRemove []*list.Element
	if count >= 0 {
		for e := lv.data.Front(); e != nil; e = e.Next() {
			if e.Value.(string) == value {
				toRemove = append(toRemove, e)
				if count > 0 && len(toRemove) >= abs {
					break
				}
			}
		}
	} else {
		for e := lv.data.Back(); e != nil; e = e.Prev() {
			if e.Value.(string) == value {
				toRemove = append(toRemove, e)
				if len(toRemove) >= abs {
					break
				}
			}
		}
	}

	for _, e := range toRemove {
		lv.data.Remove(e)
		removed++
	}
	return removed, nil
}

// LPushX 仅当 key 存在时从头部插入
func (db *GoRedis) LPushX(key string, values ...string) (int, error) {
	lv, err := db.getList(key)
	if err != nil {
		return 0, err
	}
	if lv == nil {
		return 0, nil
	}

	lv.mu.Lock()
	for _, v := range values {
		lv.data.PushFront(v)
	}
	length := lv.data.Len()
	lv.mu.Unlock()
	return length, nil
}

// RPushX 仅当 key 存在时从尾部插入
func (db *GoRedis) RPushX(key string, values ...string) (int, error) {
	lv, err := db.getList(key)
	if err != nil {
		return 0, err
	}
	if lv == nil {
		return 0, nil
	}

	lv.mu.Lock()
	for _, v := range values {
		lv.data.PushBack(v)
	}
	length := lv.data.Len()
	lv.mu.Unlock()
	return length, nil
}
