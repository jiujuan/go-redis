package engine

import (
	"errors"
	"fmt"
	"strconv"
)

// ---------- 错误定义 ----------

var (
	ErrKeyNotFound   = errors.New("key not found")
	ErrWrongType     = errors.New("WRONGTYPE operation against a key holding the wrong kind of value")
	ErrOutOfRange    = errors.New("index out of range")
	ErrEmptyList     = errors.New("list is empty")
	ErrMemberNotFound = errors.New("member not found")
)

// ---------- 值类型枚举 ----------

type ValueType uint8

const (
	TypeString ValueType = iota + 1
	TypeHash
	TypeList
	TypeSet
	TypeZSet
)

func (t ValueType) String() string {
	switch t {
	case TypeString:
		return "string"
	case TypeHash:
		return "hash"
	case TypeList:
		return "list"
	case TypeSet:
		return "set"
	case TypeZSet:
		return "zset"
	default:
		return "unknown"
	}
}

// ---------- 值包装器 ----------

// entry 是存储在 shardedMap 中的统一包装结构
type entry struct {
	typ  ValueType
	val  interface{}
}

// ---------- 工具函数 ----------

// formatScore 将 float64 score 格式化为字符串
func formatScore(score float64) string {
	if score == float64(int64(score)) {
		return strconv.FormatInt(int64(score), 10)
	}
	return strconv.FormatFloat(score, 'f', -1, 64)
}

// parseScore 解析 score 字符串
func parseScore(s string) (float64, error) {
	score, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("value is not a valid float")
	}
	return score, nil
}

// wrongTypeErr 生成类型错误
func wrongTypeErr() error {
	return ErrWrongType
}
