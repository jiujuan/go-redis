package server

import (
	"math"
	"strconv"
	"strings"

	"github.com/go-redis/go-redis/internal/engine"
	"github.com/go-redis/go-redis/internal/resp"
)

// cmdHandler 命令处理函数类型
type cmdHandler func(db *engine.GoRedis, args []string, w *resp.Writer) error

// commandTable 命令路由表
var commandTable map[string]cmdHandler

func init() {
	commandTable = map[string]cmdHandler{
		// 通用命令
		"ping":    cmdPing,
		"echo":    cmdEcho,
		"del":     cmdDel,
		"exists":  cmdExists,
		"type":    cmdType,
		"keys":    cmdKeys,
		"scan":    cmdScan,
		"dbsize":  cmdDBSize,
		"flushdb": cmdFlushDB,
		"rename":  cmdRename,
		"select":  cmdSelect,

		// String 命令
		"set":    cmdSet,
		"get":    cmdGet,
		"getset": cmdGetSet,
		"setnx":  cmdSetNX,
		"mset":   cmdMSet,
		"mget":   cmdMGet,
		"incr":   cmdIncr,
		"decr":   cmdDecr,
		"incrby": cmdIncrBy,
		"decrby": cmdDecrBy,
		"append": cmdAppend,
		"strlen": cmdStrLen,

		// Hash 命令
		"hset":    cmdHSet,
		"hget":    cmdHGet,
		"hdel":    cmdHDel,
		"hgetall": cmdHGetAll,
		"hexists": cmdHExists,
		"hlen":    cmdHLen,
		"hkeys":   cmdHKeys,
		"hvals":   cmdHVals,
		"hmget":   cmdHMGet,
		"hmset":   cmdHMSet,
		"hsetnx":  cmdHSetNX,
		"hincrby": cmdHIncrBy,

		// List 命令
		"lpush":  cmdLPush,
		"rpush":  cmdRPush,
		"lpop":   cmdLPop,
		"rpop":   cmdRPop,
		"lrange": cmdLRange,
		"llen":   cmdLLen,
		"lindex": cmdLIndex,
		"lset":   cmdLSet,
		"lrem":   cmdLRem,
		"lpushx": cmdLPushX,
		"rpushx": cmdRPushX,

		// Set 命令
		"sadd":      cmdSAdd,
		"srem":      cmdSRem,
		"smembers":  cmdSMembers,
		"sismember": cmdSIsMember,
		"scard":     cmdSCard,
		"sinter":    cmdSInter,
		"sunion":    cmdSUnion,
		"sdiff":     cmdSDiff,
		"smove":     cmdSMove,

		// ZSet 命令
		"zadd":          cmdZAdd,
		"zscore":        cmdZScore,
		"zrem":          cmdZRem,
		"zrange":        cmdZRange,
		"zrevrange":     cmdZRevRange,
		"zrangebyscore": cmdZRangeByScore,
		"zcard":         cmdZCard,
		"zrank":         cmdZRank,
		"zincrby":       cmdZIncrBy,
		"zcount":        cmdZCount,
	}
}

// dispatch 分发命令
func dispatch(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) == 0 {
		return w.WriteError("empty command")
	}
	name := strings.ToLower(args[0])
	handler, ok := commandTable[name]
	if !ok {
		return w.WriteError("unknown command '" + args[0] + "'")
	}
	return handler(db, args[1:], w)
}

// ---- 通用命令 ----

func cmdPing(_ *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) > 0 {
		return w.WriteBulkString(args[0])
	}
	return w.WriteSimpleString("PONG")
}

func cmdEcho(_ *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 1 {
		return w.WriteError("wrong number of arguments for 'echo'")
	}
	return w.WriteBulkString(args[0])
}

func cmdDel(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 1 {
		return w.WriteError("wrong number of arguments for 'del'")
	}
	return w.WriteInteger(int64(db.Del(args...)))
}

func cmdExists(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 1 {
		return w.WriteError("wrong number of arguments for 'exists'")
	}
	return w.WriteInteger(int64(db.Exists(args...)))
}

func cmdType(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 1 {
		return w.WriteError("wrong number of arguments for 'type'")
	}
	return w.WriteSimpleString(db.Type(args[0]))
}

func cmdKeys(db *engine.GoRedis, args []string, w *resp.Writer) error {
	pattern := "*"
	if len(args) > 0 {
		pattern = args[0]
	}
	return w.WriteStringArray(db.Keys(pattern))
}

func cmdDBSize(db *engine.GoRedis, _ []string, w *resp.Writer) error {
	return w.WriteInteger(int64(db.DBSize()))
}

func cmdFlushDB(db *engine.GoRedis, _ []string, w *resp.Writer) error {
	db.FlushDB()
	return w.WriteOK()
}

func cmdRename(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 2 {
		return w.WriteError("wrong number of arguments for 'rename'")
	}
	if err := db.Rename(args[0], args[1]); err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteOK()
}

func cmdSelect(_ *engine.GoRedis, _ []string, w *resp.Writer) error {
	// v0.1 只有单数据库，SELECT 0 成功，其他返回错误
	return w.WriteOK()
}

// ---- String 命令 ----

func cmdSet(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 2 {
		return w.WriteError("wrong number of arguments for 'set'")
	}
	db.Set(args[0], args[1])
	return w.WriteOK()
}

func cmdGet(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 1 {
		return w.WriteError("wrong number of arguments for 'get'")
	}
	val, err := db.Get(args[0])
	if err != nil {
		if err == engine.ErrKeyNotFound {
			return w.WriteNilBulk()
		}
		return writeEngineError(w, err)
	}
	return w.WriteBulkString(val)
}

func cmdGetSet(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 2 {
		return w.WriteError("wrong number of arguments for 'getset'")
	}
	old, err := db.GetSet(args[0], args[1])
	if err != nil && err != engine.ErrKeyNotFound {
		return writeEngineError(w, err)
	}
	if err == engine.ErrKeyNotFound {
		return w.WriteNilBulk()
	}
	return w.WriteBulkString(old)
}

func cmdSetNX(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 2 {
		return w.WriteError("wrong number of arguments for 'setnx'")
	}
	if db.SetNX(args[0], args[1]) {
		return w.WriteInteger(1)
	}
	return w.WriteInteger(0)
}

func cmdMSet(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 2 || len(args)%2 != 0 {
		return w.WriteError("wrong number of arguments for 'mset'")
	}
	db.MSet(args...)
	return w.WriteOK()
}

func cmdMGet(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 1 {
		return w.WriteError("wrong number of arguments for 'mget'")
	}
	vals := db.MGet(args...)
	if err := w.WriteArrayHeader(len(vals)); err != nil {
		return err
	}
	for _, v := range vals {
		if v == "" {
			if err := w.WriteNilBulk(); err != nil {
				return err
			}
		} else {
			if err := w.WriteBulkString(v); err != nil {
				return err
			}
		}
	}
	return nil
}

func cmdIncr(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 1 {
		return w.WriteError("wrong number of arguments for 'incr'")
	}
	n, err := db.Incr(args[0])
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteInteger(n)
}

func cmdDecr(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 1 {
		return w.WriteError("wrong number of arguments for 'decr'")
	}
	n, err := db.Decr(args[0])
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteInteger(n)
}

func cmdIncrBy(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 2 {
		return w.WriteError("wrong number of arguments for 'incrby'")
	}
	delta, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return w.WriteError("value is not an integer")
	}
	n, err := db.IncrBy(args[0], delta)
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteInteger(n)
}

func cmdDecrBy(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 2 {
		return w.WriteError("wrong number of arguments for 'decrby'")
	}
	delta, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return w.WriteError("value is not an integer")
	}
	n, err := db.IncrBy(args[0], -delta)
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteInteger(n)
}

func cmdAppend(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 2 {
		return w.WriteError("wrong number of arguments for 'append'")
	}
	n, err := db.Append(args[0], args[1])
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteInteger(int64(n))
}

func cmdStrLen(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 1 {
		return w.WriteError("wrong number of arguments for 'strlen'")
	}
	n, err := db.StrLen(args[0])
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteInteger(int64(n))
}

// ---- Hash 命令 ----

func cmdHSet(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return w.WriteError("wrong number of arguments for 'hset'")
	}
	n, err := db.HSet(args[0], args[1:]...)
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteInteger(int64(n))
}

func cmdHGet(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 2 {
		return w.WriteError("wrong number of arguments for 'hget'")
	}
	val, err := db.HGet(args[0], args[1])
	if err != nil {
		if err == engine.ErrKeyNotFound || err == engine.ErrMemberNotFound {
			return w.WriteNilBulk()
		}
		return writeEngineError(w, err)
	}
	return w.WriteBulkString(val)
}

func cmdHDel(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 2 {
		return w.WriteError("wrong number of arguments for 'hdel'")
	}
	n, err := db.HDel(args[0], args[1:]...)
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteInteger(int64(n))
}

func cmdHGetAll(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 1 {
		return w.WriteError("wrong number of arguments for 'hgetall'")
	}
	result, err := db.HGetAll(args[0])
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteStringArray(result)
}

func cmdHExists(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 2 {
		return w.WriteError("wrong number of arguments for 'hexists'")
	}
	ok, err := db.HExists(args[0], args[1])
	if err != nil {
		return writeEngineError(w, err)
	}
	if ok {
		return w.WriteInteger(1)
	}
	return w.WriteInteger(0)
}

func cmdHLen(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 1 {
		return w.WriteError("wrong number of arguments for 'hlen'")
	}
	n, err := db.HLen(args[0])
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteInteger(int64(n))
}

func cmdHKeys(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 1 {
		return w.WriteError("wrong number of arguments for 'hkeys'")
	}
	keys, err := db.HKeys(args[0])
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteStringArray(keys)
}

func cmdHVals(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 1 {
		return w.WriteError("wrong number of arguments for 'hvals'")
	}
	vals, err := db.HVals(args[0])
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteStringArray(vals)
}

func cmdHMGet(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 2 {
		return w.WriteError("wrong number of arguments for 'hmget'")
	}
	vals, err := db.HMGet(args[0], args[1:]...)
	if err != nil {
		return writeEngineError(w, err)
	}
	if err := w.WriteArrayHeader(len(vals)); err != nil {
		return err
	}
	for _, v := range vals {
		if v == "" {
			w.WriteNilBulk()
		} else {
			w.WriteBulkString(v)
		}
	}
	return nil
}

func cmdHMSet(db *engine.GoRedis, args []string, w *resp.Writer) error {
	return cmdHSet(db, args, w)
}

func cmdHSetNX(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 3 {
		return w.WriteError("wrong number of arguments for 'hsetnx'")
	}
	ok, err := db.HSetNX(args[0], args[1], args[2])
	if err != nil {
		return writeEngineError(w, err)
	}
	if ok {
		return w.WriteInteger(1)
	}
	return w.WriteInteger(0)
}

func cmdHIncrBy(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 3 {
		return w.WriteError("wrong number of arguments for 'hincrby'")
	}
	delta, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return w.WriteError("value is not an integer")
	}
	n, err := db.HIncrBy(args[0], args[1], delta)
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteInteger(n)
}

// ---- List 命令 ----

func cmdLPush(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 2 {
		return w.WriteError("wrong number of arguments for 'lpush'")
	}
	n, err := db.LPush(args[0], args[1:]...)
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteInteger(int64(n))
}

func cmdRPush(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 2 {
		return w.WriteError("wrong number of arguments for 'rpush'")
	}
	n, err := db.RPush(args[0], args[1:]...)
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteInteger(int64(n))
}

func cmdLPop(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 1 {
		return w.WriteError("wrong number of arguments for 'lpop'")
	}
	val, err := db.LPop(args[0])
	if err != nil {
		if err == engine.ErrKeyNotFound || err == engine.ErrEmptyList {
			return w.WriteNilBulk()
		}
		return writeEngineError(w, err)
	}
	return w.WriteBulkString(val)
}

func cmdRPop(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 1 {
		return w.WriteError("wrong number of arguments for 'rpop'")
	}
	val, err := db.RPop(args[0])
	if err != nil {
		if err == engine.ErrKeyNotFound || err == engine.ErrEmptyList {
			return w.WriteNilBulk()
		}
		return writeEngineError(w, err)
	}
	return w.WriteBulkString(val)
}

func cmdLRange(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 3 {
		return w.WriteError("wrong number of arguments for 'lrange'")
	}
	start, err1 := strconv.Atoi(args[1])
	stop, err2 := strconv.Atoi(args[2])
	if err1 != nil || err2 != nil {
		return w.WriteError("value is not an integer")
	}
	result, err := db.LRange(args[0], start, stop)
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteStringArray(result)
}

func cmdLLen(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 1 {
		return w.WriteError("wrong number of arguments for 'llen'")
	}
	n, err := db.LLen(args[0])
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteInteger(int64(n))
}

func cmdLIndex(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 2 {
		return w.WriteError("wrong number of arguments for 'lindex'")
	}
	idx, err := strconv.Atoi(args[1])
	if err != nil {
		return w.WriteError("value is not an integer")
	}
	val, err := db.LIndex(args[0], idx)
	if err != nil {
		if err == engine.ErrKeyNotFound || err == engine.ErrOutOfRange {
			return w.WriteNilBulk()
		}
		return writeEngineError(w, err)
	}
	return w.WriteBulkString(val)
}

func cmdLSet(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 3 {
		return w.WriteError("wrong number of arguments for 'lset'")
	}
	idx, err := strconv.Atoi(args[1])
	if err != nil {
		return w.WriteError("value is not an integer")
	}
	if err := db.LSet(args[0], idx, args[2]); err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteOK()
}

func cmdLRem(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 3 {
		return w.WriteError("wrong number of arguments for 'lrem'")
	}
	count, err := strconv.Atoi(args[1])
	if err != nil {
		return w.WriteError("value is not an integer")
	}
	n, err := db.LRem(args[0], count, args[2])
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteInteger(int64(n))
}

func cmdLPushX(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 2 {
		return w.WriteError("wrong number of arguments for 'lpushx'")
	}
	n, err := db.LPushX(args[0], args[1:]...)
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteInteger(int64(n))
}

func cmdRPushX(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 2 {
		return w.WriteError("wrong number of arguments for 'rpushx'")
	}
	n, err := db.RPushX(args[0], args[1:]...)
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteInteger(int64(n))
}

// ---- Set 命令 ----

func cmdSAdd(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 2 {
		return w.WriteError("wrong number of arguments for 'sadd'")
	}
	n, err := db.SAdd(args[0], args[1:]...)
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteInteger(int64(n))
}

func cmdSRem(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 2 {
		return w.WriteError("wrong number of arguments for 'srem'")
	}
	n, err := db.SRem(args[0], args[1:]...)
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteInteger(int64(n))
}

func cmdSMembers(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 1 {
		return w.WriteError("wrong number of arguments for 'smembers'")
	}
	members, err := db.SMembers(args[0])
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteStringArray(members)
}

func cmdSIsMember(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 2 {
		return w.WriteError("wrong number of arguments for 'sismember'")
	}
	ok, err := db.SIsMember(args[0], args[1])
	if err != nil {
		return writeEngineError(w, err)
	}
	if ok {
		return w.WriteInteger(1)
	}
	return w.WriteInteger(0)
}

func cmdSCard(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 1 {
		return w.WriteError("wrong number of arguments for 'scard'")
	}
	n, err := db.SCard(args[0])
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteInteger(int64(n))
}

func cmdSInter(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 1 {
		return w.WriteError("wrong number of arguments for 'sinter'")
	}
	result, err := db.SInter(args...)
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteStringArray(result)
}

func cmdSUnion(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 1 {
		return w.WriteError("wrong number of arguments for 'sunion'")
	}
	result, err := db.SUnion(args...)
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteStringArray(result)
}

func cmdSDiff(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 1 {
		return w.WriteError("wrong number of arguments for 'sdiff'")
	}
	result, err := db.SDiff(args...)
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteStringArray(result)
}

func cmdSMove(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 3 {
		return w.WriteError("wrong number of arguments for 'smove'")
	}
	ok, err := db.SMove(args[0], args[1], args[2])
	if err != nil {
		return writeEngineError(w, err)
	}
	if ok {
		return w.WriteInteger(1)
	}
	return w.WriteInteger(0)
}

// ---- ZSet 命令 ----

func cmdZAdd(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return w.WriteError("wrong number of arguments for 'zadd'")
	}
	total := 0
	for i := 1; i < len(args); i += 2 {
		score, err := strconv.ParseFloat(args[i], 64)
		if err != nil {
			return w.WriteError("value is not a valid float")
		}
		n, err := db.ZAdd(args[0], score, args[i+1])
		if err != nil {
			return writeEngineError(w, err)
		}
		total += n
	}
	return w.WriteInteger(int64(total))
}

func cmdZScore(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 2 {
		return w.WriteError("wrong number of arguments for 'zscore'")
	}
	score, ok, err := db.ZScore(args[0], args[1])
	if err != nil {
		return writeEngineError(w, err)
	}
	if !ok {
		return w.WriteNilBulk()
	}
	return w.WriteBulkString(strconv.FormatFloat(score, 'f', -1, 64))
}

func cmdZRem(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 2 {
		return w.WriteError("wrong number of arguments for 'zrem'")
	}
	n, err := db.ZRem(args[0], args[1:]...)
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteInteger(int64(n))
}

func cmdZRange(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 3 {
		return w.WriteError("wrong number of arguments for 'zrange'")
	}
	start, err1 := strconv.Atoi(args[1])
	stop, err2 := strconv.Atoi(args[2])
	if err1 != nil || err2 != nil {
		return w.WriteError("value is not an integer")
	}
	withScore := len(args) > 3 && strings.ToUpper(args[3]) == "WITHSCORES"
	result, err := db.ZRange(args[0], start, stop, withScore)
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteStringArray(result)
}

func cmdZRevRange(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 3 {
		return w.WriteError("wrong number of arguments for 'zrevrange'")
	}
	start, err1 := strconv.Atoi(args[1])
	stop, err2 := strconv.Atoi(args[2])
	if err1 != nil || err2 != nil {
		return w.WriteError("value is not an integer")
	}
	withScore := len(args) > 3 && strings.ToUpper(args[3]) == "WITHSCORES"
	result, err := db.ZRevRange(args[0], start, stop, withScore)
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteStringArray(result)
}

func cmdZRangeByScore(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 3 {
		return w.WriteError("wrong number of arguments for 'zrangebyscore'")
	}
	minScore, err := parseScoreArg(args[1])
	if err != nil {
		return w.WriteError("min value is not a float")
	}
	maxScore, err := parseScoreArg(args[2])
	if err != nil {
		return w.WriteError("max value is not a float")
	}
	withScore := len(args) > 3 && strings.ToUpper(args[3]) == "WITHSCORES"
	result, err := db.ZRangeByScore(args[0], minScore, maxScore, withScore)
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteStringArray(result)
}

func cmdZCard(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 1 {
		return w.WriteError("wrong number of arguments for 'zcard'")
	}
	n, err := db.ZCard(args[0])
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteInteger(int64(n))
}

func cmdZRank(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 2 {
		return w.WriteError("wrong number of arguments for 'zrank'")
	}
	rank, err := db.ZRank(args[0], args[1])
	if err != nil {
		return writeEngineError(w, err)
	}
	if rank < 0 {
		return w.WriteNilBulk()
	}
	return w.WriteInteger(int64(rank))
}

func cmdZIncrBy(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 3 {
		return w.WriteError("wrong number of arguments for 'zincrby'")
	}
	delta, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return w.WriteError("value is not a valid float")
	}
	score, err := db.ZIncrBy(args[0], delta, args[2])
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteBulkString(strconv.FormatFloat(score, 'f', -1, 64))
}

func cmdZCount(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 3 {
		return w.WriteError("wrong number of arguments for 'zcount'")
	}
	minScore, err := parseScoreArg(args[1])
	if err != nil {
		return w.WriteError("min value is not a float")
	}
	maxScore, err := parseScoreArg(args[2])
	if err != nil {
		return w.WriteError("max value is not a float")
	}
	n, err := db.ZCount(args[0], minScore, maxScore)
	if err != nil {
		return writeEngineError(w, err)
	}
	return w.WriteInteger(int64(n))
}

// ---- 工具函数 ----

func writeEngineError(w *resp.Writer, err error) error {
	return w.WriteErrorRaw(err.Error())
}

// parseScoreArg 解析 score 参数，支持 -inf / +inf
func parseScoreArg(s string) (float64, error) {
	switch strings.ToLower(s) {
	case "-inf":
		return math.Inf(-1), nil
	case "+inf", "inf":
		return math.Inf(1), nil
	}
	return strconv.ParseFloat(s, 64)
}

// cmdScan 实现游标扫描（v0.4 迁移使用）
// 语法: SCAN cursor [MATCH pattern] [COUNT count]
// 返回: *2 [nextCursor, [keys...]]
func cmdScan(db *engine.GoRedis, args []string, w *resp.Writer) error {
	if len(args) < 1 {
		return w.WriteError("wrong number of arguments for 'scan'")
	}

	cursor, err := strconv.Atoi(args[0])
	if err != nil || cursor < 0 {
		return w.WriteError("invalid cursor")
	}

	pattern := "*"
	count := 100
	for i := 1; i+1 < len(args); i += 2 {
		switch strings.ToLower(args[i]) {
		case "match":
			pattern = args[i+1]
		case "count":
			if n, e := strconv.Atoi(args[i+1]); e == nil && n > 0 {
				count = n
			}
		}
	}

	allKeys := db.Keys(pattern)
	total := len(allKeys)
	nextCursor := 0
	end := cursor + count
	if end >= total {
		end = total
		nextCursor = 0
	} else {
		nextCursor = end
	}
	batch := allKeys[cursor:end]

	if err := w.WriteArrayHeader(2); err != nil {
		return err
	}
	if err := w.WriteBulkString(strconv.Itoa(nextCursor)); err != nil {
		return err
	}
	return w.WriteStringArray(batch)
}
