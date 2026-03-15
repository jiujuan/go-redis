package migration

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/jiujuan/go-redis/internal/resp"
	"github.com/jiujuan/go-redis/pkg/pool"
)

// NodeAgent 通过 RESP 协议与节点通信，实现 KeyFetcher 和 KeyMover 接口
//
// 实现说明：
//   - ScanKeys: 使用 SCAN 命令游标扫描（需服务端支持 SCAN）
//   - MoveKey:  DUMP key → RESTORE key on dst → DEL key on src
//     其中 RESTORE 使用 REPLACE 选项，保证幂等；
//     写入新节点前检查 newRing 上该 key 是否已有更新的值（通过比较写入时间戳 TTL 字段），
//     实际上对于无 TTL 的内存数据库，直接用 SET 系列命令重写即可。
type NodeAgent struct {
	pools   map[string]*pool.Pool
	poolCfg *pool.Config
}

// NewNodeAgent 创建节点代理
func NewNodeAgent(nodes []string, poolCfg *pool.Config) *NodeAgent {
	if poolCfg == nil {
		poolCfg = pool.DefaultConfig()
	}
	na := &NodeAgent{
		pools:   make(map[string]*pool.Pool),
		poolCfg: poolCfg,
	}
	for _, node := range nodes {
		na.pools[node] = pool.NewPool(node, poolCfg)
	}
	return na
}

// AddNode 动态注册节点连接池
func (na *NodeAgent) AddNode(node string) {
	if _, ok := na.pools[node]; !ok {
		na.pools[node] = pool.NewPool(node, na.poolCfg)
	}
}

// Close 关闭所有连接池
func (na *NodeAgent) Close() {
	for _, p := range na.pools {
		p.Close()
	}
}

// ---- KeyFetcher 实现 ----

// ScanKeys 使用 SCAN 命令游标扫描节点上的 key
// 返回 (nextCursor, keys, error)，nextCursor=0 表示全部扫描完毕
func (na *NodeAgent) ScanKeys(node string, cursor int, count int) (int, []string, error) {
	v, err := na.sendCmd(node, "SCAN", strconv.Itoa(cursor), "COUNT", strconv.Itoa(count))
	if err != nil {
		return 0, nil, fmt.Errorf("SCAN on %s: %w", node, err)
	}

	// SCAN 返回 *2 数组: [cursor, [keys...]]
	if v.Type != '*' || len(v.Array) != 2 {
		return 0, nil, fmt.Errorf("unexpected SCAN response from %s", node)
	}

	nextCursor, err := strconv.Atoi(v.Array[0].Str)
	if err != nil {
		return 0, nil, fmt.Errorf("parse cursor: %w", err)
	}

	keysVal := v.Array[1]
	keys := make([]string, 0, len(keysVal.Array))
	for _, item := range keysVal.Array {
		if item.Str != "" {
			keys = append(keys, item.Str)
		}
	}

	return nextCursor, keys, nil
}

// ---- KeyMover 实现 ----

// MoveKey 将 key 从 srcNode 搬运到 dstNode
//
// 迁移流程（保证不丢数据、不覆盖新写入）：
//  1. 读取 key 的类型和数据（TYPE + 对应的 GET/HGETALL 等）
//  2. 在 dstNode 上使用 SET/HSET/... 写入（新节点上若已存在则跳过，避免覆盖新写入）
//  3. 确认 dstNode 写入成功后，在 srcNode 上 DEL
func (na *NodeAgent) MoveKey(key, srcNode, dstNode string) error {
	// Step 1: 获取 key 类型
	typeVal, err := na.sendCmd(srcNode, "TYPE", key)
	if err != nil {
		return fmt.Errorf("TYPE %s on %s: %w", key, srcNode, err)
	}
	keyType := typeVal.Str
	if keyType == "none" {
		// key 已被删除（可能已被其他并发迁移处理）
		return nil
	}

	// Step 2: 检查 dstNode 是否已有该 key（避免覆盖新写入）
	existsVal, err := na.sendCmd(dstNode, "EXISTS", key)
	if err != nil {
		return fmt.Errorf("EXISTS %s on %s: %w", key, dstNode, err)
	}
	if existsVal.Integer > 0 {
		// dstNode 上已有该 key（很可能是新写入），直接删除 srcNode 上的旧副本
		log.Printf("[node-agent] key %s already exists on dst %s, skipping write, deleting src copy", key, dstNode)
		_, err = na.sendCmd(srcNode, "DEL", key)
		return err
	}

	// Step 3: 按类型读取并写入
	switch keyType {
	case "string":
		if err := na.moveString(key, srcNode, dstNode); err != nil {
			return err
		}
	case "hash":
		if err := na.moveHash(key, srcNode, dstNode); err != nil {
			return err
		}
	case "list":
		if err := na.moveList(key, srcNode, dstNode); err != nil {
			return err
		}
	case "set":
		if err := na.moveSet(key, srcNode, dstNode); err != nil {
			return err
		}
	case "zset":
		if err := na.moveZSet(key, srcNode, dstNode); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported type %s for key %s", keyType, key)
	}

	// Step 4: 写入成功后，删除 srcNode 上的旧副本
	_, err = na.sendCmd(srcNode, "DEL", key)
	if err != nil {
		// DEL 失败不是致命错误（下次迁移会再次 EXISTS 检查），记录日志即可
		log.Printf("[node-agent] DEL %s on src %s failed: %v (non-fatal)", key, srcNode, err)
	}
	return nil
}

func (na *NodeAgent) moveString(key, srcNode, dstNode string) error {
	v, err := na.sendCmd(srcNode, "GET", key)
	if err != nil || v.IsNil {
		return fmt.Errorf("GET %s: %w", key, err)
	}
	_, err = na.sendCmd(dstNode, "SET", key, v.Str)
	return err
}

func (na *NodeAgent) moveHash(key, srcNode, dstNode string) error {
	v, err := na.sendCmd(srcNode, "HGETALL", key)
	if err != nil || len(v.Array) == 0 {
		return fmt.Errorf("HGETALL %s: %w", key, err)
	}
	args := []string{"HSET", key}
	for _, item := range v.Array {
		args = append(args, item.Str)
	}
	_, err = na.sendCmd(dstNode, args...)
	return err
}

func (na *NodeAgent) moveList(key, srcNode, dstNode string) error {
	v, err := na.sendCmd(srcNode, "LRANGE", key, "0", "-1")
	if err != nil || len(v.Array) == 0 {
		return fmt.Errorf("LRANGE %s: %w", key, err)
	}
	args := []string{"RPUSH", key}
	for _, item := range v.Array {
		args = append(args, item.Str)
	}
	_, err = na.sendCmd(dstNode, args...)
	return err
}

func (na *NodeAgent) moveSet(key, srcNode, dstNode string) error {
	v, err := na.sendCmd(srcNode, "SMEMBERS", key)
	if err != nil || len(v.Array) == 0 {
		return fmt.Errorf("SMEMBERS %s: %w", key, err)
	}
	args := []string{"SADD", key}
	for _, item := range v.Array {
		args = append(args, item.Str)
	}
	_, err = na.sendCmd(dstNode, args...)
	return err
}

func (na *NodeAgent) moveZSet(key, srcNode, dstNode string) error {
	v, err := na.sendCmd(srcNode, "ZRANGE", key, "0", "-1", "WITHSCORES")
	if err != nil || len(v.Array) == 0 {
		return fmt.Errorf("ZRANGE %s WITHSCORES: %w", key, err)
	}
	// Array: [member, score, member, score, ...]
	args := []string{"ZADD", key}
	for i := 0; i+1 < len(v.Array); i += 2 {
		member := v.Array[i].Str
		score := v.Array[i+1].Str
		args = append(args, score, member)
	}
	_, err = na.sendCmd(dstNode, args...)
	return err
}

// ---- 内部通信工具 ----

func (na *NodeAgent) sendCmd(node string, args ...string) (*resp.Value, error) {
	p, ok := na.pools[node]
	if !ok {
		return nil, fmt.Errorf("no pool for node %s", node)
	}

	conn, err := p.Get()
	if err != nil {
		return nil, fmt.Errorf("get conn: %w", err)
	}

	val, err := sendRESP(conn, args...)
	if err != nil {
		p.Put(conn, true) // 丢弃错误连接
		return nil, err
	}
	p.Put(conn, false)

	if val.Type == '-' {
		return val, fmt.Errorf("redis error: %s", val.Str)
	}
	return val, nil
}

func sendRESP(conn net.Conn, args ...string) (*resp.Value, error) {
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	defer conn.SetDeadline(time.Time{})

	w := resp.NewWriter(conn)
	if err := w.WriteArrayHeader(len(args)); err != nil {
		return nil, err
	}
	for _, a := range args {
		if err := w.WriteBulkString(a); err != nil {
			return nil, err
		}
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}

	r := resp.NewReader(conn)
	return r.Read()
}

// ---- SCAN 命令支持（需要在 server/command.go 中注册）----

// ScanResult SCAN 命令返回结果
type ScanResult struct {
	Cursor int
	Keys   []string
}

// FormatScanArgs 构建 SCAN 参数
func FormatScanArgs(cursor int, match string, count int) []string {
	args := []string{"SCAN", strconv.Itoa(cursor)}
	if match != "" && match != "*" {
		args = append(args, "MATCH", match)
	}
	args = append(args, "COUNT", strconv.Itoa(count))
	return args
}

var _ = strings.ToLower // suppress unused import
