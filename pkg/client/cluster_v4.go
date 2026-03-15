// Package client 实现 go-redis 的分布式集群客户端（v0.3/v0.4）。
//
// v0.4 新增：
//   - AddNode 触发自动渐进式数据迁移
//   - 迁移期间双环路由：写 newRing，读 newRing miss 兜底 oldRing
//   - 迁移进度查询、取消、事件回调
package client

import (
	"fmt"
	"hash/crc32"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jiujuan/go-redis/internal/resp"
	"github.com/jiujuan/go-redis/pkg/migration"
	"github.com/jiujuan/go-redis/pkg/pool"
)

const defaultVirtualReplicas = 150

// ClusterClient 分布式集群客户端（v0.4：含自动迁移）
type ClusterClient struct {
	mu              sync.RWMutex
	nodes           []string
	pools           map[string]*pool.Pool
	dead            map[string]time.Time
	virtualReplicas int
	poolCfg         *pool.Config

	// v0.4: 迁移支持
	ringMgr   *migration.RingManager
	agent     *migration.NodeAgent
	migrator  *migration.Migrator
	migCfg    *migration.MigrationConfig
	migTaskID int64 // 递增任务 ID
}

// Option 集群客户端选项
type Option func(*ClusterClient)

// WithVirtualReplicas 设置虚拟节点数
func WithVirtualReplicas(n int) Option {
	return func(c *ClusterClient) {
		if n > 0 {
			c.virtualReplicas = n
		}
	}
}

// WithPoolConfig 设置连接池配置
func WithPoolConfig(cfg *pool.Config) Option {
	return func(c *ClusterClient) {
		c.poolCfg = cfg
	}
}

// WithMigrationConfig 设置迁移配置
func WithMigrationConfig(cfg *migration.MigrationConfig) Option {
	return func(c *ClusterClient) {
		c.migCfg = cfg
	}
}

// NewClusterClient 创建集群客户端
func NewClusterClient(nodes []string, opts ...Option) *ClusterClient {
	c := &ClusterClient{
		pools:           make(map[string]*pool.Pool),
		dead:            make(map[string]time.Time),
		virtualReplicas: defaultVirtualReplicas,
		poolCfg:         pool.DefaultConfig(),
		migCfg:          migration.DefaultMigrationConfig(),
	}
	for _, opt := range opts {
		opt(c)
	}

	// 初始化连接池
	for _, node := range nodes {
		c.pools[node] = pool.NewPool(node, c.poolCfg)
	}
	c.nodes = append(c.nodes, nodes...)

	// 初始化哈希环管理器
	c.ringMgr = migration.NewRingManager(nodes, c.virtualReplicas)

	// 初始化节点代理（用于迁移）
	c.agent = migration.NewNodeAgent(nodes, c.poolCfg)

	return c
}

// ========== v0.4：节点扩缩容 ==========

// AddNode 添加新节点并触发自动数据迁移
//
// 迁移在后台进行，期间读写操作照常进行：
//   - 新写入直接落到新环目标节点
//   - 读请求 miss 时自动兜底旧环（对业务透明）
//
// 返回迁移任务 ID，可用于查询进度
func (c *ClusterClient) AddNode(newNode string) (string, error) {
	return c.AddNodes([]string{newNode})
}

// AddNodes 批量添加新节点并触发迁移
func (c *ClusterClient) AddNodes(newNodes []string) (string, error) {
	c.mu.Lock()
	// 过滤已存在的节点
	toAdd := newNodes[:0]
	for _, n := range newNodes {
		if _, ok := c.pools[n]; !ok {
			toAdd = append(toAdd, n)
		}
	}
	if len(toAdd) == 0 {
		c.mu.Unlock()
		return "", fmt.Errorf("all nodes already exist")
	}

	// 注册连接池
	for _, node := range toAdd {
		c.pools[node] = pool.NewPool(node, c.poolCfg)
		c.nodes = append(c.nodes, node)
		c.agent.AddNode(node)
	}
	c.mu.Unlock()

	// 生成任务 ID
	c.mu.Lock()
	c.migTaskID++
	taskID := fmt.Sprintf("mig-%d-%d", time.Now().Unix(), c.migTaskID)
	c.mu.Unlock()

	// 创建迁移器
	c.migrator = migration.NewMigrator(c.migCfg, c.ringMgr, c.agent, c.agent).
		OnProgress(func(p migration.Progress) {
			log.Printf("[cluster] migration progress: %.1f%% (migrated=%d)",
				p.Percent, p.Task.MigratedKeys)
		}).
		OnDone(func(task *migration.MigrationTask) {
			log.Printf("[cluster] migration done: task=%s elapsed=%s",
				task.ID, task.FinishedAt.Sub(task.StartedAt))
		}).
		OnError(func(key string, err error) {
			log.Printf("[cluster] migrate key %s failed: %v", key, err)
		})

	if err := c.migrator.Start(taskID, toAdd); err != nil {
		return "", fmt.Errorf("start migration: %w", err)
	}

	log.Printf("[cluster] added nodes %v, migration task %s started", toAdd, taskID)
	return taskID, nil
}

// MigrationProgress 查询当前迁移进度
func (c *ClusterClient) MigrationProgress() *migration.Progress {
	if c.migrator == nil {
		return nil
	}
	p := c.migrator.Progress()
	return &p
}

// WaitMigration 等待当前迁移任务完成（阻塞）
func (c *ClusterClient) WaitMigration() {
	if c.migrator != nil {
		c.migrator.Wait()
	}
}

// CancelMigration 取消迁移（回滚到旧环）
func (c *ClusterClient) CancelMigration() {
	if c.migrator != nil {
		c.migrator.Cancel()
	}
}

// IsMigrating 是否正在迁移
func (c *ClusterClient) IsMigrating() bool {
	return c.ringMgr.IsMigrating()
}

// RemoveNode 从集群中移除节点
func (c *ClusterClient) RemoveNode(node string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	p, ok := c.pools[node]
	if !ok {
		return
	}
	p.Close()
	delete(c.pools, node)

	newNodes := c.nodes[:0]
	for _, n := range c.nodes {
		if n != node {
			newNodes = append(newNodes, n)
		}
	}
	c.nodes = newNodes
	log.Printf("[cluster] node removed: %s", node)
}

// ========== 路由核心：双环读写 ==========

// getWriteNode 写操作节点：始终走 newRing
func (c *ClusterClient) getWriteNode(key string) (string, error) {
	node := c.ringMgr.GetWriteNode(key)
	if node == "" {
		return "", fmt.Errorf("no available nodes")
	}
	return node, nil
}

// execute 带双环兜底的执行入口
//   - 写命令：isWrite=true，只走 newRing
//   - 读命令：isWrite=false，newRing miss 时兜底 oldRing
func (c *ClusterClient) execute(key string, isWrite bool, args ...string) (*resp.Value, error) {
	if isWrite {
		return c.executeWrite(key, args...)
	}
	return c.executeRead(key, args...)
}

func (c *ClusterClient) executeWrite(key string, args ...string) (*resp.Value, error) {
	node, err := c.getWriteNode(key)
	if err != nil {
		return nil, err
	}
	return c.sendToNode(node, args...)
}

func (c *ClusterClient) executeRead(key string, args ...string) (*resp.Value, error) {
	primary, fallback := c.ringMgr.GetReadNodes(key)
	if primary == "" {
		return nil, fmt.Errorf("no available nodes")
	}

	val, err := c.sendToNode(primary, args...)
	if err != nil {
		c.markDead(primary)
		// 尝试兜底节点
		if fallback != "" {
			return c.sendToNode(fallback, args...)
		}
		return nil, err
	}

	// 读到 nil（key 不存在于新节点）且迁移中 → 兜底旧节点
	if val.IsNil && fallback != "" && c.ringMgr.IsMigrating() {
		fallbackVal, ferr := c.sendToNode(fallback, args...)
		if ferr == nil && !fallbackVal.IsNil {
			return fallbackVal, nil
		}
	}

	return val, nil
}

func (c *ClusterClient) sendToNode(node string, args ...string) (*resp.Value, error) {
	c.mu.RLock()
	p, ok := c.pools[node]
	c.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no pool for node %s", node)
	}

	conn, err := p.Get()
	if err != nil {
		c.markDead(node)
		return nil, fmt.Errorf("get conn from pool %s: %w", node, err)
	}

	val, err := c.sendCommand(conn, args...)
	if err != nil {
		p.Put(conn, true)
		c.markDead(node)
		return nil, fmt.Errorf("send to %s: %w", node, err)
	}
	p.Put(conn, false)
	return val, nil
}

func (c *ClusterClient) executeOnNode(node string, args ...string) (*resp.Value, error) {
	return c.sendToNode(node, args...)
}

func (c *ClusterClient) sendCommand(conn net.Conn, args ...string) (*resp.Value, error) {
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

func (c *ClusterClient) markDead(node string) {
	c.mu.Lock()
	c.dead[node] = time.Now()
	c.mu.Unlock()
	log.Printf("[cluster] node marked as dead: %s", node)
}

// Close 关闭所有连接池
func (c *ClusterClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range c.pools {
		p.Close()
	}
	if c.agent != nil {
		c.agent.Close()
	}
}

// ========== String API ==========

func (c *ClusterClient) Set(key, value string) error {
	v, err := c.execute(key, true, "SET", key, value)
	return checkOKReply(v, err)
}

func (c *ClusterClient) Get(key string) (string, error) {
	v, err := c.execute(key, false, "GET", key)
	if err != nil {
		return "", err
	}
	if v.IsNil {
		return "", nil
	}
	return v.Str, nil
}

func (c *ClusterClient) Del(keys ...string) (int, error) {
	groups := make(map[string][]string)
	for _, k := range keys {
		node, err := c.getWriteNode(k)
		if err != nil {
			return 0, err
		}
		groups[node] = append(groups[node], k)
	}
	total := 0
	for node, ks := range groups {
		args := append([]string{"DEL"}, ks...)
		v, err := c.executeOnNode(node, args...)
		if err != nil {
			return total, err
		}
		total += int(v.Integer)
	}
	return total, nil
}

func (c *ClusterClient) Exists(key string) (bool, error) {
	v, err := c.execute(key, false, "EXISTS", key)
	if err != nil {
		return false, err
	}
	return v.Integer > 0, nil
}

func (c *ClusterClient) Incr(key string) (int64, error) {
	v, err := c.execute(key, true, "INCR", key)
	if err != nil {
		return 0, err
	}
	return v.Integer, nil
}

func (c *ClusterClient) Decr(key string) (int64, error) {
	v, err := c.execute(key, true, "DECR", key)
	if err != nil {
		return 0, err
	}
	return v.Integer, nil
}

func (c *ClusterClient) MSet(pairs ...string) error {
	if len(pairs)%2 != 0 {
		return fmt.Errorf("MSet requires even arguments")
	}
	for i := 0; i < len(pairs); i += 2 {
		if err := c.Set(pairs[i], pairs[i+1]); err != nil {
			return err
		}
	}
	return nil
}

func (c *ClusterClient) MGet(keys ...string) ([]string, error) {
	result := make([]string, len(keys))
	for i, k := range keys {
		val, err := c.Get(k)
		if err != nil {
			return nil, err
		}
		result[i] = val
	}
	return result, nil
}

// ========== Hash API ==========

func (c *ClusterClient) HSet(key string, fieldValues ...string) (int, error) {
	args := append([]string{"HSET", key}, fieldValues...)
	v, err := c.execute(key, true, args...)
	if err != nil {
		return 0, err
	}
	return int(v.Integer), nil
}

func (c *ClusterClient) HGet(key, field string) (string, error) {
	v, err := c.execute(key, false, "HGET", key, field)
	if err != nil {
		return "", err
	}
	if v.IsNil {
		return "", nil
	}
	return v.Str, nil
}

func (c *ClusterClient) HGetAll(key string) ([]string, error) {
	v, err := c.execute(key, false, "HGETALL", key)
	if err != nil {
		return nil, err
	}
	result := make([]string, len(v.Array))
	for i, item := range v.Array {
		result[i] = item.Str
	}
	return result, nil
}

func (c *ClusterClient) HDel(key string, fields ...string) (int, error) {
	args := append([]string{"HDEL", key}, fields...)
	v, err := c.execute(key, true, args...)
	if err != nil {
		return 0, err
	}
	return int(v.Integer), nil
}

// ========== List API ==========

func (c *ClusterClient) LPush(key string, values ...string) (int, error) {
	args := append([]string{"LPUSH", key}, values...)
	v, err := c.execute(key, true, args...)
	if err != nil {
		return 0, err
	}
	return int(v.Integer), nil
}

func (c *ClusterClient) RPush(key string, values ...string) (int, error) {
	args := append([]string{"RPUSH", key}, values...)
	v, err := c.execute(key, true, args...)
	if err != nil {
		return 0, err
	}
	return int(v.Integer), nil
}

func (c *ClusterClient) LPop(key string) (string, error) {
	v, err := c.execute(key, true, "LPOP", key)
	if err != nil {
		return "", err
	}
	if v.IsNil {
		return "", nil
	}
	return v.Str, nil
}

func (c *ClusterClient) RPop(key string) (string, error) {
	v, err := c.execute(key, true, "RPOP", key)
	if err != nil {
		return "", err
	}
	if v.IsNil {
		return "", nil
	}
	return v.Str, nil
}

func (c *ClusterClient) LRange(key string, start, stop int) ([]string, error) {
	v, err := c.execute(key, false, "LRANGE", key, strconv.Itoa(start), strconv.Itoa(stop))
	if err != nil {
		return nil, err
	}
	result := make([]string, len(v.Array))
	for i, item := range v.Array {
		result[i] = item.Str
	}
	return result, nil
}

// ========== Set API ==========

func (c *ClusterClient) SAdd(key string, members ...string) (int, error) {
	args := append([]string{"SADD", key}, members...)
	v, err := c.execute(key, true, args...)
	if err != nil {
		return 0, err
	}
	return int(v.Integer), nil
}

func (c *ClusterClient) SMembers(key string) ([]string, error) {
	v, err := c.execute(key, false, "SMEMBERS", key)
	if err != nil {
		return nil, err
	}
	result := make([]string, len(v.Array))
	for i, item := range v.Array {
		result[i] = item.Str
	}
	return result, nil
}

func (c *ClusterClient) SRem(key string, members ...string) (int, error) {
	args := append([]string{"SREM", key}, members...)
	v, err := c.execute(key, true, args...)
	if err != nil {
		return 0, err
	}
	return int(v.Integer), nil
}

func (c *ClusterClient) SIsMember(key, member string) (bool, error) {
	v, err := c.execute(key, false, "SISMEMBER", key, member)
	if err != nil {
		return false, err
	}
	return v.Integer > 0, nil
}

// ========== ZSet API ==========

func (c *ClusterClient) ZAdd(key string, score float64, member string) (int, error) {
	v, err := c.execute(key, true, "ZADD", key, strconv.FormatFloat(score, 'f', -1, 64), member)
	if err != nil {
		return 0, err
	}
	return int(v.Integer), nil
}

func (c *ClusterClient) ZScore(key, member string) (string, error) {
	v, err := c.execute(key, false, "ZSCORE", key, member)
	if err != nil {
		return "", err
	}
	if v.IsNil {
		return "", nil
	}
	return v.Str, nil
}

func (c *ClusterClient) ZRange(key string, start, stop int, withScore bool) ([]string, error) {
	args := []string{"ZRANGE", key, strconv.Itoa(start), strconv.Itoa(stop)}
	if withScore {
		args = append(args, "WITHSCORES")
	}
	v, err := c.execute(key, false, args...)
	if err != nil {
		return nil, err
	}
	result := make([]string, len(v.Array))
	for i, item := range v.Array {
		result[i] = item.Str
	}
	return result, nil
}

func (c *ClusterClient) ZRem(key string, members ...string) (int, error) {
	args := append([]string{"ZREM", key}, members...)
	v, err := c.execute(key, true, args...)
	if err != nil {
		return 0, err
	}
	return int(v.Integer), nil
}

func (c *ClusterClient) ZRank(key, member string) (int64, error) {
	v, err := c.execute(key, false, "ZRANK", key, member)
	if err != nil {
		return -1, err
	}
	if v.IsNil {
		return -1, nil
	}
	return v.Integer, nil
}

// ========== 集群管理 ==========

// Nodes 返回当前集群节点列表
func (c *ClusterClient) Nodes() []string {
	c.mu.RLock()
	result := make([]string, len(c.nodes))
	copy(result, c.nodes)
	c.mu.RUnlock()
	return result
}

// NodeForKey 返回 key 的写入节点
func (c *ClusterClient) NodeForKey(key string) (string, error) {
	return c.getWriteNode(key)
}

// HealthCheck 对所有节点发送 PING
func (c *ClusterClient) HealthCheck() {
	c.mu.RLock()
	nodes := make([]string, len(c.nodes))
	copy(nodes, c.nodes)
	c.mu.RUnlock()

	for _, node := range nodes {
		v, err := c.executeOnNode(node, "PING")
		if err != nil || v == nil {
			c.markDead(node)
			continue
		}
		c.mu.Lock()
		delete(c.dead, node)
		c.mu.Unlock()
	}
}

// StartHealthCheck 启动后台健康检查
func (c *ClusterClient) StartHealthCheck(interval time.Duration) func() {
	ticker := time.NewTicker(interval)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				c.HealthCheck()
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
	return func() { close(quit) }
}

// KeyDistribution 统计各节点 key 分布（用于测试一致性哈希均匀性）
func KeyDistribution(nodes []string, keys []string, virtualReplicas int) map[string]int {
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

// ---- 工具 ----

func checkOKReply(v *resp.Value, err error) error {
	if err != nil {
		return err
	}
	if v != nil && v.Type == '-' {
		return fmt.Errorf("%s", v.Str)
	}
	return nil
}

var _ = strings.ToLower
