# go-redis 核心实现分析

本文分析以下 4 个模块的设计与实现：

- `internal/persistence`：持久化
- `internal/resp`：RESP 协议编解码
- `internal/engine/shard.go`：数据库分片
- `pkg/client`：分布式客户端

目标不是逐行翻译源码，而是说明这些模块“为什么这么设计”“核心流程是什么”“有哪些优点和限制”。

## 1. 持久化模块：`internal/persistence`

### 1.1 模块职责

持久化模块提供两套能力：

- `AOF`：追加写日志，记录写命令，重启时通过重放命令恢复数据
- `RDB`：快照式持久化，将内存状态整体编码到磁盘

这两套机制分别对应 Redis 里常见的两种持久化思路：

- AOF 更偏“操作日志”
- RDB 更偏“状态快照”

当前项目里，这个模块本身是相对独立的底层组件，负责文件格式、刷盘策略和恢复逻辑，不直接耦合命令分发细节。

### 1.2 AOF 的架构与实现

文件：`internal/persistence/aof.go`

`AOF` 结构体包含以下关键字段：

- `file`：底层文件句柄
- `writer`：带缓冲的 `bufio.Writer`
- `syncMode`：刷盘策略，支持 `always` / `everysec` / `no`
- `ticker` + `quit` + `wg`：用于后台定时刷盘
- `mu`：保护写入和刷盘的互斥锁

#### 1.2.1 写入格式

`Write(args []string)` 会把命令按 RESP Array 格式直接写入 AOF：

```text
*3
$3
SET
$1
k
$1
v
```

这个设计有两个明显好处：

- 不需要额外定义私有日志格式，直接复用客户端/服务端都已使用的 RESP 协议
- 恢复时可以按协议逐条解析，不需要做命令专用的反序列化

#### 1.2.2 刷盘策略

支持三种模式：

- `always`：每次 `Write` 后立刻 `Flush + Sync`
- `everysec`：后台 goroutine 每秒 `Flush + Sync`
- `no`：只写入用户态缓冲，不主动 `Sync`

实现上：

- `always` 直接在 `Write` 中调用 `syncLocked()`
- `everysec` 在 `NewAOF` 中创建 ticker，并启动 `syncLoop()`
- `no` 只依赖 `Close()` 或进程退出前的 flush

这是一个很典型的“吞吐 vs. durability”折中设计：

- `always` 最安全，但延迟高
- `everysec` 更接近 Redis 默认思路，平衡吞吐和数据安全
- `no` 吞吐最好，但崩溃时丢失数据最多

#### 1.2.3 并发控制

`Write` 和后台刷盘共享同一个 `mu`：

- 避免一边写缓冲一边 flush 导致缓冲内容交错
- 确保 `writer.Flush()` 与 `file.Sync()` 看到的是完整命令边界

这个锁粒度比较粗，但实现简单、安全，适合当前项目规模。

#### 1.2.4 恢复流程

`Replay(filename, execFn)` 的职责是：

1. 打开 AOF 文件
2. 循环读取一条 RESP 命令
3. 调用 `execFn(args)` 执行恢复

核心辅助函数：

- `readCommand()`：读取一个 RESP Array 命令
- `readLine()`：读取 CRLF 行

这里的恢复策略有两个特点：

- 文件不存在时返回 `nil`，把“首次启动”视为正常情况
- 解析错误或执行错误只记录日志，不一定立即中止整个进程

这说明当前设计偏向“尽量恢复能恢复的数据”，而不是“发现损坏就强一致失败”。

### 1.3 RDB 的架构与实现

文件：`internal/persistence/rdb.go`

RDB 采用 `gob` 做序列化，而不是实现 Redis 原生 RDB 格式。

#### 1.3.1 数据模型

快照对象是：

- `RDBSnapshot`
  - `CreatedAt`
  - `Entries []*RDBEntry`

单条记录是：

- `RDBEntry`
  - `Type`
  - `Key`
  - `Value interface{}`

不同类型的值再用独立结构承载：

- `RDBStringVal`
- `RDBHashVal`
- `RDBListVal`
- `RDBSetVal`
- `RDBZSetVal`

这是一种“统一 entry + 类型化 payload”的方案。优点是结构清晰，恢复时能按类型分发；代价是需要手工维护 Type 和具体 Value 结构的一致性。

#### 1.3.2 保存流程

`SaveRDB(filename, snapshot)` 的流程：

1. 创建临时文件 `filename.tmp`
2. 用 `gob.Encoder` 序列化整个快照
3. `Flush`
4. `Sync`
5. `Close`
6. `Rename(tmp, filename)` 原子替换正式文件

这是标准的“写临时文件再 rename”模式，核心目的是：

- 避免在写到一半时破坏旧快照
- 尽量保证磁盘上始终有一份完整快照

#### 1.3.3 加载流程

`LoadRDB(filename)`：

1. 文件不存在时返回 `(nil, nil)`
2. 用 `gob.Decoder` 解码整个 `RDBSnapshot`
3. 返回快照对象

模块初始化时调用 `gob.Register(...)` 注册各类值类型，保证 `interface{}` 里的具体值能正确解码。

### 1.4 持久化模块的优点

- AOF 和 RDB 分工清晰，思路容易理解
- AOF 直接复用 RESP，格式统一
- RDB 保存流程采用原子替换，可靠性比直接覆盖好
- 并发模型简单，锁边界清楚
- 恢复逻辑和存储逻辑解耦，易于嵌入其他上层模块

### 1.5 持久化模块的限制与注意点

#### AOF

- 没有 AOF 重写机制，文件会持续增长
- `Replay` 的错误恢复策略较保守，损坏文件时只做日志记录，不做更强修复
- `syncLoop()` 里对 `syncLocked()` 的错误没有向外传播

#### RDB

- 使用 `gob` 而不是 Redis 原生 RDB，因此兼容性只限 Go 内部实现
- `Value interface{}` 虽然灵活，但类型约束依赖调用方自觉维护
- 目前更适合单进程内部快照，不适合作为跨语言/跨版本标准交换格式

### 1.6 测试覆盖反映出的实现重点

从 `internal/persistence/persistence_test.go` 可以看出，当前模块重点验证了：

- AOF 三种刷盘模式
- AOF 重放正确性
- 特殊字符和大数据量
- RDB 全类型保存/加载
- 临时文件原子替换
- 文件不存在、损坏文件、错误路径

说明这个模块目前追求的是“本地可靠性”和“恢复可用性”，而不是格式兼容性。

## 2. RESP 协议模块：`internal/resp`

### 2.1 模块职责

RESP 模块负责 Redis 协议的编解码，是网络层和命令层之间的桥梁。

它做两件事：

- `Reader`：把字节流解析成结构化 `Value`
- `Writer`：把结构化值编码成 RESP 文本

### 2.2 数据模型

核心结构是 `Value`：

- `Type`：`+ - : $ *`
- `Str`
- `Integer`
- `Array`
- `IsNil`

这是一个统一 AST 风格的协议值对象。优点是：

- 服务端处理命令和客户端读响应都能复用同一套结构
- 递归数组、nil bulk、nil array 都能表达

### 2.3 Reader 实现分析

文件：`internal/resp/reader.go`

`Read()` 的流程：

1. 先读一行
2. 根据首字节判断类型
3. 进入不同分支解析

支持的 RESP 类型：

- `+` Simple String
- `-` Error
- `:` Integer
- `$` Bulk String
- `*` Array

#### 2.3.1 Bulk String 解析

`readBulkString()` 会：

1. 解析长度
2. 对 `-1` 视为 nil
3. 用 `io.ReadFull` 读取 `n+2` 个字节
4. 丢掉末尾 `\r\n`

这是标准 RESP bulk string 处理。

#### 2.3.2 Array 解析

`readArray()` 递归调用 `Read()` 解析每个元素，因此天然支持嵌套数组。

这也是 RESP 模块最关键的设计点之一：

- 解析逻辑统一
- 嵌套结构不需要额外状态机

#### 2.3.3 Inline 命令兼容

当首字符不是标准 RESP 前缀时，`Read()` 会退化到 `parseInline()`。

这意味着模块兼容：

- `PING\r\n`
- `SET key value\r\n`

适合 telnet、nc、调试终端直接输入命令。

`splitInline()` 还支持简单引号处理：

- 单引号
- 双引号

这不是完整 shell parser，但足够覆盖开发调试场景。

#### 2.3.4 ToArgs

`Value.ToArgs()` 的作用是把 RESP Array 转成命令参数切片。

它做了两个校验：

- 顶层必须是数组
- 每个元素必须是 bulk string

这让上层命令分发可以明确依赖“协议已经归一化”。

### 2.4 Writer 实现分析

文件：`internal/resp/writer.go`

Writer 以 `bufio.Writer` 为基础，提供按类型输出 RESP 的方法：

- `WriteSimpleString`
- `WriteError`
- `WriteErrorRaw`
- `WriteInteger`
- `WriteBulkString`
- `WriteNilBulk`
- `WriteArrayHeader`
- `WriteNilArray`
- `WriteStringArray`
- `WriteValue`

#### 2.4.1 设计特点

- 每种 RESP 类型对应一个明确 API，调用点清晰
- `WriteValue` 提供统一入口，适合递归数组与通用转发
- `WriteError` 会自动补 `ERR` 前缀，`WriteErrorRaw` 则允许上层输出原始错误文本

这种划分对服务端很友好：

- 业务逻辑里可以直接表达“这是 nil bulk”“这是 integer”“这是协议错误”
- 不需要手工拼字符串

### 2.5 RESP 模块的优点

- 结构简单，几乎就是 RESP 的直接映射
- Reader/Writer 对称性好
- 兼容 inline command，开发调试友好
- 支持嵌套数组
- API 粒度合理，适合服务端和客户端两端复用

### 2.6 RESP 模块的限制

- 没有实现 RESP3，只覆盖 RESP2 风格
- Inline parser 比较简单，不支持转义等更复杂语法
- `readLine()` 假设输入最终能按 `\n` 分割，对畸形输入的恢复策略较有限

### 2.7 测试覆盖反映出的实现重点

`internal/resp/resp_test.go` 覆盖很完整，说明项目对协议层质量比较重视，尤其包括：

- 所有基础类型
- nil 场景
- 嵌套数组
- inline 和 quoted inline
- `ToArgs` 类型校验
- write/read round-trip

这意味着 RESP 模块是项目中相对成熟、边界覆盖较好的基础层。

## 3. 数据库分片实现：`internal/engine/shard.go`

### 3.1 模块职责

这个文件实现的是进程内分片存储，不是分布式分片。

目标是：

- 降低全局大锁竞争
- 让不同 key 尽量分散到不同锁上
- 对上层暴露一个逻辑统一的 KV 容器

### 3.2 核心结构

#### `shard`

单个分片由两部分组成：

- `sync.RWMutex`
- `map[string]interface{}`

这意味着每个分片内部仍然是普通 map，只是锁隔离到了分片粒度。

#### `shardedMap`

上层容器包含：

- `shards []*shard`
- `shardMask uint32`

其中 `shardMask = shardCount - 1`，要求 `shardCount` 为 2 的幂，这样可以通过：

```go
hash & shardMask
```

快速定位分片，而不用取模 `%`。

### 3.3 初始化策略

`newShardedMap(shardCount int)`：

- 如果分片数 <= 0 或不是 2 的幂，退回 `defaultShardCount`
- 预先创建所有 shard 和内部 map

这样做的优点：

- 路由逻辑简单且高效
- 分片数固定，不需要运行时扩容

代价是：

- 分片数量是静态的
- 对非常小的数据量也会预先分配全部分片

### 3.4 路由算法

`getShard(key)`：

1. 对 key 做 `fnv32`
2. 用 `hash & shardMask` 定位 shard

哈希函数使用 FNV-1a：

- 实现短小
- 分布较均匀
- 对字符串 key 很常见

这不是密码学哈希，但对内存分片已经足够。

### 3.5 基础操作实现

#### 读写删除

- `get`
- `set`
- `del`
- `exists`

每次只锁 key 所在分片，不影响其他分片。

#### 条件初始化

`getOrSet(key, fn)` 使用“双重检查”：

1. 先读锁检查
2. 不存在时再上写锁
3. 写锁内再次检查
4. 仍不存在则执行 `fn`

这是典型的 lazy init 模式，适合像 list/hash/set/zset 这种“首次访问时创建容器”的场景。

#### 锁内回调

- `withWriteLock`
- `withReadLock`

允许上层把一段逻辑放进 shard 锁内部执行。这个 API 很关键，因为它让更高层的数据结构操作可以复用分片锁，而不用暴露底层 map。

### 3.6 全局操作实现

需要扫描全部分片的操作：

- `keys()`
- `count()`
- `flush()`

这些操作会遍历所有 shard，因此复杂度会明显高于单 key 操作。

这也是分片存储的常见特征：

- 单 key 操作很快
- 全局枚举或统计会退化成 O(number_of_shards + number_of_keys)

### 3.7 分片实现的优点

- 思路直接，易于维护
- 锁粒度从全局锁降到分片锁
- 基础操作和复杂容器共享同一套路由模型
- 用 bitmask 替代取模，性能友好
- `getOrSet` 和 lock callback 给上层很好的可扩展性

### 3.8 分片实现的限制

- 分片数固定，运行期不可调
- 多 key 原子操作跨分片时不能天然保证强一致
- `keys` / `count` / `flush` 这种全局操作必须全量遍历
- `map[string]interface{}` 依赖上层自己管理类型安全

### 3.9 适用场景判断

这个实现特别适合：

- 以单 key 为主的读写
- 数据结构由 key 隔离
- 高并发但不要求跨 key 事务

不太适合：

- 强事务型跨 key 操作
- 复杂多 key 原子更新
- 需要全局扫描非常频繁的工作负载

## 4. 分布式客户端实现：`pkg/client`

### 4.1 模块职责

`pkg/client/cluster_v4.go` 实现的是面向多节点的客户端路由层，核心目标包括：

- 根据 key 把请求路由到正确节点
- 管理每个节点的连接池
- 在扩容迁移期间提供“双环路由”
- 对外暴露统一的 Redis 风格 API

这个模块不是一个完整 Redis Cluster 协议客户端，而是该项目自定义分布式模型下的客户端。

### 4.2 整体架构

`ClusterClient` 由四层能力叠加而成：

#### 1. 连接层

- `pools map[string]*pool.Pool`
- `dead map[string]time.Time`

每个节点一个连接池，失败节点记录到 `dead`。

#### 2. 路由层

- `ringMgr *migration.RingManager`
- `virtualReplicas`

负责按 key 选择节点，并在迁移期间同时维护新环和旧环。

#### 3. 迁移层

- `agent *migration.NodeAgent`
- `migrator *migration.Migrator`
- `migCfg`

负责新增节点后触发后台渐进迁移。

#### 4. API 层

对外暴露 String / Hash / List / Set / ZSet 等方法。

### 4.3 路由模型

#### 4.3.1 普通模式

- 写：`newRing`
- 读：`newRing`

#### 4.3.2 迁移模式

- 写：始终走 `newRing`
- 读：先走 `newRing`
- 如果读到 nil，且存在 `fallback`，且当前正在迁移，则回退到 `oldRing`

这是一种很实用的迁移策略：

- 保证新写入不再落到旧节点
- 旧数据还没搬完时，读请求可以兜底
- 对业务端透明

### 4.4 节点扩容流程

`AddNode()` 只是 `AddNodes([]string{...})` 的语法糖。

`AddNodes()` 的流程：

1. 过滤已存在节点
2. 为新节点创建连接池
3. 把新节点加入 `nodes`
4. 让 `agent` 注册新节点
5. 生成迁移任务 ID
6. 创建 `Migrator`
7. 绑定进度/完成/错误回调
8. 调用 `migrator.Start(taskID, toAdd)`

这是典型的“控制面更新 + 数据面异步迁移”设计。

优点：

- 扩容调用很快返回
- 迁移在后台渐进进行
- 客户端读写不中断

### 4.5 请求执行模型

对外所有 API 最终都走：

- `execute(key, isWrite, args...)`

再分发到：

- `executeWrite()`
- `executeRead()`

#### 4.5.1 写请求

1. `getWriteNode(key)`
2. `sendToNode(node, args...)`

#### 4.5.2 读请求

1. 从 `ringMgr.GetReadNodes(key)` 拿到 `primary` 和 `fallback`
2. 先请求 `primary`
3. 如果网络失败并且有 fallback，则退回 fallback
4. 如果 primary 返回 nil 且当前在迁移，则再读 fallback

这里把“节点故障兜底”和“迁移读兜底”都合并到了一个流程里。

### 4.6 网络与连接池实现思路

`sendToNode()` 的关键流程：

1. 查找节点对应连接池
2. `p.Get()` 获取连接
3. `sendCommand(conn, args...)`
4. 成功则 `Put(conn, false)`
5. 失败则 `Put(conn, true)` 并标记 dead

`sendCommand()` 使用 `resp.Writer` 和 `resp.Reader`：

- 按 RESP Array 写请求
- 读一个 RESP 响应

这说明客户端和服务端共享同一套 RESP 模块，协议栈相对统一。

### 4.7 API 层设计特点

#### 4.7.1 单 key API

例如：

- `Set`
- `Get`
- `HGet`
- `LPop`
- `ZScore`

这些 API 非常自然地适配一致性哈希路由，因为 key 唯一决定节点。

#### 4.7.2 多 key API

像 `Del(keys...)` 会先按 key 分组，再逐节点发送批量命令：

1. 每个 key 算出写节点
2. `map[node][]keys`
3. 每个节点发一次 `DEL`

这是典型的“客户端拆分聚合”模式。

而像 `MSet` / `MGet` 当前实现更简单：

- `MSet` 直接循环调用 `Set`
- `MGet` 直接循环调用 `Get`

优点是逻辑简单、跨节点天然支持；缺点是：

- 网络往返多
- 缺少批量优化

### 4.8 健康检查

`HealthCheck()`：

1. 遍历 `nodes`
2. 对每个节点执行 `PING`
3. 失败或无响应时 `markDead`
4. 成功时从 `dead` 表移除

`StartHealthCheck(interval)` 则在后台周期性执行。

这是一种轻量级探活机制，但目前 `dead` 更多是状态记录，源码里没有看到更强的“剔除 dead 节点后重路由”策略。

### 4.9 一致性哈希分布

模块里内置 `KeyDistribution()` 测试辅助函数，用于评估 key 在节点间的分布均匀性。

实现思路：

- 使用 `crc32(node#replicaIndex)` 构造虚拟节点
- 对 key 的 `crc32` 结果在环上二分查找

这和 `migration/ring.go` 的思路是一致的，说明客户端与迁移模块共享一致性哈希模型。

### 4.10 分布式客户端的优点

- 路由、连接、迁移、API 分层清楚
- 扩容流程对业务侧透明
- 迁移期间读写策略明确
- 与 RESP 模块和 migration 模块解耦良好
- API 风格接近 Redis，使用成本低

### 4.11 分布式客户端的限制

- `dead` 节点只记录状态，尚未形成更强的故障转移闭环
- `MGet` / `MSet` 等多 key 场景缺少更高效的批处理策略
- 强依赖 key 级路由，不适合复杂多 key 原子语义
- 当前实现更像“项目内部分布式客户端”，不是 Redis Cluster 标准客户端
- 一些语义是项目自定义的，例如迁移期间 fallback 读取逻辑

### 4.12 从测试看设计重点

`pkg/client/cluster_v4_test.go` 反映出客户端当前最重视这些点：

- 路由选择正确
- API 行为正确
- 参数校验
- 健康检查
- 扩容迁移控制流
- 迁移期间读兜底
- 使用 fake RESP server 做协议级 mock/stub 测试

这说明客户端模块的重点是“行为正确”和“迁移可用”，而不是极致性能优化。

## 5. 模块之间的关系

把这几个模块放在一起看，项目的层次其实很清楚：

- `internal/resp`
  - 最底层协议编解码
- `internal/engine/shard.go`
  - 最底层内存数据分片容器
- `internal/persistence`
  - 为存储层提供持久化能力
- `pkg/client`
  - 面向多节点的访问层和迁移协调入口

可以把它们理解成四个方向：

- RESP 解决“怎么说话”
- shard 解决“内存里怎么存”
- persistence 解决“掉电后怎么恢复”
- client 解决“多节点时怎么访问”

## 6. 总结

这个项目目前的实现风格很一致：

- 优先追求简单、清晰、可维护
- 用 Go 标准库和少量基础抽象完成核心能力
- 在分布式和持久化上采用“够用且可解释”的方案，而不是完全对齐 Redis 原生实现

从工程角度看，它更像一个“教学友好、可持续扩展的 Redis-like 系统”，而不是一个完全追求 Redis 协议/格式/集群兼容性的替代品。

## 7. 后续演进

后续要继续演进，增强方向：

- AOF rewrite / compaction
- 更标准化的 RDB 格式或快照恢复路径
- 更完整的 dead node 处理策略
- 多 key API 的跨节点批量优化
- RESP3 或更严格的协议健壮性处理
