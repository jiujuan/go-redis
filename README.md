# go-redis

一个用 Go 语言实现的简化版 Redis，支持五种核心数据结构，分三个版本迭代。

## 项目结构

```
go-redis/
├── cmd/
│   └── server/             # v0.2+ 服务端入口
│       └── main.go
├── config/                 # 配置定义
│   └── config.go
├── internal/
│   ├── engine/             # v0.1 核心存储引擎（分片锁 + 数据结构）
│   │   ├── db.go           # GoRedis 主结构，对外 API
│   │   ├── shard.go        # 分片锁管理
│   │   ├── string.go       # String 数据结构
│   │   ├── hash.go         # Hash 数据结构
│   │   ├── list.go         # List 数据结构
│   │   ├── set.go          # Set 数据结构
│   │   ├── zset.go         # ZSet 数据结构（跳表实现）
│   │   └── skiplist.go     # 跳表实现
│   ├── resp/               # v0.2 RESP 协议编解码
│   │   ├── reader.go
│   │   └── writer.go
│   ├── server/             # v0.2 TCP 服务端
│   │   ├── server.go
│   │   ├── handler.go
│   │   └── command.go
│   └── persistence/        # v0.2 持久化（AOF / RDB）
│       ├── aof.go
│       └── rdb.go
├── pkg/
│   ├── pool/               # v0.3 连接池
│   │   └── pool.go
│   └── client/             # v0.3 分布式客户端 SDK
│       └── cluster.go
├── test/
│   ├── benchmark/          # 基准测试
│   └── integration/        # 集成测试
└── go.mod
```

## 版本说明

| 版本 | 核心功能 | 关键技术 |
|------|----------|----------|
| v0.1 | 单机内存数据结构 | 分片锁、跳表 |
| v0.2 | 网络服务、持久化 | RESP协议、TCP并发、AOF/RDB |
| v0.3 | 分布式分片 | 一致性哈希、连接池、健康检查 |

## 快速开始

### v0.1 嵌入式使用

```go
db := engine.NewGoRedis()
db.Set("name", "tom")
val, _ := db.Get("name")

db.HSet("user:1", "age", "20")
db.LPush("queue", "a", "b", "c")
db.SAdd("tags", "go", "redis")
db.ZAdd("ranking", 100, "alice")
```

### v0.2 启动服务端

```bash
go run cmd/server/main.go --port 6379 --aof yes
redis-cli -p 6379 SET name tom
```

### v0.3 集群客户端

```go
nodes := []string{"127.0.0.1:6379", "127.0.0.1:6380", "127.0.0.1:6381"}
c := client.NewClusterClient(nodes)
c.Set("name", "tom")
```
