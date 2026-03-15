package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config 全局配置
type Config struct {
	// 服务端配置
	Host     string
	Port     int
	Password string

	// 存储引擎配置
	ShardCount int // 分片数量，必须是 2 的幂次

	// 持久化配置
	AOFEnabled  bool
	AOFFilename string
	AOFSync     string // always | everysec | no

	RDBEnabled  bool
	RDBFilename string

	// 网络配置
	MaxConnections int
	ReadTimeout    int // 秒
	WriteTimeout   int // 秒

	// 集群配置（v0.3）
	ClusterNodes    []string
	VirtualReplicas int // 虚拟节点数
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		Host:            "0.0.0.0",
		Port:            6379,
		ShardCount:      256,
		AOFEnabled:      false,
		AOFFilename:     "appendonly.aof",
		AOFSync:         "everysec",
		RDBEnabled:      false,
		RDBFilename:     "dump.rdb",
		MaxConnections:  10000,
		ReadTimeout:     0,
		WriteTimeout:    0,
		VirtualReplicas: 150,
	}
}

// ParseFlags 解析命令行参数
func ParseFlags() *Config {
	cfg := DefaultConfig()

	flag.StringVar(&cfg.Host, "host", cfg.Host, "server host")
	flag.IntVar(&cfg.Port, "port", cfg.Port, "server port")
	flag.StringVar(&cfg.Password, "password", cfg.Password, "server password")
	flag.IntVar(&cfg.ShardCount, "shards", cfg.ShardCount, "shard count (power of 2)")
	flag.BoolVar(&cfg.AOFEnabled, "aof", cfg.AOFEnabled, "enable AOF persistence")
	flag.StringVar(&cfg.AOFFilename, "aof-file", cfg.AOFFilename, "AOF filename")
	flag.StringVar(&cfg.AOFSync, "aof-sync", cfg.AOFSync, "AOF sync policy: always|everysec|no")
	flag.BoolVar(&cfg.RDBEnabled, "rdb", cfg.RDBEnabled, "enable RDB persistence")
	flag.StringVar(&cfg.RDBFilename, "rdb-file", cfg.RDBFilename, "RDB filename")
	flag.IntVar(&cfg.MaxConnections, "max-conn", cfg.MaxConnections, "max connections")

	var nodes string
	flag.StringVar(&nodes, "cluster", "", "cluster nodes, comma separated, e.g. 127.0.0.1:6379,127.0.0.1:6380")

	flag.Parse()

	if nodes != "" {
		cfg.ClusterNodes = strings.Split(nodes, ",")
	}

	return cfg
}

// Addr 返回服务监听地址
func (c *Config) Addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// ParseConfigFile 解析简单的配置文件（key value 格式）
func ParseConfigFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	cfg := DefaultConfig()
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		key := strings.ToLower(parts[0])
		val := parts[1]

		switch key {
		case "host":
			cfg.Host = val
		case "port":
			if p, err := strconv.Atoi(val); err == nil {
				cfg.Port = p
			}
		case "password":
			cfg.Password = val
		case "aof":
			cfg.AOFEnabled = val == "yes"
		case "aof-file":
			cfg.AOFFilename = val
		case "rdb":
			cfg.RDBEnabled = val == "yes"
		case "rdb-file":
			cfg.RDBFilename = val
		case "max-connections":
			if n, err := strconv.Atoi(val); err == nil {
				cfg.MaxConnections = n
			}
		}
	}

	return cfg, nil
}
