#!/bin/sh
# docker-entrypoint.sh
#
# 职责：
#   1. 将环境变量转换为命令行参数（环境变量优先级高于默认值，低于显式命令行参数）
#   2. 修正 /data 目录权限
#   3. 以 goredis 用户身份启动服务（su-exec）
#
# 支持的环境变量：
#   GO_REDIS_HOST         监听地址，默认 0.0.0.0
#   GO_REDIS_PORT         监听端口，默认 6379
#   GO_REDIS_PASSWORD     访问密码，默认空
#   GO_REDIS_AOF          是否开启 AOF，yes/no，默认 no
#   GO_REDIS_AOF_FILE     AOF 文件路径，默认 /data/appendonly.aof
#   GO_REDIS_AOF_SYNC     AOF sync 策略 always|everysec|no，默认 everysec
#   GO_REDIS_RDB          是否开启 RDB，yes/no，默认 no
#   GO_REDIS_RDB_FILE     RDB 文件路径，默认 /data/dump.rdb
#   GO_REDIS_MAX_CONN     最大连接数，默认 10000
#   GO_REDIS_SHARDS       分片数（2的幂次），默认 256
#   GO_REDIS_CLUSTER      集群节点列表（逗号分隔），默认空

set -e

# ---------- 修正数据目录权限 ----------
# 在 host 挂载 volume 时，目录可能属于 root，需修正为 goredis 用户
if [ "$(stat -c %U /data 2>/dev/null)" != "goredis" ]; then
    chown -R goredis:goredis /data 2>/dev/null || true
fi

# ---------- 构建参数列表 ----------
# 若用户直接传入了命令行参数（如 docker run ... --port 6380），则跳过环境变量解析，直接使用
# 判断：$1 以 "--" 开头则认为是完整命令行参数
if [ "${1#--}" != "$1" ] || [ "$#" -eq 0 ]; then
    # 用户传入了 --xxx 参数 或 无参数 → 从环境变量构建参数
    ARGS=""

    # host
    HOST="${GO_REDIS_HOST:-0.0.0.0}"
    ARGS="$ARGS --host $HOST"

    # port
    PORT="${GO_REDIS_PORT:-6379}"
    ARGS="$ARGS --port $PORT"

    # password
    if [ -n "$GO_REDIS_PASSWORD" ]; then
        ARGS="$ARGS --password $GO_REDIS_PASSWORD"
    fi

    # shards
    SHARDS="${GO_REDIS_SHARDS:-256}"
    ARGS="$ARGS --shards $SHARDS"

    # max connections
    MAX_CONN="${GO_REDIS_MAX_CONN:-10000}"
    ARGS="$ARGS --max-conn $MAX_CONN"

    # AOF
    AOF="${GO_REDIS_AOF:-no}"
    if [ "$AOF" = "yes" ]; then
        ARGS="$ARGS --aof"
        AOF_FILE="${GO_REDIS_AOF_FILE:-/data/appendonly.aof}"
        ARGS="$ARGS --aof-file $AOF_FILE"
        AOF_SYNC="${GO_REDIS_AOF_SYNC:-everysec}"
        ARGS="$ARGS --aof-sync $AOF_SYNC"
    fi

    # RDB
    RDB="${GO_REDIS_RDB:-no}"
    if [ "$RDB" = "yes" ]; then
        ARGS="$ARGS --rdb"
        RDB_FILE="${GO_REDIS_RDB_FILE:-/data/dump.rdb}"
        ARGS="$ARGS --rdb-file $RDB_FILE"
    fi

    # cluster nodes
    if [ -n "$GO_REDIS_CLUSTER" ]; then
        ARGS="$ARGS --cluster $GO_REDIS_CLUSTER"
    fi

    # 将用户传入的额外 --xxx 参数追加到末尾
    ARGS="$ARGS $@"

    echo "[entrypoint] starting go-redis-server with args: $ARGS"
    exec su-exec goredis /usr/local/bin/go-redis-server $ARGS
else
    # 用户传入了非 --xxx 参数（如直接传 sh），直接执行
    exec "$@"
fi
