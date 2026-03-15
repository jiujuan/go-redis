# ============================================================
# Stage 1: Builder
# 使用官方 Go 镜像编译，利用模块缓存分层加速后续构建
# ============================================================
FROM golang:1.21-alpine AS builder

# 安装基础工具（git 用于 go mod 拉取依赖，ca-certificates 用于 HTTPS）
RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /build

# 先单独复制依赖文件，利用 Docker 层缓存：
# 只要 go.mod/go.sum 不变，依赖层就不会重新下载
COPY go.mod go.sum* ./
RUN go mod download

# 复制全部源码
COPY . .

# 编译：
#   CGO_ENABLED=0  纯静态二进制，不依赖 libc，可在 scratch/alpine 运行
#   -trimpath      去除调试路径，减小体积
#   -ldflags       去除符号表和调试信息，进一步缩小体积
#   注入版本信息（构建时通过 --build-arg 传入，默认 dev）
ARG VERSION=dev
ARG BUILD_TIME
ARG GIT_COMMIT

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -trimpath \
    -ldflags="-s -w \
      -X 'main.Version=${VERSION}' \
      -X 'main.BuildTime=${BUILD_TIME}' \
      -X 'main.GitCommit=${GIT_COMMIT}'" \
    -o /build/go-redis-server \
    ./cmd/server

# ============================================================
# Stage 2: Runtime
# 使用极简 alpine 镜像，只包含运行时必需文件
# 最终镜像体积约 10~15MB
# ============================================================
FROM alpine:3.19

# 安装运行时依赖：
#   ca-certificates  TLS 证书（集群模式 TLS 扩展预留）
#   tzdata           时区数据
#   su-exec          以非 root 用户启动进程（比 gosu 更轻量）
RUN apk add --no-cache ca-certificates tzdata su-exec \
    && update-ca-certificates

# 创建专用非 root 用户，增强安全性
RUN addgroup -S goredis && adduser -S -G goredis goredis

# 数据目录：AOF 和 RDB 持久化文件存放于此，挂载 volume 保证数据持久
RUN mkdir -p /data && chown goredis:goredis /data

# 配置目录：可挂载自定义配置文件
RUN mkdir -p /etc/go-redis && chown goredis:goredis /etc/go-redis

# 从 builder 阶段复制编译产物
COPY --from=builder /build/go-redis-server /usr/local/bin/go-redis-server

# 复制默认配置文件（可被挂载覆盖）
COPY --from=builder /build/go-redis.conf /etc/go-redis/go-redis.conf 2>/dev/null || true

# 复制启动脚本（处理环境变量 → 命令行参数的转换）
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# 持久化数据目录声明为 VOLUME，方便 docker run -v 挂载
VOLUME ["/data"]

# 工作目录设为数据目录，AOF/RDB 文件默认写到这里
WORKDIR /data

# 暴露默认端口
EXPOSE 6379

# 健康检查：每 10 秒用 PING 命令探测服务是否正常
# 连续 3 次失败才标记为 unhealthy，给服务足够的启动时间
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD /usr/local/bin/go-redis-server -ping 2>/dev/null || \
        (echo "PING\r\n" | nc -w1 localhost 6379 | grep -q PONG) || exit 1

# 以非 root 用户运行（通过 entrypoint 内部 su-exec 切换）
ENTRYPOINT ["docker-entrypoint.sh"]

# 默认启动参数（可被 docker run 末尾参数覆盖）
CMD ["--host", "0.0.0.0", "--port", "6379"]
