# Makefile - go-redis 常用操作快捷命令

IMAGE_NAME  := go-redis
IMAGE_TAG   := 0.4.0
VERSION     := $(IMAGE_TAG)
BUILD_TIME  := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
GIT_COMMIT  := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

.PHONY: help build run stop clean logs test docker-build docker-run docker-cluster

## help: 显示此帮助信息
help:
	@echo "go-redis Makefile"
	@echo ""
	@echo "本地开发："
	@echo "  make run           本地运行服务端（需要 Go 环境）"
	@echo "  make test          运行单元测试"
	@echo "  make bench         运行基准测试"
	@echo ""
	@echo "Docker 单节点："
	@echo "  make docker-build  构建镜像"
	@echo "  make docker-run    运行单节点容器（端口 6379）"
	@echo "  make docker-aof    运行带 AOF 的单节点（端口 6380）"
	@echo "  make docker-stop   停止并删除容器"
	@echo "  make docker-logs   查看容器日志"
	@echo ""
	@echo "Docker 集群："
	@echo "  make cluster-up    启动三节点集群（端口 7001-7003）"
	@echo "  make cluster-down  停止集群"
	@echo ""
	@echo "清理："
	@echo "  make clean         删除镜像和数据卷"

# ---- 本地开发 ----

run:
	go run ./cmd/server --port 6379

test:
	go test ./test/... -v -count=1

bench:
	go test ./test/... -bench=. -benchmem -run='^$$' -count=3

build-local:
	CGO_ENABLED=0 go build -o bin/go-redis-server ./cmd/server

# ---- Docker 单节点 ----

docker-build:
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		--build-arg GIT_COMMIT=$(GIT_COMMIT) \
		-t $(IMAGE_NAME):$(IMAGE_TAG) \
		-t $(IMAGE_NAME):latest \
		.
	@echo "✓ Built $(IMAGE_NAME):$(IMAGE_TAG)"

## docker-run: 启动单节点，无持久化
docker-run: docker-build
	docker run -d \
		--name go-redis \
		-p 6379:6379 \
		-v go-redis-data:/data \
		--restart unless-stopped \
		$(IMAGE_NAME):$(IMAGE_TAG)
	@echo "✓ go-redis started on port 6379"
	@echo "  connect: redis-cli -p 6379"

## docker-aof: 启动单节点，开启 AOF + RDB 持久化
docker-aof: docker-build
	docker run -d \
		--name go-redis-aof \
		-p 6380:6379 \
		-v go-redis-aof-data:/data \
		-e GO_REDIS_AOF=yes \
		-e GO_REDIS_RDB=yes \
		--restart unless-stopped \
		$(IMAGE_NAME):$(IMAGE_TAG)
	@echo "✓ go-redis (with AOF+RDB) started on port 6380"

docker-stop:
	-docker stop go-redis go-redis-aof 2>/dev/null
	-docker rm   go-redis go-redis-aof 2>/dev/null
	@echo "✓ containers stopped and removed"

docker-logs:
	docker logs -f go-redis

docker-shell:
	docker exec -it go-redis sh

# ---- Docker 集群 ----

cluster-up: docker-build
	docker compose --profile cluster up -d
	@echo "✓ 3-node cluster started"
	@echo "  node1: localhost:7001"
	@echo "  node2: localhost:7002"
	@echo "  node3: localhost:7003"

cluster-down:
	docker compose --profile cluster down
	@echo "✓ cluster stopped"

cluster-logs:
	docker compose --profile cluster logs -f

# ---- 清理 ----

clean: docker-stop cluster-down
	-docker rmi $(IMAGE_NAME):$(IMAGE_TAG) $(IMAGE_NAME):latest 2>/dev/null
	-docker volume rm go-redis-data go-redis-aof-data \
		go-redis-node1-data go-redis-node2-data go-redis-node3-data 2>/dev/null
	-rm -rf bin/
	@echo "✓ cleaned up"
