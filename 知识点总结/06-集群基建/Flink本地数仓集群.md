h# Flink 本地数仓集群

## 目录与 Compose

- 集群目录：`/Users/nanan/flink-cluster`
- Compose 文件：`/Users/nanan/flink-cluster/docker-compose.yml`
- Docker 网络：`flink-cluster_flink-network`（Compose 内服务网络名为 `flink-network`）
- Flink 自定义镜像：`flink-with-connectors:1.18`

## 服务与地址

Q：为啥有些没有http 有些有
A：`http://` 只适用于提供 **HTTP/Web API** 的服务；其他服务可能使用 Kafka、MySQL、RPC 等专用协议，不能直接用浏览器打开

| 服务 | 容器名 / 容器内地址 | 宿主机地址 | 用途 |
| --- | --- | --- | --- |
| Flink JobManager | `jobmanager` | http://localhost:8081 | Flink Web UI |
| Flink TaskManager | `taskmanager1`、`taskmanager2` | 无 | 执行 Flink 作业 |
| Elasticsearch | `elasticsearch:9200` | http://localhost:9200 | ES 样例数据源 |
| Kafka | `kafka:29092` | `localhost:9092` | 消息队列 |
| Zookeeper | `zookeeper:2181` | 无 | Kafka / Fluss 协调服务 |
| MinIO S3 API | `minio:9000` | http://localhost:9000 | Paimon 仓库对象存储 |
| MinIO Console | `minio:9001` | http://localhost:9001 | MinIO 管理界面 |
| Fluss Coordinator | `fluss-coordinator:9123` | `localhost:9123` | Fluss 协调服务 |
| Fluss Tablet | `fluss-tablet` | 无 | Fluss 存储节点 |
| Doris FE Web | `doris:8030` | http://localhost:8030 | Doris 管理页面 |
| Doris BE Web | `doris:8040` | http://localhost:8040 | Doris BE 页面 |
| Doris MySQL | `doris:9030` | `localhost:9030` | SQL 连接入口 |

容器之间使用“容器内地址”；Mac 本地程序使用“宿主机地址”。容器访问宿主机上的服务时使用 `host.docker.internal`。

## 本地账号与示例数据

- MinIO：用户名 `admin`，密码 `omgyskm333
- Doris：用户名 `root`，当前 Compose 未设置密码
- Elasticsearch：未启用认证，仅限本机开发用途
- ES 索引：`flink_source_demo`
- ES 样例字段：`id`、`user_name`、`amount`、`event_time`

ES 初始化容器 `elasticsearch-init` 在 ES 就绪后创建索引并写入 3 条固定样例数据。ES 数据使用 Docker 卷 `elasticsearch-data` 持久化。

## 启动与停止

首次启动或更新了 Flink 连接器依赖时，先下载 JAR 并构建镜像：

```bash
cd /Users/nanan/flink-cluster/docker/flink
./download-jars.sh

cd /Users/nanan/flink-cluster
docker compose up -d --build
```

后续正常启动整个集群：

```bash
cd /Users/nanan/flink-cluster
docker compose up -d
docker compose ps
```

停止但保留容器与数据：

```bash
docker compose stop
```

停止并删除当前 Compose 创建的容器和网络：

```bash
docker compose down
```

只启动 ES 及其样例数据初始化：

```bash
docker compose up -d elasticsearch elasticsearch-init
```

## 常用验证命令

```bash
# 查看所有服务状态
docker compose ps

# ES：查询样例数据
curl 'http://localhost:9200/flink_source_demo/_search?pretty'

# ES：查看索引
curl 'http://localhost:9200/_cat/indices?v'

# Flink：查看运行中的作业
docker exec jobmanager /opt/flink/bin/flink list -r

# Doris：连接并查询
mysql -h 127.0.0.1 -P 9030 -uroot -e 'SHOW DATABASES;'

# 跟踪 JobManager 日志
docker logs -f jobmanager
```

## 当前数据链路

现有演示主链路为：`Flink datagen → Fluss → Paimon（MinIO）→ Doris`。

ES 作为独立的模拟源端，Flink 容器可通过 `http://elasticsearch:9200` 访问。Flink 1.18 官方 Elasticsearch SQL 连接器用于写入 ES，不提供 ES 表源；实现 ES → Flink 时需使用基于 Scroll/PIT API 的自定义 Source，或选择验证过兼容性的第三方连接器。
