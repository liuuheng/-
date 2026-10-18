# Docker 网络与地址选择

## 1. 最容易误解的地址：`localhost`

`localhost` 不是固定表示 Mac。它表示：

```text
当前进程所在环境自身
```

| 代码运行位置 | `localhost` 表示什么 |
|---|---|
| Mac 上的 IDEA | Mac |
| JobManager 容器 | JobManager 容器 |
| TaskManager 容器 | 对应 TaskManager 容器 |
| MinIO 容器 | MinIO 容器 |

因此，TaskManager 容器中访问：

```text
localhost:8888
```

不会访问 Mac 上的 `nc -lk 8888`，而是尝试访问 TaskManager 容器自己的 8888 端口。

## 2. Docker 容器如何访问 Mac

在 Docker Desktop 中，容器访问宿主机通常使用：

```text
host.docker.internal
```

当前 TaskManager 中可解析为：

```text
192.168.65.254 host.docker.internal
```

因此 Mac 上运行：

```bash
nc -lk 8888
```

Docker TaskManager 应连接：

```text
host.docker.internal:8888
```

## 3. Docker 容器之间如何访问

在同一个 Docker 网络中，通常可以使用容器名或 Compose Service Name 访问其他容器。

例如当前 MinIO 容器名：

```text
minio
```

其他容器访问 MinIO：

```text
http://minio:9000
```

Mac 宿主机访问 MinIO：

```text
http://localhost:9000
```

因为 Docker 将宿主机端口映射到容器端口：

```text
9000:9000
```

## 4. `hadoop102` 是什么

原课程代码大量使用：

```java
env.socketTextStream("hadoop102", 8888);
```

以及：

```text
jdbc:mysql://hadoop102:3306/test
hadoop102:9092,hadoop103:9092,hadoop104:9092
hdfs://hadoop102:8020
thrift://hadoop102:9083
```

`hadoop102` 不是 Java 变量，也不是 Flink 自动识别的地址。它是原课程虚拟机环境中的主机名。

通常通过每台机器的 `/etc/hosts` 映射：

```text
192.168.10.102 hadoop102
192.168.10.103 hadoop103
192.168.10.104 hadoop104
```

当前 Mac 与 Docker 网络中没有该映射，因此需要根据实际运行位置替换。

## 5. 常见服务地址对照表

| 服务 | Mac 本地程序访问 | Docker 容器访问 |
|---|---|---|
| Mac 上的 `nc -lk 8888` | `localhost:8888` | `host.docker.internal:8888` |
| MinIO | `http://localhost:9000` | `http://minio:9000` |
| Docker Kafka | 取决于 advertised listeners 配置 | 通常可使用 `kafka:9092` |
| Docker Flink JobManager Web UI | `http://localhost:8081` | 通常 `http://jobmanager:8081` |
| 本地 MiniCluster Web UI | `http://localhost:5678` | 不适用于 Docker 集群 |

## 6. 端口映射是什么意思

Compose 中常见：

```yaml
ports:
  - "9000:9000"
```

格式：

```text
宿主机端口:容器端口
```

例如：

```text
Mac localhost:9000
        ↓
MinIO 容器 9000
```

没有映射时，容器之间仍可能通过 Docker 网络互相访问，但 Mac 不一定可以直接访问。

## 7. 检查网络的常用命令

查看容器与端口：

```bash
docker ps --format 'table {{.Names}}\t{{.Ports}}'
```

查看 Docker 网络：

```bash
docker network inspect flink-cluster_flink-network
```

检查容器能否解析宿主机：

```bash
docker exec taskmanager1 getent hosts host.docker.internal
```

查看本机端口是否监听：

```bash
lsof -nP -iTCP:8888 -sTCP:LISTEN
```

检查 Web UI REST API：

```bash
curl http://localhost:8081/overview
```
