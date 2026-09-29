## docker集群启动
1、集群的停止
```bash
cd ~/flink-cluster
docker compose stop
```
2、集群的启动
```bash
cd ~/flink-cluster
docker compose up -d
```
3、删除孤儿容器
```shell
docker compose up -d --remove-orphans
```

## docker-flink常用命令

查看作业：

```bash
docker exec jobmanager flink list
```

取消作业：

```bash
docker exec jobmanager flink cancel <JobID>
```

查看 JobManager 日志：

```bash
docker logs -f jobmanager
```


命令讲解：
logs 是 docker 命令的一个子命令，意思是查看容器的日志输出。

`docker logs -f jobmanager`

可以拆成：

|部分|含义|
|---|---|
|docker|调用 Docker 命令行工具|
|logs|查看容器日志|
|-f|持续跟踪新日志，类似实时刷新|
|jobmanager|容器名称|

查看 TaskManager 日志：

```bash
docker logs -f taskmanager1
docker logs -f taskmanager2
```


## docker-访问

前提需要理解：`localhost` 不是固定表示 Mac。它表示：当前进程所在环境自身

Docker 容器如何访问 Mac

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

![[99-images/Pasted image 20260531165541.png]]