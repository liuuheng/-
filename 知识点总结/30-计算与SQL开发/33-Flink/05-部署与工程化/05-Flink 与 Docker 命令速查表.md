# 命令速查表

## 1. Java 与 Maven

查看本机 Java：

```bash
java -version
```

加载 SDKMAN 并查看 Maven 使用的 Java：

```bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
mvn -version
```

查看 Docker Flink Java：

```bash
docker exec jobmanager java -version
```

## 2. Docker 容器与网络

查看容器：

```bash
docker ps --format 'table {{.Names}}\t{{.Image}}\t{{.Ports}}'
```

查看网络：

```bash
docker network inspect flink-cluster_flink-network
```

确认容器可以解析 Mac：

```bash
docker exec taskmanager1 getent hosts host.docker.internal
```

## 3. Socket 输入源

在 Mac 启动：

```bash
nc -lk 8888
```

检查端口：

```bash
lsof -nP -iTCP:8888 -sTCP:LISTEN
```

## 4. 本地 IDEA 运行

入口类：

```text
com.atguigu.flink.architecture.Flink01_Parallelism
```

Web UI：

```text
http://localhost:5678
```

REST 检查：

```bash
curl http://localhost:5678/overview
```

## 5. 构建 thin JAR

```bash
source "$HOME/.sdkman/bin/sdkman-init.sh"

mvn -pl Flink -am clean compile -DskipTests

jar --create \
  --file Flink/target/Flink-thin.jar \
  -C Flink/target/classes .
```

查看内容：

```bash
jar tf Flink/target/Flink-thin.jar
```

检查入口：

```bash
jar tf Flink/target/Flink-thin.jar \
  | grep Flink01_Parallelism
```

## 6. 手动提交 Docker 集群

复制 JAR：

```bash
docker cp \
  Flink/target/Flink-thin.jar \
  jobmanager:/tmp/Flink-thin.jar
```

提交：

```bash
docker exec jobmanager flink run -d \
  -c com.atguigu.flink.architecture.Flink01_Parallelism \
  /tmp/Flink-thin.jar \
  host.docker.internal \
  8888
```

Web UI：

```text
http://localhost:8081
```

## 7. 使用脚本提交

```bash
./Flink/submit-parallelism-to-docker.sh
```

覆盖 Socket 地址：

```bash
./Flink/submit-parallelism-to-docker.sh \
  host.docker.internal \
  8888
```

## 8. 管理 Docker Flink 作业

查看作业：

```bash
docker exec jobmanager flink list
```

查看 REST 状态：

```bash
curl http://localhost:8081/jobs/overview
```

取消作业：

```bash
docker exec jobmanager flink cancel <JobID>
```

查看 TaskManager 输出：

```bash
docker logs -f taskmanager1
docker logs -f taskmanager2
```

## 9. 检查字节码版本

```bash
javap -verbose \
  -classpath Flink/target/Flink-thin.jar \
  com.atguigu.flink.architecture.Flink01_Parallelism \
  | grep 'major version'
```

Java 11 兼容结果：

```text
major version: 55
```

## 10. Maven 依赖检查

```bash
mvn -pl Flink dependency:tree
```

只检查 Flink 核心依赖：

```bash
mvn -pl Flink dependency:tree \
  -Dincludes=org.apache.flink:flink-streaming-java,org.apache.flink:flink-clients
```

检查日志依赖：

```bash
mvn -pl Flink dependency:tree \
  -Dincludes=org.slf4j,org.apache.logging.log4j
```
