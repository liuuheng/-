# 常见错误排查手册

## 1. IDEA 编译报 `TypeTag :: UNKNOWN`

错误：

```text
java.lang.ExceptionInInitializerError
com.sun.tools.javac.code.TypeTag :: UNKNOWN
```

### 原因

IDEA 使用了较新的 JDK 25，而项目使用旧版 Lombok：

```text
lombok 1.18.26
```

旧 Lombok 与新版 `javac` 内部结构不兼容。

### 当前推荐解决方案

IDEA 项目 SDK 使用 Java 17：

```text
/Users/nanan/.sdkman/candidates/java/17.0.19-zulu
```

设置路径：

```text
File
→ Project Structure
→ SDKs
→ Add JDK
```

然后：

```text
Project SDK → Java 17
Module SDK → Project SDK
Run Configuration JRE → Project SDK
```

## 2. IDEA 运行报 `NoClassDefFoundError: StreamExecutionEnvironment`

错误：

```text
NoClassDefFoundError:
org/apache/flink/streaming/api/environment/StreamExecutionEnvironment
```

### 原因

编译时依赖存在，但运行时 classpath 缺少：

```text
flink-streaming-java
```

原 POM 中 scope 为：

```xml
<scope>provided</scope>
```

IDEA 本地运行时没有带上该 JAR。

### 解决

本地学习项目改为：

```xml
<scope>compile</scope>
```

`flink-clients` 也需要处理。

## 3. Web UI 根页面显示 `Not found: /`

现象：

```json
{
  "errors": [
    "Not found: /"
  ]
}
```

但是：

```text
http://localhost:5678/overview
```

可以返回 JSON。

### 原因

REST API 存在，但 Web UI 静态资源没有加载。

静态资源位于：

```text
flink-runtime-web
```

### 解决

本地学习项目中将它改为：

```xml
<scope>compile</scope>
```

然后在 IDEA 中 Reload Maven，并重启 Java 进程。

## 4. 集群提交报 `UnsupportedClassVersionError`

错误：

```text
class file version 61.0,
this Java Runtime only recognizes class file versions up to 55.0
```

### 原因

```text
宿主机编译输出：Java 17 字节码，major 61
Docker JVM：Java 11，只支持到 major 55
```

### 解决

POM 设置：

```xml
<maven.compiler.source>11</maven.compiler.source>
<maven.compiler.target>11</maven.compiler.target>
<maven.compiler.release>11</maven.compiler.release>
```

打包前执行干净编译：

```bash
mvn -pl Flink -am clean compile -DskipTests
```

确认结果：

```bash
javap -verbose \
  -classpath Flink/target/Flink-thin.jar \
  com.atguigu.flink.architecture.Flink01_Parallelism \
  | grep 'major version'
```

应为：

```text
major version: 55
```

## 5. Docker 提交时连接 `0.0.0.0:5678` 失败

错误：

```text
Connection refused: /0.0.0.0:5678
```

### 原因

代码无条件设置：

```java
conf.setInteger("rest.port", 5678);
```

本地运行时，它用于 MiniCluster Web UI。

集群提交时，它可能覆盖 Client 连接配置，导致 Client 错误地尝试向 `5678` 提交，而不是 Docker JobManager 的 `8081`。

### 解决

只在本地无参数运行时设置：

```java
if (args.length == 0) {
    conf.setInteger("rest.port", 5678);
}
```

## 6. Docker TaskManager 无法连接 `localhost:8888`

### 原因

容器中的：

```text
localhost
```

表示容器自身，不是 Mac。

### 解决

Mac 启动：

```bash
nc -lk 8888
```

Docker 中使用：

```text
host.docker.internal:8888
```

检查解析：

```bash
docker exec taskmanager1 \
  getent hosts host.docker.internal
```

## 7. `SLF4J: Class path contains multiple SLF4J bindings`

警告：

```text
SLF4J: Class path contains multiple SLF4J bindings.
```

### 原因

多个依赖引入了不同日志实现，例如：

```text
hadoop-client → slf4j-reload4j
hive-exec → log4j-slf4j-impl
```

### 影响

通常只是警告，不一定导致程序失败。但日志实现选择可能不稳定。

### 后续优化

使用：

```bash
mvn -pl Flink dependency:tree \
  -Dincludes=org.slf4j,org.apache.logging.log4j
```

找到来源后，通过 `<exclusions>` 排除多余实现。学习阶段可暂时保留。

## 8. IDEA 中看不到 Docker 集群作业

### 现象

访问：

```text
http://localhost:5678
```

看不到 Docker 作业。

### 原因

这是本地 MiniCluster Web UI，不是 Docker Session Cluster。

### 正确地址

| 场景 | Web UI |
|---|---|
| IDEA 本地运行 | `http://localhost:5678` |
| Docker 集群 | `http://localhost:8081` |

## 9. Web UI 中看不到 `print()` 输出

### 原因

Web UI 展示作业状态、拓扑、指标和日志入口，不会将 `print()` 自动聚合为页面输出。

`print()` 在 TaskManager 中执行。

### 查看方式

```bash
docker logs -f taskmanager1
docker logs -f taskmanager2
```

## 10. 作业提交后立即失败，如何排查

建议顺序：

1. 查看 Web UI：
   ```text
   http://localhost:8081
   ```
2. 查看作业列表：
   ```bash
   docker exec jobmanager flink list
   ```
3. 查看 JobManager 日志：
   ```bash
   docker logs --tail 200 jobmanager
   ```
4. 查看 TaskManager 日志：
   ```bash
   docker logs --tail 200 taskmanager1
   docker logs --tail 200 taskmanager2
   ```
5. 检查 socket 是否监听：
   ```bash
   lsof -nP -iTCP:8888 -sTCP:LISTEN
   ```
6. 检查容器能否解析宿主机：
   ```bash
   docker exec taskmanager1 getent hosts host.docker.internal
   ```
7. 检查 Java 字节码版本：
   ```bash
   javap -verbose -classpath Flink/target/Flink-thin.jar \
     com.atguigu.flink.architecture.Flink01_Parallelism \
     | grep 'major version'
   ```
