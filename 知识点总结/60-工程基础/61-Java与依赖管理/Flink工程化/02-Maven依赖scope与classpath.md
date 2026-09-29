# Maven 依赖 scope 与 classpath

## 1. Maven 解决什么问题

一个 Java 项目通常依赖大量外部库。例如 Flink 项目会依赖：

```text
flink-streaming-java
flink-clients
flink-runtime-web
flink-connector-kafka
flink-connector-jdbc
```

手工下载和管理这些 JAR 很麻烦。Maven 使用 `pom.xml` 描述项目与依赖。

示例：

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-java</artifactId>
    <version>1.18.1</version>
    <scope>compile</scope>
</dependency>
```

坐标由三部分组成：

```text
groupId + artifactId + version
```

它们共同确定一个依赖。

## 2. Maven 生命周期

常用命令：

```bash
mvn clean compile
mvn package
mvn test
```

| 命令 | 含义 |
|---|---|
| `clean` | 删除 `target`，清理旧编译结果 |
| `compile` | 将主源码编译到 `target/classes` |
| `test` | 编译并运行测试 |
| `package` | 生成 JAR 或 WAR |

在多模块项目中：

```bash
mvn -pl Flink -am clean compile -DskipTests
```

| 参数 | 含义 |
|---|---|
| `-pl Flink` | 只选择 `Flink` 模块 |
| `-am` | 同时构建 `Flink` 所需的其他模块 |
| `-DskipTests` | 跳过测试执行 |

## 3. Maven scope

scope 决定依赖在哪些阶段可见。

| scope | 编译主代码 | 运行主代码 | 测试 | 常规发布时是否由项目携带 |
|---|---|---|---|---|
| `compile` | 是 | 是 | 是 | 通常是 |
| `provided` | 是 | 默认否 | 是 | 通常否 |
| `runtime` | 否 | 是 | 是 | 视打包方式而定 |
| `test` | 否 | 否 | 是 | 否 |

### 3.1 `compile`

默认 scope。如果省略 `<scope>`，通常就是 `compile`。

```xml
<scope>compile</scope>
```

适合运行时必须存在、且运行环境未必提供的依赖。

### 3.2 `provided`

```xml
<scope>provided</scope>
```

含义：

```text
编译时需要
运行时由外部环境提供
```

例如：将 Flink 作业提交到已经安装 Flink 的集群时，Flink 核心依赖应由集群提供。

但是 IDEA 直接运行本地 `main()` 时，外部集群并不存在。如果 IDEA 不把 `provided` 依赖加入运行 classpath，就会报类缺失。

### 3.3 `runtime`

常用于编译时不直接引用，但运行时需要的库，例如某些 JDBC Driver。

### 3.4 `test`

只在测试代码中使用，例如 JUnit。

## 4. 为什么本地 IDEA 运行和集群提交有差异

### 4.1 集群提交

Docker Flink 镜像已经在 `/opt/flink/lib` 中包含 Flink 核心 JAR：

```text
flink-dist-1.18.1.jar
flink-table-runtime-1.18.1.jar
flink-json-1.18.1.jar
...
```

因此业务 JAR 不需要重复携带 Flink 核心依赖。

### 4.2 IDEA 本地运行

本地直接运行：

```text
Flink01_Parallelism.main()
```

需要在当前 JVM 中创建 MiniCluster。当前 JVM 必须能加载：

```text
StreamExecutionEnvironment
flink-clients
flink-runtime-web
```

如果这些依赖只是 `provided`，而 IDEA 不将它们放入运行 classpath，就会报错。

## 5. 当前项目遇到的两个典型问题

### 5.1 核心 Flink 类缺失

错误：

```text
NoClassDefFoundError:
org/apache/flink/streaming/api/environment/StreamExecutionEnvironment
```

原因：

```xml
<artifactId>flink-streaming-java</artifactId>
<scope>provided</scope>
```

本地 IDEA 运行时没有带上该 JAR。

解决：本地学习项目将其改成：

```xml
<scope>compile</scope>
```

`flink-clients` 同理。

生产中解决的方法：

> 生产团队通常不会频繁切换 local 和 cluster profile。更常见的是：

1. pom.xml 中始终将 Flink 核心依赖设为 provided。
2. 在 IDEA 中勾选“Include dependencies with Provided scope”。
3. 使用 Maven、IDE 或测试框架提供的 classpath 本地调试。
4. Docker 镜像固定 Flink、插件和 Connector 版本。
5. CI 使用同一套命令生成业务 JAR。

### 5.2 REST 可访问但 Web UI 根页面不存在

现象：访问：

```text
http://localhost:5678
```

得到：

```json
{"errors":["Not found: /"]}
```

但访问：

```text
http://localhost:5678/overview
```

可以得到 JSON。

原因：REST 服务已经启动，但 Web UI 静态资源来自：

```text
flink-runtime-web
```

该依赖被标记为 `provided`，本地 classpath 中不存在。

解决：本地学习项目将它改成：

```xml
<scope>compile</scope>
```

## 6. 如何查看依赖树

查看 Flink 核心依赖：

```bash
mvn -pl Flink dependency:tree \
  -Dincludes=org.apache.flink:flink-streaming-java,org.apache.flink:flink-clients
```

查看 SLF4J 相关依赖：

```bash
mvn -pl Flink dependency:tree \
  -Dincludes=org.slf4j,org.apache.logging.log4j
```

查看某个依赖由谁引入：

```bash
mvn -pl Flink dependency:tree -Dverbose
```

## 7. `SLF4J: multiple bindings` 是什么

示例警告：

```text
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in slf4j-reload4j...
SLF4J: Found binding in log4j-slf4j-impl...
```

含义：classpath 中存在多个日志实现，SLF4J 不确定应该选择哪个。

当前项目中常见来源：

```text
hadoop-client
  → slf4j-reload4j

hive-exec
  → log4j-slf4j-impl
```

这通常是警告，不一定导致作业退出。但生产项目应整理依赖排除规则，避免日志行为不一致。

## 8. 学习项目与生产项目的 scope 策略

### 学习项目

为了 IDEA 直接运行方便，可以使用：

```text
flink-streaming-java → compile
flink-clients → compile
flink-runtime-web → compile
```

### 生产项目

为了避免将集群已有依赖重复打入 fat JAR，常见策略是：

> 生产环境中的做法：在本地运行时：在 IDEA 中勾选“Include dependencies with Provided scope”。

```text
Flink 核心依赖 → provided
业务额外依赖 → compile
```

更进一步，可以通过 Maven Profile 区分：

```text
local profile：适合 IDEA 运行
cluster profile：适合集群打包
```
