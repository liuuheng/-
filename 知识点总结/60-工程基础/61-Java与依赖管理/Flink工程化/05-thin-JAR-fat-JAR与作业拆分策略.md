# thin JAR、fat JAR 与作业拆分策略

## 1. 为什么需要打包

Flink 集群不能直接运行项目中的 `.java` 源码。需要先：

```text
源码
→ 编译为 .class
→ 打包为 .jar
→ 提交到 Flink 集群
```

## 2. thin JAR 是什么

thin JAR 是轻量 JAR，通常只包含：

```text
业务代码
自定义 POJO
自定义函数
配置文件
少量必须随作业发布的代码
```

当前构建方式：

```bash
jar --create \
  --file Flink/target/Flink-thin.jar \
  -C Flink/target/classes .
```

拆解：

| 部分 | 含义 |
|---|---|
| `jar` | JDK 自带 JAR 工具 |
| `--create` | 创建新 JAR |
| `--file Flink/target/Flink-thin.jar` | 输出文件路径 |
| `-C Flink/target/classes` | 将工作目录切换到编译结果目录 |
| `.` | 将该目录下全部内容加入 JAR |

当前 thin JAR 大约：

```text
311 KB
```

## 3. fat JAR 是什么

fat JAR 也称 uber JAR。它不仅包含业务代码，还将依赖一起放入归档。

例如：

```text
业务 class
Flink 相关依赖
Hadoop 依赖
Hive 依赖
Paimon 依赖
第三方工具库
```

当前项目执行 Maven Shade 后，曾生成约：

```text
345 MB
```

的大 JAR。

## 4. 为什么集群提交通常不打包 Flink 自身依赖

Docker Flink 集群已经包含：

```text
flink-dist-1.18.1.jar
flink-table-runtime-1.18.1.jar
flink-json-1.18.1.jar
...
```

如果业务 JAR 再携带另一份 Flink 依赖，可能出现：

- 体积过大。
- 上传速度慢。
- 类加载冲突。
- 本地依赖版本与集群版本不一致。
- 难以判断实际生效的是哪一份类。

因此基本原则：

```text
集群已有的基础依赖，不重复打包
集群没有但业务需要的依赖，随作业打包或部署到集群 lib
```

## 5. 当前 thin JAR 包含什么

命令：

```bash
jar --create \
  --file Flink/target/Flink-thin.jar \
  -C Flink/target/classes .
```

会把 `Flink/target/classes` 下所有编译结果打包进去。

因此不仅包含：

```text
Flink01_Parallelism.class
Flink01_Parallelism$1.class
WordCount.class
```

也包含：

```text
Flink02_OperatorChain.class
Flink03_TaskSlots.class
sql 目录中的示例
state 目录中的示例
配置资源文件
```

## 6. 多个 `main()` 是否会同时运行

不会。

JAR 只是归档文件。提交时通过 `-c` 选择入口：

```bash
flink run -d \
  -c com.atguigu.flink.architecture.Flink01_Parallelism \
  /tmp/Flink-thin.jar
```

本次只调用：

```java
Flink01_Parallelism.main(args)
```

其他入口类不会自动执行。

## 7. 何时使用统一 JAR

学习项目适合统一 JAR：

```text
Flink-thin.jar
├── Flink01_Parallelism.main()
├── Flink02_OperatorChain.main()
├── Flink03_TaskSlots.main()
└── ...
```

优点：

- 简单。
- 修改后只需重新打一个包。
- 可以通过 `-c` 灵活切换示例。
- 便于快速实验。

## 8. 何时拆分独立 JAR

生产项目通常按业务作业拆分：

```text
order-statistics-job.jar
user-behavior-job.jar
realtime-risk-control-job.jar
```

优点：

- 独立发布。
- 独立回滚。
- 独立扩缩容。
- 独立配置 checkpoint 与资源。
- 减少依赖冲突。
- 一个作业升级不会迫使其他作业一起发布。

## 9. 不要按单个 Java 类机械拆包

一个作业往往包含多个支持类：

```text
order-statistics-job.jar
├── OrderStatisticsJob.main()
├── OrderEvent
├── OrderSource
├── OrderAggregateFunction
├── OrderSink
└── JsonUtil
```

正确拆分单位通常是：

```text
可以独立部署、独立运行、独立扩缩容的业务作业
```

而不是：

```text
每一个 Java 文件
```

## 10. 多模块生产结构示例

```text
flink-jobs/
├── pom.xml
├── flink-common/
│   ├── pom.xml
│   └── 公共 POJO、工具类
├── order-statistics-job/
│   ├── pom.xml
│   └── OrderStatisticsJob.java
└── user-behavior-job/
    ├── pom.xml
    └── UserBehaviorJob.java
```

打包某个作业：

```bash
mvn -pl order-statistics-job -am package -DskipTests
```

## 11. 集群缺少第三方依赖时怎么办

假设业务代码使用：

```java
import com.alibaba.fastjson.JSON;
```

而集群 `/opt/flink/lib` 中没有 Fastjson。仅提交 thin JAR 会报：

```text
ClassNotFoundException: com.alibaba.fastjson.JSON
```

常见解决方案：

1. 使用 Maven Shade 将业务额外依赖打进作业 JAR。
2. 将共享依赖放入每个 Flink 节点的 `/opt/flink/lib`。
3. 对 connector 使用 Flink 推荐的插件或 lib 部署方式。

原则：

```text
不要盲目打包全部依赖
也不要假设集群一定已经拥有所有依赖
```
