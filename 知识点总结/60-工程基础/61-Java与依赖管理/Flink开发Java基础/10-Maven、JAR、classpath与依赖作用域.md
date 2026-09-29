# 10 Maven、JAR、classpath 与依赖作用域

## 1. 从源码到运行

Java 源文件不能直接由 JVM 执行。典型过程：

```text
.java 源码
   ↓ javac 编译
.class 字节码
   ↓ jar 打包
.jar 文件
   ↓ JVM 加载 classpath 中的类
程序运行
```

Flink 作业提交时，通常提交一个 JAR：

```bash
flink run your-job.jar
```

## 2. Maven 解决什么问题

Maven 主要负责：

1. 管理第三方依赖。
2. 约定项目目录结构。
3. 编译代码。
4. 运行测试。
5. 打包 JAR。

典型结构：

```text
project
├── pom.xml
└── src
    ├── main
    │   ├── java
    │   └── resources
    └── test
        └── java
```

## 3. 坐标

Maven 使用坐标定位依赖：

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-java</artifactId>
    <version>1.17.2</version>
</dependency>
```

三个核心字段：

- `groupId`：组织或项目。
- `artifactId`：模块名称。
- `version`：版本。

## 4. 常见 scope

| scope | 编译可用 | 测试可用 | 运行可用 | 常见用途 |
| --- | --- | --- | --- | --- |
| `compile` | 是 | 是 | 是 | 默认依赖 |
| `provided` | 是 | 是 | 否 | 运行环境已经提供 |
| `runtime` | 否 | 是 | 是 | 仅运行期需要 |
| `test` | 否 | 是 | 否 | JUnit 等测试依赖 |

## 5. 为什么 Flink 依赖常用 provided

Flink 集群本身已经提供 Flink 运行时依赖。用户作业通常不需要把整个 Flink 再打进 JAR。

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-java</artifactId>
    <version>1.17.2</version>
    <scope>provided</scope>
</dependency>
```

注意：在 IDE 本地直接运行时，`provided` 依赖仍需要出现在本地运行 classpath 中。不同 IDE 和插件配置可能影响结果。

## 6. thin JAR 和 fat JAR

### thin JAR

只包含项目自己的代码，不包含依赖。

优点：

- 文件小。
- 构建快。

缺点：

- 运行环境必须准备依赖。

### fat JAR

包含项目代码和需要随作业提交的第三方依赖。

优点：

- 部署方便。

缺点：

- 文件更大。
- 可能出现依赖冲突。

常见原则：

- Flink 集群已有的核心依赖设为 `provided`。
- 业务使用且集群没有提供的依赖，通常需要打入作业 JAR。
- Kafka、JDBC 等 Connector 是否需要随作业打包，要结合部署方式判断。

## 7. classpath

classpath 是 JVM 查找类的位置集合。

常见错误：

```text
ClassNotFoundException
NoClassDefFoundError
```

可能原因：

1. 依赖没有打入 JAR。
2. 集群 `lib` 目录没有对应依赖。
3. scope 配置错误。
4. 编译时和运行时依赖版本不一致。

## 8. 依赖冲突

当不同依赖间接引入同一个库的不同版本时，可能出现：

```text
NoSuchMethodError
ClassCastException
```

查看依赖树：

```bash
mvn dependency:tree
```

这能帮助确认最终使用了哪个版本。

## 9. 常见 Maven 命令

```bash
mvn clean
mvn compile
mvn test
mvn package
mvn clean package
mvn dependency:tree
```

## 10. Flink 作业入口

```java
public class OrderJob {
    public static void main(String[] args)
            throws Exception {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment
                .getExecutionEnvironment();

        DataStream<String> lines =
            env.fromElements("o-1,u-1,20.0");

        lines.print();

        env.execute("order-job");
    }
}
```

客户端运行 `main()` 构建作业图，`env.execute()` 触发提交。

## 11. 自测

1. `.java`、`.class` 和 `.jar` 分别是什么？
2. Maven 的 `provided` scope 表达什么含义？
3. thin JAR 和 fat JAR 有什么区别？
4. `ClassNotFoundException` 常见原因有哪些？
5. 如何查看 Maven 依赖树？

## 12. 阅读时常见问题

### 问题 1：代码在 IDEA 中能运行，为什么提交到 Flink 集群后会报 `ClassNotFoundException`？

IDEA 本地运行时会自动将本地 Maven 依赖加入 classpath。提交到集群后，TaskManager 只能看到：

1. Flink 集群已经提供的依赖。
2. 集群 `lib` 目录中的依赖。
3. 作业 JAR 中打包进去的依赖。

如果业务依赖只存在于开发电脑上，集群就找不到对应类。

### 问题 2：是不是所有依赖都应该打进 fat JAR？

不是。Flink 核心依赖通常由集群提供，重复打包可能增加 JAR 大小并导致类冲突。

常见做法：

- Flink 核心依赖使用 `provided`。
- 业务依赖和集群未提供的第三方依赖打入作业 JAR。
- Connector 是否放入作业 JAR 或集群 `lib`，根据部署方式决定。

### 问题 3：`provided` 是不是表示编译时也不能使用？

不是。`provided` 表示编译和测试时可用，但运行时预期由外部环境提供。

```text
本地编译：可以找到
集群运行：期望 Flink 集群提供
```

### 问题 4：`ClassNotFoundException` 和 `NoSuchMethodError` 的排查方向一样吗？

不完全一样。

- `ClassNotFoundException`：通常是某个类完全不在运行 classpath 中。
- `NoSuchMethodError`：通常是类存在，但运行时加载了不兼容版本，该版本没有代码期望的方法。

遇到后者，要重点检查依赖冲突：

```bash
mvn dependency:tree
```

### 问题 5：为什么提交作业时只提交 JAR，不直接提交 `.java` 文件？

JVM 执行的是编译后的字节码，不是 Java 源码。JAR 是便于分发的一组 `.class` 文件和相关资源。集群节点只需要合适的 JVM 和依赖，就可以加载执行。

### 问题 6：`env.execute()` 为什么必须写？

前面的 DataStream API 调用主要是在构建作业拓扑。`env.execute()` 负责触发执行或提交：

```java
source
    .map(...)
    .filter(...);

env.execute("job-name");
```

没有它，程序通常只是描述了计算流程，并没有真正启动作业。
