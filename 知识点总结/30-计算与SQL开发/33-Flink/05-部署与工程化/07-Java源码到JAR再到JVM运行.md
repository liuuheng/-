# Java 源码到 JAR 再到 JVM 运行

## 1. 整体过程

一个 Java 程序不是直接运行 `.java` 文件。常见过程是：

```text
.java 源码
   ↓ javac 编译
.class 字节码
   ↓ jar 打包
.jar 文件
   ↓ JVM 根据 classpath 加载类
调用入口类的 main() 方法
```

以当前 Flink 示例为例：

```text
Flink/src/main/java/com/atguigu/flink/architecture/Flink01_Parallelism.java
   ↓
Flink/target/classes/com/atguigu/flink/architecture/Flink01_Parallelism.class
   ↓
Flink/target/Flink-thin.jar
   ↓
Flink 集群加载入口类
com.atguigu.flink.architecture.Flink01_Parallelism
```

## 2. `.java` 和 `.class` 的区别

`.java` 是源码，适合人阅读：

```java
public class Hello {
    public static void main(String[] args) {
        System.out.println("hello");
    }
}
```

`.class` 是 JVM 可以读取的字节码。JVM 通常不直接读取源码。

执行：

```bash
javac Hello.java
```

会得到：

```text
Hello.class
```

## 3. JAR 是什么

JAR 可以理解为 Java 世界中的压缩归档文件。它通常包含：

```text
META-INF/
com/example/Hello.class
com/example/model/User.class
application.properties
log4j.properties
```

查看 JAR 内容：

```bash
jar tf Flink/target/Flink-thin.jar
```

其中：

| 参数 | 含义 |
|---|---|
| `t` | list，列出内容 |
| `f` | file，后面跟 JAR 文件路径 |

## 4. `main()` 方法是什么

Java 程序常见入口：

```java
public static void main(String[] args) {
    // 程序入口
}
```

一个 JAR 中可以存在多个带 `main()` 的类，例如：

```text
Flink01_Parallelism.main()
Flink02_OperatorChain.main()
Flink03_TaskSlots.main()
```

它们不会同时运行。启动程序时，必须选择一个入口。

Flink 命令中使用 `-c` 指定入口：

```bash
flink run -c com.atguigu.flink.architecture.Flink01_Parallelism job.jar
```

Web UI 提交时，在 `Entry Class` 中填写完整类名。

## 5. 内部类为什么也需要打包

示例中包含匿名内部类：

```java
new FlatMapFunction<String, WordCount>() {
    @Override
    public void flatMap(String value, Collector<WordCount> out) {
        // ...
    }
}
```

编译后除了主类：

```text
Flink01_Parallelism.class
```

还会生成：

```text
Flink01_Parallelism$1.class
```

如果手工只打包主类，遗漏 `$1.class`，运行时会报类缺失错误。因此长期维护时不建议逐个手写 class 文件清单。

## 6. classpath 是什么

classpath 是 JVM 查找类的位置集合。

运行：

```bash
java -cp app.jar:lib/example.jar com.example.Main
```

表示 JVM 可以到以下位置找类：

```text
app.jar
lib/example.jar
```

如果程序引用了某个类，但 classpath 中没有对应 JAR，会出现：

```text
ClassNotFoundException
```

或：

```text
NoClassDefFoundError
```

## 7. `ClassNotFoundException` 与 `NoClassDefFoundError`

两者都与类缺失有关，但语义略有不同。

### 7.1 `ClassNotFoundException`

通常表示程序主动通过类加载器加载某个类，但没有找到：

```java
Class.forName("com.example.Driver");
```

### 7.2 `NoClassDefFoundError`

通常表示编译时类存在，但运行时 JVM 加载不到：

```text
java.lang.NoClassDefFoundError:
org/apache/flink/streaming/api/environment/StreamExecutionEnvironment
```

在当前项目中，原因是 Flink 核心依赖被标成 `provided`，IDEA 直接运行时没有将其加入运行 classpath。

## 8. Java 版本与 class file version

Java 字节码具有版本号。常见对应关系：

| Java 版本 | class file major version |
|---|---:|
| Java 8 | 52 |
| Java 11 | 55 |
| Java 17 | 61 |
| Java 21 | 65 |
| Java 25 | 69 |

关键规则：

```text
高版本 JVM 通常可以运行低版本 class
低版本 JVM 无法运行高版本 class
```

例如：

```text
Mac Java 17 编译出 major version 61
Docker Java 11 只能识别到 major version 55
```

会报错：

```text
UnsupportedClassVersionError:
class file version 61.0,
this Java Runtime only recognizes class file versions up to 55.0
```

## 9. 使用 Java 17 编译器输出 Java 11 字节码

当前项目在 `Flink/pom.xml` 中设置：

```xml
<maven.compiler.source>11</maven.compiler.source>
<maven.compiler.target>11</maven.compiler.target>
<maven.compiler.release>11</maven.compiler.release>
```

含义：

- 本机仍可使用 Java 17 编译器。
- 编译结果兼容 Java 11。
- 编译时只允许使用 Java 11 标准库 API。

推荐优先关注：

```xml
<maven.compiler.release>11</maven.compiler.release>
```

它比只写 `source` 和 `target` 更完整。

## 10. 常用检查命令

查看当前 Java：

```bash
java -version
```

查看 Maven 使用的 Java：

```bash
mvn -version
```

查看 Docker JobManager 中的 Java：

```bash
docker exec jobmanager java -version
```

查看 class major version：

```bash
javap -verbose \
  -classpath Flink/target/Flink-thin.jar \
  com.atguigu.flink.architecture.Flink01_Parallelism \
  | grep 'major version'
```

当前 Docker Java 11 兼容结果应该是：

```text
major version: 55
```
