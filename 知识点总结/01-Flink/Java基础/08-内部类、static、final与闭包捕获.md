# 08 内部类、static、final 与闭包捕获

## 1. 为什么这一章与 Flink 关系很大

Flink 会将用户函数发送到集群节点执行。如果函数对象不小心持有外部对象引用，序列化时就可能把无关对象一起带上，甚至直接失败。

## 2. 成员变量与局部变量

成员变量属于对象：

```java
public class Parser {
    private String delimiter = ",";
}
```

局部变量只存在于方法范围内：

```java
public void run() {
    String delimiter = ",";
}
```

## 3. `static`

`static` 表示成员属于类，而不是某个具体对象。

```java
public class Constants {
    public static final String DEFAULT_DELIMITER = ",";
}
```

访问：

```java
Constants.DEFAULT_DELIMITER
```

静态方法中不能直接访问实例字段，因为静态方法不依赖某个具体对象。

## 4. `final`

`final` 常见用法：

### final 变量

只能赋值一次：

```java
final int parallelism = 4;
```

### final 引用

引用不能指向新对象，但对象内部仍可能改变：

```java
final List<String> users = new ArrayList<>();
users.add("u-1"); // 合法
// users = new ArrayList<>(); // 不合法
```

### final 方法

子类不能重写。

### final 类

不能被继承，例如 `String`。

## 5. 非静态内部类

```java
public class Job {
    private String prefix = "order:";

    public class AddPrefix
            implements MapFunction<String, String> {
        @Override
        public String map(String value) {
            return prefix + value;
        }
    }
}
```

`AddPrefix` 是非静态内部类，它会隐式持有外部 `Job` 对象的引用。

在 Flink 中，这可能导致：

1. 外部对象也被尝试序列化。
2. 外部对象包含不可序列化字段时提交失败。
3. 序列化对象体积变大。

## 6. 静态内部类

```java
public class Job {
    public static class AddPrefix
            implements MapFunction<String, String> {

        private final String prefix;

        public AddPrefix(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public String map(String value) {
            return prefix + value;
        }
    }
}
```

静态内部类不隐式持有外部实例，适合定义 Flink 函数。

## 7. Lambda 的闭包捕获

```java
String prefix = "order:";
DataStream<String> result =
    lines.map(line -> prefix + line);
```

Lambda 捕获了局部变量 `prefix`。该变量必须是 final 或 effectively final。

捕获简单字符串通常没问题。但捕获连接对象风险很大：

```java
SomeClient client = new SomeClient();
orders.map(order -> client.query(order.getUserId()));
```

这会让 Lambda 持有 `client`。Flink 尝试分发函数时，也需要处理它。

## 8. 配置对象与运行时资源

可以序列化的小型配置通常可作为函数构造参数：

```java
public static class AmountFilter
        implements FilterFunction<Order> {

    private final double threshold;

    public AmountFilter(double threshold) {
        this.threshold = threshold;
    }

    @Override
    public boolean filter(Order order) {
        return order.getAmount() >= threshold;
    }
}
```

数据库连接、线程池、文件句柄等运行时资源，通常在 RichFunction 的 `open()` 中创建。

## 9. 自测

1. `static` 成员属于类还是对象？
2. `final List<String>` 是否意味着列表内容不能变化？
3. 为什么 Flink 函数更适合写成静态内部类？
4. Lambda 捕获外部数据库客户端为什么危险？

## 10. 阅读时常见问题

### 问题 1：静态内部类和普通内部类最关键的区别是什么？

非静态内部类会隐式持有外部对象引用：

```java
Job.this
```

静态内部类不会。对于需要被 Flink 分发的函数，静态内部类通常更可控。

### 问题 2：外部类本身很简单，非静态内部类是不是就可以放心使用？

仍然不推荐形成习惯。外部类今天可能很简单，之后可能增加不可序列化字段。静态内部类明确表达“不依赖外部实例”，更容易维护。

### 问题 3：`static final` 是否等同于常量？

对于不可变的基础类型或字符串，通常可以这样理解：

```java
public static final String DELIMITER = ",";
```

但如果是可变对象：

```java
public static final List<String> USERS =
    new ArrayList<>();
```

列表内容仍然可以修改。`final` 只限制引用改指向。

### 问题 4：Lambda 捕获字符串阈值可以，为什么捕获客户端对象不推荐？

字符串、数字等小型配置容易序列化，也没有运行时连接状态：

```java
double threshold = 100.0;
orders.filter(order -> order.getAmount() >= threshold);
```

客户端对象可能包含 Socket、线程池、文件句柄等资源，通常不可序列化，也不应该从提交作业的客户端机器直接搬到 TaskManager。

### 问题 5：字段标记为 `transient` 后，是不是就不需要在意它了？

不是。它不会被序列化，因此算子被分发或恢复后，该字段通常为 null。必须在合适的生命周期方法中重新初始化：

```java
@Override
public void open(Configuration parameters) {
    client = new SomeClient();
}
```
