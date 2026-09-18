# 06 POJO、JavaBean、Tuple 与序列化

## 1. 什么是序列化

序列化是将对象转换成可传输或可存储形式的过程。反序列化则是把数据恢复成对象。

Flink 作业中的对象经常需要：

1. 在算子之间通过网络发送。
2. 写入状态后端。
3. 保存进 Checkpoint。
4. 从 Checkpoint 恢复。

因此，序列化不是边缘知识，而是 Flink 运行机制的一部分。

## 2. 什么是 POJO

POJO 是 Plain Old Java Object，即普通 Java 对象。它不要求继承复杂框架类。

```java
public class Order {
    private String orderId;
    private String userId;
    private double amount;

    public Order() {
    }

    public Order(String orderId, String userId, double amount) {
        this.orderId = orderId;
        this.userId = userId;
        this.amount = amount;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }
}
```

## 3. Flink 1.17 识别 POJO 的主要规则

一个类要被 Flink 当作 POJO，通常需要满足：

1. 类是 `public`。
2. 类是独立类或静态内部类，不能是非静态内部类。
3. 有公开的无参构造方法。
4. 所有非静态、非 transient 字段都是 public 且非 final，或者具有符合 JavaBean 规范的公开 getter 和 setter。
5. 字段类型能够被 Flink 支持的序列化器处理。

## 4. JavaBean 命名规范

字段：

```java
private String userId;
```

getter 和 setter：

```java
public String getUserId() {
    return userId;
}

public void setUserId(String userId) {
    this.userId = userId;
}
```

布尔字段常使用：

```java
public boolean isPaid() {
    return paid;
}
```

## 5. 为什么 POJO 值得优先使用

Flink 能够理解 POJO 的字段结构，因此可以使用更合适的序列化方式。与被当作黑盒处理的普通类相比，POJO 更容易调试，也通常更高效。

示例：

```java
DataStream<Order> orders = ...;
orders.keyBy(Order::getUserId);
```

字段语义清楚，可读性好。

## 6. 什么情况可能退回 Kryo

如果自定义类不满足 POJO 规则，Flink 可能将其作为通用类型，使用 Kryo 序列化。

常见原因：

- 没有公开无参构造方法。
- 字段是 `final` 且没有符合条件的写入方式。
- 使用非静态内部类。
- 字段类型复杂，Flink 无法识别。
- getter 或 setter 命名不符合规范。

Kryo 不是绝对不能用，但不应该在不知情的情况下依赖回退行为。

开发阶段可考虑禁止通用类型回退，尽早暴露问题：

```java
env.getConfig().disableGenericTypes();
```

## 7. Java 自带 Serializable 与 Flink 序列化

Java 有 `java.io.Serializable`：

```java
public class Order implements Serializable {
    ...
}
```

但 Flink 并不只是机械使用 Java 原生序列化。Flink 有自己的类型系统和序列化器体系。

应当区分：

- Java 对象是否能够被序列化。
- Flink 能否识别其结构并使用高效序列化器。
- 函数对象能否被分发到 TaskManager。

## 8. Tuple

Flink 提供 `Tuple1` 到 `Tuple25`：

```java
Tuple2<String, Double> total =
    Tuple2.of("u-1", 100.0);

System.out.println(total.f0); // u-1
System.out.println(total.f1); // 100.0
```

适用场景：

- 两三个字段的临时中间结果。
- WordCount 等教学示例。
- 简短聚合结果。

不适用场景：

- 字段很多。
- 业务含义复杂。
- 代码需要长期维护。

对比：

```java
value.f3
```

和：

```java
order.getAmount()
```

后者更容易理解。

## 9. Row

`Row` 可容纳任意数量字段，并支持 null。它常见于 Table API、SQL 或动态结构场景。

```java
Row row = Row.of("u-1", 100.0);
```

DataStream 入门阶段，优先掌握 POJO 和 Tuple。

## 10. `transient`

`transient` 表示字段不应参与常规序列化：

```java
private transient SomeClient client;
```

常见用途是保存运行时临时资源，例如连接对象。注意：反序列化后该字段会丢失，需要在 `open()` 中重新初始化。

## 11. 自测

1. Flink 为什么需要序列化？
2. Flink 1.17 识别 POJO 的核心规则有哪些？
3. POJO 和 Tuple 分别适合什么场景？
4. `transient` 字段反序列化后会怎样？
5. `implements Serializable` 是否等于一定会使用 Flink 的高效 POJO 序列化？

## 12. 阅读时常见问题

### 问题 1：对象本来就在内存里，为什么 Flink 还要序列化？

因为对象不一定只在当前 JVM 内使用。Flink 作业可能跨线程、跨进程、跨机器运行。网络只能传输字节，状态后端和 Checkpoint 也需要保存可持久化的数据形式。

可以把序列化理解为：

```text
Java 对象 -> 字节 -> 网络或磁盘 -> 字节 -> Java 对象
```

### 问题 2：实现 `Serializable` 后，为什么还要关心 Flink POJO 规则？

`Serializable` 只表示对象允许使用 Java 序列化机制处理。Flink 还有自己的类型系统，会尽量识别对象结构并选择更适合的序列化器。

一个类即使实现了：

```java
implements Serializable
```

如果不满足 Flink POJO 规则，也可能被当作通用类型处理。两者不是一回事。

### 问题 3：为什么 POJO 需要无参构造方法？

Flink 在反序列化 POJO 时，需要先构造对象，再恢复字段。可以粗略理解为：

```java
Order order = new Order();
order.setUserId("u-1");
order.setAmount(20.0);
```

因此公开无参构造方法很重要。

### 问题 4：构造方法已经能设置全部字段，为什么还需要 setter？

普通业务代码可以通过全参构造方法创建对象。但 Flink POJO 的反序列化过程通常依赖字段可写。全参构造方法方便开发者使用，无参构造方法和 setter 则方便框架恢复对象。

### 问题 5：Kryo 回退是不是一定意味着代码错误？

不一定。Kryo 可以处理许多通用类型，有时是合理选择。

问题在于：如果你原本希望使用结构清晰的 POJO，却因为漏写无参构造方法或 setter 意外退回 Kryo，就会失去类型透明度，也可能影响性能和状态演进。开发阶段可使用：

```java
env.getConfig().disableGenericTypes();
```

尽早发现这类情况。

### 问题 6：`transient` 是否表示这个字段永远不能使用？

不是。它表示该字段不随对象一起序列化。对于数据库连接、HTTP 客户端等运行时资源，这是合理的：

```java
private transient SomeClient client;
```

对象被发送到 TaskManager 后，`client` 初始为 null，需要在 `open()` 中重新创建。

### 问题 7：业务代码应该优先使用 POJO 还是 Tuple？

短小中间结果可以使用 Tuple：

```java
Tuple2<String, Double>
```

业务字段较多、含义重要、需要长期维护时，优先使用 POJO：

```java
UserTotal {
    String userId;
    double totalAmount;
}
```

代码可读性通常比少写几行更重要。
