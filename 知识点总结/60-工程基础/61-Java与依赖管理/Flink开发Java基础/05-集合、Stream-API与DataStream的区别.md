# 05 集合、Stream API 与 DataStream 的区别

## 1. 为什么要复习集合

解析配置、构造测试数据、缓存少量静态维表、编写单元测试时，都会使用 Java 集合。

最常用的三个接口：

| 接口 | 特征 | 常见实现 |
| --- | --- | --- |
| `List` | 有顺序，可重复 | `ArrayList` |
| `Set` | 元素不重复 | `HashSet` |
| `Map` | key-value 映射 | `HashMap` |

## 2. List

```java
List<String> users = new ArrayList<>();
users.add("u-1");
users.add("u-2");
users.add("u-1");
```

适合保存有序数据。根据索引访问：

```java
System.out.println(users.get(0));
```

## 3. Set

```java
Set<String> users = new HashSet<>();
users.add("u-1");
users.add("u-1");
System.out.println(users.size()); // 1
```

适合去重和成员判断：

```java
if (users.contains("u-1")) {
    ...
}
```

## 4. Map

```java
Map<String, Double> totals = new HashMap<>();
totals.put("u-1", 20.0);
totals.put("u-2", 80.0);
```

滚动累加：

```java
totals.merge("u-1", 30.0, Double::sum);
System.out.println(totals.get("u-1")); // 50.0
```

注意：普通 `HashMap` 只存在于当前 JVM 内存中，不具备 Flink 状态的容错、恢复和重新分配能力。

## 5. 遍历集合

增强 for 循环：

```java
for (String user : users) {
    System.out.println(user);
}
```

Map 遍历：

```java
for (Map.Entry<String, Double> entry : totals.entrySet()) {
    System.out.println(entry.getKey() + "=" + entry.getValue());
}
```

## 6. Java Stream API

Java Stream API 用于声明式处理本地集合：

```java
List<String> activeUserIds = users.stream()
    .filter(User::isActive)
    .map(User::getUserId)
    .collect(Collectors.toList());
```

典型操作：

| 操作 | 作用 |
| --- | --- |
| `filter` | 保留符合条件的元素 |
| `map` | 将每个元素转换成另一个元素 |
| `flatMap` | 将嵌套结构摊平 |
| `distinct` | 去重 |
| `sorted` | 排序 |
| `collect` | 收集结果 |
| `reduce` | 聚合 |

示例：

```java
double total = orders.stream()
    .filter(Order::isPaid)
    .map(Order::getAmount)
    .reduce(0.0, Double::sum);
```

## 7. Optional

`Optional<T>` 用于表达“可能没有值”。

```java
Optional<Order> first = orders.stream()
    .filter(order -> order.getAmount() > 100)
    .findFirst();

first.ifPresent(System.out::println);
```

常用方法：

```java
value.orElse(defaultValue);
value.orElseGet(() -> createDefault());
value.map(Order::getUserId);
value.ifPresent(System.out::println);
```

不要无脑调用 `get()`，因为空值时会抛出异常。

## 8. Java Stream 与 Flink DataStream 的相似点

两者都有链式 API：

```java
// Java Stream API
orders.stream()
    .filter(Order::isPaid)
    .map(Order::getUserId);

// Flink DataStream API
orders
    .filter(Order::isPaid)
    .map(Order::getUserId);
```

## 9. Java Stream 与 Flink DataStream 的本质区别

| 维度 | Java Stream API | Flink DataStream |
| --- | --- | --- |
| 数据位置 | 当前 JVM 内存 | 分布式系统 |
| 数据规模 | 通常是有限集合 | 可以是无限数据流 |
| 执行方式 | 本地执行 | 构建执行图后提交给 Flink |
| 容错 | 无内建分布式容错 | 支持 Checkpoint 和状态恢复 |
| 时间语义 | 通常不关心事件时间 | 支持事件时间、Watermark、窗口 |
| 状态 | 普通 Java 变量 | Flink 托管状态 |

这一点非常关键：DataStream 的 `map()` 不是立即遍历一个本地集合，而是在描述将来的计算步骤。

```java
DataStream<String> result = source.map(...);
env.execute("job-name");
```

`env.execute()` 才会触发作业执行。

## 10. 自测

1. `List`、`Set`、`Map` 的典型用途分别是什么？
2. `Optional.get()` 为什么要谨慎使用？
3. Java Stream 和 Flink DataStream 都有 `map()`，它们为什么不是同一个东西？
4. 为什么不能直接用普通 `HashMap` 替代 Flink 状态？

## 11. 阅读时常见问题

### 问题 1：Java Stream 和 Flink DataStream 方法名很像，是不是底层也差不多？

不是。两者借用了相似的声明式编程风格，但执行模型不同。

Java Stream 通常立即在当前 JVM 中处理已有集合：

```java
List<String> result = names.stream()
    .filter(name -> name.startsWith("A"))
    .collect(Collectors.toList());
```

Flink DataStream 通常先描述计算拓扑，之后由 `env.execute()` 触发提交，并在 Flink 集群中持续执行：

```java
DataStream<String> result =
    source.filter(name -> name.startsWith("A"));

env.execute();
```

### 问题 2：Flink DataStream 里的数据是不是都已经在内存中？

不是。`DataStream<T>` 更像对一条逻辑数据流的描述。数据可能来自 Kafka、文件、Socket 或其他 Source，并持续到达。它不等同于一个已经装满数据的 `List<T>`。

### 问题 3：为什么不能将整个 DataStream 转换成 List 再处理？

无限流可能永远没有“全部数据到齐”的时刻。即使是有限数据集，全部收集到单机也可能造成内存压力，并失去分布式处理优势。

### 问题 4：普通 `HashMap` 是否完全不能出现在 Flink 算子中？

不是。它可以用于少量、只读、可重建的辅助数据，例如在 `open()` 中加载的小型静态配置。

但如果数据属于业务状态，需要容错、恢复、扩缩容或按 key 管理，就应该使用 Flink State API，而不是普通 `HashMap`。

### 问题 5：`Optional` 是否适合用作 Flink POJO 字段？

入门阶段不建议。`Optional` 更适合作为方法返回值，用于表达“结果可能不存在”。事件字段通常使用明确字段类型，并通过校验、默认值或业务约定处理缺失情况。
