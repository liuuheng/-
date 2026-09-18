# 03 Lambda、函数式接口与方法引用

## 1. Lambda 解决什么问题

下面的代码使用匿名内部类筛选大额订单：

```java
orders.filter(new FilterFunction<Order>() {
    @Override
    public boolean filter(Order order) {
        return order.getAmount() >= 100.0;
    }
});
```

使用 Lambda 后：

```java
orders.filter(order -> order.getAmount() >= 100.0);
```

Lambda 的价值是简洁表达“传入一段行为”。

## 2. 函数式接口

只有一个抽象方法的接口称为函数式接口。

```java
@FunctionalInterface
public interface Calculator {
    int calculate(int a, int b);
}
```

可以使用 Lambda 创建实现：

```java
Calculator add = (a, b) -> a + b;
System.out.println(add.calculate(2, 3)); // 5
```

Flink 的 `MapFunction`、`FilterFunction`、`KeySelector` 等都可以配合 Lambda 使用。

## 3. Lambda 语法

单参数时可省略括号：

```java
value -> value.length()
```

多个参数需要括号：

```java
(left, right) -> left + right
```

只有一条表达式时，可以省略大括号和 `return`：

```java
order -> order.getAmount() >= 100
```

多条语句需要大括号：

```java
line -> {
    String[] fields = line.split(",");
    return new Order(fields[0], fields[1],
        Double.parseDouble(fields[2]));
}
```

## 4. `map`、`flatMap`、`filter` 和 `keyBy`

### `map`

一条输入对应一条输出：

```java
DataStream<String> userIds =
    orders.map(order -> order.getUserId());
```

### `filter`

返回 `true` 保留数据，返回 `false` 丢弃数据：

```java
DataStream<Order> paidOrders =
    orders.filter(order -> order.isPaid());
```

### `flatMap`

一条输入可以产生零条、一条或多条输出：

```java
lines.flatMap((String line, Collector<String> out) -> {
    for (String word : line.split(" ")) {
        out.collect(word);
    }
});
```

Lambda 参数包含泛型时，Flink 有时需要额外类型提示：

```java
lines
    .flatMap((String line, Collector<String> out) -> {
        for (String word : line.split(" ")) {
            out.collect(word);
        }
    })
    .returns(Types.STRING);
```

### `keyBy`

从对象中提取分组键：

```java
orders.keyBy(order -> order.getUserId());
```

含义是：同一个用户的数据进入同一逻辑分组。它不是 SQL 的最终聚合操作，只是为后续状态和聚合建立分组。

## 5. 方法引用

当 Lambda 只是调用已有方法时，可以用方法引用简化。

```java
orders.map(Order::getUserId);
```

常见形式：

| 形式 | 示例 |
| --- | --- |
| 静态方法引用 | `Integer::parseInt` |
| 某个对象的实例方法 | `System.out::println` |
| 某类对象的实例方法 | `Order::getUserId` |
| 构造方法引用 | `ArrayList::new` |

## 6. effectively final

Lambda 可以捕获外部局部变量，但变量必须是 final 或 effectively final，也就是赋值后不再修改。

```java
double threshold = 100.0;
orders.filter(order -> order.getAmount() >= threshold);
```

下面无法编译：

```java
double threshold = 100.0;
threshold = 200.0;
orders.filter(order -> order.getAmount() >= threshold);
```

## 7. Flink 场景中的额外注意点

Flink 作业需要分发到集群执行。Lambda 捕获的外部对象也可能被一起序列化。

危险示例：

```java
SomeClient client = new SomeClient();
orders.map(order -> client.query(order.getUserId()));
```

问题：

1. `client` 可能不可序列化，导致作业提交失败。
2. 即使能序列化，连接对象通常也不应该从客户端直接传到 TaskManager。
3. 外部请求是慢操作，可能阻塞流处理。

更合适的做法是使用 RichFunction 的 `open()` 初始化资源，并进一步学习异步 I/O。

## 8. Lambda 还是显式类

适合 Lambda：

- 一行转换。
- 简单过滤。
- 简单 key 提取。

适合显式类：

- 多分支业务逻辑。
- 需要状态或生命周期。
- 需要单元测试。
- 类型推断不稳定。
- 希望报错堆栈更清晰。

## 9. 自测

1. 函数式接口的定义是什么？
2. `map` 和 `flatMap` 的输出数量有什么区别？
3. `keyBy(order -> order.getUserId())` 做了什么？
4. 为什么 Lambda 捕获数据库连接对象可能导致问题？

## 10. 阅读时常见问题

### 问题 1：Lambda 是不是一种特殊的方法？

更准确地说，Lambda 是函数式接口实例的简洁写法。下面两段代码表达的行为接近：

```java
orders.filter(order -> order.getAmount() >= 100);
```

```java
orders.filter(new FilterFunction<Order>() {
    @Override
    public boolean filter(Order order) {
        return order.getAmount() >= 100;
    }
});
```

Lambda 不是凭空存在的。编译器需要结合上下文，推断它要实现哪个函数式接口。

### 问题 2：`map`、`flatMap` 和 `filter` 最简单的区分方法是什么？

看一条输入最多产生多少条输出：

| 算子 | 一条输入对应的输出数量 |
| --- | --- |
| `map` | 恰好一条 |
| `filter` | 零条或一条，类型通常不变 |
| `flatMap` | 零条、一条或多条 |

例如一行文本拆成多个单词，需要使用 `flatMap`。订单对象提取用户 ID，需要使用 `map`。

### 问题 3：为什么 `flatMap` 使用 `Collector`，而不是直接 `return List<String>`？

`flatMap` 的输出数量不固定。通过：

```java
out.collect(word);
```

可以逐条向下游发送结果，不要求先在内存中构造完整列表。这更适合持续到来的数据流。

### 问题 4：`keyBy()` 是不是等同于 SQL 的 `GROUP BY`？

它们有联系，但不完全相同。`keyBy()` 只负责按照 key 重新分区，让相同 key 的数据进入同一个逻辑分组。真正的统计还需要后续算子：

```java
orders
    .keyBy(Order::getUserId)
    .reduce(...);
```

### 问题 5：为什么 Lambda 捕获的局部变量必须 effectively final？

局部变量原本存在于方法栈中，方法结束后就会消失。Lambda 需要保存它使用的值。Java 要求被捕获的局部变量不能继续变化，从而避免“Lambda 最终读取哪个版本”这类难以理解的问题。

### 问题 6：简单 Lambda 也一定可以被 Flink 正确推断返回类型吗？

不一定。Java 编译器和泛型擦除会限制运行时类型信息。简单类型通常可以推断；复杂泛型返回值可能需要：

```java
.returns(new TypeHint<Tuple2<String, Integer>>() {})
```

具体原因见 [[04-泛型、类型擦除与Flink类型信息]]。
