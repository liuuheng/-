# 04 泛型、类型擦除与 Flink 类型信息

## 1. 为什么泛型是 Flink 学习重点

Flink API 到处都是泛型：

```java
DataStream<Order>
MapFunction<String, Order>
Tuple2<String, Integer>
KeySelector<Order, String>
```

泛型让编译器知道数据类型，提前发现错误。

```java
List<String> names = new ArrayList<>();
names.add("Alice");
// names.add(100); // 编译错误
```

## 2. 泛型类

```java
public class Box<T> {
    private T value;

    public void setValue(T value) {
        this.value = value;
    }

    public T getValue() {
        return value;
    }
}
```

使用时指定具体类型：

```java
Box<String> box = new Box<>();
box.setValue("hello");
```

## 3. 泛型接口

```java
public interface Converter<IN, OUT> {
    OUT convert(IN input);
}
```

`IN` 和 `OUT` 是类型参数：

```java
public class StringToOrder
        implements Converter<String, Order> {
    @Override
    public Order convert(String line) {
        return parse(line);
    }
}
```

Flink 的 `MapFunction<IN, OUT>` 采用同样的设计。

## 4. 泛型方法

```java
public static <T> T first(List<T> values) {
    return values.get(0);
}
```

`<T>` 写在返回值之前，表示这是一个泛型方法。

## 5. 通配符

### `?`

表示未知类型：

```java
List<?> values;
```

### `? extends T`

表示某种 `T` 或其子类，适合读取：

```java
public double sum(List<? extends Number> values) {
    double result = 0;
    for (Number value : values) {
        result += value.doubleValue();
    }
    return result;
}
```

### `? super T`

表示某种 `T` 或其父类，适合写入：

```java
public void addIntegers(List<? super Integer> values) {
    values.add(1);
    values.add(2);
}
```

记忆方式：PECS，Producer Extends，Consumer Super。

## 6. 类型擦除

Java 泛型主要服务于编译期。编译后，大量泛型信息会被擦除。

```java
List<String> names = new ArrayList<>();
List<Integer> counts = new ArrayList<>();
```

运行时，这两个对象的类都是 `ArrayList`：

```java
System.out.println(names.getClass() == counts.getClass()); // true
```

这会给 Flink 带来问题：Flink 需要知道流中元素的准确类型，才能选择合适的序列化器。

## 7. Flink 为什么关心类型信息

Flink 数据需要：

1. 在网络中传输。
2. 在不同算子之间交换。
3. 保存在状态中。
4. 写入 Checkpoint。
5. 从 Checkpoint 恢复。

Flink 使用 `TypeInformation<T>` 描述类型：

```java
TypeInformation<String> info =
    TypeInformation.of(String.class);
```

泛型类型需要 `TypeHint`：

```java
TypeInformation<Tuple2<String, Double>> info =
    TypeInformation.of(
        new TypeHint<Tuple2<String, Double>>() {}
    );
```

## 8. 为什么 `new TypeHint<...>() {}` 后面有大括号

这是一个匿名子类。虽然 Java 会擦除很多泛型信息，但匿名子类的父类泛型签名仍可被反射读取。

```java
new TypeHint<Tuple2<String, Double>>() {}
```

可以把它理解为：通过创建一个匿名子类，暂时把泛型信息“钉”在类签名上，让 Flink 在运行时读取。

## 9. `.returns(...)`

简单情况下，Flink 可以自动推断类型：

```java
DataStream<Integer> lengths =
    lines.map(line -> line.length());
```

复杂 Lambda 或泛型返回值可能无法可靠推断：

```java
DataStream<Tuple2<String, Integer>> counts =
    lines
        .map(line -> Tuple2.of(line, 1))
        .returns(new TypeHint<Tuple2<String, Integer>>() {});
```

非泛型类型可以直接传 Class：

```java
stream.map(...).returns(Order.class);
```

Flink 自带基础类型常量：

```java
stream.map(...).returns(Types.STRING);
```

## 10. Tuple

Flink Java API 提供 `Tuple1` 到 `Tuple25`：

```java
Tuple2<String, Integer> wordCount =
    Tuple2.of("hello", 1);

System.out.println(wordCount.f0); // hello
System.out.println(wordCount.f1); // 1
```

Tuple 适合短小的中间结果，但业务字段较多时 POJO 可读性更好。

```java
orders
    .map(order -> Tuple2.of(order.getUserId(),
        order.getAmount()))
    .returns(new TypeHint<Tuple2<String, Double>>() {});
```

## 11. 常见排错思路

看到类型推断异常时：

1. 先确认算子真实输出类型。
2. 尝试改成显式函数类。
3. 为结果添加 `.returns(...)`。
4. 泛型结果使用 `TypeHint`。
5. 检查 POJO 是否符合规范。

## 12. 自测

1. Java 类型擦除是什么？
2. Flink 为什么不能完全忽略泛型信息？
3. `TypeInformation` 和 `TypeHint` 分别解决什么问题？
4. 什么情况下需要主动调用 `.returns(...)`？

## 13. 阅读时常见问题

### 问题 1：Java 编译器明明知道 `List<String>`，为什么运行时会丢失类型信息？

Java 泛型主要用于编译期类型检查。为了兼容较早版本的 Java，编译器通常会擦除泛型参数。运行时：

```java
List<String> names = new ArrayList<>();
List<Integer> counts = new ArrayList<>();
```

两者本质上都是 `ArrayList` 对象。JVM 通常不知道其中一个原本写的是 `String`，另一个写的是 `Integer`。

### 问题 2：类型擦除既然存在，为什么普通代码仍然可以从 `List<String>` 取出字符串？

编译器会在合适位置补充类型转换，并在编译期阻止明显错误：

```java
String name = names.get(0);
```

开发者使用体验仍然像是强类型集合。但 Flink 要在运行时选择序列化器，仅靠编译期检查还不够。

### 问题 3：为什么 Flink 比普通 Java 业务代码更关心运行时类型？

普通方法调用往往只是在当前 JVM 中传递对象引用。Flink 数据则需要被序列化、跨网络发送、写入状态后端和 Checkpoint，再在另一个位置恢复。

Flink 必须知道数据结构，才能选择合适的序列化器。

### 问题 4：为什么 `new TypeHint<Tuple2<String, Double>>() {}` 后面必须有 `{}`？

`{}` 创建了匿名子类。匿名子类的父类签名中保存了：

```java
TypeHint<Tuple2<String, Double>>
```

Flink 可以通过反射读取它。如果只创建普通 `TypeHint`，泛型信息仍然无法保留。

### 问题 5：是不是每个 `map()` 后面都应该写 `.returns(...)`？

不是。能够正确推断时不需要额外声明：

```java
lines.map(line -> line.length());
```

当返回值是复杂泛型、使用某些 Lambda、泛型函数或报类型推断错误时，再补充 `.returns(...)`。

### 问题 6：POJO 和 `TypeHint` 是不是同一个层面的问题？

不是。

- POJO 讨论的是：某个自定义类能否被 Flink 识别为结构化类型。
- `TypeHint` 讨论的是：Java 泛型擦除后，如何显式告诉 Flink 完整类型。

例如 `Order` 可以是合规 POJO，而 `Tuple2<String, Order>` 仍可能在某些场景需要 `TypeHint`。
