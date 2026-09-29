# 13 容易漏掉但高频的 Java 细节

## 1. 为什么还需要这一章

前面的章节已经覆盖 Flink 学习的主干知识。但真正开始写代码时，很多报错来自看起来很小的 Java 细节：

- `int` 和 `Integer` 有什么区别？
- 为什么一个 `Integer` 自动拆箱时会抛出 `NullPointerException`？
- `String.split("|")` 为什么结果不符合预期？
- 金额为什么不适合随意使用 `double`？
- `enum` 比字符串常量好在哪里？

这些问题不复杂，但会直接影响数据解析和业务正确性。

## 2. 基本类型与包装类型

Java 有 8 种基本类型：

| 基本类型 | 包装类型 | 示例 |
| --- | --- | --- |
| `byte` | `Byte` | `byte level = 1;` |
| `short` | `Short` | `short port = 8080;` |
| `int` | `Integer` | `int count = 10;` |
| `long` | `Long` | `long timestamp = 1000L;` |
| `float` | `Float` | `float ratio = 0.5F;` |
| `double` | `Double` | `double amount = 20.5;` |
| `char` | `Character` | `char separator = ',';` |
| `boolean` | `Boolean` | `boolean paid = true;` |

基本类型直接表示值，不能为 `null`：

```java
int count = 0;
```

包装类型是对象，可以为 `null`：

```java
Integer count = null;
```

泛型中不能直接使用基本类型：

```java
List<Integer> counts = new ArrayList<>();
// List<int> counts; // 不合法
```

## 3. 自动装箱与自动拆箱

基本类型和包装类型之间，Java 可以自动转换。

自动装箱：

```java
Integer count = 10;
```

可以粗略理解为：

```java
Integer count = Integer.valueOf(10);
```

自动拆箱：

```java
Integer boxed = 10;
int count = boxed;
```

可以粗略理解为：

```java
int count = boxed.intValue();
```

## 4. 自动拆箱与空指针

下面的代码会抛出 `NullPointerException`：

```java
Integer count = null;
int result = count;
```

原因是自动拆箱相当于：

```java
int result = count.intValue();
```

但 `count` 是 null。

Flink 场景中，如果输入字段可能缺失，要明确决定：

1. 缺失值是否允许存在。
2. 使用默认值还是过滤数据。
3. 是否将数据输出到脏数据流。

不要让空值在下游悄悄触发异常。

## 5. `double` 与金额精度

教学示例常使用：

```java
double amount = 0.1 + 0.2;
System.out.println(amount); // 可能不是精确的 0.3
```

`double` 使用二进制浮点表示，某些十进制小数无法被精确表示。

如果金额正确性要求较高，应考虑：

### 使用最小货币单位

例如使用分：

```java
long amountInCents = 1999L;
```

### 使用 BigDecimal

```java
BigDecimal amount =
    new BigDecimal("19.99");
```

注意，优先使用字符串构造：

```java
new BigDecimal("0.1");
```

避免直接使用浮点数构造：

```java
new BigDecimal(0.1);
```

后者会把浮点误差带进来。

教学代码使用 `double` 是为了简洁；生产系统中的金额类型要结合业务精度要求设计。

## 6. String 不可变

`String` 是不可变对象：

```java
String name = "flink";
name.toUpperCase();
System.out.println(name); // flink
```

`toUpperCase()` 不会修改原字符串，而是返回新字符串：

```java
name = name.toUpperCase();
System.out.println(name); // FLINK
```

字符串不可变带来的好处：

- 更容易安全共享。
- 可以缓存哈希值。
- 适合作为 `HashMap` 的 key。
- 更适合作为 `keyBy()` 提取出的 key。

## 7. 字符串比较

不要用 `==` 比较字符串内容：

```java
String left = new String("paid");
String right = new String("paid");

System.out.println(left == right);      // false
System.out.println(left.equals(right)); // true
```

`==` 比较引用，`equals()` 比较内容。

如果变量可能为 null，可以使用：

```java
Objects.equals(left, right);
```

或将确定非 null 的常量放在前面：

```java
"paid".equals(status);
```

## 8. `split()` 的参数是正则表达式

解析 CSV 时常写：

```java
String[] fields = line.split(",");
```

但要注意，`split()` 接收的是正则表达式，不是普通字符串。

例如，竖线 `|` 在正则表达式中有特殊含义：

```java
String line = "u-1|20.0";
String[] fields = line.split("\\|");
```

点号 `.` 也需要转义：

```java
"a.b".split("\\.");
```

## 9. `split()` 与尾部空字段

默认情况下，`split()` 会丢弃尾部空字符串：

```java
String line = "o-1,u-1,";
String[] fields = line.split(",");
System.out.println(fields.length); // 2
```

希望保留尾部空字段时：

```java
String[] fields = line.split(",", -1);
System.out.println(fields.length); // 3
```

处理 CSV、日志或 CDC 数据时，这个细节很重要。否则“字段缺失”和“字段为空字符串”可能被混为一谈。

## 10. 数组

数组长度固定：

```java
String[] fields = line.split(",");
```

读取：

```java
String orderId = fields[0];
```

数组索引从 0 开始。如果越界，会抛出：

```text
ArrayIndexOutOfBoundsException
```

因此解析前先检查长度：

```java
if (fields.length != 3) {
    throw new IllegalArgumentException(
        "invalid order line: " + line);
}
```

数组适合字段数量固定的临时结构；需要动态增删元素时，通常使用 `List`。

## 11. enum

如果状态只能取有限值，优先考虑枚举：

```java
public enum OrderStatus {
    CREATED,
    PAID,
    CANCELLED
}
```

比字符串更可靠：

```java
if ("padi".equals(status)) {
    ...
}
```

字符串拼写错误只能在运行时发现。枚举写错通常会在编译期暴露。

解析字符串：

```java
OrderStatus status =
    OrderStatus.valueOf("PAID");
```

输入不合法时会抛出 `IllegalArgumentException`，因此外部数据需要校验。

## 12. 时间戳使用 long

流处理中经常遇到时间戳：

```java
long eventTime = 1710000000000L;
```

常见约定：

- 秒级时间戳通常是 10 位。
- 毫秒级时间戳通常是 13 位。
- Flink 时间语义中经常使用毫秒。

看到时间偏差巨大时，先检查单位是否混淆。

## 13. 常量与魔法值

不推荐将业务含义埋在数字和字符串中：

```java
if (amount >= 1000) {
    ...
}
```

更容易阅读的写法：

```java
private static final long VIP_THRESHOLD_IN_CENTS = 100_000L;

if (amountInCents >= VIP_THRESHOLD_IN_CENTS) {
    ...
}
```

常量名同时解释了单位和业务含义。

## 14. 阅读时常见问题

### 问题 1：POJO 字段应该用 `int` 还是 `Integer`？

如果字段业务上一定存在，可以优先使用 `int`，避免 null 和自动拆箱风险。

如果确实需要区分“值为 0”和“没有值”，可以使用 `Integer`，但必须明确处理 null。

### 问题 2：为什么 Flink 的计数经常使用 `Long` 或 `long`？

数据流可能长期运行，累计数量可能超过 `int` 的上限：

```text
2,147,483,647
```

`long` 范围更大，更适合累计计数和时间戳。

### 问题 3：使用 `double` 做订单统计一定错误吗？

教学演示中可以使用，因为代码更简洁。生产环境是否使用，要看精度要求。

如果涉及真实金额结算，优先考虑以分为单位的 `long` 或 `BigDecimal`。

### 问题 4：为什么 `"paid".equals(status)` 比 `status.equals("paid")` 更稳妥？

如果 `status` 为 null：

```java
status.equals("paid");
```

会抛出 `NullPointerException`。而常量 `"paid"` 不会为 null：

```java
"paid".equals(status);
```

结果只是 `false`。

### 问题 5：真实 CSV 数据都能用 `split(",")` 解析吗？

不能。简单教学数据可以这样处理。但真实 CSV 可能包含引号、转义字符和字段内逗号：

```text
o-1,"Shanghai, China",20.0
```

这时应使用成熟 CSV 解析库，而不是手写 `split()`。

### 问题 6：枚举能直接解决所有外部状态字段问题吗？

不能。枚举可以提高内部代码可靠性，但外部输入仍然可能拼写错误、大小写不同或出现新状态。解析时仍需要校验和兼容策略。

## 15. 自测

1. `Integer count = null; int value = count;` 为什么会抛异常？
2. 为什么 `new BigDecimal("0.1")` 通常优于 `new BigDecimal(0.1)`？
3. 为什么字符串内容比较应该使用 `equals()`？
4. `line.split(",", -1)` 中的 `-1` 有什么作用？
5. 什么情况下枚举比字符串更适合表示状态？

