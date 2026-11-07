# SortJsonArrayV3：预计算排序键与数值稳定排序

对应代码：[SortJsonArrayV3.java](SortJsonArrayV3.java)。该类直接继承 `GenericUDF`，对一行 JSON 对象数组按某个数值字段排序，返回重新序列化的数组字符串。

## 先确定排序语义

三个参数依次为 JSON 数组文本、字段名、是否升序的布尔值。`true` 为升序，`false` 为降序。字段缺失或为 JSON `null` 时，无论升降序都排在末尾；相同排序键保留输入顺序。

例如输入 `[{"id":"a","n":10},{"id":"b","n":2.5},{"id":"c"}]`，按 `n` 升序后对象顺序为 `b、a、c`。

这是行内排序，不会改变表中各行的顺序。字段名按当前对象的键直接查找，传入 `'a.b'` 代表名为 `a.b` 的键，不会自动沿嵌套路径读取。

## initialize 与 evaluate 各自负责什么

`initialize` 检查三个参数，前两个准备字符串转换器，第三个必须具备 `BooleanObjectInspector`，再将其转换为 `BooleanWritable`。返回类型声明为 `STRING`。

`evaluate` 读取参数并处理 SQL `NULL`，将实际计算交给 `calculate`。`calculate` 只接收普通 Java 参数，完成解析、构建排序键、排序与序列化，因此数据算法和 Hive 的对象表示没有混在一起。

`Row` 是本文件内部的私有静态类，只保存一个 JSON 对象与它的排序键，不需要额外的 Java 文件。

## 预计算排序键：把转换放在比较之前

解析得到的整数可能是 `Integer` 或 `Long`，小数也可能采用其他数值表示，不能直接强转成 `BigDecimal`。构造 `Row` 时将字段转成数值文本，再创建 `BigDecimal`：

```java
Object value = object.get(field);
this.key = value == null ? null : new BigDecimal(value.toString());
```

每个对象只转换一次。排序比较为 `O(n log n)`，如果在比较器内部转换，相同值会被重复解析；预计算把转换次数降到 `O(n)`，并保证只有一个元素的数组也会校验排序键。

比较直接调用 `BigDecimal.compareTo`，避免先转为 `double` 而丢失某些大整数的差异。它不能恢复 JSON 输入在之前的浮点运算中已经丢失的精度，实际解析精度也取决于解析库。

## 空值比较必须独立于升降序

```java
if (left.key == null || right.key == null) {
    return left.key == right.key ? 0 : left.key == null ? 1 : -1;
}
return ascending ? left.key.compareTo(right.key) : right.key.compareTo(left.key);
```

先决定空值位置，再处理有效数值的升降序。如果把整个升序比较器直接反转，降序时空值也会被移动到开头，违背“空值始终置后”的契约。

相等键返回 `0`，配合 `List.sort` 的稳定排序特性，使相同分值的对象保留输入先后顺序；`1` 与 `1.0` 按数值比较也会相等。

## 异常和空间代价

SQL 空输入、JSON `null` 根值、无法按 JSON 数组文本解析的输入返回 `NULL`；空数组返回 `'[]'`。数组元素不是对象，或排序字段不能转为数值时抛出异常，由 `evaluate` 包装为 `HiveException`。缺字段允许排末尾，但错误字段不能被伪装成空值。

`Row` 列表与结果数组需要 `O(n)` 附加空间，解析和序列化的成本还取决于文本大小。该实现适合规模可控的行内数组，不能将大规模表排序任务塞进单个 JSON 字段。

实现按 Hive 1.2.1 API 编写，数值处理使用 Java 标准库，JSON 处理采用 `com.alibaba.fastjson` 的 Fastjson 1.x 接口。

## 跨引擎实现对照

[Flink 2.3 实现讲解](<../../../../30-计算与SQL开发/33-Flink/11-UDF原理与实现/05-JSON数组排序/05-FlinkJSON数组排序：预计算数值键与稳定排序.md>)；[Spark 3.5 实现讲解](<../../../../30-计算与SQL开发/32-Spark/04-UDF原理与实现/05-JSON数组排序/05-SparkJSON数组排序：预计算数值键与稳定排序.md>)；[三引擎类型与状态对比](<../../../../30-计算与SQL开发/31-SQL开发/04-Hive、Flink与Spark UDF：类型、展开与聚合状态对比.md>)。业务规则尽量一致；参数形状、展开结构和撤回能力的差异见对应文章。
