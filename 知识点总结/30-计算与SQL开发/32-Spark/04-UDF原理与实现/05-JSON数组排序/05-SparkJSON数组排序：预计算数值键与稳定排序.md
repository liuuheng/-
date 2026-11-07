# SparkJSON数组排序：预计算数值键与稳定排序

对应代码：[SparkSortJsonArray.java](SparkSortJsonArray.java)。适用版本：Spark 3.5.7 / Scala 2.12 / Java 17。本篇解释同目录这一份实现，保留与 Hive 案例一致的业务规则，并明确引擎接口上的变化。

## 业务规则与返回契约

输入为 JSON 对象数组、排序字段名和升序标记。按指定字段的**数值**排序；字段缺失或为 JSON `null` 时始终放到末尾。`[{"n":10},{},{"n":2}]` 升序得到 `[{"n":2},{"n":10},{}]`。

任一必要参数为 SQL 空值时返回 `NULL`；无法解析为数组的文本返回 `NULL`；数组元素不是对象、或排序字段无法转换为数值时抛异常。空数组返回 `[]`。字段名是对象的直接键，不是 JSONPath，不会自动访问嵌套路径。

## 为什么先生成排序键

每个对象只构造一次 `BigDecimal` 排序键，随后比较器只比较已生成的键。若把数值转换放在比较器里，排序过程会对同一字段重复转换；预计算将转换次数限制为元素个数。

先判断空键，再比较数字，确保降序也不会把空字段放到最前。降序通过交换两个键的比较方向实现，不把整个比较器结果直接取反，以免同时翻转空值策略。Java 列表排序保持相等键的原有顺序，原对象的其他字段一起保留。

## 精度与复杂度

`BigDecimal.compareTo` 按数值比较，`1.0` 与 `1.00` 排序相等；数值字符串也能转换，但普通文本不能参与数字排序。代码不主动经由 `doubleValue()` 比较，避免大整数被浮点化后丢失区分。

排序时间通常为 `O(n log n)`，额外保存 `O(n)` 个对象与键。它仍需在单条记录中解析和容纳整个数组，不适合把无限数据塞进一个 JSON 字段。输出是重新序列化的 JSON，空白和字段书写形式不保证与输入一致。JSON 依赖沿用 `com.alibaba.fastjson` 1.x API。

## Spark 的返回类型与执行边界

类实现 `UDF3<String,String,Boolean,String>`，入口为 `call`，注册返回 `DataTypes.StringType`。SQL `BOOLEAN` 映射为可空的 Java `Boolean`，不能把空值与降序混为一谈。

这是把一个 JSON 文本字段变成另一个文本字段的标量 UDF，排序发生在单条记录的数组内部；它不是对整个 DataFrame 做 `ORDER BY`。Spark 不会因为函数名包含 sort 就替你重新分区或为数组排序逻辑生成专用算子。

## 对照与接口依据

[Spark 3.5.7 UDFRegistration API](https://spark.apache.org/docs/3.5.7/api/java/org/apache/spark/sql/UDFRegistration.html)用于核对引擎接口；本篇业务规则以配套代码为准。

[Hive 同例讲解](<../../../../40-存储与查询服务/43-Hive/02-UDF开发与迁移/05-JSON数组排序/05-JSON数组排序UDF：数值精度、稳定排序与空字段.md>)；[Flink 同例讲解](<../../../33-Flink/11-UDF原理与实现/05-JSON数组排序/05-FlinkJSON数组排序：预计算数值键与稳定排序.md>)；[三引擎类型与状态对比](<../../../31-SQL开发/04-Hive、Flink与Spark UDF：类型、展开与聚合状态对比.md>)。
