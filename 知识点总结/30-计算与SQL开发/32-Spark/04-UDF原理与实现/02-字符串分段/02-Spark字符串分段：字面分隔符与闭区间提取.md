# Spark字符串分段：字面分隔符与闭区间提取

对应代码：[SparkSplitPart.java](SparkSplitPart.java)。适用版本：Spark 3.5.7 / Scala 2.12 / Java 17。本篇解释同目录这一份实现，保留与 Hive 案例一致的业务规则，并明确引擎接口上的变化。

## 业务规则与返回契约

按字面分隔符切开文本，选择从 `start` 到 `end` 的分段，再用原分隔符连接。位置从 **1** 开始，区间两端都包含。`a,b,` 取第 2～3 段得到 `b,`；这说明末尾空段属于数据。

任一必要参数为 `NULL` 或 `start < 1` 时返回 `NULL`；`end < start` 抛参数异常；起点超过分段数返回空串；终点超过分段数时截到末尾。分隔符为空串时直接返回原文，这是一项明确保留的业务规则。

## 字面分隔与区间换算

Java 的 `split` 参数是正则表达式。先执行 `Pattern.quote(separator)`，才能把 `|`、`.` 等字符按原样处理。`split(..., -1)` 保留尾部空段，避免默认拆分行为悄悄改变字段数量。

外部的闭区间 `[start,end]` 转为数组的半开区间 `[start-1,min(end,length))`。起点合法性在复制前检查，之后 `Arrays.copyOfRange` 取片段，`String.join` 恢复选中部分的分隔符。

## 设计代价

实现完整拆分当前字符串，耗时和临时内存与输入长度、分段数有关。它的优势是规则清晰，适合普通字段处理；如果文本极大且只取前一小段，可以另写按分隔符扫描的实现，但仍须保留尾部空段和闭区间语义。

## Spark 用固定四参数接口

类实现 `UDF4<String,String,Integer,Integer,String>`，入口为四参数 `call`，注册返回类型为 `DataTypes.StringType`。只取第 2 段时传 `start=2,end=2`。

Java 的 `UDF4` 描述固定参数个数，本例没有把 Java 方法重载当成 SQL 的可选参数。迁移 Flink/Hive 的三参数调用时，应补齐终点，或另行注册一个明确的三参数函数。不要直接使用同名内置函数替换：原生分段函数可能采用不同的位置、空段和分隔符规则。

## 对照与接口依据

[Spark 3.5.7 UDFRegistration API](https://spark.apache.org/docs/3.5.7/api/java/org/apache/spark/sql/UDFRegistration.html)用于核对引擎接口；本篇业务规则以配套代码为准。

[Hive 同例讲解](<../../../../40-存储与查询服务/43-Hive/02-UDF开发与迁移/02-字符串分段/02-字符串分段UDF：字面分隔符与闭区间提取.md>)；[Flink 同例讲解](<../../../33-Flink/11-UDF原理与实现/02-字符串分段/02-Flink字符串分段：字面分隔符与闭区间提取.md>)；[三引擎类型与状态对比](<../../../31-SQL开发/04-Hive、Flink与Spark UDF：类型、展开与聚合状态对比.md>)。
