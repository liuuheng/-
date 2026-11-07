# Spark多列展开：固定行结构与对齐补齐

对应代码：[SparkTransArray.java](SparkTransArray.java)。适用版本：Spark 3.5.7 / Scala 2.12 / Java 17。本篇解释同目录这一份实现，保留与 Hive 案例一致的业务规则，并明确引擎接口上的变化。

## 为什么统一输出键与片段数组

Hive 原实现可以根据参数确定若干键列和展开列。本例迁移时固定为一列字符串键与一个字符串数组，输出结构是 `row_key STRING, parts ARRAY<STRING>`。固定结构使优化器在执行前确定类型，也避免写多个几乎相同的类。

输入 `key=u`、分隔符逗号、数据列 `['a,b','10']`，输出两行：第一行的 `parts` 为 `['a','10']`，第二行为 `['b',NULL]`。这是按位置对齐的 zip 展开，**不是多列之间做笛卡尔积**。

## 拆分一次，以最长列决定行数

分隔符先用 `Pattern.quote` 转成字面模式，每列只拆一次；`-1` 保留尾部空串。以最长拆分结果决定行数，短列和原本为空值的列按位置填 `NULL`。所有数据列都为空值时仍输出一行空片段，保留键对应的记录。

分隔符为空值或空串、数据列数组为空值或零长度时抛异常；数组内某个元素为空值则是合法的数据缺失。键可以为空值。只支持字符串列，多个键或其他类型的键应另行定义清晰的结构。

## 每行都创建快照

每个输出位置都分配新的片段数组和新的行对象。若在循环里复用同一个数组并修改它，先前发送出去的行可能看到后续的值；创建独立对象使一行输出对应一个稳定快照。

设输入列数为 `c`、最大分段数为 `r`，对齐输出需要处理约 `c × r` 个位置，还需保存各列拆分后的结果。输出行数显著增加时，下游算子和网络成本也会一起增加。

## Spark 用数组标量 UDF 接上 explode

本例面向 Spark 3.5 的 Java SQL API：类实现 `UDF3<String,String,Seq<String>,List<Row>>`，先返回行数组，再由内置 `explode` 将数组元素变成多行。这与 Flink 在表函数内逐次 `collect` 的发送方式不同。

`resultType()` 显式构建 `ARRAY<STRUCT<row_key:STRING,parts:ARRAY<STRING>>>`，注册时必须使用这份类型。每个结构体不可为空，结构体内的键和片段元素可以为空。数组输入用 Scala `Seq` 接收，结构体中的字符串数组用 Java List 交给 SQL 类型转换。

关系表达式形状为 `SELECT x.row_key,x.parts FROM (SELECT explode(trans_note(k, ',', array(a,b))) AS x FROM source) t`。这是调用方式说明，不额外保存启动程序。下游投影片段位置时，Spark `parts[0]` 从 0 开始，和 Flink SQL 的下标约定不同。

本实现会先在单次调用中构造全部输出行的 `List<Row>`，额外占用约 `O(c × r)` 的结果内存。它适合每行拆分规模可控的场景，不应据此推导出高膨胀比输入的最优性能。

## 对照与接口依据

[Spark 3.5.7 UDFRegistration API](https://spark.apache.org/docs/3.5.7/api/java/org/apache/spark/sql/UDFRegistration.html)用于核对引擎接口；本篇业务规则以配套代码为准。

[Hive 同例讲解](<../../../../40-存储与查询服务/43-Hive/02-UDF开发与迁移/06-多列展开/06-多列展开UDTF：对齐拆分、空值补齐与行快照.md>)；[Flink 同例讲解](<../../../33-Flink/11-UDF原理与实现/06-多列展开/06-Flink多列展开：固定行结构与对齐补齐.md>)；[三引擎类型与状态对比](<../../../31-SQL开发/04-Hive、Flink与Spark UDF：类型、展开与聚合状态对比.md>)。
