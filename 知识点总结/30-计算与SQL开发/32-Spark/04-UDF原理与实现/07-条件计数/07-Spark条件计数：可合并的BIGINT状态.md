# Spark条件计数：可合并的BIGINT状态

对应代码：[SparkCountIf.java](SparkCountIf.java)。适用版本：Spark 3.5.7 / Scala 2.12 / Java 17。本篇解释同目录这一份实现，保留与 Hive 案例一致的业务规则，并明确引擎接口上的变化。

## 一组数据只需要一个 long 状态

只统计布尔值 `TRUE`，`FALSE` 和 SQL `NULL` 都不增加计数。输入 `[TRUE,FALSE,NULL,TRUE]` 得到 `2`。空状态结果为 `0`；SQL 空表是否产生一行，还取决于是否有分组键，不能只由函数的初始值推断。

状态和输出都使用 Java `long`/`Long`，对应 SQL `BIGINT`，避免旧式 `INT` 计数过早溢出。递增和合并使用 `Math.addExact`，超过范围时抛异常，不让计数静默绕回负数。

## 为什么可以先局部计算，再合并

一条记录只对状态贡献 0 或 1，所以两个分区的中间状态直接相加即可得到合并状态。零是单位元，正常取值范围内的加法满足结合关系，分区数量或合并顺序不会改变结果。

`merge` 合并的是**中间计数**，不能把别的分区的计数再当成一条布尔输入。函数对象本身不持有某个分组的计数，状态由引擎按分组管理，以免不同键互相污染。每组状态为 `O(1)`，每次更新与合并也为 `O(1)`。

## Spark 3.5 使用 Aggregator

类继承 `Aggregator<Boolean,Long,Long>`：输入是真值，buffer 是中间计数，输出是最终计数。`zero` 返回 0，`reduce` 吸收一条输入，`merge` 合并两个 buffer，`finish` 返回结果；`bufferEncoder` 和 `outputEncoder` 都返回 `Encoders.LONG()`。

buffer 是不可变的包装数值，更新方法必须返回新值供引擎继续使用，不能只修改一个局部变量后丢弃。使用 `functions.udaf(new SparkCountIf(),Encoders.BOOLEAN())` 包装后，再交给 `spark.udf().register` 注册为 SQL 聚合。

本例没有沿用已弃用的 `UserDefinedAggregateFunction`。`Aggregator` 本身也没有 Flink 的 `retract` 入口；不能直接把它理解为能够删除历史输入的通用流式状态函数。Spark 3.5 已有内置 `count_if`，普通查询可优先使用内置函数，本例用于理解状态和跨分区合并。

## 对照与接口依据

[Spark 3.5.7 Aggregator API](https://spark.apache.org/docs/3.5.7/api/java/org/apache/spark/sql/expressions/Aggregator.html) 与 [Encoders API](https://spark.apache.org/docs/3.5.7/api/java/org/apache/spark/sql/Encoders.html)用于核对引擎接口；本篇业务规则以配套代码为准。

[Hive 同例讲解](<../../../../40-存储与查询服务/43-Hive/02-UDF开发与迁移/07-条件计数/07-条件计数UDAF：BIGINT状态与分阶段聚合.md>)；[Flink 同例讲解](<../../../33-Flink/11-UDF原理与实现/07-条件计数/07-Flink条件计数：可合并的BIGINT状态.md>)；[三引擎类型与状态对比](<../../../31-SQL开发/04-Hive、Flink与Spark UDF：类型、展开与聚合状态对比.md>)。
