# Flink条件计数：可合并的BIGINT状态

对应代码：[FlinkCountIf.java](FlinkCountIf.java)。适用版本：Flink 2.3.0 / Java 17。本篇解释同目录这一份实现，保留与 Hive 案例一致的业务规则，并明确引擎接口上的变化。

## 一组数据只需要一个 long 状态

只统计布尔值 `TRUE`，`FALSE` 和 SQL `NULL` 都不增加计数。输入 `[TRUE,FALSE,NULL,TRUE]` 得到 `2`。空状态结果为 `0`；SQL 空表是否产生一行，还取决于是否有分组键，不能只由函数的初始值推断。

状态和输出都使用 Java `long`/`Long`，对应 SQL `BIGINT`，避免旧式 `INT` 计数过早溢出。递增和合并使用 `Math.addExact`，超过范围时抛异常，不让计数静默绕回负数。

## 为什么可以先局部计算，再合并

一条记录只对状态贡献 0 或 1，所以两个分区的中间状态直接相加即可得到合并状态。零是单位元，正常取值范围内的加法满足结合关系，分区数量或合并顺序不会改变结果。

`merge` 合并的是**中间计数**，不能把别的分区的计数再当成一条布尔输入。函数对象本身不持有某个分组的计数，状态由引擎按分组管理，以免不同键互相污染。每组状态为 `O(1)`，每次更新与合并也为 `O(1)`。

## Flink 把分组状态交给 Accumulator

继承 `AggregateFunction<Long,Accumulator>`。`createAccumulator` 创建零计数状态，`accumulate` 接收一条输入，`merge` 合并其他局部状态，`getValue` 返回当前值，`resetAccumulator` 清空复用状态。

`Accumulator` 为公开静态类，字段 `count` 公开，便于框架提取类型和保存状态。`accumulate`、`retract`、`merge` 等约定方法保持公开且签名明确；这些方法不是把 Hive 的四个聚合阶段换个名字。

## 撤回为什么还要检查状态

`retract(TRUE)` 将计数减一，撤回 `FALSE` 或空值不改变计数。若计数已经为零却又撤回真值，代码抛出状态异常，提示输入撤回序列与已有状态不一致。实现这些方法为需要撤回的执行计划提供能力，实际是否调用、窗口是否支持，还由查询计划决定。

## 对照与接口依据

[Flink 2.3 用户自定义函数文档](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/functions/udfs/)用于核对引擎接口；本篇业务规则以配套代码为准。

[Hive 同例讲解](<../../../../40-存储与查询服务/43-Hive/02-UDF开发与迁移/07-条件计数/07-条件计数UDAF：BIGINT状态与分阶段聚合.md>)；[Spark 同例讲解](<../../../32-Spark/04-UDF原理与实现/07-条件计数/07-Spark条件计数：可合并的BIGINT状态.md>)；[三引擎类型与状态对比](<../../../31-SQL开发/04-Hive、Flink与Spark UDF：类型、展开与聚合状态对比.md>)。
