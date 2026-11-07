# Spark最长连续日期：去重、合并与状态边界

对应代码：[SparkConsecutiveDays.java](SparkConsecutiveDays.java)。适用版本：Spark 3.5.7 / Scala 2.12 / Java 17。本篇解释同目录这一份实现，保留与 Hive 案例一致的业务规则，并明确引擎接口上的变化。

## 结果是去重后的最长自然日连续段

输入严格的八位 `uuuuMMdd` 日期字符串，输出 `INT`。例如 `20261001,20261002,20261002,20261004` 的最长连续段为 `2`；重复日期不能把连续长度增加到 `3`。空值、非八位文本和非法日历日期忽略，无有效日期时返回 `0`。

使用 `ResolverStyle.STRICT` 拒绝 `20260230`；使用 `uuuu` 表示可严格解析的年份。`LocalDate.toEpochDay()` 将自然日转成整数，跨月、跨年也只需判断两个值是否相差 1，不经由时区时间戳，不用固定毫秒数模拟一天。

## 为什么不能只保存局部最长长度

一个分区可能有 `01,02`，另一个有 `03,04`；两个分区的局部最长长度都是 2，合并后的答案却是 4。只传递局部最大值会丢失边界能否连接的信息。本例保留日期级状态，在分区合并后重新判断连续关系。

取值时对有效日期排序，维护 `previous`、`current` 和 `longest`：当前日等于前一日加 1 则延长当前段，否则从 1 重新开始。排序后的每个有效日期只扫描一次。

## 状态大小的边界

若一组内有 `d` 个不同有效日期，日期级状态至少随 `d` 增长，本实现取值需要约 `O(d log d)` 的排序时间和 `O(d)` 临时空间。它没有把精确的无限历史统计变成固定内存算法。应先定义按用户、窗口或业务周期保留哪些日期，再选择状态生命周期；随意清理仍可能改变最长连续段。

## Spark 3.5 的三种类型

类继承 `Aggregator<String,Buffer,Integer>`：输入是日期文本，中间类型是公开静态 Java Bean `Buffer`，结果为最长长度。`zero` 创建新的 buffer；`reduce` 解析有效日期并加入状态；`merge` 合并日期集合；`finish` 对去重日期排序扫描。

`Buffer` 用 `Map<Long,Boolean>` 表示集合：键为日期，值恒为真。选择 Map 是为了用 Java Bean encoder 将其表达成可编码的结构化状态，避免直接把任意 `TreeSet` 当作 SQL 字段。setter 复制框架还原的 Map，确保后续 `put` 可以修改它；每个分组通过 `zero` 获得独立容器。

`bufferEncoder()` 返回 `Encoders.bean(Buffer.class)`，`outputEncoder()` 返回 `Encoders.INT()`。Java SQL 注册使用 `functions.udaf(new SparkConsecutiveDays(),Encoders.STRING())`；`String` 输入的 encoder、buffer encoder 与结果 encoder 各自负责一个边界，不能只提供最终结果类型。

## 集合合并适用于追加聚合

本例只需要知道某天是否存在，重复日期在 `reduce` 和 `merge` 时自然合并；Map 合并使用键并集，不把局部最长值当最终答案。单次 Map 插入通常为均摊 `O(1)`，最终排序约 `O(d log d)`，但实际聚合还存在状态编码与 shuffle 成本。

这里没有实现删除输入的接口，也没有证明它可直接用于任意 Structured Streaming 查询。若业务有撤回、滑动窗口删除或严格的迟到事件修正，应按对应流式状态 API 单独设计，不能仅因换成新 `Aggregator` 就认定语义齐备。

## 对照与接口依据

[Spark 3.5.7 Aggregator API](https://spark.apache.org/docs/3.5.7/api/java/org/apache/spark/sql/expressions/Aggregator.html) 与 [Encoders API](https://spark.apache.org/docs/3.5.7/api/java/org/apache/spark/sql/Encoders.html)用于核对引擎接口；本篇业务规则以配套代码为准。

[Hive 同例讲解](<../../../../40-存储与查询服务/43-Hive/02-UDF开发与迁移/08-连续日期/08-连续日期UDAF：严格日期、去重与跨分区合并.md>)；[Flink 同例讲解](<../../../33-Flink/11-UDF原理与实现/08-连续日期/08-Flink最长连续日期：去重、合并与状态边界.md>)；[三引擎类型与状态对比](<../../../31-SQL开发/04-Hive、Flink与Spark UDF：类型、展开与聚合状态对比.md>)。
