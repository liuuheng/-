# Spark coalesce、repartition 与 AQE 的关系

## 结论概览

`coalesce()` 和 `repartition()` 都可以改变 Spark DataFrame / Dataset 的分区数，但它们对 Shuffle 和 AQE 的影响不同。

- `coalesce()` 默认不触发 Shuffle，只合并已有分区，AQE无法优化这个合并动作本身。
- `repartition()` 会触发 Shuffle，AQE可以基于 Shuffle 统计信息做部分运行时优化。
- 有 Shuffle 不代表 AQE一定能解决倾斜；AQE有适用条件和边界。
- 是否选择 `coalesce()` 或 `repartition()`，核心取决于是否需要重新均衡数据分布。

## coalesce()：默认无 Shuffle

`coalesce(n)` 默认是窄依赖操作，不触发 Shuffle。它只是把多个已有分区合并到更少的 Task 中执行，数据不会被重新打散，也不会根据 key 或数据量重新分布。

因此，如果原始分区本身不均衡，或者多个大分区被合并到同一个新分区中，`coalesce()` 可能会制造或放大长尾 Task。

典型风险：

- 原来就倾斜的分区不会被重新均衡。
- 多个大分区可能被合并到同一个 Task。
- 分区数减少后，单个 Task 的数据量变大。
- 后续写出可能变慢，甚至因为单 Task 数据量过大导致失败。

## 为什么 AQE无法优化 coalesce() 本身

AQE的很多优化依赖 Shuffle 阶段产生的运行时统计信息，例如 Shuffle 分区大小、Map 输出大小、Task 分布等。

默认 `coalesce()` 不产生 Shuffle stage，因此 AQE没有对应的 Shuffle statistics 可用，也就无法对这个分区合并动作本身做动态调整。

准确说法是：

> AQE无法优化 `coalesce()` 这个无 Shuffle 的分区合并动作本身。

但不能扩展成：

> 使用了 `coalesce()` 后，整个作业都不会被 AQE优化。

如果 `coalesce()` 后面还有会触发 Shuffle 的算子，例如 `join`、`groupBy`、`distinct`、`orderBy`、`repartition`、窗口排序等，AQE仍然可能基于后续 Shuffle 的统计信息介入优化。

## repartition()：触发 Shuffle

`repartition(n)` 会触发全量 Shuffle。Spark会将数据重新打散后分配到新的分区中。

它通常用于：

- 增加分区数，提高并行度。
- 在数据分布不均时重新洗牌。
- 写出前控制输出文件数量。
- Join 或聚合前调整数据分布。

因为 `repartition()` 会产生 Shuffle，AQE可以基于 Shuffle 统计信息做运行时优化。

常见 AQE优化包括：

- 合并过小的 Shuffle 分区。
- 根据实际数据量调整 post-shuffle partition 数量。
- 在满足条件时处理 Shuffle Join 的倾斜分区。
- 在运行时调整部分 Join 策略。

## repartition() 不等于自动解决倾斜

需要注意，`repartition()` 触发 Shuffle，并不代表 AQE一定能从根源上解决倾斜。

如果使用：

```scala
df.repartition(col("key"))
```

Spark会按指定 key 做 Hash 分区。如果 `key` 存在热点值，那么相同热点 key 仍然会进入同一个或少数几个分区，倾斜仍然可能存在。

如果使用：

```scala
df.repartition(200)
```

不指定列时，Spark通常会通过 Round-Robin 等方式重新分布数据，更偏向均衡数据量，但代价是完整 Shuffle，并且可能破坏后续算子原本可以利用的数据分布。

## AQE的边界

AQE依赖 Shuffle 统计信息，但不是万能的性能优化器。

AQE是否介入，取决于：

- Spark版本。
- AQE相关配置是否开启。
- 具体算子类型是否支持 AQE优化。
- Shuffle 分区大小是否达到相关阈值。
- 倾斜判断是否满足配置条件。
- 物理计划中是否存在可优化的 Query Stage。

即使开启 AQE，以下问题仍可能需要手工治理：

- 极端热点 key。
- 写出阶段倾斜。
- 单个 key 数据量过大。
- 上游分区设计不合理。
- 文件数量过多或文件大小不均。
- 数据源过滤不足导致扫描放大。
- 聚合、去重、窗口函数导致的长尾 Task。

## 使用建议

### 适合使用 coalesce() 的场景

`coalesce()` 适合在数据量已经变小，且只是想减少输出文件或减少后续 Task 数量时使用。

例如：

- 过滤后数据量明显减少。
- 写出前希望减少小文件。
- 数据分布基本均衡。
- 不希望引入额外 Shuffle 成本。

使用时要注意：`coalesce()` 只是减少分区数，不会重新均衡数据。

### 适合使用 repartition() 的场景

`repartition()` 适合在需要重新分布数据时使用。

例如：

- 当前分区明显不均。
- 需要提高并行度。
- 后续计算需要更均匀的数据分布。
- 写出前需要更均衡的文件大小。
- Join 或聚合前需要按特定 key 组织数据。

使用时要注意：`repartition()` 会产生完整 Shuffle，成本较高；如果按热点 key 分区，仍然可能倾斜。

## 对比总结

| 方法 | 是否触发 Shuffle | 是否重新均衡数据 | AQE能否优化该动作本身 | 典型风险 |
|---|---:|---:|---:|---|
| `coalesce(n)` | 默认否 | 否 | 否 | 合并后形成大分区、长尾 Task、写出慢 |
| `repartition(n)` | 是 | 通常是 | 可以基于 Shuffle 统计优化 | Shuffle 成本高 |
| `repartition(col)` | 是 | 按 key 分布 | 可以基于 Shuffle 统计优化 | 热点 key 仍可能倾斜 |

## SQL Hint 参数规则

Spark SQL 中也有对应的分区 Hint：

```sql
SELECT /*+ COALESCE(10) */ *
FROM t;

SELECT /*+ REPARTITION */ *
FROM t;

SELECT /*+ REPARTITION(200) */ *
FROM t;

SELECT /*+ REPARTITION(c) */ *
FROM t;

SELECT /*+ REPARTITION(200, c) */ *
FROM t;
```

`COALESCE` hint 必须带分区数量，例如 `COALESCE(10)`。如果写成无参形式：

```sql
SELECT /*+ COALESCE */ *
FROM t;
```

Spark会报错：

```text
COALESCE Hint expects a partition number as a parameter
```

原因是 `COALESCE` 的语义是“把当前分区数减少到指定数量”。这个目标数量就是它的核心参数。如果不指定数量，Spark无法判断要合并到多少个分区。

`REPARTITION` hint 可以不带参数，因为它的语义是“重新 Shuffle 分区”。无参时，Spark可以使用默认 Shuffle 分区数，通常来自：

```text
spark.sql.shuffle.partitions
```

因此：

```sql
SELECT /*+ REPARTITION */ *
FROM t;
```

可以理解为：触发一次 Shuffle 重新分布，分区数量使用 Spark SQL 的默认 Shuffle 分区配置。

参数规则总结：

| Hint | 参数是否可省略 | 含义 |
|---|---:|---|
| `COALESCE(n)` | 不可省略 | 合并到指定的 n 个分区 |
| `REPARTITION` | 可省略 | Shuffle 重分区，分区数走默认配置 |
| `REPARTITION(n)` | 可指定数量 | Shuffle 到 n 个分区 |
| `REPARTITION(col)` | 可只指定列 | 按列 Shuffle，分区数走默认配置 |
| `REPARTITION(n, col)` | 可同时指定 | 按列 Shuffle 到 n 个分区 |
| `REPARTITION_BY_RANGE(col)` | 必须指定列，数量可省略 | 按 range 分区，数量可走默认配置 |
| `REBALANCE` | 可省略 | AQE开启时尽力均衡输出分区 |

## 推荐记法

- 减少分区且不需要重新均衡：用 `coalesce(n)`。
- 需要重新均衡数据分布：用 `repartition(n)`。
- 需要按某个 key 分布数据：用 `repartition(col)`，但必须检查热点 key。
- 想让 AQE介入：必须存在 Shuffle stage。
- 有 Shuffle 不代表 AQE一定能解决所有倾斜。
- 判断性能问题时，要结合 Spark UI 中的 Stage、Task 分布、Shuffle read/write、Spill、Max/Median Task Time 比例一起看。

## 更准确的表述

原说法中“`coalesce()` 无 Shuffle，AQE无法介入”需要补充边界：

> `coalesce()` 默认不触发 Shuffle，因此 AQE无法优化 `coalesce()` 这个分区合并动作本身；但如果后续算子触发 Shuffle，AQE仍然可能对后续 Shuffle stage 进行优化。

原说法中“`repartition()` 触发全 Shuffle，AQE会自动优化”需要收窄：

> `repartition()` 会触发 Shuffle，AQE可以基于 Shuffle 统计信息进行运行时优化；但 AQE是否生效取决于配置、版本、算子和阈值条件，也不能保证自动解决所有数据倾斜。
