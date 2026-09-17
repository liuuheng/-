---
tags:
  - spark
  - join
  - aqe
status: active
---

# Spark 执行模型、Join 与 AQE

返回：[[README|企业微信知识地图]]

## 1. 从 Application 到 Task

一个 Spark Application 通常包含一个 Driver 和多个 Executor。Driver 解析代码、构建逻辑/物理计划、切分 Job 和 Stage，并调度 Task；Executor 是长期运行的进程，在自己的 JVM 中用线程执行 Task。

常见层级：

```text
Application
└── Job：由 action 或一次 SQL 执行触发
    └── Stage：通常被 Shuffle 依赖边界切分
        └── Task：Stage 在一个输出 partition 上的执行实例
```

转换通常是惰性的。`filter`、`select` 等只记录计算关系，遇到 action、写表或需要物化的查询时才真正执行。这使优化器能合并表达式、做列裁剪和谓词下推。

## 2. Stage 和 Task 的正确理解

一般情况下，同一个 Stage 的 Task 执行同一套算子流水线，只是输入分区不同。因此某个 Task 很慢，优先怀疑分区数据量、热点 key、节点性能或外部 I/O，而不是认为调度器给它安排了另一段业务代码。

但“所有 Task 必然执行完全相同的 lineage”也不绝对。`UnionRDD` 的最终分区由父 RDD 分区相加，不同输出 partition 可能分别沿 union 的不同父分支计算。因此：

- Stage 的 Task 数由该 Stage 最终 RDD 的 partition 数决定。
- FileScanRDD 的 partition 由文件 split 规划决定。
- ShuffledRDD 通常由 shuffle partition 数决定。
- UnionRDD 的 partition 通常是各父 RDD partition 数之和。

不要把这个例外泛化为 Spark 会随意让同一 Stage 的 Task 做不同任务。

## 3. 一个 Executor 能同时执行多少 Task

大致并发上限为：

```text
同时运行 Task 数 ≈ 活跃 Executor 数 × 每个 Executor 可用 core 数 ÷ spark.task.cpus
```

动态资源分配下 Executor 数会随 backlog 和空闲时间变化，不能只用 `maxExecutors × cores` 解释任何时刻的实际并发。集群队列资源、Executor 启动延迟、数据本地性和 Stage 可运行 Task 数也会限制并发。

## 4. Shuffle 为什么昂贵

Shuffle 把相同 key 或目标分区的数据跨 Executor 重分布，通常涉及序列化、网络传输、排序/聚合缓冲、磁盘文件和下游拉取。`GROUP BY`、`DISTINCT`、大表 join、`repartition` 和窗口分区都可能触发 Shuffle。

Spark 使用 Sort-based Shuffle，不等于每个 Join 算子都再次排序：Shuffle 写出路径的组织成本已经发生，但 Shuffle Hash Join 不会因为数据已按 shuffle 方式组织就自动获得 Sort Merge Join 的归并优势。应以物理计划中的 Exchange、Sort 和 Join 节点判断成本。

## 5. Broadcast Hash Join

当一侧足够小时，Driver 会触发作业计算广播侧数据，将结果收集、构建广播关系并分发到各 Executor；Executor 在本地用广播表探测另一侧数据。它避免大表 Shuffle，所以通常最快。

但“广播小表”不是无条件规则：

- 广播结果要进入 Driver 和每个 Executor 的内存，过大会导致 Driver OOM、Executor 内存压力或超时。
- 某些 Join 类型不支持把指定一侧作为广播构建端。
- `BROADCAST` hint 优先级高于自动阈值，但 hint 不是绝对命令，优化器仍需满足语义约束。

排查时应看 `EXPLAIN`、SQL UI 和统计信息，而不是仅看表名是否像维表。

## 6. Shuffle Hash Join 与 Sort Merge Join

Shuffle Hash Join 先按 join key Shuffle，一侧在每个分区内构建哈希表，另一侧 probe。它省去 join 阶段的归并，但构建侧必须能装入 Task 可用内存。

Sort Merge Join 将两侧按 key Shuffle 并排序，再线性归并。排序有成本，但可以溢写到磁盘，对两个超大数据集更稳健，因此长期是 Spark 等值大表 join 的常见策略。

不能只用 `O(n)` 与 `O(n log n)` 判断谁更快，因为真实作业还包括网络、序列化、内存、溢写和数据分布。优化器根据统计、配置和版本选择策略，最终应通过计划和实测验证。

## 7. Bucket Join 为什么可能减少 Shuffle

如果两张表按 join key 以兼容方式分桶，Spark 可以利用已有数据分布，避免一侧或两侧 Exchange。若桶内还满足排序元数据和执行版本要求，Sort Merge Join 的排序也可能被减少。

原记录中“Bucket Join 一定少一次 Shuffle、一定不 Sort”过于绝对。能否复用取决于：

- join key 是否覆盖分桶列。
- 两侧 bucket 数是否兼容。
- 表是否通过 Spark 可识别的 bucket 元数据创建和读取。
- 是否有数据重写破坏了元数据假设。
- Spark 版本和相关优化配置是否支持。

最可靠的方法是查看物理计划中是否仍有 `Exchange` 和 `Sort`。

```sql
CREATE TABLE tmp_keys
USING PARQUET
CLUSTERED BY (design_id, brandgood_id)
SORTED BY (design_id, brandgood_id)
INTO 400 BUCKETS
AS SELECT ...;
```

`CLUSTERED BY` 不是简单等价于任意查询中的 `DISTRIBUTE BY + SORT BY`；前者还声明持久化 bucket 元数据，后者通常只影响一次查询的输出分布。后续读取不会因为曾经写过 `DISTRIBUTE BY user_id` 就自动被优化器当成已满足所有分布要求。

## 8. 统计信息为什么影响 Join

优化器需要估算行数和字节数，才能决定广播侧、join 顺序和聚合策略。新建空表后再 INSERT，Metastore 统计可能不存在或过期，可以使用 `ANALYZE TABLE ... COMPUTE STATISTICS`，并用 `DESCRIBE EXTENDED` 或 `EXPLAIN COST` 检查。

Spark 2.4 与 3.x 对文件源大小估算、AQE 和运行时统计的利用能力不同，因此“旧版本无统计就不广播、新版本一定能从文件判断”只能作为排查线索，不能当成所有数据源的固定规律。

## 9. AQE 的工作方式

AQE 先执行到 Shuffle 或 Broadcast Exchange 边界，将中间结果物化为 QueryStage，收集真实统计，再优化后续物理计划：

```text
初始物理计划
→ 物化 ShuffleQueryStage/BroadcastQueryStage
→ 收集真实大小与分区分布
→ 合并小分区、处理倾斜或转换 Join
→ 执行后续计划
```

因此开启 AQE 后，Spark UI 可能出现多个辅助 Job。不能据此得出“一个 Job 等于一个 Stage”。Job、Spark Stage、AQE QueryStage 是不同概念。

AQE 常见能力：

- 合并过小的 post-shuffle partitions，减少大量小 Task。
- 根据运行时大小把 Sort Merge Join 转换为 Broadcast/Shuffle Hash Join。
- 将倾斜分区拆分处理。
- 局部读取 Shuffle 数据，减少网络传输。

AQE 不是万能修复。统计只有到物化边界后才能获得，广播仍受内存约束，业务上极端热点 key 仍可能需要拆分。

## 10. 数据倾斜的定位与处理

先在 Spark UI 比较同 Stage Task 的 input、shuffle read、duration、spill 和 peak memory。如果少数 Task 远大于中位数，再按 join/group key 做频次分布定位热点。

处理手段要匹配原因：

- 无业务价值的 NULL/异常 key：过滤或单独统计。
- Group By 单个热点：只对热点 key 加盐做局部聚合，再去盐二次聚合。
- 大表 join 热点：大表热点行随机加盐，小表对应 key 复制多个盐值，再 join。
- 小维表：广播避免按热点 key Shuffle，但要评估内存。
- AQE skew join：让 Spark 按运行时阈值拆分倾斜分区。

加盐会修改逻辑和放大数据，不能对全表盲目使用。示例：

```sql
WITH salted AS (
  SELECT
    user_id,
    CASE WHEN user_id = 'HOT'
         THEN CAST(FLOOR(RAND() * 20) AS INT)
         ELSE 0 END AS salt,
    amount
  FROM orders
), partial AS (
  SELECT user_id, salt, SUM(amount) AS part_amount
  FROM salted
  GROUP BY user_id, salt
)
SELECT user_id, SUM(part_amount)
FROM partial
GROUP BY user_id;
```

RAND 是非确定函数；用于一次批计算打散可以接受，但若要求可重复结果，应使用稳定 hash 取模。

## 11. Count Distinct 为什么可能出现多层聚合

`COUNT(DISTINCT logo)` 常先以 `group key + distinct key` 做局部和全局去重，再以 group key 做部分计数和最终计数。物理计划可能看到多层 HashAggregate 与 Exchange，这是为了在 map 端减少 Shuffle 数据，并避免单个 reduce 为每组维护巨大 distinct set。

聚合层数受 Spark 版本、表达式数量、是否有多个 distinct 组和优化规则影响，不能写成“单层 SQL 最多固定四个 HashAggregate”。应从每个节点的 key 和 aggregate function 解释其职责。

## 12. 排序语义

- `SORT BY` 只保证每个输出分区内部有序。
- `ORDER BY` 保证全局顺序，通常需要 range partition 和全局协调，成本更高。
- `DISTRIBUTE BY` 控制相同 key 进入相同分区，但不保证分区内顺序。
- `CLUSTER BY key` 在部分方言中相当于按同一 key distribute 和 sort，但不等于建表 bucket 元数据。

## 排查清单

- 执行计划是否出现意外 Exchange 或 Sort？
- 统计信息是否缺失或严重过期？
- 广播侧实际大小是否包含过滤后大小，而不是表总大小？
- 少数 Task 是否明显大于中位数？
- AQE 最终计划是否与初始计划不同？
- Bucket 元数据是否真的被识别？
- Join key 是否有 NULL、默认值或超级热点？

## 参考依据

- [Spark RDD 编程指南：惰性、Shuffle 与分区](https://spark.apache.org/docs/4.0.1/rdd-programming-guide.html)
- [Spark SQL 性能调优：统计、广播和 AQE](https://spark.apache.org/docs/latest/sql-performance-tuning.html)

