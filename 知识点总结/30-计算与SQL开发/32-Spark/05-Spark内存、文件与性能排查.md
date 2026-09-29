---
tags:
  - spark
  - memory
  - performance
status: active
---

# Spark 内存、文件与性能排查

返回：[[README|企业微信知识地图]]

## 1. 先区分进程预算与 Spark 管理内存

Executor 容器总内存不只有 `spark.executor.memory`。还包含 memory overhead、直接内存、线程栈、Netty、Python worker、Arrow、JNI 等。开启 Spark managed off-heap 后，`spark.memory.offHeap.size` 也要纳入容器总预算。

因此“Java heap space”和“容器被 OOMKilled”是不同问题：

- JVM 抛 `Java heap space`：Executor/Driver 堆内不足或对象膨胀。
- `Direct buffer memory`/native OOM：直接内存或本地内存不足。
- 日志没有 JVM OOM，但 YARN/K8s 报容器超限：总 RSS 超过容器额度，常与 memoryOverhead、Python/Arrow 或网络缓冲相关。

不能看到 Executor Lost 就立即增加 heap；先查 NodeManager/Pod 退出原因。

## 2. Execution、Storage 与 User Memory

Spark 统一内存管理中，Execution Memory 用于 shuffle、sort、聚合和 join 等临时结构；Storage Memory 用于 cache 和持久化块，两者可在统一区域内借用/竞争。User Memory 是 JVM 堆中未被 Spark 管理器直接控制的对象，例如 UDF 创建的大集合。

Execution 数据常可以 spill 到磁盘，但 spill 不是无限保险：单条记录过大、Hash 表峰值、磁盘不足或 User Memory 对象都可能使任务仍然 OOM。

## 3. 常见 OOM 分类

### 3.1 执行内存 OOM

常见于大分区 join、group by、sort、window、distinct。处理优先级：确认是否倾斜 → 降低单分区数据量 → 改 Join/聚合策略 → 确认 spill 磁盘 → 最后再增加内存。

### 3.2 User Memory OOM

UDF 或 `mapPartitions` 把整个分区放进 List/Map，Spark 无法自动 spill 用户容器。应改成流式处理、限制批大小或使用 Spark 原生表达式。

### 3.3 Driver OOM

常见于 `collect()`、`toPandas()`、过大广播收集或结果元数据过多。增加 Driver 内存只能缓解，根因通常是把分布式数据拉回单机。

### 3.4 Storage/缓存压力

缓存过多会挤占内存并触发逐出或重算。缓存不一定报 OOM，但可能让执行内存频繁 spill、GC 增加，作业反而变慢。

### 3.5 容器总内存 OOM

PySpark、Arrow、Netty 和本地库不完全计入 JVM heap。需要提高 memoryOverhead 或减少同 Executor 并发，而不是只增加 `executor.memory`。

## 4. Spill 指标怎么读

- `Spill (Memory)` 是被溢写数据在内存中的估算体积。
- `Spill (Disk)` 是序列化/压缩后真正写盘的体积。

因此 Memory Spill 大于 Disk Spill 很正常。前者反映内存压力，后者反映磁盘 I/O。大量 spill 不等于作业一定失败，但说明分区过大或算子内存峰值高，应结合 Task duration、GC、input 和 shuffle read 判断。

## 5. 临时视图与缓存不是一回事

`CREATE TEMP VIEW` 或 `createOrReplaceTempView` 只注册逻辑计划，不会自动把整表放入内存。每次 action 仍按血缘计算。

`CACHE TABLE`/`df.cache()` 在第一次 action 时物化数据，之后复用缓存。Spark SQL 通常采用列式缓存并只扫描需要的列，但缓存有物化和内存成本。只有数据被多次复用且重算昂贵时才值得缓存，用完应 `UNCACHE TABLE` 或 `unpersist()`。

不要把 `CACHE TABLE ...; UNCACHE TABLE ...;` 连续写在没有复用的同一流程中，否则可能只增加开销。

## 6. 输入文件如何组成 Task

`spark.sql.files.maxPartitionBytes` 控制文件源扫描分区的目标最大字节量，`openCostInBytes` 把“打开一个文件”的固定开销折算成虚拟字节，影响多个小文件如何装箱。

简化理解：Spark 尝试让每个 FilePartition 的“文件字节 + 打开成本”不超过目标。但以下情况会使直觉不精确：

- 文件格式是否可切分。
- 文件被压缩后的大小与实际读取列大小不同。
- 列裁剪和谓词下推使 Spark UI Input Size 小于文件总大小。
- 单个文件大于阈值时是否可切分取决于格式、压缩和 reader。

因此原测试中 63MB 文件在某些配置下一个文件一个 Task，是装箱和 open cost 共同作用的结果，不能概括成“文件超过 64MB 就绝对不能合并”。

## 7. 如何选择 maxPartitionBytes

目标是在单 Task 数据量和 Task 数量之间平衡：

- 太大：Task 少但单 Task 输出、内存和 spill 峰值高，长尾明显。
- 太小：Task 数量巨大，调度、文件打开和任务启动开销上升。

正确方法是用同一份数据测试 128/256/512MB 等候选值，比较 Stage 总耗时、Task 中位数与 P95、GC、spill、input/output，而不是只看单 Task 是否更快。聊天记录中的实测显示 256MB 和 512MB 总耗时相近、128MB 任务更多且更慢，这说明瓶颈未必只是单 Task 大小。

## 8. 小文件问题的本质

小文件问题是“一个分区目录内有大量很小的文件”，会增加元数据、文件打开和 Task 调度成本。一个分区只有一个小文件不属于典型小文件爆炸，虽然存储利用率可能不高。

合并小文件不是在 HDFS 上把字节直接拼接，而是重新读取记录，`repartition/coalesce` 后覆盖写回。覆盖前必须验证动态分区配置和并发写入策略，避免多个任务同时覆盖同一目录导致丢数。

## 9. repartition、coalesce 与 Hint

- `repartition(n)` 可增可减分区并执行全量 Shuffle，分布更均衡。
- `coalesce(n)` 主要用于减少分区，通常避免全量 Shuffle，但可能让少数分区过大；不能笼统写成任何情况下都不 Shuffle。
- `REPARTITION(col)` 按列重新 hash 分区。
- `REPARTITION_BY_RANGE(col)` 按范围分区，适合范围排序或区间处理。
- `REBALANCE` 依赖版本和 AQE，用于让输出分区更均衡。

输出文件数通常接近输出 partition 数，但空分区、动态分区、提交协议和并发写入会使两者不严格相等。

## 10. Window 与 Group By 的性能

窗口函数保留明细行，通常需要按 partition key Shuffle；带窗口 ORDER BY 时还需要分区内排序。GROUP BY 折叠行数，常使用 HashAggregate 并可 map-side partial aggregate。

但“GROUP BY + JOIN 一定比 Window 快”不成立：

- 多个窗口共享同一 partition/order 时，排序可能复用。
- GROUP BY 后 join 会增加一次扫描和一次 join。
- 聚合结果很小且能广播时，GROUP BY + JOIN 可能更优。
- 数据倾斜会改变两者成本。

应比较物理计划的 Exchange、Sort、Aggregate、Join 数量以及运行指标。

## 11. Shuffle UI 指标

Shuffle Write/Read 通常展示序列化和压缩后的网络/磁盘字节。Shuffle Read 可能与上游 Write 不完全相等，原因包括本地读取、重试、压缩统计口径和 AQE 分区合并。不能简单用两者差值判定丢数据。

## 12. 动态资源分配的理解

`spark.dynamicAllocation.maxExecutors` 只设上限；实际 Executor 数还受初始/最小值、积压超时、空闲回收、外部 Shuffle Service 或 Shuffle Tracking、队列资源影响。把 `executorAllocationRatio` 调低会限制扩容积极性，可能节省资源但增加排队时间。

调整参数前应问：瓶颈是没有足够并发，还是少数超大 Task？如果是倾斜，再多 Executor 也无法加速那一个热点 partition。

## 13. OOM 排查顺序

1. 确认失败在 Driver、Executor JVM 还是容器层。
2. 找到失败 Stage 和算子，查看是否 join/sort/aggregate/UDF。
3. 比较 Task 数据量与长尾，确认是否倾斜。
4. 看 GC、Peak Execution Memory、Spill、Shuffle 和磁盘错误。
5. 检查缓存、广播、collect、Arrow/Python 使用。
6. 先优化分区和算法，再按测得的峰值调整 heap、off-heap 或 overhead。

## 14. 容易误用的参数

- `spark.network.timeout` 解决等待与心跳问题，不直接解决内存不足。
- `spark.sql.broadcastTimeout` 只改变广播等待时间，不降低广播数据大小。
- 增大 `spark.sql.shuffle.partitions` 可降低单分区数据量，但任务开销会增加。
- `spark.memory.offHeap.enabled=true` 不是免费增加内存；off-heap 仍占容器 RSS，并需要设置 size 和总资源预算。
- `spark.speculation` 能复制慢 Task 尝试，适合偶发慢节点，不解决确定性的热点 key。

## 参考依据

- [Spark SQL 性能调优](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Spark RDD 持久化与 Shuffle](https://spark.apache.org/docs/4.0.1/rdd-programming-guide.html)
