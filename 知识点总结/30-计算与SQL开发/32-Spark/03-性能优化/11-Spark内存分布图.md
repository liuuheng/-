---
tags:
  - spark
  - memory
  - performance
  - spark-ui
status: active
---

# Spark Executors 内存指标与调参映射

这篇笔记解决一个具体问题：在 Spark History Server 的 Executors 页面看到某项内存指标偏高时，应该检查哪一部分内存，什么证据成立后才能调整对应参数。

结论先限定清楚：UI 指标与配置参数不是严格的一一对应关系。一个指标显示得大，只能说明该区域曾经使用较多内存；是否需要调参，还要同时检查配置上限、GC、Spill、Executor Loss Reason、Task 数据量和物理算子。

关联笔记：

- [[30-计算与SQL开发/32-Spark/03-性能优化/01-Spark内存、文件与性能排查|Spark 内存、文件与性能排查]]
- [[30-计算与SQL开发/32-Spark/01-执行模型与UI/03-Spark执行模型、Join与AQE|Spark 执行模型、Join 与 AQE]]

## 1. 在 Executors 页面看什么

进入 Spark History Server 的 Application，再打开 `Executors` 页面。在 `Show Additional Metrics` 中可以选择要展示的附加列：

![[99-images/Spark-Executors-Additional-Metrics.png]]

如果 UI 没有展示完整字段，可以读取 REST：

```text
/api/v1/applications/<application_id>/<attempt_id>/executors
```

每个 Executor 的 `peakMemoryMetrics` 中包含：

```json
{
  "JVMHeapMemory": 5771591488,
  "JVMOffHeapMemory": 149046528,
  "OnHeapExecutionMemory": 2264924160,
  "OffHeapExecutionMemory": 0,
  "OnHeapStorageMemory": 199052234,
  "OffHeapStorageMemory": 0,
  "OnHeapUnifiedMemory": 2463976394,
  "DirectPoolMemory": 20637018,
  "MappedPoolMemory": 0
}
```

这些值的单位是字节。换算 GiB 时除以 `1024³`。

## 2. 先建立内存包含关系

`spark.executor.memory` 配置的是 Executor JVM Heap 上限。Heap 内部不只有 Spark 的 Execution Memory 和 Storage Memory，还包括用户对象、反序列化对象、文件读取对象、算子元数据等未被 Spark 统一内存管理器直接统计的对象。

可以按下面的关系理解：

- `Peak JVM Memory OnHeap`：整个 JVM Heap 的使用峰值。
- `Peak Execution Memory OnHeap`：Heap 内被 Spark 记录为执行内存的部分。
- `Peak Storage Memory OnHeap`：Heap 内被 Spark 记录为存储内存的部分。
- User Memory 和其他 JVM 对象：属于 JVM Heap，但不会完整出现在 Execution 或 Storage 指标里。

因此不能计算：

```text
JVM Heap + Execution Memory + Storage Memory
```

Execution 和 Storage 本来就包含在 JVM Heap 中，相加会重复计算。

Executor 容器的总内存范围又比 JVM Heap 更大。它还可能包含：

- `spark.executor.memoryOverhead` 覆盖的 JVM 非堆和 Native 内存；
- Netty、Direct Buffer、线程栈和 JNI；
- 显式启用的 Spark managed off-heap；
- PySpark Worker、Arrow等进程或本地内存。

所以 `Java heap space` 和 YARN 的 `exceeding physical memory limits` 不是同一类问题。

## 3. UI 指标与参数怎么对应

| UI 指标 | 实际观察对象 | 主要关联参数 | 何时考虑调整 |
|---|---|---|---|
| `On Heap Memory` | 当前堆内 Storage 使用量和上限 | `spark.executor.memory`、统一内存配置 | 只适合观察当前缓存；任务结束后可能为 0，不能单独判断 Heap 是否浪费 |
| `Off Heap Memory` | 当前 Spark 堆外 Storage 使用量和上限 | `spark.memory.offHeap.enabled`、`spark.memory.offHeap.size` | 只有显式启用 Spark managed off-heap 时才有直接调参意义 |
| `Peak JVM Memory OnHeap` | 整个 Executor JVM Heap 峰值 | `spark.executor.memory`、`spark.executor.cores` | Heap 长期远低于上限才评估缩减；接近上限并伴随 Full GC/OOM 时评估扩容或降低并发 |
| `Peak JVM Memory OffHeap` | JVM 非堆区域峰值，如 Metaspace、Code Cache | `spark.executor.memoryOverhead` | 只有同时存在容器 RSS 压力或 YARN 物理内存超限时才增加 Overhead |
| `Peak Execution Memory OnHeap` | Sort、Aggregate、Join、Shuffle 等堆内执行内存峰值 | Shuffle/AQE 分区参数、`spark.executor.memory`、`spark.executor.cores` | 同时出现普遍 Spill、GC 或 OOM 时，先降低单 Task 数据量，再考虑增加 Heap 或降低每 Executor 并发 |
| `Peak Execution Memory OffHeap` | Spark managed off-heap 中的执行内存峰值 | `spark.memory.offHeap.enabled`、`spark.memory.offHeap.size` | 只在显式启用 off-heap 后调整，同时核对容器总内存预算 |
| `Peak Storage Memory OnHeap` | Cache、persist、广播块等堆内存储峰值 | 缓存策略、`spark.executor.memory`、`spark.memory.storageFraction` | 先检查缓存对象、复用次数和 `UNCACHE`；通常不先调整 `storageFraction` |
| `Peak Storage Memory OffHeap` | Spark managed off-heap 中的存储峰值 | `spark.memory.offHeap.enabled`、`spark.memory.offHeap.size` | 检查堆外缓存和块淘汰，同时核对容器 Overhead |
| `Peak Pool Memory Direct` | JVM NIO Direct Buffer 峰值 | `spark.executor.memoryOverhead` | Direct Memory OOM或容器RSS超限时处理；增加 JVM Heap 不能直接解决 |
| `Peak Pool Memory Mapped` | 内存映射文件缓冲峰值 | `spark.executor.memoryOverhead` | 只有它确实推高容器RSS时才调整，不凭单个数值下结论 |
| `Resources` | GPU、FPGA等自定义资源 | `spark.executor.resource.<name>.*` | 普通CPU SparkSQL任务通常为空 |
| `Resource Profile Id` | Executor采用的资源Profile编号 | Resource Profile配置 | 只说明配置归属，不是压力指标 |
| `Exec Loss Reason` | Executor退出或丢失原因 | 取决于退出类型 | Heap OOM看 Heap；容器物理内存超限看 Overhead；动态回收不需要加内存 |

## 4. 四类现象分别怎么处理

### 4.1 JVM Heap 接近上限

不能只看单个 Executor 的最大值。应统计所有 Executor 的中位数、P95和最大值，再检查：

- `Peak JVM Memory OnHeap` 与 `spark.executor.memory` 的距离；
- Major/Full GC次数和GC时间占比；
- 是否出现 `Java heap space`、GC overhead limit或Executor Lost；
- 高峰是否集中在少数Executor；
- 同一个Executor同时运行了多少Task。

判断方式：

- P95长期明显低于Heap上限，GC低、无OOM，才有缩减 `spark.executor.memory` 的空间。
- P95接近上限但GC低、无OOM，说明当前余量正在保护运行稳定，不能直接减内存。
- P95接近上限且Full GC/OOM明显，先定位是大分区、缓存、广播还是用户对象，再决定扩大Heap或减少并发Task。

降低 `spark.executor.cores` 可以减少同一个 JVM 内并发Task之间的竞争，但会增加Executor进程数量、广播副本和JVM固定开销，因此不能把 `memory / cores` 当作固定的单Task配额。

### 4.2 Execution Memory 高并伴随 Spill

Execution Memory高并不自动等于配置错误。Sort、HashAggregate和Join本来就会使用执行内存。需要同时看到大量成功Task持续Spill，才说明单Task工作集普遍偏大。

处理顺序：

1. 检查是否只有少数倾斜Task Spill。
2. 检查AQE最终读取分区的实际大小，而不是只看初始Shuffle分区数。
3. 降低单Task数据量，调整AQE目标分区或Shuffle分区。
4. 检查Join和聚合策略是否制造了不必要的大工作集。
5. 仍然不足时，再评估增加 `spark.executor.memory` 或减少 `spark.executor.cores`。

常见关联参数：

```properties
spark.sql.adaptive.advisoryPartitionSizeInBytes
spark.sql.adaptive.coalescePartitions.minPartitionSize
spark.sql.adaptive.coalescePartitions.initialPartitionNum
spark.sql.shuffle.partitions
spark.executor.memory
spark.executor.cores
```

`advisoryPartitionSizeInBytes` 是目标值，不是最终分区大小的严格上限。必须从实际 `AQEShuffleRead` 和Task指标确认是否生效。

### 4.3 Storage Memory 高

Storage Memory主要来自缓存和持久化块。看到它高时，先回答：

- 缓存结果是否被多次消费；
- 是否错误缓存了原始大表，而不是聚合后的较小结果；
- 是否执行了 `UNCACHE TABLE` 或 `unpersist()`；
- 是否出现缓存块逐出和重复计算；
- 广播关系的实际大小是多少。

`spark.memory.storageFraction` 表示统一内存区域中受保护的Storage区域，不是缓存硬上限。直接增大它可能压缩Execution的可用空间，让Shuffle和聚合更容易Spill。

### 4.4 OffHeap、Direct Pool或容器RSS高

如果日志没有JVM OOM，但YARN报告容器超过物理内存，才重点检查：

```properties
spark.executor.memoryOverhead
spark.executor.memoryOverheadFactor
```

还要检查PySpark、Arrow、Netty、Direct Buffer和Native库。`Peak JVM Memory OffHeap`、`DirectPoolMemory`和`MappedPoolMemory`只覆盖各自统计范围，不能代表容器全部RSS。

增加 `spark.executor.memory` 只扩大JVM Heap，不能直接解决Direct Memory或容器RSS超限。

## 5. `maxExecutors` 与单Executor内存不是一回事

`spark.dynamicAllocation.maxExecutors` 限定Application最多申请多少个Executor。Spark根据积压申请资源，YARN根据队列容量决定实际分配数量。它主要改变同时可运行的Task数量和作业时长，不直接说明单Executor内存是否足够。

因此要分开判断：

- 想控制任务峰值并发：观察动态资源和队列竞争，再考虑Executor数量上限。
- 想判断单Executor是否浪费内存：观察所有Executor的Heap峰值分布、GC、Spill和退出原因。

定时任务运行在共享集群时，不能只追求单任务最快。还要比较峰值Executor/Core、Executor-core-seconds以及运行时间变长后与其他定时任务的重叠窗口。

## 6. 本次真实任务怎么判断

案例：

- `application_1783495977342_2273138`
- `application_1783495977342_2274082`
- Spark `3.2.2-dt`
- `spark.executor.memory=8g`
- `spark.executor.cores=2`
- 50个Executor

两个同业务日Application的Executor峰值分布接近：

| 指标 | `2273138` | `2274082` |
|---|---:|---:|
| JVM Heap峰值中位数 | 5.68 GiB | 5.69 GiB |
| JVM Heap峰值P95 | 7.10 GiB | 6.99 GiB |
| JVM Heap峰值最大值 | 7.19 GiB | 7.16 GiB |
| Execution Memory峰值P95 | 2.75 GiB | 2.69 GiB |
| Unified Memory峰值P95 | 2.81 GiB | 2.74 GiB |
| GC时间占比 | 2.97% | 2.49% |
| Memory/Disk Spill | 0 | 0 |

这组证据支持以下判断：

- 8 GiB Heap提供约0.8～1.0 GiB峰值余量，当前运行稳定。
- 直接降到7 GiB已经低于观测到的Heap P95和最大值，可能增加GC或OOM风险。
- 没有Spill且GC占比低，只能说明当前配置运行稳定，不能说明Heap大量闲置。
- Storage峰值较低也不能推出Heap浪费，因为JVM Heap还包含Execution、User Memory和其他对象。
- 当前不调整 `spark.executor.memory=8g`、`spark.executor.cores=2` 和 `spark.dynamicAllocation.maxExecutors=50`。

这里的结论只适用于这两个Application及其当前SQL、资源和输入规模。数据增长、Join策略、缓存或Executor cores变化后，需要重新采集峰值分布。

## 7. 一次调参前的检查清单

判断 Executor 内存前，至少记录：

- Spark版本、Application和Attempt；
- `spark.executor.memory`、`spark.executor.cores`和memory overhead；
- 所有Executor的JVM Heap峰值中位数、P95和最大值；
- Execution、Storage、Direct和Mapped峰值；
- GC时间与Major GC；
- Memory/Disk Spill的Task数量和分布；
- Executor Loss Reason和YARN容器退出原因；
- Task输入、Shuffle Read、分区大小和长尾；
- 缓存与广播的实际大小。

只有这些条件形成同一条证据链，才能决定应该调整Heap、Overhead、分区、缓存还是每Executor并发。

## 参考依据

- [[30-计算与SQL开发/32-Spark/03-性能优化/01-Spark内存、文件与性能排查|Spark 内存、文件与性能排查]]
- 当前案例本地记录：`spark-runtime-model/real-runs/application_1783495977342_2273138/`、`spark-runtime-model/real-runs/application_1783495977342_2274082/`
