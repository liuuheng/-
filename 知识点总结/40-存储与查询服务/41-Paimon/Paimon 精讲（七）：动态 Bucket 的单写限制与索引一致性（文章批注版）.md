---
title: "Paimon 精讲（七）：动态 Bucket 的单写限制与索引一致性（文章批注版）"
aliases:
  - "Paimon 精讲（七）：Paimon动态Bucket踩坑：主键表为什么只能单写？"
author: "胖泽的技术笔记"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s/PDZA6b1FPtprbbo1LtSs1Q"
published_at: "2026-08-04"
reviewed_at: "2026-10-14"
tags:
  - Flink
  - Paimon
  - Paimon-精讲
---

# Paimon 精讲（七）：动态 Bucket 的单写限制与索引一致性（文章批注版）

> [!info] 来源、整理方式与版本边界
> 原文：[《Paimon 精讲（七）：Paimon动态Bucket踩坑：主键表为什么只能单写？》](https://mp.weixin.qq.com/s/PDZA6b1FPtprbbo1LtSs1Q)，胖泽的技术笔记，2026-08-04。正文按原文章节顺序保留技术内容、代码、表格、字段和案例；新增内容统一标为“批注”“勘误”或“版本边界”。原文没有声明准确的 Paimon/Flink 版本，内部类、默认值和源码路径不视为稳定 API。

> [!warning] 勘误：当前官方边界是“同一分区单写”，不是整张表绝对单写
> Dynamic Bucket 模式下，同一分区只能由一个 Job 写入；互不重叠的分区可以规划为独立写入范围。多个 Job 同时给同一分区分配 Key，可能独立产生冲突映射并造成重复。
> `write-only=true` 配独立 Compaction 只能隔离 Compaction 资源，不能解除 Dynamic Bucket 的同分区并发写限制。
## 引言

动态 Bucket 是 Paimon 解决数据倾斜的一大利器：不需要预先指定桶数，写入时自动扩缩，避免了固定 Bucket 模式下某些桶被撑爆的问题。但很多人用上动态 Bucket 之后，发现了一个奇怪的限制——同一张表在同一时刻只允许一个 writer 写入。

这个限制与性能无关，而是**数据质量**的考量。动态 Bucket 依靠索引维护 key 到 bucket 的映射，如果允许多个 writer 并发，索引的一致性难以保障，可能导致数据错乱或丢失。单写正是为了守住这条质量红线。

当然，代价也很明显：写入吞吐受限，跨分区场景下启动时还需扫描全表 key 初始化索引，大数据量表性能损耗显著。

本文从索引机制出发，拆解单写限制的底层原理，帮你理解它为什么存在、代价是什么。

## 核心结论

主键 Paimon 表在动态 bucket（`bucket=-1`）模式下：

1. **能保证相同主键写入同一个 bucket**
2. **但只能有一个写入作业（Job），不支持多 Job 并发写入**

---

## 一、动态 Bucket 的两种模式


> [!note] 批注：两种索引路径解决的问题不同
> 主键包含分区列时，映射范围通常局限在分区内；主键排除可变分区列时，需要 Cross-Partition Upsert 索引定位旧分区和 Bucket。后者的启动扫描、状态大小和本地磁盘成本更高，且具体行为还受 Merge Engine 影响。
`bucket=-1` 在 Paimon 内部对应`BucketMode` 枚举的两种不同模式（源码：`BucketMode.java`）：

| 模式 | 触发条件 | 索引存储位置 | 多写支持 |
| --- | --- | --- | --- |
|`HASH_DYNAMIC` | 主键包含所有分区键 | Hash Index 文件（HDFS/OSS 等） | **不支持** |
|`CROSS_PARTITION` | 主键不包含所有分区键（跨分区 upsert） | RocksDB（本地磁盘） | **不支持** |

---

## 二、相同主键如何保证写入同一 Bucket

### 整体拓扑（以 Flink Sink 为例）


```
输入数据
  → [阶段1] 按主键 Hash 做 Shuffle（相同主键路由到同一个 Assigner subtask）
  → [阶段2] HashBucketAssignerOperator（查询/更新 Hash 索引，决定 bucket）
  → [阶段3] 按 partition + bucket 再次 Shuffle
  → Writer → Committer

```


源码：`DynamicBucketSink.java:75-117`

### 关键机制一：阶段1 按主键 Hash Shuffle


```
// DynamicBucketSink.java:90
DataStream<T> partitionByKeyHash =
    partition(input, assignerChannelComputer(numAssigners), assignerParallelism);

```


相同主键的记录通过`assignerChannelComputer` 路由到**同一个 Assigner 并发实例**，这是整个机制的核心前提：保证同一主键只由一个 Assigner subtask 处理，避免并发冲突。

### 关键机制二：内存 hash2Bucket 映射

`PartitionIndex.assign()` 的逻辑（源码：`PartitionIndex.java:70-123`）：


```
public int assign(int hash, IntPredicate bucketFilter, ...) {
    // 1. 该 hash 之前出现过 → 直接返回已记录的 bucket（同主键同 bucket）
    if (hash2Bucket.containsKey(hash)) {
        return hash2Bucket.get(hash);
    }
    // 2. 新主键 → 找一个未满的 bucket 分配，并记录映射
    // 3. 所有 bucket 都满 → 新建 bucket
}

```


`hash2Bucket`（`Int2ShortHashMap`）记录了`主键hash → bucket` 的映射，同一个 hash 必然返回同一个 bucket。

### 关键机制三：Hash Index 文件持久化（HASH_DYNAMIC 模式）

> [!note] 补充批注：扩容不会自动拆散已经形成的热点键
> 持久化 Hash Index 保存既有主键到 Bucket 的映射；新增 Bucket 主要承接后续尚未建立映射的新键。若热点来自少数既有主键，单纯增加 Bucket 数并不能把这些键重新均摊。

Assigner 初始化时从存储加载历史索引（源码：`PartitionIndex.loadIndex`）：


```
// 从 IndexManifest 扫描当前 partition 的所有 hash index 文件
List<IndexManifestEntry> files = indexFileHandler.scanEntries(HASH_INDEX, partition);
// 将历史 hash → bucket 映射加载到内存
mapBuilder.put(hash, (short) file.bucket());

```


每次 Checkpoint 时将更新写回存储（源码：`HashIndexMaintainer.prepareCommit`），保证重启后一致性。

### 关键机制四：RocksDB 全局索引（CROSS_PARTITION 模式）

> [!warning] 补充批注：索引 TTL 会改变跨分区更新的正确性边界
> `cross-partition-upsert.index-ttl` 可以降低状态规模，但旧键位置过期后，系统可能无法定位其原分区。启用 TTL 前必须确认业务不会在 TTL 之后再次更新旧键，或可以接受相应语义变化。

使用`GlobalIndexAssigner`，以 RocksDB 在本地磁盘维护全量`key → (partition, bucket)` 映射：


```
// GlobalIndexAssigner.java:253
PositiveIntInt partitionBucket = keyIndex.get(key);
if (partitionBucket != null) {
    // 找到历史记录 → 发到历史 bucket（并可能发 UPDATE_BEFORE 到旧分区）
    collect(value, previousBucket);
} else {
    // 新主键 → 分配新 bucket 并写入 RocksDB
    processNewRecord(partition, partId, key, value);
}

```


Job 启动时会读取表中所有已有 key 来初始化 RocksDB 索引（bootstrap 阶段）。

---

## 三、为什么不支持多 Job 并发写入


> [!warning] 勘误：把“多 Job”补全为“多个 Job 写同一分区”
> 官方限制的判断单位是分区。生产上可以让实时任务写当前分区、批任务覆盖不相交的历史分区，但仍要检查 Snapshot 发布竞争、覆盖范围和所用存储/Catalog 的原子提交机制。
### HASH_DYNAMIC 模式

每个写入 Job 的各个 Assigner subtask 在内存中各自维护`hash2Bucket` 映射，不同 Job 之间**互不感知**对方已分配的 bucket。
后果：同一主键在两个 Job 中可能被分配到不同的 bucket，破坏主键唯一性约束。

### CROSS_PARTITION 模式

RocksDB 是 **本地磁盘状态**，每个 Job 有独立的 RocksDB 实例。
Job 启动的 bootstrap 阶段只读取一次历史存量数据，之后两个 Job 的写入互不感知。
源码中明确报错信息（`GlobalIndexAssigner.java:222`）：


```
same dynamic bucket table, it only supports single write

```


---

## 四、单 Job 内的并发机制

虽然不支持多 Job，但**单个 Job 内部可以多并发**（多个 subtask）：

- 阶段1 的 Shuffle 保证：相同主键 → 同一个 Assigner subtask
- 每个 Assigner subtask 独立负责一部分 bucket（通过`assignId % numAssigners` 分片）
- 因此多个 subtask 之间处理的 hash 值天然不重叠，不存在竞争

---

## 五、批写（INSERT OVERWRITE）的特殊处理


> [!warning] 操作边界
> `INSERT OVERWRITE` 会替换目标范围内的表状态，不应与写同一分区的流任务并行。即使分桶分配器不读取历史 Dynamic Index，也不能据此推导覆盖提交与流式追加天然兼容。
批写场景使用`SimpleHashBucketAssigner`，不依赖历史 Hash Index 文件，直接在内存中构建新的`hash2Bucket` 映射。
虽然相对宽松，但**也不建议与流写作业并行运行**，存在数据一致性风险。

---

## 六、使用建议

- 动态 bucket 主键表，**生产环境只允许一个 Flink 写入作业同时运行**
- 若需要多数据源写入同一张表，建议通过 CDC 汇聚后由单个 Job 写入
- 需要跨分区 upsert（如用分区字段做时间分区、主键不含分区键）时，使用`CROSS_PARTITION` 模式，注意 RocksDB 的本地磁盘和内存开销

---

## 本篇结论

动态 Bucket 的单写限制，本质是**用串行化保证索引一致性**，是数据质量的防线，而非性能瓶颈。

代价也很清楚：写入吞吐受限，跨分区场景下启动初始化慢。

理解这一点，你就能做出更理性的选型判断——**没有完美的模式，只有适合你业务特征的取舍。**

## 复核资料

- [Primary-Key Data Distribution](https://paimon.apache.org/docs/master/primary-key-table/data-distribution/)
- [Concurrency Control](https://paimon.apache.org/docs/master/concepts/concurrency-control/)
- [Primary-Key Compaction](https://paimon.apache.org/docs/master/primary-key-table/compaction/)

## 系列导航

- 上一篇：[[Paimon 精讲（六）：File Index 的存储与读写链路（文章批注版）]]
- 下一篇：[[Paimon 精讲（八）：查询裁剪、统计信息与 Files Table（文章批注版）]]
