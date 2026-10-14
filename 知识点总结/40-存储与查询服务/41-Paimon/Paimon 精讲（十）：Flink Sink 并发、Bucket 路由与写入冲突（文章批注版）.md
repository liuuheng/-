---
title: "Paimon 精讲（十）：Flink Sink 并发、Bucket 路由与写入冲突（文章批注版）"
aliases:
  - "Paimon 精讲（十）：Flink并发数超过Bucket数会怎样？原来如此"
author: "胖泽的技术笔记"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s/po4y9l0ZNtowdHKgVrfvvA"
published_at: "2026-09-03"
reviewed_at: "2026-10-14"
tags:
  - Flink
  - Paimon
  - Paimon-精讲
---

# Paimon 精讲（十）：Flink Sink 并发、Bucket 路由与写入冲突（文章批注版）

> [!info] 来源、整理方式与版本边界
> 原文：[《Paimon 精讲（十）：Flink并发数超过Bucket数会怎样？原来如此》](https://mp.weixin.qq.com/s/po4y9l0ZNtowdHKgVrfvvA)，胖泽的技术笔记，2026-09-03。正文按原文章节顺序保留技术内容、代码、表格、字段和案例；新增内容统一标为“批注”“勘误”或“版本边界”。原文没有声明准确的 Paimon/Flink 版本，内部类、默认值和源码路径不视为稳定 API。

> [!warning] 批注：本文结论绑定固定 Bucket 与特定 Connector 源码
> Writer 并行度调整、分区哈希和 Channel 路由属于 Flink Connector 内部实现，版本升级后可能变化。先用目标版本的 `EXPLAIN`、JobGraph 和 TaskManager 日志确认实际 Writer 并行度，不能把示例中的 512/800 当成通用阈值。
> Bucket 数不是通用并行度上限。当前官方建议先按每 Bucket 约 200 MB～1 GB 估算 MOR 布局，再用热点、小文件、Compaction 和查询指标校准。
## 引言

在生产环境中，为了追求更高的写入吞吐，我们经常会把 Flink 作业的 sink 并行度调到一个比较大的值，比如 800。可 Paimon 主键表默认的 bucket 数往往只有 512，甚至更小。于是问题就来了：当写入并发（800）明显大于 bucket 数（512）时，到底会发生什么？

很多人心里都会冒出一连串问号：

- 这会不会直接报错，作业根本起不来？
- 一个 bucket 被多个 writer 同时写，数据会不会冲突、写错？
- Paimon 不是保证"一个 bucket 只由一个 writer 单写"吗？现在并发比 bucket 还多，这个语义是不是被破坏了？
- 既然不会冲突，那是不是可以放心大胆地堆并发？有没有什么隐藏代价？

本文就围绕 bucket=512、write-only=true、写入并发=800 这个典型场景，从源码层面把上面的疑问逐一拆解：先讲清楚数据到底是怎么路由到 writer subtask 的（无分区表和有分区表行为完全不同），再论证为什么"多 writer 写同一个 bucket"其实不会冲突，最后说清 write-only=true 在此场景下真正的代价是什么、又该如何应对。

结论先行：不会报错、不会写错、不会并发冲突，但有一个长期运行必须正视的隐患。

---

## 一、Sink 并发与 Bucket 数的关系

### 路由逻辑

Flink Sink 通过`FlinkStreamPartitioner` +`ChannelComputer` 将数据路由到下游 writer subtask。对于固定 bucket 主键表，路由由`RowDataChannelComputer` 完成（`paimon-flink-common`）。每条记录先通过 bucket-key（默认为主键）的  hash 计算出`bucket_id`（取值范围`0~511`），再由下面两个`select` 方法之一映射到 writer channel（即 sink subtask 编号）：


```
// ChannelComputer.java
// 有分区表
static int select(BinaryRow partition, int bucket, int numChannels) {
    int startChannel = Math.abs(partition.hashCode()) % numChannels;
    return (startChannel + bucket) % numChannels;
}

// 无分区表
static int select(int bucket, int numChannels) {
    return bucket % numChannels;
}

```

> 关键参数：`numChannels` = sink writer 的并行度（即 writer subtask 数量），本文场景`numChannels = 800`，writer subtask 编号`0~799`；`bucket` = bucket-key（默认为主键）哈希算出的 bucketId，范围`0~511`。

两类表的路由差异巨大，下面分别用`numChannels=800` 完整演算。

### 场景 A：无分区表 ——`select(bucket, 800)`公式：`channel = bucket % 800`。由于 bucketId 最大才 511，永远小于 800，取模结果就等于 bucketId 本身：

| bucketId | 计算 bucket % 800 | 目标 writer channel(subtask) |
| --- | --- | --- |
| 0 | 0 % 800 = 0 | 0 |
| 1 | 1 % 800 = 1 | 1 |
| 100 | 100 % 800 = 100 | 100 |
| 511 | 511 % 800 = 511 | 511 |
| —— | channel 512~799 数学上取不到 | **subtask 512~799 全程空闲** |

👉 结论：bucketId 只能映射到`0~511` 这 512 个 channel，**subtask 512~799（共 288 个）永远收不到数据，完全空闲**。这是"写并发 > bucket 数"在无分区表上最直接的资源浪费，也是下文自动限流要把 800 降回 512 的根本原因。

### 场景 B：有分区表 ——`select(partition, bucket, 800)`公式分两步：

1.`startChannel = Math.abs(partition.hashCode()) % 800`（按分区打散，范围`0~799`）
2.`channel = (startChannel + bucket) % 800`

关键差异：`startChannel` 随分区变化，能把**不同分区的同一个 bucketId 打散到不同 subtask**。设三个分区及其 hashCode：

| 分区 | partition.hashCode() | startChannel = abs(hash) % 800 |
| --- | --- | --- |
| dt='2026-08-26' | 1234 | 1234 % 800 = 434 |
| dt='2026-08-27' | 2001 | 2001 % 800 = 401 |
| dt='2026-08-28' | 3500 | 3500 % 800 = 300 |

代入第二步`channel = (startChannel + bucket) % 800`（注意 startChannel + bucket 可能 ≥ 800，需取模回绕）：

**分区 dt='2026-08-26'，startChannel=434**

| bucketId | 计算 (434 + bucket) % 800 | 目标 writer channel |
| --- | --- | --- |
| 0 | (434+0) % 800 = 434 | 434 |
| 100 | (434+100) % 800 = 534 | 534 |
| 365 | (434+365) % 800 = 799 | 799 |
| 366 | (434+366) % 800 = 0 | 0（回绕） |
| 511 | (434+511) % 800 = 145 | 145 |

**分区 dt='2026-08-27'，startChannel=401**

| bucketId | 计算 (401 + bucket) % 800 | 目标 writer channel |
| --- | --- | --- |
| 0 | (401+0) % 800 = 401 | 401 |
| 511 | (401+511) % 800 = 112 | 112 |

**分区 dt='2026-08-28'，startChannel=300**

| bucketId | 计算 (300 + bucket) % 800 | 目标 writer channel |
| --- | --- | --- |
| 0 | (300+0) % 800 = 300 | 300 |
| 511 | (300+511) % 800 = 11 | 11 |

### 关键现象：同一 bucketId、不同分区 → 不同 writer

> [!note] 补充批注：分区参与路由只能分散不同分区的数据
> 如果作业同一时刻只有少量活跃分区，或热点集中在一个分区内，提高 Sink 并发仍可能出现部分 Task 空闲、部分 Task 过载。并发设置应按活跃的 `partition × bucket` 组合数和倾斜分布验证。

以 bucketId=0 为例，对比三个分区：

| 分区 | startChannel | channel | 落到哪个 writer |
| --- | --- | --- | --- |
| dt=2026-08-26 | 434 | 434 | subtask-434 |
| dt=2026-08-27 | 401 | 401 | subtask-401 |
| dt=2026-08-28 | 300 | 300 | subtask-300 |

👉 **同一个 bucketId=0，被三个分区分别发到 subtask-434 / 401 / 300 三个不同的 writer。** 这正是后文「自动限流」表里说的"有分区表同一 bucket 可能被多个 writer 写入"——但要注意：这里的"多写"指的是**同一 bucketId 跨不同分区**被不同 writer 写，每个 writer 写的是各自分区下的文件；对"同一个分区+bucket"组合而言，startChannel 固定、channel 唯一，**仍然只有一个 writer 在写，bucket 单写语义没有被破坏**。

### 为什么有分区表不触发自动限流？

把 bucketId=0 代入公式：`channel = (startChannel + 0) % 800 = startChannel`，而`startChannel` 范围是`0~799`。也就是说，**只要分区足够多、hashCode 分布够散，单是 bucketId=0 这一个桶就能覆盖到全部 800 个 channel**，其余 511 个桶同理。因此有分区表的 800 个 subtask 不存在「数学上永远到不了」的 channel（对比无分区表 512~799 永远到不了），也就没有把并发从 800 降回 512 的理由。代价是：同一 bucketId 被拆给多个 writer，每个 writer 各自产 Level-0 文件，并发越高、小文件越多（详见后文）。

### 自动限流：仅对无分区表生效

`FlinkSinkBuilder.buildForFixedBucket()`（`:297`）有一段保护逻辑：


```
int bucketNums = table.bucketSpec().getNumBuckets();  // 512
if (parallelism == null
        && bucketNums < input.getParallelism()         // 512 < 800，成立
        && table.partitionKeys().isEmpty()) {           // 无分区键时才触发
    LOG.warn("...parallelism of writerOperator will be set to bucketNums.");
    parallelism = bucketNums;                          // 强制降为 512
}

```


| 表类型 | 行为 |
| --- | --- |
| **无分区表** | Writer 并发自动从 800 降为 512，打印 WARNING |
| **有分区表** | 无自动限制，Writer 并发保持 800，同一 bucket 可能被多个 writer 写入 |

---

## 二、多个 Writer 写同一个 Bucket 会冲突吗？


> [!warning] 勘误：“不会冲突”和“不可能触发”过于绝对
> Paimon 使用乐观并发控制：Writer 独立准备文件，提交时针对最新 Snapshot 验证。纯 ADD 文件通常容易重试合并，但 Snapshot 发布竞争、Compaction/Overwrite 产生的 DELETE、同 Level 非法重叠、锁配置和存储原子性仍可能导致提交失败或作业重启。
### 结论：不会冲突

**原因**：每个 writer subtask 在本地内存中独立维护自己的 MemTable，flush 时生成**全新的独立文件**（文件名含 UUID），commit 时产生的全部是`ADD` 操作，**不产生任何`DELETE`**。

Paimon 的两种并发冲突触发前提都依赖`DELETE` 操作的存在：

### Paimon 并发控制中的两种冲突

### 1 . Snapshot Conflict（快照冲突）—— 可自动恢复

**触发条件**：两个作业同时抢占同一个 snapshot ID。

**处理**：`tryCommit` 的 while 循环自动重试，递增 snapshot ID 重新提交，业务无感知。

### 2 . Files Conflict（文件冲突）—— 必须 failover 重启

> [!warning] 补充批注：“必须 failover”不是脱离上下文的通用结论
> 冲突能否在提交层重试，取决于冲突类型、并发操作、锁配置、存储一致性和当前版本实现。实际排障应先读取 Commit 异常及同时运行的写入、Compaction、Overwrite 操作，再决定重试作业还是消除并发写入。

**触发条件**：作业 A 想 DELETE 某个文件，但该文件已被作业 B 的 Compaction 提前 DELETE，导致 manifest 无法合并。

**触发路径**：


```
作业A（写入）              作业B（Compaction）
  |                           |
  | 读 snapshot-3             |
  | 生成 file-x (L0)          |
  |                           | 读 snapshot-3
  |                           | 合并 file-x → file-y (L1)
  |                           | 提交 snapshot-4：
  |                           |   ADD file-y, DELETE file-x ✓
  |                           |
  | 触发自身 Compaction        |
  | 也要 DELETE file-x        |
  | 提交 snapshot-5：          |
  |   DELETE file-x ← 已不存在！⚡ FILES CONFLICT

```


**检测代码**（`FileStoreCommitImpl.java:1290`）：


```
private void assertNoDelete(Collection<SimpleFileEntry> mergedEntries, ...) {
    for (SimpleFileEntry entry : mergedEntries) {
        // merge 后净结果是 DELETE → 即删除一个不存在的文件 → 报错
        Preconditions.checkState(entry.kind() != FileKind.DELETE, ...);
    }
}

```


**LSM 冲突检测**（`FileStoreCommitImpl.java:1264`）仅检查 level ≥ 1 的文件的 key range 重叠，**Level-0 文件不做此检测**，天然允许 key range 重叠：


```
for (SimpleFileEntry entry : mergedEntries) {
    int level = entry.level();
    if (level >= 1) {  // Level-0 直接跳过！
        levels.computeIfAbsent(...).add(entry);
    }
}

```


### 多 Writer 写同一 Bucket 为何不冲突

| 检查点 | 多 Writer 写同一 Bucket 的情况 |
| --- | --- |
| Snapshot Conflict | 可能发生，但自动重试解决，无影响 |
| Files Conflict | **不可能触发** ，因为 writer 只有 ADD，无 DELETE |
| LSM Key Range 冲突 | **不可能触发** ，新写入的都是 Level-0 文件 |

---

## 三、`write-only=true` 的作用

> [!warning] 勘误：`write-only` 不会消除全部冲突
> 它关闭摄入 Writer 自身的 Compaction，把文件重写交给独立 Compaction Job，从而减少写入链路中的 DELETE 竞争；但 Snapshot 发布竞争、多个 Writer 写同一分区、多个 Compaction Job 重叠等限制仍然存在。开启后必须监控 L0 文件数和 Compaction backlog。
`write-only=true` 让写入作业只产生`ADD` 操作，彻底消除了 Files Conflict 的可能性。Compaction 由独立作业负责，避免写入作业与 Compaction 作业之间的 DELETE 竞争。

但在**有分区表 + 写并发 > bucket 数**的场景下，同一 bucket 被多个 writer 写入，Level-0 文件数随写入并发线性增长。叠加`write-only=true` 没有自动 Compaction，文件会持续堆积，**长期运行后读放大越来越严重**，需定期手动触发 Compaction。

---

## 四、总结


> [!warning] 批注：不要把“Bucket 数是并行度整数倍”写成铁律
> 整数倍只可能让特定路由下的任务分配更整齐，不能解决热点 Key、跨分区写入或文件大小失衡。正确做法是先保证语义与并发边界，再依据每 Bucket 数据量、活跃分区数、Flush 文件数和 Compaction 能力调整。
回头看，整条逻辑链其实环环相扣。我们最担心的**写并发 > bucket 数会不会出事**，本质上是沿着三层逐级设防。

**第一层：路由分发层**Paimon 通过`(startChannel + bucket) % numChannels` 将记录路由到对应的 writer subtask。

- 无分区表：bucketId上限固定511，如果sink并发开到800，受路由公式数学约束，会有288个subtask永远接收不到数据，实际有效写入并发被限制在512；
- 有分区表：依靠`partition.hashCode()`，把相同bucketId、不同分区的数据打散到全部800个subtask，不会出现subtask结构性空闲。 也正是这个机制：**只有有分区表场景，才会出现同一个bucket被多个writer写入的现象**。

**第二层：并发冲突层**多个writer写入同一个bucket，听上去风险很高，但Paimon的文件冲突（Files Conflict）只有存在DELETE操作时才会真正触发。 各个writer flush产出带UUID的独立数据文件，commit提交均为ADD类型；同时Level‑0本身允许key‑range重叠。 因此这种“多写”在源码层面并不会引发数据冲突，正确性由机制保障，并非靠运气。

**第三层：代价与取舍层**`write‑only=true` 将Compaction从写入作业剥离，彻底消除写入链路与Compaction之间的DELETE竞争，让上面两层安全逻辑更加纯粹。 但它本身不消除小文件：写入并发越高，同一个bucket被拆分得越零散，Level‑0文件就越多；叠加写入作业不再执行自动Compaction，读放大会随时间持续累积。

由此得到两条实践铁律：

1. 写入并发尽量 ≤ bucket数，或者把bucket数设为写入并发的整数倍，从源头降低同一个bucket被多subtask拆分写入的概率；
2. 一旦开启`write‑only=true`，必须配套部署独立Compaction作业定期合并小文件，否则今天省下的计算资源，后续会以查询性能恶化的形式连本带利还回来。

## 复核资料

- [Concurrency Control](https://paimon.apache.org/docs/master/concepts/concurrency-control/)
- [Primary-Key Data Distribution](https://paimon.apache.org/docs/master/primary-key-table/data-distribution/)
- [Primary-Key Compaction](https://paimon.apache.org/docs/master/primary-key-table/compaction/)

## 系列导航

- 上一篇：[[Paimon 精讲（九）：Append 表的 Bucket Key 与文件索引（文章批注版）]]
- 延伸阅读：[[Flink 实时维表性能优化（上）：用对 Paimon Lookup 的完整实践（文章批注版）]]
- 延伸阅读：[[Flink 实时维表性能优化（下）：用对 Paimon Lookup 的完整实践（文章批注版）]]
