---
title: "Paimon 精讲（三）：Partition、Snapshot 与数据生命周期（文章批注版）"
aliases:
  - "Paimon 精讲（三）：Partition 和 Snapshot 傻傻分不清？一文搞定"
author: "胖泽的技术笔记"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s/zmwgd4-7f1qrul_RcfUGBA"
published_at: "2026-07-25"
reviewed_at: "2026-10-14"
tags:
  - Flink
  - Paimon
  - Paimon-精讲
---

# Paimon 精讲（三）：Partition、Snapshot 与数据生命周期（文章批注版）

> [!info] 来源、整理方式与版本边界
> 原文：[《Paimon 精讲（三）：Partition 和 Snapshot 傻傻分不清？一文搞定》](https://mp.weixin.qq.com/s/zmwgd4-7f1qrul_RcfUGBA)，胖泽的技术笔记，2026-07-25。正文按原文章节顺序保留技术内容、代码、表格、字段和案例；新增内容统一标为“批注”“勘误”或“版本边界”。原文没有声明准确的 Paimon/Flink 版本，内部类、默认值和源码路径不视为稳定 API。

> [!warning] 批注：Partition 与 Snapshot 不是两个并列的数据副本
> Partition 是数据布局和过滤维度；Snapshot 是一次提交后的表状态，依靠 Manifest 引用各分区的有效文件。Snapshot 文件本身不包含业务数据，分区目录也不能独立证明某个版本可读。
> “分区保留一年、Snapshot 保留一天就一定安全”过于绝对。最新 Snapshot 仍引用的当前文件不会因旧 Snapshot 过期而消失，但被覆盖、Compaction 替换或分区删除的旧文件，失去 Snapshot/Tag 引用后可以被清理；历史时间旅行和落后消费者会因此失去所需版本。
## 引言

“Partition 和 Snapshot 的区别与联系到底是什么？”

这个问题我问过身边不少用 Paimon 的同事，得到的答案五花八门：有人说 Partition 是文件夹，Snapshot 是备份；有人觉得它俩差不多，都是用来管理数据的；还有人干脆摆摆手说“能用就行，管那么多干嘛”。

其实分不清很正常——因为这两者在 Paimon 里确实长得像，但角色完全不同。Partition 是数据的**物理切分**，决定数据怎么存；Snapshot 是数据的**逻辑快照**，决定数据怎么看。一个管空间，一个管时间，配合起来才构成了 Paimon 的湖存储核心。

本文不讲晦涩的源码，用最直白的比喻和图示，帮你一次性理清 Partition 和 Snapshot 的关系、各自的作用，以及它们是如何协同工作的。看完如果还分不清，欢迎随时留言沟通。

## 一、两者的本质定位

| 概念 | 本质 | 存储形式 |
| --- | --- | --- |
| **Snapshot** | 整张表在某一时刻的**全局状态视图**，是读写一致性的基本单元 | 一个 JSON 文件（`snapshot/snapshot-N`） |
| **Partition** | 数据的**物理组织单元**，决定数据文件的目录布局 | 无独立元数据文件，信息散落在 Manifest 中 |

**核心关系**：Partition 没有自己独立的版本概念，**Partition 永远附属于 Snapshot 存在**。同一个分区（如`dt=20260710`）在不同 Snapshot 下对应的数据文件集合可以完全不同。

---

## 二、数据结构：三层连接关系


```
Snapshot（JSON 文件）
  ├── baseManifestList  ──→  ManifestList 文件（累计历史 manifest 指针列表）
  │                              ├── ManifestFileMeta（manifest 文件的元数据）
  │                              │     ├── fileName        # 实际 manifest 文件名
  │                              │     ├── partitionStats  # 该 manifest 覆盖的分区 min/max（粗过滤用）
  │                              │     ├── numAddedFiles
  │                              │     └── numDeletedFiles
  │                              └── ManifestFileMeta ...
  │
  └── deltaManifestList ──→  ManifestList 文件（仅本次提交的变更 manifest 指针）
                                  └── ManifestFileMeta（只包含本次写入/删除的文件）

```

```
ManifestFile（实际 manifest 文件，包含一条条 ManifestEntry）
  └── ManifestEntry（每条对应一个数据文件的增删事件）
        ├── kind       # ADD 或 DELETE
        ├── partition  # BinaryRow，分区值（如 dt=20260710）
        ├── bucket     # 桶编号
        └── file       # DataFileMeta（数据文件的完整元数据）
                           ├── fileName         # 数据文件名
                           ├── fileSize
                           ├── rowCount
                           ├── minKey / maxKey  # 主键范围（主键表）
                           ├── level            # LSM 层级
                           └── creationTime

```


---

## 三、写入时：Partition 数据如何更新 Snapshot

> [!note] 补充批注：Snapshot 是表级提交边界
> 一次提交可以同时修改多个 Partition，Paimon 不会为每个 Partition 单独生成一套 Snapshot。分区负责组织和裁剪数据，Snapshot 负责描述某次表级提交后的可见状态，两者不能按“一分区一快照”理解。

每次 commit（写入一批数据）的完整链路：


```
写入数据到分区 dt=20260710
        │
        ▼
生成 DataFileMeta（文件名、大小、行数、min/max key 等）
        │
        ▼
封装为 ManifestEntry（kind=ADD, partition=dt=20260710, bucket=0, file=DataFileMeta）
        │
        ▼
写入 ManifestFile（delta manifest 文件）
        │
        ▼
FileStoreCommitImpl.tryCommitOnce()
  ├── 读取上一个 Snapshot 的 baseManifestList（mergeBeforeManifests）
  ├── ManifestFileMerger.merge()  → 将旧 manifest 文件合并压缩（控制文件数量）
  ├── 生成新的 baseManifestList（累计全量 = 旧 base + 本次 delta）
  ├── 生成 deltaManifestList（仅本次变更）
  └── 原子写入新 Snapshot 文件（包含新 baseManifestList + deltaManifestList 指针）

```


**关键设计**：

-`baseManifestList` = 本次 Snapshot 可见的**所有存活数据文件**的累计清单
-`deltaManifestList` = **仅本次提交**新增/删除的文件清单（用于流式增量读和快速 expire）
- 两者都是对 ManifestFileMeta 列表的**引用**，不复制数据文件本身

---

## 四、读取时：如何通过 Snapshot 定位到 Partition 数据


```
1. 锁定 Snapshot（plan() 时调用 snapshotManager.latestSnapshot()，固定 Snapshot ID）
        │
        ▼
2. Snapshot → 读 baseManifestList → 获取 List<ManifestFileMeta>
        │
        ▼
3. ManifestsReader.filterManifestFileMeta()  【分区粗过滤】
   ├── 检查 ManifestFileMeta.partitionStats.minValues/maxValues
   ├── dt 范围不包含查询分区 → 直接跳过，不打开该 manifest 文件
   └── dt 范围包含查询分区 → 保留，进入下一步
        │
        ▼
4. 读取保留的 ManifestFile → 获取 List<ManifestEntry>  【分区精确过滤】
   ├── entry.partition != 目标分区 → 跳过
   └── entry.partition == 目标分区 且 kind==ADD → 保留
        │
        ▼
5. 按 (partition, bucket) 分组 ManifestEntry → 生成 DataSplit
   每个 DataSplit 包含：snapshotId + partitionValue + bucketId + List<DataFileMeta>
        │
        ▼
6. Executor 按 DataSplit 打开物理数据文件，执行行级读取

```


---

## 五、Snapshot 视角下 Partition 的演变

以`dt=20260710` 分区的三次写入为例：


```
Snapshot-1（首次写入 dt=20260710）
  baseManifestList → [manifest-A]
    manifest-A: [ADD dt=20260710/bucket-0/data-001.orc]

Snapshot-2（追加写入 dt=20260710，同时写 dt=20260711）
  baseManifestList → [manifest-A, manifest-B]  ← manifest-A 被复用！
    manifest-B: [ADD dt=20260710/bucket-0/data-002.orc,
                 ADD dt=20260711/bucket-0/data-003.orc]

Snapshot-3（compact dt=20260710，合并 data-001 和 data-002）
  baseManifestList → [manifest-A*, manifest-C]  ← manifest-A 被合并压缩为 A*
    manifest-C: [DELETE dt=20260710/bucket-0/data-001.orc,
                 DELETE dt=20260710/bucket-0/data-002.orc,
                 ADD    dt=20260710/bucket-0/data-004.orc]  ← compact 后的新文件

```


**读取 Snapshot-3 中 dt=20260710 的数据**：

- 扫描全部 ManifestEntry → 得到 {ADD data-001, ADD data-002, DELETE data-001, DELETE data-002, ADD data-004}
- mergeEntries 后 → 存活文件只有 {data-004}
- 最终读取 data-004.orc

**读取 Snapshot-1 中 dt=20260710 的数据**：

- 只扫描 manifest-A → {ADD data-001}
- 读取 data-001.orc（Snapshot-1 时刻的一致性视图，不受后续 compact 影响）

---

## 六、ScanMode 对 Partition 读取的影响


> [!warning] 版本边界：内部 ScanMode 不等于稳定的 SQL 配置接口
> `ALL`、`DELTA`、`CHANGELOG` 可帮助理解源码分支，但用户侧应以 `scan.mode`、`scan.snapshot-id`、`scan.tag-name`、`incremental-between` 等当前文档公开选项为准。恢复自 Savepoint 时，Flink 状态中的消费位点还可能覆盖首次启动参数。
| ScanMode | 读取哪个 ManifestList | 适用场景 |
| --- | --- | --- |
|`ALL` | baseManifestList + deltaManifestList | Batch 全量读（默认） |
|`DELTA` | deltaManifestList only | Streaming 增量读，只读本次 commit 的变更 |
|`CHANGELOG` | changelogManifestList | Streaming changelog 消费 |


```
// ManifestList.java
public List<ManifestFileMeta> readDataManifests(Snapshot snapshot) {
    result.addAll(read(snapshot.baseManifestList()));   // 全量历史
    result.addAll(readDeltaManifests(snapshot));        // 本次增量
    return result;
}

public List<ManifestFileMeta> readDeltaManifests(Snapshot snapshot) {
    return read(snapshot.deltaManifestList());          // 仅增量
}

```


---

## 七、Snapshot 过期时如何清理 Partition 数据



> [!note] 批注：物理删除需要同时失去所有有效引用
> Manifest 中的 DELETE 先把文件移出新 Snapshot 的逻辑状态，不等于立即删除物理文件。只有 Snapshot、Tag、消费者保护以及其他保留规则都不再引用它时，清理流程才可能删除文件。
```
过期 Snapshot-1、Snapshot-2，保留 Snapshot-3 及以上

        ↓
ExpireSnapshotsImpl.expireUntil()

Step 1：清理数据文件（扫描被删除 snapshot 的 deltaManifestList）
  ├── Snapshot-2 的 delta 中有 DELETE dt=20260711/data-003?
  │   → data-003 仍被 Snapshot-3 的 base 引用，不删除（构建 skippingSet）
  └── Snapshot-1 的 delta 中有 ADD data-001，但 Snapshot-3 的 base 已有 DELETE data-001
      → data-001 不再被任何存活 Snapshot 引用，可以删除

Step 2：清理 Manifest 文件（引用计数归零的 manifest 文件）
  ├── 构建 skippingSet（所有存活 Snapshot 引用的 manifest 文件名集合）
  └── 清理不在 skippingSet 中的旧 manifest 文件

Step 3：清理空目录
  └── 分区目录 dt=20260710/bucket-0/ 如果为空则删除

```


**核心原则**：**数据文件和 Manifest 文件的删除以 Snapshot 为粒度，而非以 Partition 为粒度**。一个分区的数据文件是否可以删除，取决于是否还有存活的 Snapshot 引用它。

---

## 八、Partition 统计信息如何与 Snapshot 关联

> [!warning] 补充批注：统计信息主要服务于裁剪，不等同于最终逻辑行数
> 主键表可能同时保留同一主键的多个物理版本、删除记录和待合并文件。需要核对业务行数时，应按表的 Merge Engine 读取合并后的结果，不能直接把文件或 Manifest 中的物理记录数当成业务记录数。

`PartitionEntry` 是从 Manifest 动态聚合计算出的分区统计，**不是独立存储的元数据**：


```
// AbstractFileStoreScan.readPartitionEntries()
public List<PartitionEntry> readPartitionEntries() {
    List<ManifestFileMeta> manifests = readManifests().filteredManifests;
    // 遍历所有 manifest 文件，按 partition 聚合 rowCount/fileSize/fileCount
    Map<BinaryRow, PartitionEntry> partitions = new ConcurrentHashMap<>();
    manifests.forEach(m -> PartitionEntry.merge(readManifest(m), partitions));
    return partitions.values().stream()
            .filter(p -> p.fileCount() > 0)  // 过滤掉已被删除的分区
            .collect(Collectors.toList());
}

```


因此，`SHOW PARTITIONS`、分区列表查询，本质上是**在当前 Snapshot 的 baseManifestList 上做全量扫描聚合**，反映的是当前 Snapshot 时刻的分区状态。

---

## 九、关系总结


```
┌─────────────────────────────────────────────────────────────────┐
│                        一张 Paimon 表                            │
│                                                                 │
│  Snapshot-1 ─── baseManifestList ─── [m1, m2]                  │
│      │                                  │                       │
│      │              ┌───────────────────┘                       │
│      │              ▼                                           │
│      │         ManifestEntry(ADD, dt=20260710, bucket=0, f1)   │
│      │         ManifestEntry(ADD, dt=20260711, bucket=0, f2)   │
│      │                                                          │
│  Snapshot-2 ─── baseManifestList ─── [m1, m2, m3]             │
│      │                                  │                       │
│      │              ┌───────────────────┘                       │
│      │              ▼                                           │
│      │         ManifestEntry(ADD, dt=20260710, bucket=0, f3)   │  ← 同一分区，新文件
│      │         ManifestEntry(DEL, dt=20260710, bucket=0, f1)   │  ← 旧文件被 compact 删
│      │                                                          │
│  Snapshot-3 ─── baseManifestList ─── [m2, m3, m4]  ← m1 合并  │
│                                         │                       │
│                         ┌───────────────┘                       │
│                         ▼                                       │
│                    ManifestEntry(ADD, dt=20260712, bucket=0, f4)│
└─────────────────────────────────────────────────────────────────┘

读 Snapshot-1 中 dt=20260710 → 只看 [m1,m2] 中相关 entry → {f1}
读 Snapshot-3 中 dt=20260710 → 看 [m2,m3,m4] 中相关 entry → merge 后 → {f3}

```


| 关系维度 | 说明 |
| --- | --- |
| **Snapshot 不存储 Partition 元数据** | 分区信息通过 ManifestEntry 中的 partition 字段隐式记录 |
| **Partition 跨多个 Snapshot 共享物理文件** | 同一数据文件可被多个 Snapshot 引用，直到被显式 DELETE |
| **同一 Partition 在不同 Snapshot 下内容不同** | compact、新写入、OVERWRITE 都会改变分区的文件集合 |
| **Partition 过期 = 某 Snapshot 的 OVERWRITE** | 分区过期通过 dropPartitions → tryOverwrite 产生新 Snapshot |
| **分区可见性由 Snapshot 决定** | 没有 Snapshot 引用的数据文件，分区在逻辑上也不可见 |

---

## 十、分区长期保留与 Snapshot 短期保留的交互问题


> [!warning] 勘误：Consumer ID 保护的是消费进度依赖的历史，不是无限保留承诺
> `consumer-id` 用于记录消费者的下一 Snapshot 位置，并参与过期保护；仍需把保留策略覆盖最长停机、排队和恢复窗口。需要长期按日回看时应建立 Tag，不能只依赖普通 Snapshot 保留参数。
### 典型场景

`partition.expiration-time = 1 year`（分区数据保留 1 年）+`snapshot.time-retained = 1 day`（Snapshot 只保留 1 天），这两个配置组合是否会造成数据丢失？

### 核心结论：分区数据文件是安全的

Snapshot 过期不会删除仍被存活 Snapshot 引用的数据文件，其核心机制是`ExpireSnapshotsImpl` 中的 **skippingSet**：


```
Snapshot 过期流程（expireUntil）：

  保留 Snapshot：[Snapshot-N-1000, ..., Snapshot-N]  ← 最近 1 天的
  过期 Snapshot：[Snapshot-1, ..., Snapshot-N-1001]  ← 超过 1 天的

  Step 1：构建 skippingSet
    ├── 遍历所有「保留中 Snapshot」的 baseManifestList
    ├── 收集它们引用的所有数据文件名
    └── 这些文件绝对不会被删除

  Step 2：删除数据文件
    ├── 扫描过期 Snapshot 的 deltaManifestList
    ├── 找出 ADD 条目对应的文件
    └── 只删除「不在 skippingSet 中」的文件

```


最新 Snapshot 的`baseManifestList` 累计了所有存活数据文件（包括 1 年前写入但未过期的分区文件）的引用，因此这些文件都进入`skippingSet`，**不会被删除**：


```
dt=2025-07-14 的数据文件（1年前写入，分区未过期）
      │
      ▼
manifest-A → ADD dt=2025-07-14/data-001.orc
      │
      └── 被 Snapshot-N（今天的最新快照）的 baseManifestList 引用
                 ↓
          进入 skippingSet → 不会被删除 ✅

```


### 四种场景的具体影响

| 场景 | 是否有问题 | 说明 |
| --- | --- | --- |
| Batch 读 1 年内任意分区 | ✅ 安全 | 数据文件完好，最新 Snapshot 可读到所有存活分区 |
| 时间旅行（读历史 Snapshot） | ⚠️ 受限 | 只能回溯 1 天以内，超过 1 天的历史 Snapshot 已过期 |
| Streaming 增量消费（无 consumer-id） | 🚨 高风险 | lag 超过 1 天 → checkpoint 对应的 Snapshot 被删 → 作业 crash |
| 分区过期（partition.expiration-time=1年） | ✅ 正常工作 | 过期后生成 OVERWRITE Snapshot → 旧 Snapshot 在 1 天内自然过期 |

### 最严重的风险：Streaming 消费者 Lag 超过 Snapshot 保留窗口


```
正常情况：
  Snapshot-100（Flink checkpoint 记录的消费位点）
  Snapshot-101
  ...
  Snapshot-200（当前最新）

作业停止 36 小时后重启（Snapshot 保留 1 天）：
  Snapshot-100 已被过期清理！
  Flink 尝试从 Snapshot-100 恢复 → 抛出异常：
  "Snapshot file does not exist: snapshot/snapshot-100"

```


**解决方案：配置 `consumer-id`**
```
CREATE TABLE my_table (...)
WITH (
  'consumer-id' = 'my-flink-job',
  'snapshot.time-retained' = '1 d'
);

```


`ConsumerManager` 记录各消费者的消费位点，`ExpireSnapshotsImpl` 过期时执行检查：


```
// ExpireSnapshotsImpl.java
long minConsumerSnapshotId = consumerManager.minNextSnapshot();
// 所有消费者还未消费的 Snapshot 不会被删除
safelyExpire = Math.min(safelyExpire, minConsumerSnapshotId - 1);

```


### 时间旅行能力受限


```
-- 失败（snapshot-1000 是 2 天前的，已过期）
SELECT * FROM my_table /*+ OPTIONS('scan.snapshot-id'='1000') */;

-- 成功（最近 1 天内的 Snapshot）
SELECT * FROM my_table /*+ OPTIONS('scan.snapshot-id'='99000') */;

```


若有历史数据回查需求，需增大`snapshot.time-retained`，或使用 **Tag** 机制固定重要历史版本。

### 配置建议


```
# 分区数据保留（数据层面）
partition.expiration-time = 365 d

# Snapshot 保留（版本层面，建议比最长可能 lag 更长）
snapshot.time-retained = 7 d

# 流式消费必配（防止消费位点对应的 Snapshot 被过期）
consumer-id = your-job-name

# 如有历史回查需求，配合 Tag 保留关键版本
tag.automatic-creation = process-time
tag.num-retained-max = 30

```

> **一句话总结**：分区保留 1 年 + Snapshot 保留 1 天是合理组合——数据文件由最新 Snapshot 的 baseManifestList 保护不会丢失，但流式消费者的 lag 风险必须通过`consumer-id` 保护消费位点。

---

## 十一、关键源码位置

| 功能 | 文件 | 关键行 |
| --- | --- | --- |
| Snapshot 数据结构 |`paimon-core/.../Snapshot.java` | baseManifestList / deltaManifestList 字段 |
| ManifestEntry 数据结构 |`paimon-core/.../manifest/ManifestEntry.java` | partition / kind / file 字段 |
| ManifestFileMeta（含 partitionStats） |`paimon-core/.../manifest/ManifestFileMeta.java` | partitionStats 字段 |
| PartitionEntry（分区聚合统计） |`paimon-core/.../manifest/PartitionEntry.java` | fromManifestEntry() 聚合逻辑 |
| commit 时更新 base/delta manifest |`paimon-core/.../operation/FileStoreCommitImpl.java` | L884~L993 |
| 读取时分区粗过滤 |`paimon-core/.../operation/ManifestsReader.java` | filterManifestFileMeta() |
| 动态聚合分区统计 |`paimon-core/.../operation/AbstractFileStoreScan.java` | readPartitionEntries() |
| Snapshot 过期清理数据文件 |`paimon-core/.../table/ExpireSnapshotsImpl.java` | expireUntil() |

## 本篇结论

回到开头的那个问题：Partition 和 Snapshot 到底有什么区别与联系？

现在你应该清楚了——

**Partition 是空间维度的切分**，告诉 Paimon 数据应该存在哪个目录下，方便按分区过滤和高效查询。它就像图书馆的书架，把书按类别分开放置。

**Snapshot 是时间维度的快照**，告诉 Paimon 在某个时刻数据处于什么状态，支撑时间旅行和增量读取。它就像给书架定期拍照，记录每次整理后的样子。

两者的协作机制可以概括为一句话：**Partition 决定数据写到哪里，Snapshot 决定数据读到哪一刻。**

理解这一点，你在使用 Paimon 时会更有底气：

- 知道为什么分区裁剪能加速查询
- 明白为什么 Snapshot 过期会影响时间旅行范围
- 懂得如何利用两者的关系优化写入和读取性能

## 复核资料

- [Basic Concepts](https://paimon.apache.org/docs/master/concepts/basic-concepts/)
- [Snapshot Specification](https://paimon.apache.org/docs/master/concepts/spec/snapshot/)
- [Manifest Specification](https://paimon.apache.org/docs/master/concepts/spec/manifest/)
- [Manage Tags](https://paimon.apache.org/docs/master/maintenance/manage-tags/)

## 系列导航

- 上一篇：[[Paimon 精讲（二）：Flink SQL 写入与 Changelog 生成机制（文章批注版）]]
- 下一篇：[[Paimon 精讲（四）：Fixed、Dynamic 与 Unaware Bucket（文章批注版）]]
