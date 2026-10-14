---
title: "Paimon 精讲（五）：Deletion Vector 与 Merge On Write（文章批注版）"
aliases:
  - "Paimon 精讲（五）：Deletion Vector 是怎么让主键表删除变得更快的？"
author: "胖泽的技术笔记"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s/kfeLqDE6mopIrU1gDaRgFA"
published_at: "2026-08-01"
reviewed_at: "2026-10-14"
tags:
  - Flink
  - Paimon
  - Paimon-精讲
---

# Paimon 精讲（五）：Deletion Vector 与 Merge On Write（文章批注版）

> [!info] 来源、整理方式与版本边界
> 原文：[《Paimon 精讲（五）：Deletion Vector 是怎么让主键表删除变得更快的？》](https://mp.weixin.qq.com/s/kfeLqDE6mopIrU1gDaRgFA)，胖泽的技术笔记，2026-08-01。正文按原文章节顺序保留技术内容、代码、表格、字段和案例；新增内容统一标为“批注”“勘误”或“版本边界”。原文没有声明准确的 Paimon/Flink 版本，内部类、默认值和源码路径不视为稳定 API。

> [!warning] 批注：Deletion Vector 解决存储读取路径，不替代 Changelog
> `deletion-vectors.enabled=true` 使主键表采用 MOW：写入侧 Lookup 旧行位置并记录失效位置，读取侧跳过这些位置。它不改变 Merge Engine 定义的逻辑结果，也不替代 Changelog Producer 对流式变更契约的控制。
> 当前文档中 DV 表支持 `none`、`input`、`lookup`，不支持 `full-compaction` Producer。原文“对 changelog-producer 有要求”应具体化为版本和消费者契约，不能理解为必须配置 `lookup`。
## 引言

Paimon 主键表做更新删除，长期面临一个两难选择：

- **MOR（Merge On Read）**：写入快，但读取要跨文件归并，并发受限
- **COW（Copy On Write）**：读取快，但每次写入全量合并，写放大严重

从 0.8 版本开始，Paimon 给出了第三种选择——**Deletion Vectors**。

它的核心思想是 **Merge On Write**：写入时通过 LSM 点查定位被删除的数据，用 RoaringBitmap 在独立的删除文件中标记出来。读取时只需“数据文件 + 删除向量过滤”，无需跨文件 merge，读取性能显著提升。

但它并非银弹：

- 仅支持主键表，对`changelog-producer`、`merge-engine` 等参数有约束
- 写入路径有额外 lookup 开销
- 删除向量文件格式、位图位数选择等细节需要留意

本文从底层原理出发，拆解 Deletion Vectors 的生成过程、在 LSM 树中的维护机制、以及开启后的代价与适用场景。

---

## 一、Deletion Vector 是什么

Deletion Vector（删除向量）是 Paimon 主键表的一种**延迟物化删除**优化机制。

传统方式在 Compaction 时需要重写旧数据文件，而 DV 改为在独立的索引文件中用 Bitmap（RoaringBitmap）记录哪些行已被删除，读取时由`ApplyDeletionVectorReader` 过滤掉这些行，从而避免频繁重写数据文件，降低写放大。

### 核心类

| 类 | 路径 | 作用 |
| --- | --- | --- |
|`DeletionVector` （接口） |`paimon-core/.../deletionvectors/DeletionVector.java` | 定义 delete/isDeleted/serialize 等核心方法 |
|`BitmapDeletionVector` |`paimon-core/.../deletionvectors/BitmapDeletionVector.java` | 用 RoaringBitmap 存储被删除的行号 |
|`DeletionVectorsMaintainer` |`paimon-core/.../deletionvectors/DeletionVectorsMaintainer.java` | 维护 bucket 内"文件名→DV"映射，负责写出索引文件 |
|`DeletionVectorsIndexFile` |`paimon-core/.../deletionvectors/DeletionVectorsIndexFile.java` | DV 索引文件的读写管理 |

---

## 二、适用场景

> [!warning] 补充批注：Deletion Vector 不是所有主键表的通用加速开关
> 它更适合读取占比高、希望减少读时合并成本的场景，但可用性还受 Merge Engine、Bucket 模式、索引及版本能力约束。启用前需要按当前 Paimon 版本核对兼容矩阵。

### 前提条件

- 必须是**主键表**（Primary Key Table）
- Changelog Producer 只能是`NONE` /`INPUT` /`LOOKUP` 三种
- **不能**与`FIRST_ROW` merge engine 同时使用
- 通过配置项`deletion-vectors.enabled = true` 开启

### 适合场景

写多读少、更新频繁、希望降低 Compaction 开销的主键表，典型例子是 **CDC 数据入湖**。

### 与传统 Compaction 对比

|  | 传统方式 | Deletion Vector |
| --- | --- | --- |
| 旧数据处理 | 重写整个数据文件 | 仅写索引文件标记删除 |
| 写放大 | 高 | 低 |
| 读时开销 | 无需额外过滤 | 需通过 DV 过滤已删除行 |
| 适用表类型 | Primary Key / Append | **仅 Primary Key** |

---

## 三、生成时机


> [!warning] 批注：还要检查新写 Level-0 数据的可见性
> 当前 MOW 默认可能在 Lookup Compaction 发布前不让普通 Batch Reader 读取待处理的 Level-0 数据；需要立即合并读取时要核对 `deletion-vectors.merge-on-read`。因此“写入成功”与“所有读取路径立即看到最终行”不能画等号。
DV 在 **Lookup Compaction** 过程中生成，流程如下：


```
写入新数据
  → Lookup Compaction 触发
  → 发现高层文件中存在旧版本的同主键记录
  → 调用 DeletionVectorsMaintainer.notifyNewDeletion(fileName, rowPosition)
  → 将该行号标记到对应文件的 BitmapDeletionVector（RoaringBitmap）
  → Compaction 结束时写出 DV 索引文件（DeletionVectorsIndexFile）

```


**关键点**：旧数据文件本身不被重写，只是在独立的 DV 索引文件中记录"第 N 行已删除"。

### DV 文件的生命周期

DV 索引文件不会永久积累，触发 **Full Compaction** 时会被彻底清空（详见第七节）。

---

## 四、并发写入的风险分析


> [!warning] 版本边界：源码时序不能推广为所有版本的固定缺陷
> DV Index Manifest、Commit 冲突检查和 Bucket Combiner 都属于内部实现。原文给出的丢失时序应在目标 Connector 版本上用并发写、Compaction 和故障恢复测试复现；生产上仍应遵守所选 Bucket 模式的并发限制，并避免多个 Compaction Job 重叠处理同一分区。
### 基本模型

Paimon 主键表按`hash(主键) % bucket数` 路由数据，**同一条主键永远只落入同一个 bucket**。正常多任务并发写入时，不同 writer 负责不同 bucket，天然隔离，不会互相干扰。

### 风险场景

当**两个 writer 被分配到同一个 bucket** 时存在风险，例如：

- Flink 任务 rescale（并行度变化后重启）
- 两个独立 Flink 作业双写同一张表
- Spark 批写 + Flink 流写同时进行

### 风险根因：`CommonBucketCombiner` 的覆盖逻辑
DV 索引文件在 commit 时通过`IndexManifestFileHandler.CommonBucketCombiner` 合并，其逻辑本质是一个以`(partition, bucket, indexType)` 为 key 的 HashMap：


```
// 旧条目入 Map
for (IndexManifestEntry entry : prevIndexFiles) {
    indexEntries.put(identifier(entry), entry);
}
// 新条目直接覆盖同 bucket 的旧条目
for (IndexManifestEntry entry : added) {
    indexEntries.put(identifier(entry), entry);
}

```


**设计假设**：新 DV 文件是旧 DV 文件的超集（包含所有历史删除标记）。
**单 writer 下**：假设成立，因为 writer 初始化时会读取当前最新 DV 状态作为基础。
**多 writer 并发下**：假设被打破。

### 并发导致 DV 丢失的时序


```
snapshot S 的 DV：{ f1.row5 }
        ↙                       ↘
  Writer A                    Writer B
  基于 snapshot S 初始化        基于 snapshot S 初始化
  新增标记 f1.row10             新增标记 f1.row20
  生成 DV_A:                    生成 DV_B:
  { f1.row5, f1.row10 }         { f1.row5, f1.row20 }
        ↓                              ↓
    先 commit → snapshot S+1       后 commit → snapshot S+2
                                          ↓
                           CommonBucketCombiner:
                           put(bucket0, DV_B) ← 直接替换 DV_A！
                                          ↓
                           最终 DV：{ f1.row5, f1.row20 }
                           f1.row10 的删除标记消失 → 幽灵行出现！

```


### 现有保护机制的局限

| 机制 | 保护内容 | 能防住 DV 覆盖吗 |
| --- | --- | --- |
| 乐观锁 + commit 冲突检测 | LSM 数据文件的 key range 重叠 | **否** ，不检查 index 文件 |
| bucket 路由 | 不同主键落不同 bucket | 是（bucket 级别隔离） |
|`CommonBucketCombiner` | 按 bucket 合并 DV 索引 | **否** ，后者直接覆盖前者 |

---

## 五、实践建议

> [!note] 补充批注：上线验证不能只比较单次查询耗时
> 至少同时检查 L0 文件积压、Compaction backlog、Deletion Vector 索引体积、批流读取结果一致性，以及任务重启后的恢复结果。这样才能区分“读性能改善”与“写入或维护成本被转移”。

| 场景 | 风险等级 | 建议 |
| --- | --- | --- |
| 单 Flink 作业，固定并行度 | 极低 | 安全 |
| 多 Flink 作业双写同一张表 | **高** | 避免，或使用 dedicated writer |
| Flink 任务 rescale 重启 | 中（短暂窗口） | 使用`write-only` 模式 + 独立 Compaction 任务降低风险 |
| Spark 批写 + Flink 流写 | **高** | 避免同时写，用时间窗口隔离 |

---

## 七、Full Compaction 对 DV 的处理


> [!note] 批注：Full Compaction 清理的是被重写文件对应的物理失效标记
> 新文件只包含当前有效行时，旧文件及其 DV 可以随新 Snapshot 被逻辑替换；物理文件何时删除仍由 Snapshot、Tag 和清理策略决定。不能把“DV 清零”理解为历史 Snapshot 立即失效或文件马上消失。
### 结论

Full Compaction 完成后，旧 DV 索引文件被废弃，**新生成的数据文件不会产生新的 DV**，该 bucket 的 DV 索引完全清空。

### 原因：dropDelete=true

`MergeTreeCompactManager` 在 DV 模式下，只要输出 level >= 1，就强制开启`dropDelete=true`：


```
boolean dropDelete =
    unit.outputLevel() != 0
        && (unit.outputLevel() >= levels.nonEmptyHighestLevel()
                || dvMaintainer != null);  // DV 模式下强制 true

```


`dropDelete=true` 意味着合并时直接丢弃所有被删除的行，产出的新数据文件只含存活行，本身就是"干净"的，不需要任何 DV 来标记删除。

### 旧 DV 条目的清理流程


```
Full Compaction 执行
  → LookupMergeTreeCompactRewriter.notifyRewriteCompactBefore()
  → 对所有被 compact 的旧文件调用：
      dvMaintainer.removeDeletionVectorOf(fileName)
  → 内存中该 bucket 的 DV map 被清空
  → CompactDeletionFile.generateFiles() 调用 writeDeletionVectorsIndex()
  → 返回空列表，不写出任何新 DV 索引文件
  → CommonBucketCombiner 中旧 DV 条目被"空"替换，从 index manifest 消失

```


### Full Compaction 前后对比


```
Full Compaction 之前：
  数据文件 f1（含已删除行 row10、row20）
  DV 索引：{ f1.row10, f1.row20 }

Full Compaction 之后：
  新数据文件 f2（仅含存活行，row10/row20 已被丢弃）
  DV 索引：空（该 bucket 无任何 DV 条目）

```


### 完整 DV 生命周期总结

| 阶段 | DV 变化 |
| --- | --- |
| Lookup Compaction（第1次） | 旧文件中被覆盖的行 → 写入 DV 索引（初始标记） |
| Lookup Compaction（后续） | 新的被删除行继续追加到 DV（在旧 DV 基础上叠加，DV 是累积的超集） |
| Full Compaction | 旧文件 + 旧 DV 全部废弃；新文件干净，**DV 索引清零** |

---

## 六、相关核心代码位置

| 文件 | 关键内容 |
| --- | --- |
|`paimon-core/.../deletionvectors/DeletionVectorsMaintainer.java` | DV 初始化（固定读取某一 snapshot）、写出逻辑 |
|`paimon-core/.../manifest/IndexManifestFileHandler.java` |`CommonBucketCombiner` ，按 bucket 覆盖替换 DV |
|`paimon-core/.../operation/FileStoreCommitImpl.java` | commit 冲突检测（`noConflictsOrFail`，不涉及 index 文件） |
|`paimon-core/.../sink/LookupChangelogMergeFunctionWrapper.java` | Lookup Compaction 中触发`notifyNewDeletion` 的逻辑 |
|`paimon-core/.../mergetree/compact/MergeTreeCompactManager.java` | Full Compaction 中`dropDelete=true` 的判断逻辑 |
|`paimon-core/.../mergetree/compact/LookupMergeTreeCompactRewriter.java` | Full Compaction 前调用`removeDeletionVectorOf` 清空旧 DV 条目 |
|`paimon-core/.../compact/CompactDeletionFile.java` | 触发`writeDeletionVectorsIndex()`，Full Compaction 后写出空列表 |

---

## 本篇结论

回到开头的问题：主键表的更新删除，到底该选 MOR 还是 COW？

Deletion Vectors 给出了第三种答案：**都不选，用 Merge On Write。**

它在写入时付出一次 lookup 的代价，换来读取时彻底摆脱跨文件归并的开销。对于读取密集、对查询延迟敏感的场景，这是一个非常有吸引力的折中方案。

| 模式 | 写入性能 | 读取性能 | 适用场景 |
| --- | --- | --- | --- |
| MOR | 优 | 差（需跨文件 merge） | 写入密集，读取容忍延迟 |
| COW | 差（写放大严重） | 优 | 小表、低频写入 |
| Deletion Vectors | 中等（有 lookup 开销） | 良（无需跨文件 merge） | 读取密集的主键表 |

但需要注意它的约束条件：仅支持主键表、对 changelog-producer 有要求、写入路径有额外开销。它不是万能的，但在对的场景下，它能帮你打破 MOR 和 COW 之间的两难困局。

## 复核资料

- [Table Mode](https://paimon.apache.org/docs/master/primary-key-table/table-mode/)
- [Changelog Producer](https://paimon.apache.org/docs/master/primary-key-table/changelog-producer/)
- [Primary-Key Compaction](https://paimon.apache.org/docs/master/primary-key-table/compaction/)

## 系列导航

- 上一篇：[[Paimon 精讲（四）：Fixed、Dynamic 与 Unaware Bucket（文章批注版）]]
- 下一篇：[[Paimon 精讲（六）：File Index 的存储与读写链路（文章批注版）]]
