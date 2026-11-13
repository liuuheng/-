---
title: "Paimon 精讲 （十一）：Append表查询越来越慢？Paimon 增量 Clustering 的低成本解法"
author: "胖泽的技术笔记"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s/w6a6sjDcDels1qBxXMCHmg"
published_at: "2026-11-08T11:28:04+08:00"
---

# Paimon 精讲 （十一）：Append表查询越来越慢？Paimon 增量 Clustering 的低成本解法

> Source: wechat
> Author: 胖泽的技术笔记
> Original: https://mp.weixin.qq.com/s/w6a6sjDcDels1qBxXMCHmg

---

![cover](images/cover.jpg)

---

## 引言

Paimon 的 Append 表在持续写入下，新落盘的文件布局无序，文件级 min/max 范围互相重叠，数据跳过逐渐失效，查询不得不扫描大量文件，导致查询性能越来越差。Clustering 通过按指定列排序恢复跳过效率，但如何在持续写入的实时链路里低成本地维护布局，是落地时的核心矛盾。Paimon 的 Incremental Clustering 以「增量重写、渐进收敛」解决这一矛盾：每轮只处理一小批候选文件，高层稳定数据不被反复触碰。

本文围绕 Incremental Clustering 展开，覆盖三个层面：工作机制、读写并发隔离、20TB/天规模的实践要点。

阅读路径建议：

- 想快速做决策：看「六、并发场景与使用建议」与「七、总结」。
- 想理解原理：从「二、Clustering 的工作机制」开始顺序阅读。
- 要在大规模链路落地：先看「二」理解机制，再看「五、大规模实践（20TB/天）」。

---

## 一、为什么需要 Clustering

Clustering 是 Paimon 对 Append 表做数据布局优化的能力：按指定列（clustering key）对数据排序，使文件级 min/max 统计更有效，从而提升点查与范围过滤的数据跳过效率。

传统做法是对全表做一次性重排，代价随表大小线性增长，无法在持续写入的实时链路中复用。Paimon 的 Incremental Clustering 把布局优化从「一次性离线重写」转变为「持续低成本的增量维护」：借鉴 LSM 的分层思想，每轮只处理一小批候选文件，多次运行后整体布局渐进收敛。

---

## 二、Clustering 的工作机制

## 2 .1 写入与聚簇解耦

Incremental Clustering 不是写入时实时触发，而是采用「持续写入 + 周期性聚簇作业」的分离架构：


```
Flink/Spark Streaming 持续写入 append 表
         ↓
   数据文件（布局未排序，处于低层）
         ↓
   周期性运行 clustering 作业
         ↓
   数据布局逐步收敛到最优

```


实时写入照常进行，新文件不会被立即排序。Clustering 作业运行时按层级选取部分候选文件做排序重写，而非全表重排。

## 2 .2 借鉴 LSM 分层思想：只动该动的文件

LSM Tree 把数据文件组织成多个层级（Level）：

- 越低层：越新、越小、越乱、越容易被重写。
- 越高层：越老、越大、越稳定、不轻易动。

LSM 的 Universal Compaction 核心思想是：每次只挑「值得合并」的一组文件，而不是全部重排。判断依据通常是文件大小比例或 sorted run 数量阈值。相比 Leveled Compaction，写放大更低。

Paimon 借鉴这一思路：为每个数据文件维护一个层级标记（level），用同样的分层逻辑决定本轮动哪些文件：

- level=0 的文件：每个是独立的 sorted run（新写入的乱序文件）。
- level>0 的文件：同层文件合并视为更稳定的 sorted run。

每轮 Clustering 只从层级结构中挑出候选文件做排序重写，其他文件不动。


```
初始状态（刚写入的乱序文件）：
  L0: [F1] [F2] [F3] [F4] [F5] [F6]   ← 所有文件 level=0

第一轮 clustering（只挑 L0 的部分文件）：
  L0: [F5] [F6]           ← 未处理
  L1: [F1+F2 sorted]      ← 已排序重写，晋升到 level=1
  L1: [F3+F4 sorted]

第二轮 clustering（挑 L1 的相邻 run）：
  L0: 新写入的 F7 F8
  L2: [F1+F2+F3+F4 sorted] ← 进一步合并，晋升到 level=2

多轮后：大部分数据已按 clustering key 排好序

```


## 2 .3 增量聚簇为什么优于全表重排

**全表重排**：每次都重写整表，单次代价随表大小线性增长。更关键的是，已经排好序的稳定数据每轮也会被重复重写——这部分是纯浪费；而实时链路需要持续维护布局，这个线性代价是反复支付的。TB 级数据反复重写，代价无法接受。

**增量聚簇**：每轮只处理「新写入的低层文件」或「两个相邻 sorted run」，单次代价只与本轮候选文件大小成正比，可控。高层稳定数据永远不被触碰，写放大集中在低层，整体大幅降低。

## 2 .4 执行本质：排序重写 + 原子替换提交

理解 Clustering 的并发行为，关键在于它做完之后怎么落盘。一个常见的误解是把聚簇想成「把现有文件里的数据原地排个序」——其实不是。Clustering 不修改任何已有文件，它做的是两件事的组合：

**第一步：排序重写（产出新文件）。** 按增量策略挑出本轮候选文件（低层乱序文件），读出其数据、按 clustering key 排序，写成一批新的、内部有序的数据文件。这一步只产出新文件，旧文件一个字节都不动。

**第二步：原子替换提交（用新文件换掉旧文件）。** 把这次重写登记成一次提交，语义上等价于 SQL 的`INSERT OVERWRITE`：把本轮要替换的旧文件登记为 DELETE 条目，把新排序文件登记为 ADD 条目，在一个新 Snapshot 里原子完成「旧文件 → 新文件」的替换。提交要么整体生效、要么不生效，不存在「换了一半」的中间状态。

> 全量模式与增量模式的区别只在重写范围：全量聚类重写整个目标分区的文件；增量聚类只重写本轮候选文件，未被选中的高层稳定文件不被重写，数据原样保留到新快照。

这个执行本质直接决定了第三章的全部并发行为：

| 这一本质 | 导致的并发后果 |
| --- | --- |
| 旧文件不被原地修改，替换靠生成新快照完成 | 绑定旧快照的读取不受影响（→ 3.1 快照隔离） |
| 提交是「替换一个范围内文件」的语义 | 另一写操作并发动了同一范围时，冲突检查会发现并重试或失败（→ 3.4 同分区并发写） |
| 快照里是「删旧 + 加新」的替换 | 流读跨过该快照时，看到旧文件消失、新文件出现的「数据跳变」（→ 3.4 streaming 风险） |

一句话：Clustering 不是原地排序，而是「读出来排序写成新文件，再用新快照原子替换旧文件」。正因为旧文件不动、替换走快照，才有了读写隔离；也正因为替换是「删一片 + 加一片」的范围操作，才有了同分区并发冲突和流读跳变。

---

## 三、读写并发与隔离保障

## 3 .1 快照隔离（MVCC）：读写不互扰

Paimon 基于快照隔离实现读写分离，原理类似数据库 MVCC：

- 每次读取绑定一个固定的 Snapshot ID，读的是该快照时刻 manifest 描述的数据文件列表。
- 写操作（普通写、compact、clustering）生成新 Snapshot，不修改或删除旧快照引用的文件。

正在进行的读取不会被 clustering 写入打断，读到的始终是 clustering 开始前那个快照的数据。

## 3 .2 分区级隔离：聚簇一个分区不影响读取另一分区

这是性能层面的关键结论：读`dt=20260710` 的同时 clustering`dt=20260712`，clustering 不会变慢，分区之间完全隔离。

原因在于 manifest 文件的分区级粗过滤。每个 manifest 文件维护了所含分区的统计信息（min/max），读取 manifest 列表时先做粗过滤——只要某个 manifest 不包含目标分区，就直接跳过、不打开读取内容。


```
clustering 处理 dt=20260712 时的过滤路径：
  只过滤目标分区 dt=20260712
        ↓
  逐个检查 manifest 的分区统计
        ├── manifest 只含 dt=20260710 → 直接跳过，不读内容
        ├── manifest 只含 dt=20260711 → 直接跳过
        └── manifest 含 dt=20260712   → 保留，读取文件列表

```


`dt=20260710` 的 manifest 根本不会被打开，clustering 只处理目标分区相关文件。

## 3 .3 例外：manifest 跨分区合并时的二次过滤

若表的 manifest 文件经过大量合并（manifest compaction），导致单个 manifest 混合包含多个分区（min 跨到 20260710、max 到 20260712），则该 manifest 会通过粗过滤。但 manifest 内部仍会按分区做二次过滤，不会把无关分区的文件列入处理列表——不影响正确性，只会多读一点 manifest 内容。合理配置 manifest 目标文件大小与合并阈值可避免这种情况。

## 3 .4 不同并发场景的影响

| 场景 | 是否有影响 | 说明 |
| --- | --- | --- |
| Batch 读先确定 Snapshot，clustering 后完成 | 无影响 | 读的是旧快照文件 |
| Streaming 读消费到 clustering 的 OVERWRITE 快照 | 有风险 | 可能收到 DELETE+ADD，导致数据跳变 |
| Clustering 进行中，另一 INSERT 并发写同一分区 | 有冲突 | OVERWRITE 提交做强制冲突检查，会重试或失败 |
| 旧快照过期且未配置 consumer-id | 有风险 | 长时间运行的读任务引用的旧文件被删除，抛异常 |

---

## 四、实时场景的使用方式与限制

## 4 .1 配置与调度


```
-- 建表时启用 clustering
CREATETABLE log_events (
  user_id BIGINT,
  event_type STRING,
  ts TIMESTAMP,
  ...
) WITH (
'append.clustering.enable' = 'true',
'clustering.columns' = 'user_id,event_type'
);

-- 周期性触发聚簇作业（如每小时/每天）
CALL sys.compact('mydb.log_events', 'action' => 'clustering');

```


工作机制：实时写入照常进行，新文件不会被立即排序；Clustering 作业运行时按文件层级选取部分候选文件做排序重写；每次只处理小范围文件，多次运行后整体布局渐进收敛。

## 4 .2 使用限制

| 限制 | 说明 |
| --- | --- |
| 表类型限制 | 主要针对 append 的 unaware-bucket 表；主键表有单独的 PK Clustering Override |
| 非实时生效 | 是逐步收敛，对刚写入的新数据不立即生效 |
| 需要周期性维护 | 必须有独立的 clustering 作业调度，否则布局不会改善 |
| 不是万能加速 | 仅优化 clustering key 对应过滤查询的性能，不会让所有查询等比变快 |
| 写放大存在 | 每轮对候选文件做排序重写，远低于全量重排，但仍有代价 |
| 与 compaction 耦合 | 需与 compaction 策略合理规划，避免相互干扰 |

## 4 .3 适用与不适用场景

**适合**：Flink 实时写入的日志/事件大表，按`user_id`、`region` 等维度定期聚簇；明细宽表写入后按高频查询列做布局优化；不希望为查询优化做全量 rewrite 的场景。

**不适合**：对写入后立即查询有强实时性要求（新数据尚未被 clustering）；高频整行更新的主键表；对所有查询路径都有等均加速需求的场景。

---

## 五、大规模实践（20TB/天）

## 5 .1 核心挑战：写入速度与聚簇产能的平衡


```
20TB/天 ≈ 230GB/小时 ≈ 4GB/分钟

每一批写入 → 若干新 L0 文件（unaware-bucket，无分桶限制）
clustering 作业未及时跟上 → L0 文件持续堆积
堆积到一定程度 → min/max 统计失效 → 查询性能退化

```


写入速度与 clustering 产能的平衡是 20TB/天场景的最大风险。

## 5 .2 五个关键问题

## L0 小文件堆积

unaware-bucket 表没有 bucket 限制，每次写入都会落成新的 L0 文件。如果 clustering 作业间隔过长，L0 文件数量持续增长，每个 L0 文件是独立的 sorted run，min/max 范围互相重叠，查询时需要扫描大量文件，数据跳过失效。

建议：评估 clustering 作业运行间隔，确保每轮能消化掉当轮写入产生的 L0 文件。

## local-sort 与 global-sort 的取舍

`clustering.incremental.mode` 控制排序策略：

| 模式 | 做法 | 代价 | 效果 |
| --- | --- | --- | --- |
| local-sort | 每个 task 独立排序本地数据 | 低，无 shuffle | 同一文件内有序，跨文件不保证 |
| global-sort | 全局 shuffle + 排序 | 高，有完整 shuffle | 跨文件真正有序，数据跳过最优 |

20TB/天场景用 global-sort，等于做一次大规模 shuffle，资源消耗不容忽视。

建议：先用 local-sort 跑通，观察查询性能收益；如果收益不够再评估 global-sort 的成本是否可接受。

## 分区粒度决定聚簇效率


```
按天分区：每天一个分区 = 20TB/分区 → 每次 clustering 候选文件非常多
按小时分区：每小时约 830GB/分区 → 粒度合理，但分区数增多

```


历史分区越多，元数据管理压力越大（历史分区会额外处理 clustering 任务）。

建议：分区粒度推荐按小时或按天加子分区（如`dt/hour`），避免单分区文件数过多；定期对历史分区做一次`fullCompaction`，让历史数据稳定在高层，减少 clustering 反复处理的范围。

## 聚簇作业与写入作业的资源竞争

Clustering 当前主要是 batch 作业，而实时写入是流作业，二者在同一集群上会竞争资源：clustering 需要读候选文件、排序（可能 shuffle）、写回新文件，而 20TB/天的写入链路本身就需要大量 IO 和计算。

建议：把 clustering 作业安排在写入低谷期（如凌晨）运行；给 clustering 作业设置独立的资源队列，避免影响实时写入的 SLA。

## Deletion Vector 的开销叠加

如果表启用了 Deletion Vector，clustering 作业除了重写数据文件，还要同步重写 DV 文件。在 20TB/天且开启 DV 的场景下，实际 IO 代价 = 数据文件重写 + DV 文件重写。

建议：在设计阶段评估是否必须开启 Deletion Vector；如果只是 append 表无更新需求，可以不开 DV 以降低 clustering 开销。

## 5 .3 配置与调度参考


```
CREATE TABLE log_events (
  user_id    BIGINT,
  event_type STRING,
  ts         TIMESTAMP,
  ...
) PARTITIONED BY (dt STRING, hr STRING)
WITH (
-- clustering 基础配置
'append.clustering.enable'     = 'true',
'clustering.columns'           = 'user_id,event_type',  -- 选高过滤性字段，不超过 2~3 个
'clustering.incremental'       = 'true',
'clustering.incremental.mode'  = 'local-sort',          -- 先 local-sort，按需切 global-sort

-- 文件大小控制，避免小文件爆炸
'target-file-size'             = '256mb',
'compaction.max.file-num'      = '50'
);

```


调度策略：


```
实时写入（Flink Streaming）：持续运行
    ↓ 每小时产生约 830GB 新 L0 文件

Clustering 作业（Batch）：每 1~2 小时运行一次（低谷期加密）
    CALL sys.compact('db.log_events', 'action' => 'clustering');

全量 Compaction（Batch）：每周对 7 天前的分区运行一次
    CALL sys.compact('db.log_events', 'partitions' => 'dt=...,hr=...', 'full-compaction' => 'true');

```


## 5 .4 监控要点

| 监控项 | 告警阈值参考 | 含义 |
| --- | --- | --- |
| 每个分区 L0 文件数 | > 500 | clustering 跟不上写入，文件堆积 |
| clustering 作业单次耗时 | > 写入间隔 | 产能不足，需加频次或资源 |
| 查询扫描文件数 | 无下降趋势 | clustering 效果未生效，检查 key 选择 |
| clustering 作业 IO 写入量 | 接近 20TB | 可能退化为全表重写，检查 level 分布 |

---

## 六、并发场景与使用建议

| 场景 | 建议 |
| --- | --- |
| Batch 读 + Clustering | 安全，快照隔离保证一致性读，无需额外配置 |
| Streaming 读 + Clustering | 配置`consumer-id`，防止正在消费的快照被过期清理 |
| 并发写 + Clustering 同一分区 | 避免，clustering 的 OVERWRITE 走强制冲突检查，两者无法真正并发提交同一分区 |
| 关注 manifest 合并程度 | 合理配置`manifest.target-file-size` 和`manifest.merge-min-count`，避免 manifest 文件跨分区范围过大 |

---

## 七、总结

Incremental Clustering 的核心是把布局优化从「一次性全表重写」转变为「持续低成本的增量维护」。通过以下三点来保证：

- **机制**：借鉴 LSM 分层，低层乱序文件优先重排、高层稳定数据不触碰，每轮只动一小批候选文件——这把写放大压在低层，是「低成本」的来源。
- **执行**：排序重写产出新文件，再以新快照原子替换旧文件，旧文件本身不被修改——这决定了它和普通写一样走快照提交，是后面所有并发行为的根。
- **并发**：正因为旧文件不动、替换走快照，快照隔离让读写不互扰，manifest 分区粗过滤让跨分区聚簇互不影响；而「替换一个范围」的语义，也带来了同分区并发写冲突与流读跳变这两个需要规避的风险。
- **落地**：增量换来了低成本，但也意味着新数据不立即生效、必须独立调度聚簇作业跟上写入。20TB/天的最大风险是 L0 文件堆积，靠聚簇频次、分区粒度、资源隔离与监控兜底。

设计原则：Incremental Clustering 只优化 clustering key 上的过滤查询、只覆盖 append 的 unaware-bucket 表（主键表走 PK Clustering Override），不是全路径等比加速。

落地最小行动集——选 2~3 个高过滤性字段作 key、先用`local-sort` 验证收益、聚簇作业跑在写入低谷期并设独立资源队列、把每分区 L0 文件数与聚簇耗时纳入告警。

[#Paimon实战](javascript:;) [#Clustering原理剖析](javascript:;)
