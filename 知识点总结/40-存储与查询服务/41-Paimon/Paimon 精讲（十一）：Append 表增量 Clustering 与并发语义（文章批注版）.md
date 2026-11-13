---
title: "Paimon 精讲（十一）：Append 表增量 Clustering 与并发语义（文章批注版）"
aliases:
  - "Paimon 精讲 （十一）：Append表查询越来越慢？Paimon 增量 Clustering 的低成本解法"
author: "胖泽的技术笔记"
source_url: "https://mp.weixin.qq.com/s/w6a6sjDcDels1qBxXMCHmg"
published_at: "2026-11-08T11:28:04+08:00"
organized_at: "2026-11-13"
reviewed_source_commit: "245680119a4efee32a50d4abb8953624e8272ce6"
type: "文章批注"
tags: [Paimon, Paimon-精讲, Append, Clustering, Compaction]
---

# Paimon 精讲（十一）：Append 表增量 Clustering 与并发语义（文章批注版）

来源：[微信原文](https://mp.weixin.qq.com/s/w6a6sjDcDels1qBxXMCHmg)，胖泽的技术笔记，2026-11-08。整理日期：2026-11-13。

整理说明：按原文章节保留七章正文、8段代码/流程示例、6张表格、字段、配置与20TB/天案例。原网页没有正文技术图片，仅有封面及分享封面，均本地保存。新内容明确标为“批注”“勘误”或“版本边界”；保留原有错误示例以便对照，不将其静默改成正确配置。

阅读边界：原文未声明 Paimon/Flink/Spark 版本。复核使用1.4与2.1官方文档，并将源码固定到 Apache Paimon 提交 `245680119a4e`；开发分支实现不等于用户实际部署版本。本笔记没有运行文中SQL或作业。

增量 Clustering 重写选中的物理文件来改善布局，预期不改变表的逻辑行。理解并发时，要分别检查物理文件增删、Snapshot提交类型和下游行级变更；原文把这三层混在一起，2.4与3.4的勘误会影响后续使用判断。20TB/天换算与配置修订分别见5.1和4.1。

[[知识点总结/40-存储与查询服务/41-Paimon/00-Paimon导航|返回Paimon导航]] · [[知识点总结/99-images/paimon-incremental-clustering-20261108/来源与覆盖核对|来源副本与覆盖核对]]

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

批注：Clustering 改变数据在物理文件中的布局，目标是让过滤查询少读文件；它不负责主键合并，也不保证查询结果按 clustering key 输出。只有读取器能利用相应统计、谓词可以下推且分布改善时，布局变化才可能转为扫描收益。判断效果应检查同一数据范围和查询的实际扫描文件/字节，而不只看 level 升高或文件数减少。

传统做法是对全表做一次性重排，代价随表大小线性增长，无法在持续写入的实时链路中复用。Paimon 的 Incremental Clustering 把布局优化从「一次性离线重写」转变为「持续低成本的增量维护」：借鉴 LSM 的分层思想，每轮只处理一小批候选文件，多次运行后整体布局渐进收敛。

---

## 二、Clustering 的工作机制

### 2.1 写入与聚簇解耦

Incremental Clustering 不是写入时实时触发，而是采用「持续写入 + 周期性聚簇作业」的分离架构：


```text
Flink/Spark Streaming 持续写入 append 表
         ↓
   数据文件（布局未排序，处于低层）
         ↓
   周期性运行 clustering 作业
         ↓
   数据布局逐步收敛到最优

```

版本边界：原文描述的是独立维护作业模式。1.4 文档的 unaware-bucket 增量聚簇需要周期性批作业，并由该路径处理小文件合并；不要同时套用常规 compaction 的运行假设。当前开发源码还定义了 `clustering.incremental.optimize-write`，所以“新文件绝不会在写入阶段聚簇”不能推广到所有版本和配置。[1.4 文档](https://paimon.apache.org/docs/1.4/append-table/incremental-clustering/)、[配置定义](https://github.com/apache/paimon/blob/245680119a4efee32a50d4abb8953624e8272ce6/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#L2635-L2640)。


实时写入照常进行，新文件不会被立即排序。Clustering 作业运行时按层级选取部分候选文件做排序重写，而非全表重排。

### 2.2 借鉴 LSM 分层思想：只动该动的文件

LSM Tree 把数据文件组织成多个层级（Level）：

- 越低层：越新、越小、越乱、越容易被重写。
- 越高层：越老、越大、越稳定、不轻易动。

LSM 的 Universal Compaction 核心思想是：每次只挑「值得合并」的一组文件，而不是全部重排。判断依据通常是文件大小比例或 sorted run 数量阈值。相比 Leveled Compaction，写放大更低。

Paimon 借鉴这一思路：为每个数据文件维护一个层级标记（level），用同样的分层逻辑决定本轮动哪些文件：

- level=0 的文件：每个是独立的 sorted run（新写入的乱序文件）。
- level>0 的文件：同层文件合并视为更稳定的 sorted run。

每轮 Clustering 只从层级结构中挑出候选文件做排序重写，其他文件不动。


```text
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

读图勘误：这是原文的教学示意，不能当作每次运行必然发生的文件选择。第二轮没有交代 F5、F6 的去向，不能据图认为它们已经消失；它们如果没被选择，仍应存在于新快照。源码把 L0 每个文件作为一个 run，而同一非零 level 的文件整体作为一个 run；原图把 L1 两个产物分别画出来，不代表规划器会将同层每个文件识别成独立 run。输出 level 由选择策略确定，不保证严格按 L0、L1、L2 逐轮晋升。[层级构造源码](https://github.com/apache/paimon/blob/245680119a4efee32a50d4abb8953624e8272ce6/paimon-core/src/main/java/org/apache/paimon/append/cluster/IncrementalClusterManager.java#L182-L205)。


### 2.3 增量聚簇为什么优于全表重排

**全表重排**：每次都重写整表，单次代价随表大小线性增长。更关键的是，已经排好序的稳定数据每轮也会被重复重写——这部分是纯浪费；而实时链路需要持续维护布局，这个线性代价是反复支付的。TB 级数据反复重写，代价无法接受。

**增量聚簇**：每轮只处理「新写入的低层文件」或「两个相邻 sorted run」，单次代价只与本轮候选文件大小成正比，可控。高层稳定数据永远不被触碰，写放大集中在低层，整体大幅降低。

勘误：“高层稳定数据永远不被触碰”不成立。高层文件只是更少被选择；候选集合可能包含高层 run，full 策略也可能覆盖整个目标维护单元。原文“只处理两个相邻 run”也不是固定约束。是否重写由策略、run 数量及相对大小决定，还可能本轮没有候选。排序耗时受数据量、倾斜、排序算法、spill 和 Shuffle 等共同影响，不能把候选大小与耗时简单视为严格线性关系。[PIP-36](https://cwiki.apache.org/confluence/spaces/PAIMON/pages/386272140/PIP-36+Introduce+Incremental+Clustering+for+Paimon+Append+Table)、[选择策略源码](https://github.com/apache/paimon/blob/245680119a4efee32a50d4abb8953624e8272ce6/paimon-core/src/main/java/org/apache/paimon/append/cluster/IncrementalClusterStrategy.java#L54-L89)。

### 2.4 执行本质：排序重写 + 原子替换提交

理解 Clustering 的并发行为，关键在于它做完之后怎么落盘。一个常见的误解是把聚簇想成「把现有文件里的数据原地排个序」——其实不是。Clustering 不修改任何已有文件，它做的是两件事的组合：

**第一步：排序重写（产出新文件）。** 按增量策略挑出本轮候选文件（低层乱序文件），读出其数据、按 clustering key 排序，写成一批新的、内部有序的数据文件。这一步只产出新文件，旧文件一个字节都不动。

**第二步：原子替换提交（用新文件换掉旧文件）。** 把这次重写登记成一次提交，语义上等价于 SQL 的`INSERT OVERWRITE`：把本轮要替换的旧文件登记为 DELETE 条目，把新排序文件登记为 ADD 条目，在一个新 Snapshot 里原子完成「旧文件 → 新文件」的替换。提交要么整体生效、要么不生效，不存在「换了一半」的中间状态。

勘误：物理文件“删旧、加新”不等于 SQL 分区覆盖。对本次核对的固定源码，Spark 的 `clusterIncrementalUnAwareBucketTable` 明确不使用 overwrite，而把选中旧文件、新产物及索引变更组成 `CompactIncrement`，且 `DataIncrement` 为空；提交器对相应 compaction 变更发布 `COMPACT`。因此不能把增量 Clustering 一概记成 `OVERWRITE`。[Spark 路径](https://github.com/apache/paimon/blob/245680119a4efee32a50d4abb8953624e8272ce6/paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/procedure/CompactProcedure.java#L903-L965)、[提交器](https://github.com/apache/paimon/blob/245680119a4efee32a50d4abb8953624e8272ce6/paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java#L407-L422)。

批注：这里必须区分三个层面：Manifest 的文件条目增删、Snapshot 的 `commit_kind`、下游实际接收的行级变更。文件条目中的 DELETE/ADD 表示文件集合替换，不等于业务记录被删除后重新插入，也不能单凭它推导 Flink 的 `RowKind`。

> 全量模式与增量模式的区别只在重写范围：全量聚类重写整个目标分区的文件；增量聚类只重写本轮候选文件，未被选中的高层稳定文件不被重写，数据原样保留到新快照。

这个执行本质直接决定了第三章的全部并发行为：

| 这一本质 | 导致的并发后果 |
| --- | --- |
| 旧文件不被原地修改，替换靠生成新快照完成 | 绑定旧快照的读取不受影响（→ 3.1 快照隔离） |
| 提交是「替换一个范围内文件」的语义 | 另一写操作并发动了同一范围时，冲突检查会发现并重试或失败（→ 3.4 同分区并发写） |
| 快照里是「删旧 + 加新」的替换 | 流读跨过该快照时，看到旧文件消失、新文件出现的「数据跳变」（→ 3.4 streaming 风险） |

一句话：Clustering 不是原地排序，而是「读出来排序写成新文件，再用新快照原子替换旧文件」。正因为旧文件不动、替换走快照，才有了读写隔离；也正因为替换是「删一片 + 加一片」的范围操作，才有了同分区并发冲突和流读跳变。

复核说明：上表第一行的“旧快照仍引用旧文件”可用于理解读取视图；第二、三行及本段末尾的推导不能仅由文件替换得出。并发冲突还要看双方实际替换哪些文件、提交检查及重试机制；流读要看提交类型与扫描器。3.4 和第六章的相关建议需连同下面的批注阅读。

---

## 三、读写并发与隔离保障

### 3.1 快照隔离（MVCC）：读写不互扰

Paimon 基于快照隔离实现读写分离，原理类似数据库 MVCC：

- 每次读取绑定一个固定的 Snapshot ID，读的是该快照时刻 manifest 描述的数据文件列表。
- 写操作（普通写、compact、clustering）生成新 Snapshot，不修改或删除旧快照引用的文件。

正在进行的读取不会被 clustering 写入打断，读到的始终是 clustering 开始前那个快照的数据。

边界说明：批读固定快照的时间点是本次查询规划读取快照时，不一定早于 clustering 开始；如果维护已经提交，随后规划的查询可以读到新布局。已有查询能继续读旧文件，还要求文件在其读取期间未被过期清理。快照一致性保证的是读取视图，不保证 CPU、网络、磁盘或对象存储请求不受其他作业影响。

### 3.2 分区级隔离：聚簇一个分区不影响读取另一分区

这是性能层面的关键结论：读`dt=20260710` 的同时 clustering`dt=20260712`，clustering 不会变慢，分区之间完全隔离。

勘误：过滤不同分区可以缩小处理范围，但不能推出“不会变慢、完全隔离”。两个作业仍可能共享计算槽位、磁盘、网络、对象存储带宽和元数据服务；即使没有逻辑冲突，也可能发生资源争用。要比较耗时，必须把资源分配和实际 I/O 一起检查。

原因在于 manifest 文件的分区级粗过滤。每个 manifest 文件维护了所含分区的统计信息（min/max），读取 manifest 列表时先做粗过滤——只要某个 manifest 不包含目标分区，就直接跳过、不打开读取内容。


```text
clustering 处理 dt=20260712 时的过滤路径：
  只过滤目标分区 dt=20260712
        ↓
  逐个检查 manifest 的分区统计
        ├── manifest 只含 dt=20260710 → 直接跳过，不读内容
        ├── manifest 只含 dt=20260711 → 直接跳过
        └── manifest 含 dt=20260712   → 保留，读取文件列表

```


`dt=20260710` 的 manifest 根本不会被打开，clustering 只处理目标分区相关文件。

批注：上述跳过成立的条件是该 manifest 的分区统计能排除目标值。比如 manifest 范围仅为20260710，可排除20260712；若范围覆盖20260710到20260712，即使实际没有目标分区，也通常还需读条目再判断。min/max 是用于排除“不可能命中”的粗索引，不是精确的分区成员清单。

### 3.3 例外：manifest 跨分区合并时的二次过滤

若表的 manifest 文件经过大量合并（manifest compaction），导致单个 manifest 混合包含多个分区（min 跨到 20260710、max 到 20260712），则该 manifest 会通过粗过滤。但 manifest 内部仍会按分区做二次过滤，不会把无关分区的文件列入处理列表——不影响正确性，只会多读一点 manifest 内容。合理配置 manifest 目标文件大小与合并阈值可避免这种情况。

边界说明：调整 manifest 大小和合并阈值是在元数据数量、读取开销与统计选择性之间取舍，并不保证 manifest 不跨分区。还要区分“分区过滤范围”与“整个维护作业范围”；若启用了历史分区自动聚簇，作业可能另外选择历史分区，不能只看调用中的分区条件就断定总工作量。

### 3.4 不同并发场景的影响

| 场景 | 是否有影响 | 说明 |
| --- | --- | --- |
| Batch 读先确定 Snapshot，clustering 后完成 | 无影响 | 读的是旧快照文件 |
| Streaming 读消费到 clustering 的 OVERWRITE 快照 | 有风险 | 可能收到 DELETE+ADD，导致数据跳变 |
| Clustering 进行中，另一 INSERT 并发写同一分区 | 有冲突 | OVERWRITE 提交做强制冲突检查，会重试或失败 |
| 旧快照过期且未配置 consumer-id | 有风险 | 长时间运行的读任务引用的旧文件被删除，抛异常 |

并发语义勘误：这张表的 Streaming 和同分区 INSERT 两行不能作为通用结论。在已核对的增量路径中，Clustering 产生 `COMPACT`；普通 `DeltaFollowUpScanner` 只扫描 `APPEND`，不会把每个重写文件再当作新增业务行消费。只有进一步确认读取模式及真实 `commit_kind`，才能判断下游行为。[流读扫描器](https://github.com/apache/paimon/blob/245680119a4efee32a50d4abb8953624e8272ce6/paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/DeltaFollowUpScanner.java#L36-L53)。

并发语义批注：提交同一分区也不必然发生文件冲突。纯 append 增加新文件与维护已选旧文件，可以是不同文件集合；两个维护任务替换同一旧文件、或者与覆盖/删除操作重叠时，才需要结合冲突规则判断。官方区分“快照编号被抢占后重试”和“待移除文件已被其他作业移除”两类冲突。[并发控制文档](https://paimon.apache.org/docs/1.4/concepts/concurrency-control/)。本笔记未做并发运行验证，不承诺任意组合都能成功。

---

## 四、实时场景的使用方式与限制

### 4.1 配置与调度


```sql
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

代码勘误：这是原文示例，不可直接执行。`CREATETABLE` 应为 `CREATE TABLE`，`...` 是省略号；`append.clustering.enable` 未在本次核对的 `CoreOptions` 定义中找到，不能将它当作已确认的启用开关。官方启用项是 `clustering.incremental=true` 与非空 `clustering.columns`，unaware-bucket 示例应明确 `bucket=-1`。`sys.compact` 的参数也不能混写为原文的 `'action' => 'clustering'`。[配置源码](https://github.com/apache/paimon/blob/245680119a4efee32a50d4abb8953624e8272ce6/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#L2592-L2618)、[Spark Procedure 参数](https://paimon.apache.org/docs/1.2/spark/procedures/)。

修订示例（Spark SQL；按官方2.1文档对应入口整理，要求会话已配置并选中 Paimon Catalog；未在实际引擎运行）：

```sql
CREATE TABLE mydb.log_events (
  user_id BIGINT,
  event_type STRING,
  ts TIMESTAMP,
  dt STRING,
  hr STRING
) USING paimon
PARTITIONED BY (dt, hr)
TBLPROPERTIES (
  'bucket' = '-1',
  'clustering.incremental' = 'true',
  'clustering.columns' = 'user_id,event_type',
  'target-file-size' = '256 mb'
);

CALL sys.compact(
  table => 'mydb.log_events',
  partitions => 'dt=20261108,hr=00',
  compact_strategy => 'minor',
  options => 'clustering.incremental.mode=local-sort'
);
```

版本边界：这里的 SQL 是 Spark SQL，不应原样粘进 Flink SQL Client；原文的 `WITH` 则是另一种 DDL 方言。1.4文档列的是 Spark Procedure 和 Flink Action 两种维护入口，local-sort 等选项需按实际连接器版本确认。若部署版本或厂商分支不同，应先核对支持范围。上述分区条件约束普通选择范围；历史分区自动聚簇需另行核查是否开启。[2.1用法](https://paimon.apache.org/docs/2.1/append-table/incremental-clustering/)。


工作机制：实时写入照常进行，新文件不会被立即排序；Clustering 作业运行时按文件层级选取部分候选文件做排序重写；每次只处理小范围文件，多次运行后整体布局渐进收敛。

### 4.2 使用限制

| 限制 | 说明 |
| --- | --- |
| 表类型限制 | 主要针对 append 的 unaware-bucket 表；主键表有单独的 PK Clustering Override |
| 非实时生效 | 是逐步收敛，对刚写入的新数据不立即生效 |
| 需要周期性维护 | 必须有独立的 clustering 作业调度，否则布局不会改善 |
| 不是万能加速 | 仅优化 clustering key 对应过滤查询的性能，不会让所有查询等比变快 |
| 写放大存在 | 每轮对候选文件做排序重写，远低于全量重排，但仍有代价 |
| 与 compaction 耦合 | 需与 compaction 策略合理规划，避免相互干扰 |

版本勘误：1.4文档明确限制为 unaware-bucket Append 表；当前2.1文档还列出有条件支持的 bucketed Append 路径，要求关闭 `bucket-append-ordered`，且 DV 等限制不同。因此“只支持 unaware-bucket”应绑定版本。主键表的 PK Clustering Override 是另一套机制，不能用它证明本篇增量维护配置可直接用于主键表。[1.4范围](https://paimon.apache.org/docs/1.4/append-table/incremental-clustering/)、[2.1范围](https://paimon.apache.org/docs/2.1/append-table/incremental-clustering/)。

### 4.3 适用与不适用场景

**适合**：Flink 实时写入的日志/事件大表，按`user_id`、`region` 等维度定期聚簇；明细宽表写入后按高频查询列做布局优化；不希望为查询优化做全量 rewrite 的场景。

**不适合**：对写入后立即查询有强实时性要求（新数据尚未被 clustering）；高频整行更新的主键表；对所有查询路径都有等均加速需求的场景。

边界说明：新数据尚未聚簇，不等于查询看不到新数据。普通写入完成提交后，数据可按读取模式被访问；尚未发生的是布局优化。对新鲜度敏感但可接受扫描开销的查询，仍可读取；若要求刚落盘的数据立即具备较好裁剪效果，才需要另外设计布局和延迟预算。

---

## 五、大规模实践（20TB/天）

### 5.1 核心挑战：写入速度与聚簇产能的平衡


```text
20TB/天 ≈ 230GB/小时 ≈ 4GB/分钟

每一批写入 → 若干新 L0 文件（unaware-bucket，无分桶限制）
clustering 作业未及时跟上 → L0 文件持续堆积
堆积到一定程度 → min/max 统计失效 → 查询性能退化

```

算术勘误：按十进制单位，20TB/天是 `20000/24 ≈ 833.33 GB/小时`、约 `13.89 GB/分钟`、约 `231.48 MB/秒`。原文230GB/小时、4GB/分钟与20TB/天不符；后文每小时约830GB才接近这个量级。还需说明20TB是源端逻辑量、压缩后落盘量还是其他统计口径，才能估算读写带宽。

批注：聚簇产能不能只对照新增字节，还要包括旧 run 再次入选的重写量、读开销、排序和临时空间。即使维护周期不变，也可能因候选选择变化而出现重写峰值；应记录每轮实际选中与输出的字节、文件数和积压变化。


写入速度与 clustering 产能的平衡是 20TB/天场景的最大风险。

### 5.2 五个关键问题

#### 5.2.1 L0 小文件堆积

unaware-bucket 表没有 bucket 限制，每次写入都会落成新的 L0 文件。如果 clustering 作业间隔过长，L0 文件数量持续增长，每个 L0 文件是独立的 sorted run，min/max 范围互相重叠，查询时需要扫描大量文件，数据跳过失效。

建议：评估 clustering 作业运行间隔，确保每轮能消化掉当轮写入产生的 L0 文件。

边界说明：不要求每轮都清空所有 L0；文件选择还受触发阈值和 run 结构影响。判断维护能力应看多个周期的积压趋势和查询扫描表现。增加频次也不自动增加吞吐：若作业本已饱和或互相重叠，只会增加资源和提交争用。

#### 5.2.2 local-sort 与 global-sort 的取舍

`clustering.incremental.mode` 控制排序策略：

| 模式 | 做法 | 代价 | 效果 |
| --- | --- | --- | --- |
| local-sort | 每个 task 独立排序本地数据 | 低，无 shuffle | 同一文件内有序，跨文件不保证 |
| global-sort | 全局 shuffle + 排序 | 高，有完整 shuffle | 跨文件真正有序，数据跳过最优 |

排序范围批注：global-sort 的“全局”是本轮选中工作集在分区内的协调布局，不代表整张表或所有已有文件都变成全局有序；local-sort 省掉全局 range shuffle，也不等于整个作业绝无网络传输。`clustering.strategy` 可选择 order、zorder、hilbert，多列聚簇不应一律理解为普通 SQL 多列字典序。扫描收益仍取决于具体查询，不能无条件称为“最优”。[master排序说明](https://paimon.apache.org/docs/master/append-table/incremental-clustering/)。

20TB/天场景用 global-sort，等于做一次大规模 shuffle，资源消耗不容忽视。

建议：先用 local-sort 跑通，观察查询性能收益；如果收益不够再评估 global-sort 的成本是否可接受。

#### 5.2.3 分区粒度决定聚簇效率


```text
按天分区：每天一个分区 = 20TB/分区 → 每次 clustering 候选文件非常多
按小时分区：每小时约 830GB/分区 → 粒度合理，但分区数增多

```


历史分区越多，元数据管理压力越大（历史分区会额外处理 clustering 任务）。

建议：分区粒度推荐按小时或按天加子分区（如`dt/hour`），避免单分区文件数过多；定期对历史分区做一次`fullCompaction`，让历史数据稳定在高层，减少 clustering 反复处理的范围。

批注：按小时分区是原文容量例子，不是通用推荐。需同时考虑常见查询是否带小时条件、每分区数据量、文件数量及元数据开销。历史分区也不是只要存在就自动重写；自动历史聚簇有独立启用条件。维护已启用增量聚簇的表时，应确认走的是保留聚簇语义的 full 路径，不能把普通 full compaction 和 full clustering 当作同义操作。

#### 5.2.4 聚簇作业与写入作业的资源竞争

Clustering 当前主要是 batch 作业，而实时写入是流作业，二者在同一集群上会竞争资源：clustering 需要读候选文件、排序（可能 shuffle）、写回新文件，而 20TB/天的写入链路本身就需要大量 IO 和计算。

建议：把 clustering 作业安排在写入低谷期（如凌晨）运行；给 clustering 作业设置独立的资源队列，避免影响实时写入的 SLA。

#### 5.2.5 Deletion Vector 的开销叠加

如果表启用了 Deletion Vector，clustering 作业除了重写数据文件，还要同步重写 DV 文件。在 20TB/天且开启 DV 的场景下，实际 IO 代价 = 数据文件重写 + DV 文件重写。

建议：在设计阶段评估是否必须开启 Deletion Vector；如果只是 append 表无更新需求，可以不开 DV 以降低 clustering 开销。

DV批注：原文指出索引维护可能产生额外开销，这个方向需要纳入预算。源码确实对启用 DV 的 unaware-bucket 路径处理索引变更，但实际成本还包含读数据、应用删除、写产物及相关索引，不宜用单一相加式代替完整I/O测量。也不能仅因表名为 Append 就关闭 DV：先确认是否存在行级删除及其他依赖，再决定配置。[DV处理入口](https://github.com/apache/paimon/blob/245680119a4efee32a50d4abb8953624e8272ce6/paimon-core/src/main/java/org/apache/paimon/append/cluster/IncrementalClusterManager.java#L209-L238)。

### 5.3 配置与调度参考


```sql
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

配置勘误：原文仍含 `...`，且把分区字段写成 `PARTITIONED BY (dt STRING, hr STRING)`，不可作为通用 Flink/Spark DDL 使用；分区字段应先在列定义中声明，再按引擎语法引用。`compaction.max.file-num` 未在已核对的 CoreOptions 中找到，不应据此承诺“每轮最多50文件”；`target-file-size` 是目标大小，也不是每个输出文件都固定256MB。可用的最小修订示例见4.1，其他参数须核对部署版本。[CoreOptions](https://github.com/apache/paimon/blob/245680119a4efee32a50d4abb8953624e8272ce6/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java)。


调度策略：


```text
实时写入（Flink Streaming）：持续运行
    ↓ 每小时产生约 830GB 新 L0 文件

Clustering 作业（Batch）：每 1~2 小时运行一次（低谷期加密）
    CALL sys.compact('db.log_events', 'action' => 'clustering');

全量 Compaction（Batch）：每周对 7 天前的分区运行一次
    CALL sys.compact('db.log_events', 'partitions' => 'dt=...,hr=...', 'full-compaction' => 'true');

```

调度勘误：原文的 `action => clustering` 和 `full-compaction => true` 不是所核对 Spark `sys.compact` 的对应参数。增量与全量选择应使用该版本的 `compact_strategy`，在已启用 clustering 的表上区分 `minor` 与 `full`。即使请求 full，源码也会在无数据或最高层单run且聚簇列未变等条件下跳过重写。[Spark Procedure](https://paimon.apache.org/docs/1.2/spark/procedures/)、[full选择逻辑](https://github.com/apache/paimon/blob/245680119a4efee32a50d4abb8953624e8272ce6/paimon-core/src/main/java/org/apache/paimon/append/cluster/IncrementalClusterStrategy.java#L54-L89)。

批注：每1–2小时、7天前分区和低谷期运行都是原文调度示例。应由持续写入速率、实际重写产能、允许积压和查询需求确定周期；如果任务运行时间已接近调度间隔，应先检查产能和重叠，不能仅靠“加密调度”解决。


### 5.4 监控要点

| 监控项 | 告警阈值参考 | 含义 |
| --- | --- | --- |
| 每个分区 L0 文件数 | > 500 | clustering 跟不上写入，文件堆积 |
| clustering 作业单次耗时 | > 写入间隔 | 产能不足，需加频次或资源 |
| 查询扫描文件数 | 无下降趋势 | clustering 效果未生效，检查 key 选择 |
| clustering 作业 IO 写入量 | 接近 20TB | 可能退化为全表重写，检查 level 分布 |

监控口径批注：L0大于500没有脱离文件大小和查询范围的通用含义；“单次耗时大于写入间隔”也需先定义是维护调度间隔、源写入批次间隔还是其他周期。单轮重写20TB可能等于一个日分区，也可能只是全表很小一部分，不能据此直接判断退化为全表重写。应同时记录候选范围、原始/输出文件、commit_kind、实际读写量以及同口径的查询扫描结果。

---

## 六、并发场景与使用建议

| 场景 | 建议 |
| --- | --- |
| Batch 读 + Clustering | 安全，快照隔离保证一致性读，无需额外配置 |
| Streaming 读 + Clustering | 配置`consumer-id`，防止正在消费的快照被过期清理 |
| 并发写 + Clustering 同一分区 | 避免，clustering 的 OVERWRITE 走强制冲突检查，两者无法真正并发提交同一分区 |
| 关注 manifest 合并程度 | 合理配置`manifest.target-file-size` 和`manifest.merge-min-count`，避免 manifest 文件跨分区范围过大 |

使用建议勘误：本表需与2.4、3.4的修正合读。“Batch读安全”仍受文件保留和资源条件限制；“同分区写无法并发提交”不能覆盖所有增量Clustering与纯append组合；流读配置 `consumer-id` 也只解决进度登记及相应保留问题，不会改变提交种类或生成行级撤回。

Consumer边界：官方 Consumer ID 用于记录流式消费进度、影响依赖快照的过期及从已记录位置续读；还需检查 consumer 生命周期、实际推进和清理策略。它不是任意长批查询的自动保护开关。[Consumer ID](https://paimon.apache.org/docs/1.4/flink/consumer-id/)。对于真正的 Append `INSERT OVERWRITE`，官方说明默认流读跳过覆盖提交，开启 `streaming-read-append-overwrite` 时读新增行且不撤回被替换旧行；这也不同于原文泛称的 `DELETE+ADD`。[Append流读说明](https://paimon.apache.org/docs/master/append-table/streaming/)。

---

## 七、总结

Incremental Clustering 的核心是把布局优化从「一次性全表重写」转变为「持续低成本的增量维护」。通过以下三点来保证：

- **机制**：借鉴 LSM 分层，低层乱序文件优先重排、高层稳定数据不触碰，每轮只动一小批候选文件——这把写放大压在低层，是「低成本」的来源。
- **执行**：排序重写产出新文件，再以新快照原子替换旧文件，旧文件本身不被修改——这决定了它和普通写一样走快照提交，是后面所有并发行为的根。
- **并发**：正因为旧文件不动、替换走快照，快照隔离让读写不互扰，manifest 分区粗过滤让跨分区聚簇互不影响；而「替换一个范围」的语义，也带来了同分区并发写冲突与流读跳变这两个需要规避的风险。
- **落地**：增量换来了低成本，但也意味着新数据不立即生效、必须独立调度聚簇作业跟上写入。20TB/天的最大风险是 L0 文件堆积，靠聚簇频次、分区粒度、资源隔离与监控兜底。

设计原则：Incremental Clustering 只优化 clustering key 上的过滤查询、只覆盖 append 的 unaware-bucket 表（主键表走 PK Clustering Override），不是全路径等比加速。

落地最小行动集——选 2~3 个高过滤性字段作 key、先用`local-sort` 验证收益、聚簇作业跑在写入低谷期并设独立资源队列、把每分区 L0 文件数与聚簇耗时纳入告警。

全文适用边界：以上是原文建议，不能绕过前述版本、配置和提交语义修正。对具体表，应先确认表模式与读写路径，再用限定分区的候选维护验证逻辑结果保持一致，并比较相同查询与数据范围的扫描成本。本文只完成文章整理及静态资料核对，没有执行建表、Clustering、并发写入或性能测试。


## 关联阅读

- [[知识点总结/40-存储与查询服务/41-Paimon/Paimon 精讲（九）：Append 表的 Bucket Key 与文件索引（文章批注版）|精讲九：Append Bucket与索引]]。
- [[知识点总结/40-存储与查询服务/41-Paimon/Paimon 精讲（十）：Flink Sink 并发、Bucket 路由与写入冲突（文章批注版）|精讲十：Sink并发与写入冲突]]。
- [[知识点总结/40-存储与查询服务/41-Paimon/07-Paimon L0、L1、Snapshot 与查询裁剪|Level、Snapshot与查询裁剪]]。
- [[知识点总结/40-存储与查询服务/41-Paimon/12-Paimon Snapshot、Manifest、Parquet 与 Compaction 的关系|Snapshot、Manifest与Compaction]]。
- [[知识点总结/40-存储与查询服务/41-Paimon/17-Paimon 流式读写中的 Snapshot 与 Changelog|流读Snapshot与Changelog]]。
