

> [!info] 来源与阅读范围
> 原文：[《Paimon 精讲（三）：Partition 和 Snapshot 傻傻分不清？一文搞定》](https://mp.weixin.qq.com/s/zmwgd4-7f1qrul_RcfUGBA)，胖泽的技术笔记，2026-07-25。本文按原文的写入、读取和清理顺序整理，并用注释解释各步发生了什么；具体配置和元数据语义以文末 Paimon 官方文档为准。

> [!abstract] 一分钟记忆
> Partition 是表内按字段值划出的数据范围，例如 `dt=2026-07-10`；Snapshot 是整张表一次成功提交后的版本。查询先确定 Snapshot，再在该版本中按 Partition 裁剪文件。同一 Partition 可以跨多个 Snapshot 保持存在，也可以在写入或 Compaction 后换成另一组文件。旧 Snapshot 过期只会清理不再被存活版本或 Tag 等引用的文件；最新 Snapshot 仍引用的历史分区不会仅因 Snapshot 保留期短而丢失。

## Partition 与 Snapshot 各管什么

| 对象 | 作用范围 | 存储中的样子 | 查询时解决的问题 |
| --- | --- | --- | --- |
| Partition | 某组分区字段值对应的数据范围 | 数据文件通常位于该分区目录；分区信息也记在 Manifest 条目中 | 跳过不相关的分区 |
| Snapshot | 一次成功提交后的整张表 | `snapshot/snapshot-N` 版本文件及其元数据引用 | 固定“读哪个时刻的整张表” |

原文“Partition 管空间、Snapshot 管时间”的比喻可以用，但不能理解为“每个分区都有自己的 Snapshot”。同一个 Snapshot 可同时改变多个分区；同一分区在两个 Snapshot 中可能指向不同 Data File。

分区字段写在表定义中，例如 `PARTITIONED BY (dt)`。写入 `dt=2026-07-10` 的记录时，Paimon 通常把数据文件放到对应分区目录，再按 Bucket 分组。Partition 解决的是“记录属于哪一块数据、查询能否跳过其他块”。分区字段也可以用于数据生命周期管理。[官方数据文件布局](https://paimon.apache.org/docs/master/concepts/spec/datafile/)

Snapshot 是表级版本，不是某个分区的备份。一次提交可以修改多个分区；同一个分区也会在不同 Snapshot 中呈现不同的文件集合。Snapshot 文件记录 Schema、提交信息以及 Manifest 的引用，本身不保存业务行。读取时，Paimon 根据这些引用找到该版本有效的 Data File。[官方 Snapshot 规格](https://paimon.apache.org/docs/master/concepts/spec/snapshot/)

> [!note] 通俗注释：同一张“文件清单”的不同版本
> 可以把分区理解成清单上的分类栏，把 Snapshot 理解成清单的某次正式发布。`dt=2026-07-10` 这个分类没有因为发布了新清单就消失；变化的是新清单列出了哪些文件。Snapshot 也不是把整张表的数据复制一份，它主要发布文件引用。

对象的关系不是只有一根箭头。原文把 Snapshot 到数据文件拆成下面两层，阅读后续流程时可以按文件名一层层往下找：

```text
Snapshot（表级版本文件）
├─ baseManifestList  → Manifest List（继承的 Manifest 引用）
│                       ├─ ManifestFileMeta
│                       │   ├─ fileName          实际 Manifest 文件名
│                       │   ├─ partitionStats    覆盖的分区范围，可做粗过滤
│                       │   ├─ numAddedFiles     ADD 条目数量
│                       │   └─ numDeletedFiles   DELETE 条目数量
│                       └─ 更多 ManifestFileMeta ...
├─ deltaManifestList → Manifest List（本次提交的文件变化）
│                       └─ 同样包含 ManifestFileMeta
└─ changelogManifestList（存在变更日志时才有相应引用）

ManifestFile（由上面的 fileName 找到）
└─ ManifestEntry（一个数据文件的增删事件）
   ├─ kind       ADD / DELETE
   ├─ partition  分区值，例如 dt=20260710
   ├─ bucket     桶编号，例如 0
   └─ file       DataFileMeta（被增删的数据文件的元数据）
      ├─ fileName       数据文件名
      ├─ fileSize       文件大小
      ├─ rowCount       记录数
      ├─ minKey/maxKey  主键表可能使用的键范围
      ├─ level          文件层级
      └─ creationTime   创建时间
```

> [!note] 读图时不要把三种“文件”混成一个
> Snapshot 文件指向 **Manifest List**；Manifest List 中的 `ManifestFileMeta.fileName` 指向 **Manifest File**；Manifest File 的 Entry 中，`DataFileMeta.fileName` 才指向存业务记录的 **Data File**。`partitionStats` 只用于判断一个 Manifest *可能*覆盖哪些分区；准确的分区值要读 Entry 的 `partition`。`numAddedFiles` 是文件事件数量，不是新增业务行数。[官方 Manifest 规格](https://paimon.apache.org/docs/master/concepts/spec/manifest/)

原文把 `baseManifestList` 称为“全部存活数据文件清单”，便于初读，但字段本身保存的是 **Manifest 引用**，不是数据文件列表；读取完整状态还需要处理本次 `deltaManifestList` 中的文件变化。`DELETE` 是在新版本中取消文件引用，不意味着立刻删除物理文件。[官方 Snapshot 规格](https://paimon.apache.org/docs/master/concepts/spec/snapshot/)

## 写入：一个分区如何进入新 Snapshot

假设表中已有 `dt=2026-07-10`，流作业又写入一批该日期的数据。Writer 先按照 Partition 和 Bucket 路由记录，再将记录落为新的 Data File。提交阶段把新文件记成 `ADD` 条目，写入 Manifest；提交成功后发布新的 Snapshot。Reader 只有选到这个新 Snapshot，才会看到新文件。

```text
写入记录：dt=20260710
  ↓ Writer 先按 partition=dt 和 bucket=0 路由
写出 Data File：data-002.orc
  ↓ 得到 DataFileMeta：文件名、大小、行数、键范围等
生成 ManifestEntry：ADD / dt=20260710 / bucket=0 / data-002.orc
  ↓ 把 Entry 写入新的 Manifest File
生成本次 deltaManifestList
  ↓ 提交器基于前一个 Snapshot 的引用，必要时合并小 Manifest
生成新版本所需的 baseManifestList 与 deltaManifestList
  ↓ 发布 Snapshot-2，指向这些 Manifest List
新的 Reader 选择 Snapshot-2 后，才读到 data-002.orc
```

原文用 `FileStoreCommitImpl.tryCommitOnce()` 和 `ManifestFileMerger.merge()` 解释提交与 Manifest 合并。它们指出的是两个动作：发布新的表级版本，以及控制元数据文件数量。**Manifest 合并不等于把业务 Data File 合并**；后者属于 Data File Compaction。类名和执行细节会随 Paimon 版本变化，图保留的是可观察的提交顺序。

把原文提交器里的几个操作拆开看：`mergeBeforeManifests` 表示先取得前一个版本留下的 Manifest 引用；新写入的文件事件进入本次 delta；Manifest 文件太零碎时，`ManifestFileMerger` 可重写和合并元数据；提交器最终发布指向新 base/delta 的 Snapshot。合并 Manifest 后，某个旧 Manifest 文件名可能不再被新版本直接引用，但其仍有效的数据文件事件会保留在新 Manifest 中。不能用“新版本没引用旧 Manifest 文件名”推断业务数据被删除。

例如旧版本已经有 `f1`，本次只在同一分区新增 `f2`：新版本的有效文件是 `{f1, f2}`。`f1` 不需要重写；本次 `deltaManifestList` 记录 `f2` 的文件变化。若提交阶段没有成功发布 Snapshot，`f2` 即使已经落盘，也不能算进入了表的可见版本。

> [!note] 通俗注释：文件先落盘，不代表数据已经可查
> Data File 可以先于 Snapshot 出现在存储中。Snapshot 提交像“正式发布目录”：它成功后，新 Reader 才按新目录读到这批文件；若提交失败，不能仅凭目录中出现文件判断数据已进入表。

Snapshot 中常见的 `baseManifestList` 指向从以前提交继承的文件变化，`deltaManifestList` 指向本次提交的文件变化。读取当前完整状态时，需要结合两者并处理 `ADD/DELETE`。不能把 `baseManifestList` 单独等同于“本 Snapshot 的全部存活文件”，也不能把 `deltaManifestList` 当作“本次新增的业务行”；它记录的是文件层的变化。[官方 Snapshot 规格](https://paimon.apache.org/docs/master/concepts/spec/snapshot/)

一次提交即使只写 `dt=2026-07-10`，新的 Snapshot 仍是整张表的版本。其他未改变的分区沿用此前可见的文件，不需要重写所有分区的数据。

## 读取：先选表版本，再找目标分区

查询 `dt=2026-07-10` 时，Reader 先固定一个 Snapshot：普通批查询通常选最新版本；时间旅行查询选指定 ID、时间或 Tag。随后读取该版本的 Manifest List。Manifest List 中的分区统计可先排除不可能包含目标分区的 Manifest；保留下来的 Manifest 再按 Entry 中的准确分区值、Bucket 和文件统计继续过滤。Paimon 合并相关 `ADD/DELETE` 后形成有效文件集合，再读取文件中的记录。[官方基础概念](https://paimon.apache.org/docs/master/concepts/basic-concepts/)、[Manifest 规格](https://paimon.apache.org/docs/master/concepts/spec/manifest/)

```text
查询：WHERE dt=20260710
  ↓ ① 固定 Snapshot ID；普通批查询一般取最新版本
读取该 Snapshot 的 base/delta Manifest List
  ↓ ② 用 ManifestFileMeta.partitionStats 做粗过滤
范围不含 dt=20260710 → 整个 Manifest File 可跳过
范围可能含 dt=20260710 → 打开 Manifest File
  ↓ ③ 检查每条 ManifestEntry.partition 的准确值
不等于目标分区 → 排除
等于目标分区 → 保留 ADD/DELETE 文件事件，按文件身份合并
  ↓ ④ 得到此版本仍有效的 DataFileMeta
按 partition、bucket 组织 DataSplit（扫描任务）
  ↓ ⑤ Reader/Executor 打开 DataSplit 所列的 Data File
读出记录；主键表还需遵守其行级合并语义
```

原文的 `DataSplit` 示例包含 `snapshotId`、`partitionValue`、`bucketId` 和文件元数据列表。它是发给读取任务的“这次去读哪些文件”的安排，不是新的数据副本。原文示例中“`kind=ADD` 就保留”要放在 **ADD/DELETE 已抵消后的有效文件集合**里理解：如果一个旧文件后来出现 `DELETE`，不能只看到早期的 `ADD` 就把它读回来。

源码定位上，原文提到 `plan()` 固定 Snapshot，再由 `ManifestsReader.filterManifestFileMeta()` 查看 `partitionStats.minValues/maxValues`。例如某个 Manifest 的分区范围是 `20260701～20260705`，查询 `dt=20260710` 时不必打开它；若范围是 `20260701～20260715`，只能说明“有可能”，还要打开 Manifest 检查 `ManifestEntry.partition`。通过 Entry 的精确过滤后才形成 DataSplit，Executor 再读物理文件。

这里有两层过滤：Manifest 统计只回答“可能有没有”，Entry 的 `partition` 才能确认文件归属。文件级 `DELETE` 用来确定该版本该读哪些文件；主键表读出文件后，还可能需要按主键和 Merge Engine 的规则得到最终逻辑行，这与文件级增删是两件事。

> [!note] 通俗注释：先查目录，再取文件
> `WHERE dt=...` 并不是先遍历对象存储里的每个分区目录。Paimon 先用 Snapshot 确定应该查哪一版文件清单，再借 Manifest 的分区统计和条目缩小范围。Manifest 统计只能做粗过滤：某个 Manifest 的最小和最大分区值覆盖目标日期，不代表里面一定有该日期的文件。

在 Flink SQL 中，可先查看保留的版本，再查指定版本的分区：

```sql
SET 'execution.runtime-mode' = 'batch';

SELECT snapshot_id, commit_kind, commit_time
FROM `orders$snapshots`
ORDER BY snapshot_id DESC;

SELECT *
FROM orders /*+ OPTIONS('scan.snapshot-id' = '103') */
WHERE dt = '2026-07-10';
```

第二条 SQL 返回 Snapshot 103 时该分区的**完整逻辑状态**，不是第 103 次提交的增量。分区条件负责选择业务范围，`scan.snapshot-id` 负责选择表版本。[官方 Flink SQL Query](https://paimon.apache.org/docs/master/flink/sql-query/)

原文把扫描分成 `ALL`、`DELTA`、`CHANGELOG` 三个内部视角：

| 原文中的视角 | 主要读取的引用 | 它回答什么问题 |
| --- | --- | --- |
| `ALL` | 当前版本的 base + delta Manifest | 此版本整张表有哪些有效文件？ |
| `DELTA` | 本次提交的 delta Manifest | 这次提交有哪些文件级变化？ |
| `CHANGELOG` | 生成了 changelog 时的 changelog Manifest | 可供下游消费的行级变更是什么？ |

`DELTA` 中的文件变化不能直接当作“业务行 CDC”：一次 Compaction 会产生文件 `DELETE/ADD`，即使业务行没有变化。`CHANGELOG` 是否存在、能否覆盖所需行级语义，取决于表的 changelog 生产配置。表中三个名称用于解释原文的源码读取视角，不是可以原样写进任意 Flink SQL 的通用 `scan.mode` 取值；实际启动和增量查询选项按所用版本的连接器文档选择。

原文的 `ManifestList` 读取片段所表达的差别可保留为以下伪代码；这不是给 Flink SQL 执行的代码：

```text
readDataManifests(snapshot) =
    read(snapshot.baseManifestList()) + read(snapshot.deltaManifestList());

readDeltaManifests(snapshot) =
    read(snapshot.deltaManifestList());
```

第一个读取范围用于算完整版本，第二个只看本次文件变化。两者返回的都是 Manifest 元数据，需要继续读 Entry 并处理文件事件；它们本身都不是业务行结果。

## 同一分区为什么能在多个 Snapshot 中变化

用三个 Snapshot 和两个分区看文件集合的变化；文件名仅用于说明机制。

| 表版本 | `dt=07-10` 当前可见文件 | `dt=07-11` 当前可见文件 | 本次变化 |
| --- | --- | --- | --- |
| S1 | `f1` | 无 | 首次写入 `f1` |
| S2 | `f1`、`f2` | `f3` | 两个分区各增加文件 |
| S3 | `f4` | `f3` | Compaction 用 `f4` 替换 `f1`、`f2` |

按文件变化展开，过程更清楚：

| 提交 | `dt=07-10` 的文件事件 | `dt=07-11` 的文件事件 | 提交后的存活文件 |
| --- | --- | --- | --- |
| S1 | `ADD f1` | 无 | `07-10: {f1}` |
| S2 | `ADD f2` | `ADD f3` | `07-10: {f1, f2}`；`07-11: {f3}` |
| S3 | `DELETE f1`、`DELETE f2`、`ADD f4` | 无 | `07-10: {f4}`；`07-11: {f3}` |

对应的 Manifest 指针和条目可以按下面的示意追踪。这里假设没有触发 Manifest 文件自身的合并，方便看清“前次记录如何被沿用”；真实文件名和 Manifest 拆分由提交器决定。

```text
Snapshot-1
  deltaManifestList → [manifest-A]
  manifest-A: ADD dt=07-10 / bucket=0 / f1
  生效结果：07-10={f1}

Snapshot-2
  baseManifestList  → [manifest-A]      沿用前次 Manifest 引用
  deltaManifestList → [manifest-B]
  manifest-B: ADD dt=07-10 / bucket=0 / f2
              ADD dt=07-11 / bucket=0 / f3
  生效结果：07-10={f1,f2}；07-11={f3}

Snapshot-3（Data File Compaction：f1 + f2 → f4）
  baseManifestList  → [manifest-A, manifest-B]
  deltaManifestList → [manifest-C]
  manifest-C: DELETE dt=07-10 / bucket=0 / f1
              DELETE dt=07-10 / bucket=0 / f2
              ADD    dt=07-10 / bucket=0 / f4
  生效结果：07-10={f4}；07-11={f3}
```

S2 可以复用此前的 Manifest 引用，而不是复制 `f1`。S3 的文件变化包含 `DELETE f1`、`DELETE f2`、`ADD f4`。从 S3 读取 `dt=07-10` 时只读 `f4`；从仍保留的 S1 读取时仍读 `f1`。这解释了“同一分区跨版本存在”和“新旧版本能看到不同文件”可以同时成立。

原文末尾还用了另一组 `m1～m4` 的表级关系图，重点是“旧 Manifest 可复用，也可在后续合并中被替换”。按它的事件关系展开如下；这与上面的 `f1～f4` 三次提交是**另一组独立示例**，不能把两个示例的同名文件混在一起：

```text
S1  引用 m1、m2
    m1: ADD dt=20260710 / f1
    m2: ADD dt=20260711 / f2

S2  沿用 m1、m2，新增 m3
    m3: DELETE dt=20260710 / f1
        ADD    dt=20260710 / f3
    此时读 dt=20260710 → f3；读 dt=20260711 → f2

S3  经过元数据整理后，引用 m2、m3、m4（示意）
    m4: ADD dt=20260712 / f4
    此时读 dt=20260710 → f3；读 dt=20260712 → f4
```

S3 不再列出 `m1`，不能单看这一点就断定 S1 的 `f1` 已物理删除。新版本的有效文件集合由相关文件事件决定；S1 若仍保留，时间旅行还需要 `f1`。原文图中的 Manifest 文件名只是示意，真实的 Manifest 合并可能重写条目和引用，不能把这张图当作固定的磁盘布局。

> [!note] 通俗注释：Compaction 是换文件，不是改旧照片
> 合并前两个小文件是 `f1+f2`，合并后是 `f4`。新 Snapshot 宣布“今后读 `f4`”；仍保留的 S2 按旧清单读 `f1`、`f2`，S1 只读 `f1`。Compaction 通常只改变物理布局，不应改变该版本的逻辑业务结果。Manifest 合并用于减少元数据文件数量，与 Data File Compaction 不是同一个动作。

如果分区被 `INSERT OVERWRITE` 或分区过期逻辑移除，新 Snapshot 可以不再引用该分区的旧文件；旧 Snapshot 尚在时仍可能读取过去的分区状态。分区目录是否仍留在存储中，不是判断分区当前是否可见的依据。[官方分区管理](https://paimon.apache.org/docs/2.0/maintenance/manage-partitions/)

## 清理：分区保留期与 Snapshot 保留期并行生效

`partition.expiration-time` 控制一个分区何时从**最新表状态**中被逻辑移除；`snapshot.time-retained` 与 Snapshot 数量参数控制旧表版本保留多久。前者面向业务数据生命周期，后者面向时间旅行和增量读取的历史窗口，不能互相代替。[官方分区管理](https://paimon.apache.org/docs/2.0/maintenance/manage-partitions/)、[Snapshot 管理](https://paimon.apache.org/docs/2.0/maintenance/manage-snapshots/)

“分区保留一年”还取决于 `partition.expiration-strategy`：默认 `values-time` 用分区值解析出的时间计算到期；`update-time` 用分区最近更新时间计算。若分区名不是日期或历史分区持续更新，两种策略会得到不同的清理时间，不能只看 `partition.expiration-time` 一个参数。[官方分区过期策略](https://paimon.apache.org/docs/2.0/maintenance/manage-partitions/)

原文用 `dropPartitions → tryOverwrite` 描述分区过期的源码路径：到期后提交一个新表版本，使该分区不再属于最新逻辑状态。这里的“OVERWRITE”是文件引用层面的变化，不意味着立刻从对象存储擦掉整个分区目录；旧 Snapshot 或 Tag 仍可能需要这些文件。该调用路径须以实际 Paimon 版本为准。

例如分区保存一年、普通 Snapshot 只滚动保留一天：一年内的分区仍可从最新 Snapshot 查询，因为最新版本仍引用其文件；但普通时间旅行通常只能回到仍保留的 Snapshot。这里的“一天”不是绝对承诺，还要检查 `snapshot.num-retained.min/max`、实际提交频率，以及是否有 Tag 或 Consumer 保护历史。

> [!note] 通俗注释：旧目录过期 ≠ 当前数据过期
> 删除一份旧文件清单时，Paimon 不能顺手把清单上列出的所有文件都删掉；只要更新的清单仍引用某个文件，该文件就得留下。分区真正过期后，新 Snapshot 不再把它作为当前数据；等旧 Snapshot 或 Tag 不再需要对应文件时，才有机会物理清理。

Snapshot 保留过短还会影响长时间运行的批查询和落后的流式 Reader：它们依赖的历史版本可能已过期。生产中应以最长查询耗时、可接受停机时长和流式消费 Lag 设计保留窗口，而不是只按存储成本设置一个很短的时间。[官方 Snapshot 管理](https://paimon.apache.org/docs/2.0/maintenance/manage-snapshots/)

原文的消费位点例子值得保留：Reader 的恢复状态指向 `Snapshot-100`，当前已到 `Snapshot-200`。作业停了 36 小时，而未受保护的普通 Snapshot 只保留约一天；若 `Snapshot-100` 实际已经被清理，Reader 就不能按原位点继续增量读取。这里的风险不是半年前分区的数据文件丢了，而是**消费所需的历史版本链断了**。是否真的失败要查当时的 Snapshot、Consumer 和 Tag 状态，不能只凭“停了 36 小时”下结论。

以“分区保留一年、Snapshot 滚动保留一天”为例，清理过程可以拆成两条线：

```text
当前保留：Snapshot-3 及之后的版本
准备过期：Snapshot-1、Snapshot-2

① 先确定安全边界
   检查时间/数量保留规则，以及 Consumer、Tag 等是否仍需旧版本。
   需要保留的版本所引用的文件，不能进入本轮物理删除集合。

② 检查候选 Data File
   f1：在 S1 被 ADD，S3 已通过 Compaction 用 f4 替换。
       若其他保留版本也不再引用 f1 → 可清理。
   f3：在 S2 被 ADD，S3 的 dt=07-11 仍使用它。
       即使 S2 过期 → f3 必须保留。
   f4：S3 的 dt=07-10 当前文件 → 必须保留。

③ 清理元数据文件
   旧 Manifest File / Manifest List 若还被存活 Snapshot 引用 → 保留；
   引用解除后才清理。空目录默认不清；如需尝试清理，须显式设置
   snapshot.clean-empty-directories=true，并考虑对象存储上的额外开销。
```

原文用 `ExpireSnapshotsImpl.expireUntil()` 和 `skippingSet` 描述安全清理：先识别存活版本依赖的引用，再处理旧版本引入且已无人需要的文件。这个集合的构造与遍历顺序属于版本实现细节；判断能否删除某个文件，必须看它是否仍被保留版本、Tag 或其他保护机制需要，不能只看它最早属于哪个过期 Snapshot。原文“过期 S2 的 delta 中有 `DELETE f3`”是一个带问号的说明性分支；按上面的三次提交实例，S2 对 `f3` 实际是 `ADD`，而 S3 仍引用它。

原文的过期流程分为 Data File、Manifest、空目录三段，不能省成“删掉旧 Snapshot”：

```text
候选旧 Snapshot：S1、S2
保留 Snapshot：S3 及之后；另检查 Tag、Consumer 等保护边界

Data File：从候选版本相关文件事件寻找待清理文件；
           仍被保留状态使用的文件列入保护集合（原文称 skippingSet）。
           只清理保护集合之外、确已失去有效引用的文件。

Manifest File / Manifest List：检查存活版本是否还引用它们；
                               引用仍在就保留，解除后再清理。

目录：文件删完后，空分区/桶目录能否被清理取决于实现与底层文件系统。
```

`skippingSet` 是原文解释某版源码时使用的名称，不应推广成“Paimon 先遍历所有保留 Snapshot，再一次性收集每个 Data File 名称”的跨版本 API 保证。对使用者可验证的结果是：保留版本的查询仍能找到它所需的文件；旧版本及其文件真正被清理后，历史查询窗口缩短。

| 查询或消费行为 | 在这个示例下的结果 | 判断依据 |
| --- | --- | --- |
| 从最新 Snapshot 查半年前的分区 | 可以，前提是该分区尚未过期 | 最新版本仍引用其文件 |
| 时间旅行到两天前 | 通常不可以 | 所需 Snapshot 可能已超出保留窗口 |
| 流式 Reader 停机 36 小时后续读 | 有丢失消费起点的风险 | 所需旧 Snapshot 若已清理便无法继续；需检查 Consumer 保护和实际保留状态 |
| 一年前的分区到期 | 新 Snapshot 不再显示该分区 | 旧文件还要等历史引用都释放后才能物理清理 |

分区过期与 Snapshot 过期的先后关系也可以用一个文件追踪：假设 `dt=2026-03-14/f_old` 在半年前写入，今天仍未达到该分区的过期条件，最新 Snapshot 仍把它纳入有效文件集合；昨日的 Snapshot 被清理，不影响今天按最新版本读取 `f_old`。到了分区过期条件触发时，Paimon 提交一个让最新版本不再显示该分区的变化；只有旧版本和 Tag 等也不再依赖 `f_old`，它才有机会被物理删除。

这个例子也回答了原文的 `partition.expiration-time = 365 d` 加 `snapshot.time-retained = 1 d`：分区数据可保留接近一年，普通时间旅行不因此获得一年窗口。Snapshot 保留还受最小/最大数量、提交频率和保护机制影响；“1 d”不能保证恰好只保留 24 小时，也不表示昨天以前的分区文件都会被删。

流式读取需要保护消费进度时，`consumer-id` 配在**读取 SQL** 上；表属性 `consumer.expiration-time` 管理 Consumer 文件寿命：

```sql
ALTER TABLE orders SET ('consumer.expiration-time' = '7 d');

SET 'execution.runtime-mode' = 'streaming';
SELECT *
FROM orders /*+ OPTIONS('consumer-id' = 'orders-downstream') */;
```

应在启动 Reader 前配置 Consumer 生命周期，并让现有流式 Writer 重新加载表选项。Consumer 可阻止所需历史被正常过期，但它记录的是表侧 Snapshot 进度，不代替 Flink Checkpoint 对下游算子状态和 Sink 的恢复。过期的 Consumer 也不能保护无限期停机。[官方 Consumer ID](https://paimon.apache.org/docs/master/flink/consumer-id/)

原文用 `ConsumerManager.minNextSnapshot()` 说明过期边界：若最慢的 Consumer 下一步还要从 Snapshot 101 开始读，过期任务不能按普通时间规则把 101 及之后的必需版本一并清理。它表达的是“实际消费位点限制过期上界”，不是“配置一个 ID 就永久保存全部历史”。

原文把 `consumer-id` 写进 `CREATE TABLE ... WITH`，容易让人误以为建表一次就能替所有下游保留进度。上面的 SQL 将它配在具体的流式 Reader 上；如果有两个独立下游，应分别给它们稳定且不同的 ID。需要长期固定一个历史表状态时，使用 Tag 或调整 Snapshot 保留策略；Consumer 保护的是消费进度，不是任意历史时间旅行请求。

原文还列了分区保留、Snapshot 保留、Consumer 和自动 Tag 四类配置。它们不是一套需要全部照抄的默认值：`partition.expiration-time` 决定当前业务数据保留多久；`snapshot.time-retained` 与数量上限决定普通版本窗口；每个流式 Reader 用自己的 `consumer-id` 防止消费起点被正常过期；Tag 用于固定需要长期回查的特定版本。原文举的 `365 d`、`7 d` 和“保留 30 个 Tag”只能作为容量与恢复窗口的计算样例，上线前还需按写入频率、最长停机时间及存储预算核算。

```text
原文配置示意（不是一段可直接执行的 Flink SQL）：
partition.expiration-time = 365 d
snapshot.time-retained   = 7 d
consumer-id              = 每个流式 Reader 的独立稳定 ID
tag.automatic-creation   = process-time
tag.num-retained-max      = 30
```

其中 `consumer-id` 应放在具体 Reader 的读取选项中。`tag.num-retained-max` 针对自动创建的 Tag，不能理解为所有手动 Tag 也只保留 30 个；自动 Tag 的触发时机仍要以部署版本文档为准。每个仍存活的 Tag 都可能延长历史文件的占用时间，因此要估算存储成本。[官方配置说明](https://paimon.apache.org/docs/master/maintenance/configurations/)

时间旅行失败也要按“版本是否存在”判断，而不是按分区日期判断：同样查询 `dt=20260710`，指定已过期的 Snapshot ID 会失败或无法读到该版本；改用仍保留的 Snapshot ID，才有机会看到那一版的分区。以下两个 ID 仅示意，先通过 `$snapshots` 确认真实存在的 ID：

```sql
-- 示例：1000 已过期，不能用于时间旅行
SELECT * FROM orders /*+ OPTIONS('scan.snapshot-id' = '1000') */
WHERE dt = '20260710';

-- 示例：99000 仍保留，查询它所代表的整张表版本
SELECT * FROM orders /*+ OPTIONS('scan.snapshot-id' = '99000') */
WHERE dt = '20260710';
```

## 分区列表为什么也受 Snapshot 影响

原文提到的 `PartitionEntry` 是从选定 Snapshot 可见的 Manifest Entry 汇总出来的分区统计视图，可以理解为“按分区把存活文件的文件数、大小等重新归集”。示意过程如下；具体方法名、字段和聚合步骤以所用版本源码为准。

```text
固定 Snapshot
  ↓ 读取它引用的 Manifest List，并按条件过滤
读取相关 Manifest Entry
  ↓ 按 partition 值归组，并处理文件 ADD/DELETE
形成每个分区的 PartitionEntry：fileCount、fileSize 等统计
  ↓ 排除当前已无有效文件的分区
返回该 Snapshot 的分区列表
```

原文的 `readPartitionEntries()` 代码还表达了两个细节：遍历的是**选定版本相关的 Manifest**，不是枚举对象存储里所有 `dt=...` 目录；聚合后会过滤 `fileCount=0` 的分区，所以历史上曾存在、当前已无有效文件的分区不会继续出现在这个视图里。聚合字段如 `rowCount`、`fileSize`、`fileCount` 属于元数据统计，不代表查询已经把这些数据文件逐行读过。

因此，`orders$partitions` 回答的是“所选表版本有哪些分区及其统计”，不是“每个分区各自维护了什么 Snapshot”。旧 Snapshot 中出现过的分区，若已从最新版本移除，就不会因为对象存储还留有目录而自动出现在当前分区列表。原文引用 `AbstractFileStoreScan.readPartitionEntries()` 说明这条来源链，不能把一个源码片段误当成所有版本、所有引擎的固定执行计划。

## 排查时看哪些元数据

用 Flink SQL 排查时，先切到批模式，并替换表名与分区值。系统表字段及 Procedure 语法需以部署的 Paimon/Flink 版本为准。

```sql
SET 'execution.runtime-mode' = 'batch';

-- 哪些 Snapshot 还在，分别是什么提交类型？
SELECT snapshot_id, commit_kind, commit_time
FROM `orders$snapshots`
ORDER BY snapshot_id DESC;

-- 当前版本有哪些分区，文件数和大小是多少？
SELECT partition, file_count, file_size_in_bytes
FROM `orders$partitions`;

-- 当前版本引用哪些 Data File？
SELECT partition, bucket, file_path, level, file_size_in_bytes
FROM `orders$files`;

-- 当前版本引用哪些 Manifest？
SELECT file_name, num_added_files, num_deleted_files
FROM `orders$manifests`;

-- 是否有 Consumer 保护消费进度？
SELECT consumer_id, next_snapshot_id
FROM `orders$consumers`;
```

`$files` 反映选定 Snapshot 中的存活文件，不代表一次实际 SQL 扫描了所有这些文件。需要证明分区裁剪或文件跳过是否生效，还要看查询计划和 Scan 指标。`$partitions` 是所选表版本的分区统计视图，不表示每个分区都有一套独立的 Snapshot 历史。[官方系统表](https://paimon.apache.org/docs/master/concepts/system-tables/)

原文给出的源码入口可以按要排查的问题查找，类名仅作为阅读线索，不固定到原文的行号：

| 想确认的机制 | 原文的源码入口 | 应重点观察 |
| --- | --- | --- |
| Snapshot 保存哪些引用 | `Snapshot` | `baseManifestList`、`deltaManifestList` 等字段 |
| 文件事件包含什么 | `ManifestEntry`、`ManifestFileMeta` | `kind`、`partition`、`bucket`、文件元数据与分区统计 |
| 提交如何发布新版本 | `FileStoreCommitImpl` | 前一版本引用、Manifest 合并、Snapshot 发布顺序 |
| 分区如何提前过滤 | `ManifestsReader` | Manifest 统计粗过滤与 Entry 精过滤 |
| 分区统计从哪里来 | `AbstractFileStoreScan`、`PartitionEntry` | 从所选版本的文件条目归集统计 |
| 旧版本如何过期 | `ExpireSnapshotsImpl` | 候选版本、保留引用、Data File 与 Manifest 清理边界 |

## 原文中的几个表述边界

- `baseManifestList` 是继承状态的 Manifest List，`deltaManifestList` 是本次文件变化；把 base 单独写成“全部存活文件清单”，会漏掉本次 delta。完整状态需要按读取规则合并相关 Manifest Entry。
- 原文的 `ALL`、`DELTA`、`CHANGELOG` 是内部扫描视角，不应直接推成所有 Flink SQL 都可用的公开 `scan.mode` 值。实际 SQL 启动模式和变更语义按目标版本文档配置。
- 原文按具体 Java 类和行号描述提交、清理实现。这些属于版本相关的源码观察；本笔记保留可跨版本理解的对象关系，不把固定行号当作生产契约。
- “Snapshot 过期就一定删除旧文件”过于绝对。仍被存活 Snapshot、Tag 或其他受保护引用需要的文件不能按普通过期路径直接清理。
- `consumer-id` 不是给表建一次就自动保护全部下游。每个需要保留进度的流式 Reader 要配置自己的消费标识，并管理其生命周期。

相关笔记：[[Paimon Snapshot 历史分区查询]]、[[Paimon Snapshot、Manifest、Parquet 与 Compaction 的关系]]、[[Paimon 架构全景：写入、快照、索引与变更语义（文章批注版）]]。
