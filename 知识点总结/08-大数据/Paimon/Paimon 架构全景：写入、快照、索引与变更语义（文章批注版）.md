# Paimon 架构全景：写入、快照、索引与变更语义（文章批注版）

> [!info] 文章信息
> 原文：[《Paimon 精讲（一）：10分钟建立完整架构认知，老鸟也能查漏补缺》](https://mp.weixin.qq.com/s/VcO9_aEqneaoJp1Mu5NJtQ)  
> 作者：胖泽的技术笔记  
> 发布时间：2026-07-22  
> 内容哈希：`19eaff783e3acd4054e20a3d0ad393539cc761967b3008420bf10f7db9235128`  
> 本笔记保留原文的架构主线，批注用于补充对象关系、成立条件和生产边界，不是原文逐字备份。

> [!abstract] 一分钟复习
> Paimon 是表存储，不是独立的 SQL 计算引擎。Flink、Spark、Trino 等引擎负责解析 SQL 和执行计算，Paimon 负责表的 Schema、文件组织、提交版本、主键合并和增量变更语义。Snapshot 通过 Manifest 决定某个版本可见哪些文件；主键表再通过 LSM、Compaction 和 Merge Engine 把多个物理版本合并成逻辑行。Partition、Bucket、文件统计和 File Index 逐层缩小读取范围。Changelog 描述下游可消费的行级变化，不等于当前表状态，也不天然等于完整业务事件历史。

## 一、先建立四层心智模型

| 层次 | 主体 | 负责什么 |
| --- | --- | --- |
| 计算层 | Flink、Spark、Trino、StarRocks | 解析 SQL、生成计划、调度任务、执行 Join/聚合/过滤 |
| 表存储层 | Paimon Catalog、Table API、Reader、Writer | 管理 Schema、Snapshot、Manifest、主键合并和变更语义 |
| 元数据层 | Filesystem/Hive/JDBC/REST Catalog | 让引擎发现数据库、表和 Schema，它不执行 SQL |
| 物理存储层 | HDFS、S3、OSS、GCS | 保存 Snapshot、Manifest、索引和 ORC/Parquet 等文件 |

SQL 调用链路可以记成：`SQL → 计算引擎 Planner → Paimon Connector/Table API → Snapshot/Manifest → Data File`。

> [!note] 批注：“Paimon SQL”是一种简写
> Paimon 文档中的 Flink SQL、Spark SQL 示例，是“通过某个计算引擎操作 Paimon”，不表示 Paimon 自带 SQL Parser 和 Executor。同一项 Paimon 能力在 Flink 与 Spark 中可能有不同语法，也可能因 Connector 版本而支持程度不同。

## 二、一张表在存储上是什么

Paimon 表的数据组织主线是：`Table → Partition → Bucket → Data File`。

- Partition 按业务字段做粗粒度组织、查询裁剪和生命周期管理。
- Bucket 是分区内的逻辑分片，影响写入并行、同键数据分布和 Compaction 边界。
- Data File 保存真实记录，常见格式为 ORC 或 Parquet。
- Snapshot 和 Manifest 记录“某个表版本应该看到哪些文件”。

常见物理目录可以分为：

| 目录 | 内容 | 读取时的作用 |
| --- | --- | --- |
| `schema/` | Schema 版本、主键、分区键和表选项 | 按对应 Schema 解析不同时期的文件 |
| `snapshot/` | 已提交的表版本 | 时间旅行和当前表状态的入口 |
| `manifest/` | Manifest List、Data Manifest、Index Manifest | 找到该版本有效的数据和索引文件 |
| `index/` | 动态 Bucket 索引、Deletion Vector 等 | 支持写入路由或失效行过滤 |
| 分区/Bucket 目录 | Data File、可选 Changelog File | 保存数据和下游变更记录 |

> [!note] 批注：Snapshot 与业务分区是两个时间维度
> `dt=2026-09-19` 是表中的业务日期；Snapshot 123 是整张表的第 123 次已提交版本。一个 Snapshot 可以修改多个分区，同一个分区也可以在多个 Snapshot 中呈现不同状态。“先选 Snapshot，再用 `WHERE dt=...` 选分区”是历史查询的正确顺序。

## 三、主键表的写入链路

一条记录写入主键表时，主线是：

1. 计算引擎的 Writer 把记录交给 Paimon。
2. Paimon 根据 Partition 和 Bucket 路由数据。
3. 记录进入 MemTable/Writer Buffer，按主键排序，并可能在缓冲区内合并同键记录。
4. 达到缓冲阈值或进入提交阶段后，数据 flush 为新的 L0 Data File。
5. Manifest 记录新增文件，Snapshot 提交成功后，这批文件才对新 Reader 可见。
6. Compaction 在后续将重叠的文件归并到更高层，并按 Merge Engine 处理同键记录。

Flink 流式写入中，Checkpoint 通常是 Writer 和 Committer 协调提交的一致性边界。Checkpoint 成功表示 Flink 作业状态可恢复；Paimon Snapshot 提交成功表示这批表文件已对查询可见。

> [!note] 批注：Checkpoint 不等于 Snapshot ID
> 两者可以在流作业中建立提交关系，但 Snapshot 还可能由批写入、Compaction、Overwrite 等操作生成。一次 Writer 提交也可能伴随不同 `commit_kind` 的 Snapshot。排查历史版本时要看 `$snapshots.commit_time` 和 `commit_kind`，不要用 Flink Checkpoint ID 代替 Snapshot ID。

> [!warning] 批注：不要把 Snapshot ID 缺号直接当成提交失败
> 空提交可能被跳过，旧 Snapshot 也会因过期而被清理。当前保留列表不从 `1` 开始，不能单独证明数据不完整。

## 四、主键表的读取链路

读取一张 Paimon 表时，Reader 不是直接枚举 Warehouse 中的全部文件，而是：

1. 选择最新 Snapshot，或根据 Snapshot ID、时间戳、Tag 选择历史 Snapshot。
2. 从 Snapshot 中找到 Manifest List。
3. 结合 base/delta Manifest 得到当前版本有效的数据文件集合。
4. 用分区条件、Bucket 路由、文件统计和 File Index 缩小扫描范围。
5. 读取命中的 Data File。
6. 主键表若仍存在多个同键物理版本，则在读取阶段按序列和 Merge Engine 归并成最终逻辑行。

> [!note] 批注：Snapshot 不保证一个主键只有一个物理版本
> Snapshot 提供的一致性是“这些文件属于同一个已提交表版本”。如果 Compaction 还没有整理完同键记录，逻辑唯一性由 Reader 的 Merge-on-Read 保证。因此，“能时间旅行”与“所有数据都已 Compaction”没有必然关系。

> [!note] 批注：历史时间点是提交时间，不是业务事件时间
> 查询“昨天 15:00 的表状态”时，Paimon 会选择目标时间前已提交的 Snapshot。一条 `event_time=14:50` 的迟到数据若在 15:10 才提交，不会出现在 15:00 的 Snapshot 中。Snapshot 时间旅行还原的是表提交历史，不是按业务时间重放事件。

## 五、Snapshot 与 Manifest 如何实现 MVCC

Snapshot 是某次成功提交后的表版本入口。它保存 Schema ID、提交类型、提交时间和 Manifest List 引用等元数据，不直接保存业务行。

Manifest List 汇总多个 Manifest；Data Manifest 再用 ADD/DELETE 条目记录数据文件的版本变化。`DELETE file-A` 的含义是新 Snapshot 不再把 `file-A` 纳入当前逻辑表状态，不表示文件已立即从存储中删除。

旧 Snapshot 仍可以引用 `file-A`，所以历史 Reader 能读到过去的文件集合。只有旧 Snapshot 过期，并且没有 Tag 等对象继续引用旧文件时，这些文件才能物理清理。

> [!note] 批注：Snapshot 保留时间决定系统版本历史，分区决定业务历史
> 每日全量快照表把历史状态主动写进 `dt` 分区；只要这些分区仍属于最新表状态，旧 Snapshot 过期也不会删掉它们。Snapshot 保留则允许查询近期任意已提交版本。因此，每日分区可以保存长期日粒度业务历史，Snapshot 提供的粒度取决于实际提交频率和保留策略。

## 六、Compaction 改变什么

L0 数据文件可以相互重叠主键范围。文件数增多后，读取需要打开更多文件并归并同键记录。Compaction 读取旧文件，按主键和序列归并，生成新文件，再用新 Snapshot 发布“旧文件失效、新文件生效”。

它的收益是减少文件数和读时归并成本；代价是重写数据带来的 CPU、I/O 和写放大。

> [!note] 批注：Compaction 不是把多个 Snapshot 合并成一个文件
> Compaction 的输入是 Data File，输出也是 Data File。Snapshot 只负责发布这次文件替换。对同一逻辑表状态，Compaction 前可能需要 Reader 合并多个文件，Compaction 后可能只需读更少文件，结果原则上应保持一致。

> [!warning] 批注：`lookup` 不是“异步 Compaction 模式”
> Compaction 何时发生、使用什么策略，与 `changelog-producer=lookup` 是两个配置维度。`lookup` 为了生成旧值需要在 Lookup Compaction 过程中查表内状态，但不能因此把它概括成通用异步 Compaction 开关。

## 七、两套索引不要混在一起

### Global Index

Global Index 解决表级或 Bucket 级问题：

- 动态 Bucket 中的 Hash Index 维护主键到 Bucket 的路由关系。
- Deletion Vector 记录数据文件中哪些行已失效，使 Reader 可以直接跳过它们。

### Data File Index

Data File Index 面向单个数据文件的查询裁剪：

- Bloom Filter 服务等值谓词，可判断某值一定不在文件中。
- Bitmap 适合部分低基数字段的等值过滤。
- BSI 用于部分数值或日期字段的范围谓词。

> [!note] 批注：索引存在不等于查询一定使用
> 索引是否能裁剪取决于 Connector 版本、字段类型、谓词形态、索引覆盖文件和计划下推。`$files` 可以检查文件大小、层级、主键范围和统计信息，但不能单独证明一条 SQL 真实跳过了多少文件。还要结合执行计划、Scan 指标和 `$file_indexes`。

## 八、Append Table 与 Primary Key Table

| 维度 | Append Table | Primary Key Table |
| --- | --- | --- |
| 判定方式 | 未声明主键 | 声明 `PRIMARY KEY ... NOT ENFORCED` |
| 输入语义 | 每条输入都是需要保留的事实 | 同一主键的输入用于形成最终状态 |
| 同键处理 | 不自动去重 | 由 Merge Engine 决定 |
| 适用场景 | 日志、埋点、审计事件、不变事实 | CDC 当前状态、订单最新状态、实时宽表 |
| 主要代价 | 数据量持续增长 | 同键归并、索引、Compaction 和可能的读放大 |

Merge Engine 决定主键表中同一主键如何形成逻辑行：

- `deduplicate`：按序列或到达规则保留最新记录。
- `partial-update`：同一主键的不同输入更新不同字段。
- `aggregation`：对值字段执行 `sum`、`max` 等聚合。
- `first-row`：保留第一条到达记录。

> [!warning] 批注：先定业务语义，再调物理参数
> 如果业务要求保留每一次状态变化，用主键表只保留最终状态会破坏语义；如果业务只要当前状态，用 Append Table 会把去重和合并成本推给每一次查询。Bucket 数、文件大小和 Compaction 参数都无法弥补表语义选错。

## 九、Changelog 的选择标准

Changelog Producer 决定主键表如何向流式下游输出变化，不决定表是 Append Table 还是 Primary Key Table，也不应改变普通批查询在同一 Snapshot 上的逻辑结果。

| 模式 | 旧值来源 | 适用前提 | 主要代价/边界 |
| --- | --- | --- | --- |
| `none` | 不额外生成完整旧值 | 下游可以按主键覆盖，或由 Flink Normalize 维护旧值 | 需要撤回时，成本可能转移到下游状态 |
| `input` | Writer 收到的上游 changelog | 上游已提供完整且可直接作为表状态变化的新旧行 | 不会补造上游缺失的旧值 |
| `lookup` | 查询 Paimon 表内旧状态 | 输入缺少旧值，下游又必须获得撤回 | 增加索引、缓存、本地磁盘和 Compaction 成本 |
| `full-compaction` | 对比两次 Full Compaction 后的表状态 | 可接受更高延迟，只关心周期性状态差异 | 中间多次更新可能被折叠，且 Full Compaction 写放大较高 |

> [!note] 批注：判断 `input` 还是 `lookup` 时，不要只看“上游是 CDC”
> 要检查 Writer 实际收到的 `RowKind`、字段是否完整、是否有真实旧值，以及输入是否已等价于 Merge Engine 产生的最终逻辑行。`partial-update` 的字段增量、`aggregation` 的聚合贡献值即使携带完整 `RowKind`，也可能需要表内旧状态才能生成正确下游变更。

## 十、时间旅行与长期历史

查询某个历史时间点时，计算引擎把目标 Snapshot ID、时间戳或 Tag 交给 Paimon Reader。Reader 选定对应 Snapshot，再读取它引用的 Manifest 和 Data File。

Flink SQL 中可以先查询当前保留的 Snapshot：

```sql
SET 'execution.runtime-mode' = 'batch';

SELECT
    snapshot_id,
    commit_kind,
    commit_time
FROM `orders$snapshots`
ORDER BY commit_time DESC;
```

再按明确 Snapshot 读取：

```sql
SELECT *
FROM orders /*+ OPTIONS(
    'scan.snapshot-id' = '123'
) */
WHERE dt = '2026-09-19';
```

或者在 Flink 1.18+ 使用标准时间旅行语法：

```sql
SELECT *
FROM orders
FOR SYSTEM_TIME AS OF TIMESTAMP '2026-09-19 15:00:00'
WHERE dt = '2026-09-19';
```

> [!warning] 批注：时间旅行不是无限期历史
> 时间旅行能查多远，取决于 Snapshot 及其引用文件保留了多久。普通 Snapshot 适合滚动保留近期细粒度版本；月末、关账、发布前等少量重要版本应用 Tag 长期固定。如果需要永久保存每一次业务变化，应另建 Append 事件表、CDC 历史表或 SCD2，不应只依赖 Snapshot。

## 十一、建表时的复用顺序

1. **定义记录语义**：每条输入是必须保留的事件，还是某个业务键的当前状态？
2. **定义业务唯一性**：主键是什么？分区字段是否会变？主键不含分区字段时是否允许跨分区更新？
3. **定义同键合并**：保留最新记录、字段部分更新、聚合累加，还是保留首条？
4. **定义下游变更需求**：只要最新值，还是必须拿到完整 `-U/+U`？旧值应该来自上游、Paimon Lookup 还是周期性状态对比？
5. **设计物理分布**：根据单分区数据量、写入并行度、查询谓词和数据倾斜选分区、Bucket 和文件目标大小。
6. **根据运行指标调参**：检查 L0 文件数、单文件大小、Compaction 延迟、扫描文件数、Writer 内存、Checkpoint 和下游 Lag，再决定是否改参数。

## 十二、原文结论的使用边界

> [!warning] 原文未声明 Paimon 版本
> Snapshot 字段、参数默认值、Bucket 约束、File Index 可用性和各引擎的 SQL 能力可能随版本变化。建表或调参前，以目标版本官方文档、实际 `SHOW CREATE TABLE` 和运行指标为准。

- “Paimon = RocksDB 的 LSM-Tree + Iceberg 的 Snapshot/Manifest”适合建立粗粒度心智模型，不表示三者在实现、文件格式或兼容性上等价。
- `lookup` 不是所有 CDC 的固定选项；上游已提供完整且最终语义正确的 changelog 时，`input` 通常更直接。
- 存储上的原子发布不能一律概括为“依赖 rename”；文件系统、对象存储和 Catalog-managed 模式的提交机制可以不同。
- 不要把文章里的文件大小、缓冲区、Snapshot 保留数等默认值直接带入生产。版本、数据量、并行度和 SLA 都会改变结论。

## 十三、问题定位速查

| 现象 | 先看什么 | 不要立即下的结论 |
| --- | --- | --- |
| 新数据查不到 | Flink Checkpoint、Paimon `$snapshots`、Committer 错误 | Writer 生成了文件就已经可见 |
| 主键表查询慢 | L0 文件数、主键范围重叠、Compaction 延迟、裁剪效果 | 只要增加 Bucket 数就能解决 |
| 时间旅行查不到历史 | `$snapshots`、`$tags`、保留参数和目标时区 | `dt` 分区存在就一定有当时的系统版本 |
| 下游没有 `UPDATE_BEFORE` | Sink 实际 `RowKind`、`changelog-producer`、Merge Engine | 主键表天然产生完整 Before/After |
| 动态 Bucket 恢复慢 | IndexBootstrap 扫描范围、主键规模、本地索引和并行度 | `bootstrap-parallelism` 能消除总扫描量 |

## 十四、相关笔记

- [[Apache Paimon 表模型、存储组织与读取语义]]
- [[Paimon Snapshot、Manifest、Parquet 与 Compaction 的关系]]
- [[Paimon 流式读写中的 Snapshot 与 Changelog]]
- [[Paimon L0、L1、Snapshot 与查询裁剪]]
- [[Paimon 动态 Bucket 的索引机制与单 Writer 限制]]
- [[Paimon File Index：存储、读写与选型]]
