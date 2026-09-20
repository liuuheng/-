# Paimon 架构全景：写入、快照、索引与变更语义（文章批注版）

> [!info] 文章信息
> 原文：[《Paimon 精讲（一）：10分钟建立完整架构认知，老鸟也能查漏补缺》](https://mp.weixin.qq.com/s/VcO9_aEqneaoJp1Mu5NJtQ)  
> 作者：胖泽的技术笔记  
> 发布时间：2026-07-22  
> 内容哈希：`19eaff783e3acd4054e20a3d0ad393539cc761967b3008420bf10f7db9235128`  
> 本笔记保留原文的架构主线，批注用于补充对象关系、成立条件和生产边界，不是原文逐字备份。

> [!abstract] 一分钟复习
> Paimon 是表存储，不是独立的 SQL 计算引擎。Flink、Spark、Trino 等引擎负责解析 SQL 和执行计算，Paimon 负责表的 Schema、文件组织、提交版本、主键合并和增量变更语义。Snapshot 通过 Manifest 决定某个版本可见哪些文件；主键表再通过 LSM、Compaction 和 Merge Engine 把多个物理版本合并成逻辑行。表模型由“保留独立事件还是维护同键状态”决定，与批流读取方式无关。Dynamic Bucket 的同一分区只能由一个 Job 写；Fixed Bucket 支持确定性路由，多 Writer 还需统一 Compaction 和版本顺序。Partition、Bucket、文件统计和 File Index 逐层缩小读取范围。Changelog 描述下游可消费的行级变化，不等于当前表状态，也不天然等于完整业务事件历史。

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

## 七、三类索引分别解决什么问题

Paimon 中容易被统称为“索引”的对象至少有三类，它们的作用域不同。

| 类型 | 作用对象 | 解决的问题 | 检查入口 |
| --- | --- | --- | --- |
| 动态 Bucket Hash Index | 主键 Hash 与 Bucket 的映射 | Writer 应把同一主键路由到哪个 Bucket | `$table_indexes` |
| Deletion Vector / Global Index | 表级行位置或搜索结构 | 标记失效行，或提供独立管理的查询索引 | `$table_indexes` |
| Data File Index | 单个 Data File 中的列值 | 通过谓词排除文件、Row Group 或 Page | `$file_indexes` |

动态 Bucket Hash Index 服务写入路由，不是给 SQL 点查使用的二级索引。Data File Index 服务查询裁剪，不负责主键唯一性，也不改变同键记录的合并结果。

### 7.1 Bloom Filter：高基数字段的等值查询

Bloom Filter 适合 `event_id`、`order_id`、`trace_id` 等高基数字段：

```sql
SELECT *
FROM order_events
WHERE dt = '2026-09-20'
  AND event_id = 'evt_987654321';
```

分区裁剪先排除其他日期；Bloom Filter 再判断剩余文件是否一定不包含目标值。返回“不存在”时可以跳过文件，返回“可能存在”时仍要读取并验证。假阳性只会增加少量扫描，不会造成错误结果。

```sql
'file-index.bloom-filter.columns' = 'event_id,order_id',
'file-index.bloom-filter.event_id.fpp' = '0.01',
'file-index.bloom-filter.event_id.items' = '100000'
```

`items` 估计的是单个 Data File 内的不同值数量，不是整张表的基数。误判率越低，索引通常越大。

### 7.2 Bitmap：枚举字段的等值与集合查询

Bitmap 适合 `status`、`event_type`、`is_deleted` 等取值有限的字段：

```sql
SELECT *
FROM order_events
WHERE dt = '2026-09-20'
  AND status IN ('PAID', 'SHIPPED');
```

它为不同取值记录命中行的位置，能够处理等值和集合过滤。给几乎每行都不同的 `event_id` 建 Bitmap 会产生大量值项，通常不如 Bloom Filter 合适。

```sql
'file-index.bitmap.columns' = 'status,event_type'
```

### 7.3 Range Bitmap：数值与时间范围查询

Range Bitmap 适合金额、数量、分数和时间范围：

```sql
SELECT *
FROM order_events
WHERE dt = '2026-09-20'
  AND amount BETWEEN 100.00 AND 500.00
  AND event_time >= TIMESTAMP '2026-09-20 10:00:00'
  AND event_time <  TIMESTAMP '2026-09-20 11:00:00';
```

```sql
'file-index.range-bitmap.columns' = 'amount,event_time'
```

旧版 BSI 也用于数值范围过滤，但当前文档已将其标记为废弃，新表应使用 Range Bitmap。

### 7.4 建表、补建与验证

File Index 可以在建表时声明：

```sql
CREATE TABLE order_events (
    event_id STRING,
    order_id BIGINT,
    status STRING,
    amount DECIMAL(18, 2),
    event_time TIMESTAMP(3),
    dt STRING
)
PARTITIONED BY (dt)
WITH (
    'bucket' = '-1',
    'file.format' = 'orc',
    'file-index.bloom-filter.columns' = 'event_id,order_id',
    'file-index.bitmap.columns' = 'status',
    'file-index.range-bitmap.columns' = 'amount,event_time'
);
```

也可以给已有表增加配置：

```sql
ALTER TABLE order_events SET (
    'file-index.bloom-filter.columns' = 'event_id,order_id',
    'file-index.bitmap.columns' = 'status',
    'file-index.range-bitmap.columns' = 'amount,event_time'
);
```

`ALTER TABLE` 只影响后续生成的文件。历史 Data File 需要单独补建索引：

```sql
-- 全表补建
CALL sys.rewrite_file_index(`table` => 'dwd.order_events');

-- 大表先按分区灰度补建
CALL sys.rewrite_file_index(
    `table` => 'dwd.order_events',
    partitions => 'dt=2026-09-20'
);
```

`rewrite_file_index` 会读取目标字段并生成索引，不重写 Data File 本身，但仍有历史数据扫描成本。可用系统表检查覆盖范围：

```sql
SELECT
    column_name,
    index_type,
    storage_type,
    COUNT(DISTINCT file_path) AS indexed_file_count,
    SUM(index_size_in_bytes) AS index_bytes
FROM `order_events$file_indexes`
GROUP BY column_name, index_type, storage_type
ORDER BY column_name, index_type, storage_type;
```

`$file_indexes` 中存在记录只证明索引已生成。查询是否实际利用索引，还要检查谓词是否下推、执行计划、扫描文件数和 Scan 指标。主键表也支持 File Index；固定 Bucket 已能按完整 Bucket Key 裁剪时，再给相同主键字段配置 Bloom Filter 可能收益有限，File Index 更适合非主键过滤列。

## 八、表模型由记录语义决定，不由批流模式决定

Append Table 和 Primary Key Table 都支持批式、流式读写。判断表模型时，应检查同一业务键的新记录是否修改旧状态。

| 维度 | Append Table | Primary Key Table |
| --- | --- | --- |
| 判定方式 | 不声明主键 | 声明 `PRIMARY KEY ... NOT ENFORCED` |
| 输入语义 | 每条输入都是独立且需要保留的事件 | 同一主键的输入共同形成当前逻辑行 |
| 同键处理 | 不去重，重复写入会保留重复记录 | 由 Merge Engine 决定覆盖、部分更新或聚合 |
| 典型场景 | 日志、埋点、审计流水、CDC 原始事件 | 订单当前状态、维表、CDC Upsert、实时宽表 |

数据来自 CDC 不等于必须使用主键表。保存 CDC 原始事件历史时可以使用 Append Table；按业务键维护最新状态时才需要 Primary Key Table。下游是否流式读取也不决定表模型：实时日志流仍然是 Append 语义，批量查询订单最新状态仍然是主键语义。

### 8.1 Append 表的两种 Bucket 布局

Append 表的 `bucket=-1` 表示 Bucket-Unaware，不是主键表的动态 Bucket。物理目录可以出现 `bucket-0`，但写入并行度不被一个固定 Bucket 限制。

```sql
CREATE TABLE event_log (
    user_id BIGINT,
    event_type STRING,
    event_time TIMESTAMP(3),
    dt STRING
)
PARTITIONED BY (dt)
WITH (
    'bucket' = '-1',
    'file.format' = 'orc'
);
```

Partition、Bucket 和文件格式是三个独立维度：`dt` 负责粗粒度裁剪和生命周期；`bucket=-1` 决定不按固定 Bucket Key 分布；ORC/Parquet 决定文件编码，不能由“日志表”标签直接推出。

需要按字段确定性路由时，可以创建固定 Bucket Append 表：

```sql
WITH (
    'bucket' = '16',
    'bucket-key' = 'user_id'
)
```

相同 `user_id` 在同一分区进入同一个 Bucket，但不会去重。完整 Bucket Key 上的 `=` 或 `IN` 谓词可以计算候选 Bucket；范围谓词通常不能通过 Hash Bucket 直接裁剪。

### 8.2 主键表的 Merge Engine

| Merge Engine | 输入含义 | 结果 |
| --- | --- | --- |
| `deduplicate` | 每条记录是该主键的新完整状态 | 按序列或到达规则保留较新记录 |
| `partial-update` | 每条记录只更新部分字段 | 非 NULL 字段默认覆盖，NULL 默认表示不更新 |
| `aggregation` | 每条记录是增量贡献 | 按字段聚合函数累加或归并 |
| `first-row` | 只接受首条记录 | 已存在主键的后续输入被忽略 |

`aggregation` 只适用于增量值。例如输入 `+3`、`+5` 才能用 `sum` 得到 `8`；如果输入 `5` 表示“当前总量是 5”，继续求和会重复累计。

`partial-update` 适合订单流和物流流分别更新宽表字段。多个独立流有各自的乱序版本时，应使用 Sequence Group 分别保护字段，不能让一个流的版本号阻断另一个流的更新。多个独立 Job 写同一分区时，还必须满足 Bucket 并发约束，不能因为使用了 `partial-update` 就忽略写入拓扑。

## 九、Changelog Producer 的选择标准

Changelog Producer 决定主键表向流式下游暴露什么变化，不决定表是否为主键表，也不改变同一 Snapshot 上普通批查询的逻辑结果。

| 模式 | 旧值来源 | 适用前提 | 主要代价或边界 |
| --- | --- | --- | --- |
| `none` | 不额外生成完整旧值 | 下游按主键覆盖，或由 Flink Normalize 维护旧值 | 需要撤回时，成本可能转移到下游状态 |
| `input` | Writer 实际收到的 Changelog | 上游已有完整且可直接作为目标表状态变化的新旧行 | 不会补造缺失旧值，也不会把局部字段补成完整行 |
| `lookup` | 查询 Paimon 中受影响主键的旧状态 | 输入缺少旧值，下游必须获得完整撤回与更新 | 增加 Lookup、缓存、本地磁盘和 Compaction 成本 |
| `full-compaction` | 比较相邻两次 Full Compaction 的完整表状态 | 可接受更高延迟，只关心周期性状态差异 | 中间更新可能折叠，Full Compaction 写放大较高 |

订单从“待支付”更新到“已支付”时，完整 Changelog 应为：

```text
-U  order_id=1, status=待支付
+U  order_id=1, status=已支付
```

上游 CDC 已经提供这两条完整记录，且 Writer 收到的记录等价于目标表最终逻辑行时，使用 `input` 可以直接保存输入 Changelog。若上游只有 `+U 已支付`，而下游聚合需要撤回“待支付”，则使用 `lookup` 查询旧状态并生成完整 Before/After。

`partial-update` 和 `aggregation` 的输入往往是局部字段或增量贡献，并不等于合并后的完整行。即使输入带有完整 `RowKind`，下游需要完整合并结果时通常仍要使用 `lookup`。选择时应同时检查行镜像、变更种类和 Merge Engine 语义，不能只看“上游是 CDC”或“下游是实时任务”。

## 十、Partition、Fixed Bucket 与 Dynamic Bucket

Partition 和 Bucket 处于不同层次：Partition 是业务字段形成的目录和生命周期边界；Bucket 是每个分区内部的数据分片。Fixed/Dynamic 描述的是 Bucket，不是分区。

### 10.1 Fixed Bucket

```sql
WITH (
    'bucket' = '16'
)
```

每个分区固定使用 16 个 Bucket，同一 Bucket Key 通过确定性 Hash 路由。Fixed Bucket 适用于：

- 多个 Job 可能写入同一个分区；
- 查询经常按完整 Bucket Key 做 `=` 或 `IN` 过滤；
- Spark Bucket Join 需要兼容的分布；
- 能根据单分区规模、并行度和数据倾斜规划 Bucket 数。

“数据量稳定”只表示 Bucket 数比较容易提前规划，不是 Fixed Bucket 的定义。Bucket 太少会限制并行并形成热点，太多会产生小文件和元数据压力。

### 10.2 Dynamic Bucket

主键表省略 `bucket` 或设置 `bucket=-1` 时使用动态 Bucket。Paimon 根据数据增长增加 Bucket，并维护主键 Hash 到 Bucket 的映射。

主键包含全部分区字段时，更新不会跨分区，属于普通动态 Bucket；主键缺少分区字段且需要跨分区 Upsert 时，还要维护主键到 Partition 与 Bucket 的映射，启动扫描和索引成本更高。

动态 Bucket 的并发边界是：同一分区只能由一个写入 Job 负责。主键包含分区字段只消除了跨分区更新，没有协调多个 Job 的动态 Bucket 分配。两个 Job 各自维护索引时，同一主键可能被分配到不同 Bucket，Snapshot 的乐观并发提交不能修复这种映射分歧。

多个 Job 可以并发写不同分区，但分区归属必须在迟到数据、补数和重跑期间仍然互斥。例如流任务写当前分区、批任务覆盖历史分区是可行拓扑；两个任务都可能写当天分区则不满足约束。

### 10.3 Fixed Bucket 的多 Writer 边界

Fixed Bucket 消除了动态分配不一致：相同 Bucket Key 在所有 Job 中都会计算到同一个 Bucket。因此，多 Job 可以写同一分区，但仍要处理三类问题：

1. 多个 Writer 同时 Compaction 相同文件会产生文件冲突和作业恢复。
2. 多个 Job 更新同一主键时，需要 `sequence.field`、Sequence Group 或明确的 Snapshot Ordering 规则决定新旧。
3. 所有 Writer 必须使用兼容的 Catalog 提交与共享锁配置，尤其不能把对象存储 rename 当成 HDFS 原子 rename。

多个来源必须写动态 Bucket 同一分区时，应先在 Flink 中 `UNION ALL` 成一个写入 Job；如果必须保留多个独立 Job，应改用 Fixed Bucket，并将 Compaction 交给唯一的独立作业。

## 十一、独立 Compaction Job

多个 Writer 写固定 Bucket 表时，可以让写入 Job 只产出文件和提交 Snapshot，由第三个 Flink Job 统一 Compaction。

目标表配置：

```sql
ALTER TABLE dwd.orders SET (
    'write-only' = 'true'
);
```

`write-only=true` 关闭 Writer 内的 Compaction 和 Snapshot 过期维护。已经运行的写入 Job 通常需要重启，才能使用新的表选项。

Flink 1.19+ 可以在独立 SQL Client 或 SQL Gateway Session 中提交持续运行的 Compaction：

```sql
USE CATALOG my_catalog;

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '1 min';
SET 'pipeline.name' = 'orders-dedicated-compaction';

CALL sys.compact(
    `table` => 'dwd.orders',
    options => 'sink.parallelism=8'
);
```

SQL Procedure 需要 Flink 1.18+；Flink 1.18 通常使用对应版本的位置参数签名。生产环境也可以通过匹配当前 Paimon/Flink 版本的 Action Jar 提交：

```bash
$FLINK_HOME/bin/flink run -d \
  -Dexecution.runtime-mode=streaming \
  -Dpipeline.name=orders-dedicated-compaction \
  /opt/paimon/paimon-flink-action-<paimon-version>.jar \
  compact \
  --warehouse hdfs:///warehouse/paimon \
  --database dwd \
  --table orders \
  --table_conf sink.parallelism=8
```

同一张表、同一组分区只保留一个 Compaction Job。`write-only` 加独立 Compaction 可以消除多个 Writer 争抢 Compaction 输入文件的问题，但不能解除动态 Bucket 对同分区单 Writer 的限制。

### 11.1 一个 Job 批量维护多张表

`compact_database` 可以在一个 Flink Job 中维护一批表：

```sql
SET 'execution.runtime-mode' = 'streaming';
SET 'pipeline.name' = 'paimon-dwd-compaction';

CALL sys.compact_database(
    including_databases => 'dwd',
    mode => 'combined',
    including_tables => 'fact_.*|dim_.*',
    excluding_tables => 'fact_tmp_.*',
    table_options => 'sink.parallelism=8,continuous.discovery-interval=30s'
);
```

`combined` 使用一个组合 Sink，能够自动发现新表，适合大量中小表；`divided` 为每张表创建独立 Sink，隔离更强，但 Job Graph 更大，新增表通常需要重启。写入量大、延迟要求高或故障影响面需要隔离的表，应使用独立 Compaction Job，不要全部放进一个 `combined` 作业。

批模式适合定时执行一次：

```bash
$FLINK_HOME/bin/flink run -d \
  -Dexecution.runtime-mode=batch \
  /opt/paimon/paimon-flink-action-<paimon-version>.jar \
  compact_database \
  --warehouse hdfs:///warehouse/paimon \
  --including_databases dwd \
  --including_tables 'dwd\.fact_.*|dwd\.dim_.*' \
  --mode combined \
  --compact_strategy full \
  --table_conf sink.parallelism=8
```

持续流式维护一般使用 `minor` 策略；`full` 只用于批模式。不要让单表 Compaction Job 与 `compact_database` 同时覆盖同一张表。

## 十二、时间旅行与长期历史

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

## 十三、建表决策顺序

1. **定义记录语义**：每条输入是独立事件，还是业务键的当前状态？前者选 Append Table，后者选 Primary Key Table，不能用下游是否流式处理代替这个判断。
2. **定义业务唯一性**：主键是什么？分区字段是否会变？主键不含分区字段时是否允许跨分区更新？
3. **定义同键合并**：完整状态覆盖、字段部分更新、增量聚合，还是只保留首条？
4. **定义下游变更契约**：只需 Upsert，还是必须拿到完整 `-U/+U`？旧值来自上游、Paimon Lookup 还是 Full Compaction 对比？
5. **确认写入拓扑**：同一分区有几个独立 Job？Dynamic Bucket 只能单 Job，Fixed Bucket 的多 Writer 需要统一 Compaction 与顺序规则。
6. **设计物理分布**：根据分区生命周期、单分区数据量、查询谓词、并行度和倾斜选择 Partition、Bucket 与文件大小。
7. **按查询补充索引**：依次利用 Partition Pruning、Bucket Pruning、文件统计和 File Index，不给低选择性或重复能力的字段盲目建索引。
8. **根据运行指标调参**：检查 L0 文件数、单文件大小、Compaction 积压、扫描文件数、Writer 内存、Checkpoint 和下游 Lag。

## 十四、原文结论的使用边界

> [!warning] 原文未声明 Paimon 版本
> Snapshot 字段、参数默认值、Bucket 约束、File Index 可用性和各引擎的 SQL 能力可能随版本变化。建表或调参前，以目标版本官方文档、实际 `SHOW CREATE TABLE` 和运行指标为准。

- “Paimon = RocksDB 的 LSM-Tree + Iceberg 的 Snapshot/Manifest”适合建立粗粒度心智模型，不表示三者在实现、文件格式或兼容性上等价。
- `lookup` 不是所有 CDC 的固定选项；上游已提供完整且最终语义正确的 Changelog 时，`input` 通常更直接。
- `bucket=-1` 在 Append 表中表示 Bucket-Unaware，在主键表中表示 Dynamic Bucket，不能混用两套并发结论。
- 存储上的原子发布不能一律概括为“依赖 rename”；文件系统、对象存储和 Catalog-managed 模式的提交机制可以不同。
- 不要把文章里的文件大小、缓冲区、Snapshot 保留数等默认值直接带入生产。版本、数据量、并行度和 SLA 都会改变结论。

## 十五、问题定位速查

| 现象 | 先看什么 | 不要立即下的结论 |
| --- | --- | --- |
| 新数据查不到 | Flink Checkpoint、Paimon `$snapshots`、Committer 错误 | Writer 生成了文件就已经可见 |
| 主键表查询慢 | L0 文件数、主键范围重叠、Compaction 延迟、裁剪效果 | 只要增加 Bucket 数就能解决 |
| 时间旅行查不到历史 | `$snapshots`、`$tags`、保留参数和目标时区 | `dt` 分区存在就一定有当时的系统版本 |
| 下游没有 `UPDATE_BEFORE` | Sink 实际 `RowKind`、`changelog-producer`、Merge Engine | 主键表天然产生完整 Before/After |
| 动态 Bucket 多 Job 后出现重复键 | 是否写到同一分区、各 Job 的路由索引、Bucket 分布 | 主键包含分区字段就支持同分区多 Writer |
| 独立 Compaction 仍频繁冲突 | 是否存在重叠 Compactor、Writer 是否仍执行 Compaction | `write-only=true` 能解决所有并发问题 |
| File Index 没有改善查询 | `$file_indexes` 覆盖率、谓词下推、计划和 Scan 指标 | 建表选项存在就代表查询已使用索引 |

## 十六、官方文档入口

- [Changelog Producer](https://paimon.apache.org/docs/master/primary-key-table/changelog-producer/)
- [Partial Update](https://paimon.apache.org/docs/master/primary-key-table/merge-engine/partial-update/)
- [Append Table](https://paimon.apache.org/docs/master/append-table/)
- [Bucketed Append](https://paimon.apache.org/docs/master/append-table/bucketed/)
- [Concurrency Control](https://paimon.apache.org/docs/master/concepts/concurrency-control/)
- [Dedicated Compaction](https://paimon.apache.org/docs/master/maintenance/dedicated-compaction/)
- [Flink Compaction Procedures](https://paimon.apache.org/docs/master/flink/procedures/compaction/)
- [File Index](https://paimon.apache.org/docs/master/concepts/spec/fileindex/)
- [System Tables](https://paimon.apache.org/docs/master/concepts/system-tables/)

## 十七、相关笔记

- [[Apache Paimon 表模型、存储组织与读取语义]]
- [[Paimon Snapshot、Manifest、Parquet 与 Compaction 的关系]]
- [[Paimon 流式读写中的 Snapshot 与 Changelog]]
- [[Paimon L0、L1、Snapshot 与查询裁剪]]
- [[Paimon 动态 Bucket 的索引机制与单 Writer 限制]]
- [[Paimon File Index：存储、读写与选型]]
