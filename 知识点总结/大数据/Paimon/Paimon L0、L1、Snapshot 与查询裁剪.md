# Paimon LSM 层级、Snapshot、File Index 与查询裁剪

## 结论

L0、L1 描述主键数据文件在 LSM 结构中的层级，不是 Snapshot 层级，也通常不是磁盘目录。Snapshot 记录某次提交后哪些文件有效；Compaction 读取多个数据文件，合并相同主键的物理版本，写出新的 L1 及以上文件，再提交新 Snapshot 发布文件替换。

```text
Compaction：合并数据文件
File Index：为单个数据文件提供裁剪能力
Snapshot：发布某次提交后的有效文件集合
```

## 对象之间的关系

| 对象 | 代表什么 | 保存业务数据 |
| --- | --- | --- |
| Data File | 实际的 ORC、Parquet 等数据文件 | 是 |
| L0、L1 | Data File 的 LSM 层级 | 否，是文件元数据 |
| File Index | 针对单个 Data File 构建的索引 | 否 |
| Manifest | 记录文件的 ADD、DELETE 和 `DataFileMeta` | 否 |
| Snapshot | 一次成功提交后的表版本 | 否 |
| Compaction | 读取旧文件并写出新文件的过程 | 不单独保存数据 |

查询文件的链路是：

```text
Snapshot
→ Manifest List
→ Manifest
→ DataFileMeta
   ├─ fileName
   ├─ partition
   ├─ bucket
   ├─ level
   ├─ minKey / maxKey
   └─ Min/Max、Null Count、File Index 信息
→ Data File
```

## L0、L1 对应真实数据文件

假设 Bucket 目录中存在三个真实文件：

```text
dt=2026-09-17/
└── bucket-0/
    ├── data-A.parquet
    ├── data-B.parquet
    └── data-C.parquet
```

Manifest 可能记录：

```text
data-A.parquet → bucket=0, level=0
data-B.parquet → bucket=0, level=0
data-C.parquet → bucket=0, level=1
```

目录中通常没有单独的 `L0/`、`L1/`。Level 保存在 `DataFileMeta.level` 中，只看文件名不一定能判断层级。

### L0

L0 是新写入、尚未完成跨文件主键合并的数据文件。单个 L0 文件内部通常已经按主键排序，也可能经过 Writer 本地合并；但不同 L0 文件的主键范围可以重叠，同一个主键可能存在于多个文件中。

```text
L0 data-A：id=1, score=95, sequence=1
L0 data-B：id=1, score=80, sequence=2
```

这里的“没有整理”指没有完成跨文件合并，不代表文件内部完全无序。

### L1 及以上

Compaction 读取主键范围重叠的文件，根据主键、Sequence 和 Merge Engine 合并记录版本，写出新的文件：

```text
data-A(L0) + data-B(L0)
→ Compaction
→ data-C(L1)：id=1, score=80, sequence=2
```

在正常的 LSM 组织下，同一个 Partition 和 Bucket 内，L1 及以上同层文件的主键范围不重叠。L1 不是永久终态；后续新写入仍会产生 L0 文件，新的 Compaction 可以继续重写 L1 或生成更高层级文件。

## Snapshot 不负责合并数据

Snapshot 是元数据版本，不读取和合并业务记录。一次普通写入或 Compaction 成功提交后，Paimon都会生成新 Snapshot。

```text
Snapshot 1 → data-A(level=0)

Snapshot 2 → data-A(level=0) + data-B(level=0)

Compaction：data-A + data-B → data-C(level=1)

Snapshot 3 → data-C(level=1)
```

Snapshot 3 没有合并 Snapshot 1 和 Snapshot 2。真正发生的是：Compaction 合并 `data-A`、`data-B`，然后 Snapshot 3 记录删除旧文件、增加新文件。

旧 Snapshot 通常仍会保留一段时间：

```text
Snapshot 1 → data-A
Snapshot 2 → data-A + data-B
Snapshot 3 → data-C       ← 当前快照
```

旧 Snapshot 用于 Time Travel、增量读取和恢复。只有相关 Snapshot、Tag 等引用过期后，旧数据文件才能被物理清理。

## 为什么 L0 不能随便按 Value 裁剪

主键表的最终逻辑行来自同一主键所有相关物理版本的合并。单个 L0 文件的 value 统计只描述该文件，不能代表 Merge 后的最终值。

仍以这两个版本为例：

```text
L0 data-A：id=1, score=95, sequence=1
L0 data-B：id=1, score=80, sequence=2
```

执行：

```sql
SELECT * FROM t WHERE score > 85;
```

如果按单文件的 `score` 统计裁剪，`data-B` 会因为最大值只有 80 而被跳过。Reader 只看到 `data-A`，可能错误输出已经过期的 `score=95`。

正确顺序是：

```text
读取 data-A、data-B
→ 按 id 和 sequence 合并
→ 得到最终值 score=80
→ 判断 score > 85 不成立
```

主键条件可以安全裁剪。查询 `id=1` 时，如果某文件的主键范围是 `100～200`，可以确定它不包含 `id=1` 的任何版本，直接跳过不会破坏 Merge。

- `keyFilter` 判断文件是否可能包含目标主键，L0 可以使用。
- `valueFilter` 依赖 Merge 后的最终值，不能仅凭单个 L0 文件随意裁剪。
- L1 及以上已经完成跨文件整理，更适合逐文件执行 value 裁剪。

## Compaction 是否只生成索引

不是。Compaction 的主要工作是合并数据文件和记录版本：

```text
读取旧 Data File
→ 按主键排序、合并或聚合
→ 写出新 Data File
→ 为新文件计算统计和索引
→ Manifest 记录旧文件 DELETE、新文件 ADD
→ 提交新 Snapshot
```

如果表配置了 File Index，Compaction 写出新文件时会重新生成：

- Min/Max、Null Count
- `minKey`、`maxKey`
- Bloom Filter、Bitmap、BSI 等 File Index
- 行数、文件大小和 Sequence 范围

所以 L0 变成 L1 不只是“创建新索引”，而是生成包含合并结果的新数据文件，并为这个新文件重新计算元数据和索引。

## Min/Max 与 File Index 的执行阶段

查询裁剪分成 Scan 和 Read 两个阶段：

```text
Scan 阶段
  读取 Snapshot、Manifest List、Manifest
  → 分区和 Bucket 裁剪
  → Min/Max、Null Count 文件级裁剪
  → embedded File Index 文件级裁剪
  → 生成 DataSplit

Read 阶段
  打开保留下来的文件
  → 加载 embeddedIndex 或独立 .index 文件
  → 继续整文件裁剪
  → Bitmap/BSI 生成命中行号
  → ORC/Parquet Reader 读取数据
```

Min/Max 和 Null Count 保存在 Manifest 的 `DataFileMeta` 中，Scan 阶段无需打开数据文件即可使用。

File Index 根据大小采用不同保存方式：

```text
小索引 → embeddedIndex，嵌入 DataFileMeta，Scan 阶段可以使用
大索引 → 独立 .index 文件，通常到 Read 阶段再加载
```

因此，File Index 不是只在 Read 阶段使用。内嵌索引可以在 Scan 阶段完成文件级裁剪；独立索引和精确行号选择主要发生在 Read 阶段。

## File Index 如何选择

同一张表可以同时配置多种 File Index，不是全表只能选一种。通常按列的查询模式分配：

| 索引 | 适用条件 |
| --- | --- |
| Bloom Filter | 高基数列的等值查询，例如订单号、用户 ID |
| Bitmap | 低基数枚举列的 `=`、`IN`、`IS NULL` |
| BSI | 数值、时间字段的范围查询 |
| RangeBitmap | 范围过滤和 TopN |

例如可以给 `order_id` 配置 Bloom Filter，给 `status` 配置 Bitmap，给 `amount` 配置 BSI。同一列在底层可以包含多种索引，但每增加一种索引都会增加写入、内存、存储和读取成本，通常为一列选择最匹配查询模式的一种即可。

如果文件内部数据有序，Min/Max 已经能排除大部分文件，额外 File Index 的收益可能有限。

## 使用 `$files` 检查文件状态

`$files` 系统表返回目标 Snapshot 中所有可见数据文件的元数据，一行对应一个 Data File：

```sql
SELECT
    partition,
    bucket,
    file_path,
    level,
    record_count,
    file_size_in_bytes,
    min_key,
    max_key,
    min_value_stats,
    max_value_stats,
    null_value_counts,
    file_source,
    schema_id
FROM my_table$files
ORDER BY partition, bucket, level, file_path;
```

可以据此检查：

- 是否积累了大量 L0 文件。
- 同一 Bucket 内文件的主键范围是否重叠。
- 查询列的 Min/Max 是否过宽，导致无法裁剪。
- 文件是否过大或过碎。
- 文件来自普通写入还是 Compaction。

`file_source=COMPACT` 只能辅助判断文件来源，判断 LSM 层级应以 `level` 为准。

`$files` 不会完整展示独立索引文件、`embeddedIndex`、`extraFiles` 等内部信息，因此可以诊断文件布局和统计信息，但不能单独证明某个 File Index 已经生效。

### 返回样例与裁剪判断

假设主键表定义如下：

```sql
CREATE TABLE orders (
    dt STRING,
    order_id BIGINT,
    amount DECIMAL(10, 2),
    status STRING,
    event_time TIMESTAMP(3),
    PRIMARY KEY (dt, order_id) NOT ENFORCED
) PARTITIONED BY (dt);
```

只查询诊断需要的字段，避免 `SELECT *` 返回过宽：

```sql
SELECT
    partition,
    bucket,
    file_path,
    level,
    record_count,
    file_size_in_bytes,
    min_key,
    max_key,
    min_value_stats,
    max_value_stats,
    null_value_counts,
    file_source
FROM `orders$files`
ORDER BY partition, bucket, level, file_path;
```

下面是简化后的示例结果。统计字段的字符串格式会因 Paimon 版本和计算引擎而略有不同：

| 文件 | Partition | Bucket | Level | 大小 | `min_key`～`max_key` | `amount` Min/Max | 来源 |
| --- | --- | ---: | ---: | ---: | --- | --- | --- |
| F1 | `dt=2026-09-16` | 0 | 1 | 128 MB | `[2026-09-16,100000]`～`[2026-09-16,199999]` | 1～99 | COMPACT |
| F2 | `dt=2026-09-16` | 0 | 1 | 124 MB | `[2026-09-16,200000]`～`[2026-09-16,299999]` | 100～199 | COMPACT |
| F3 | `dt=2026-09-16` | 0 | 1 | 132 MB | `[2026-09-16,300000]`～`[2026-09-16,399999]` | 200～299 | COMPACT |
| F4 | `dt=2026-09-16` | 0 | 0 | 8 MB | `[2026-09-16,120001]`～`[2026-09-16,398876]` | 10～500 | APPEND |

执行：

```sql
SELECT *
FROM orders
WHERE dt = '2026-09-16'
  AND amount > 250;
```

文件级判断如下：

```text
F1：max(amount)=99  → 不可能满足 amount > 250，跳过
F2：max(amount)=199 → 不可能满足 amount > 250，跳过
F3：max(amount)=299 → 可能命中，保留
F4：max(amount)=500 → 可能命中，保留
```

F4 只有 8 MB，但 `amount` 范围覆盖 10～500。范围越宽，越多查询值会落入区间，Min/Max 越难排除该文件。F1～F3 的取值范围相对集中，更适合范围裁剪。

### `min_value_stats`、`max_value_stats` 的边界

在主键表中，这两个字段通常保存该文件中 value 列的独立统计。例如：

```text
min_value_stats = {amount=10.00, status=CREATED, event_time=2026-09-16 00:00:00}
max_value_stats = {amount=999.00, status=SUCCESS, event_time=2026-09-16 23:59:59}
```

它不等于“当前 Schema 中全部非主键字段的完整统计”。以下情况可能导致字段缺失或值为 null：

- 表只为部分列收集统计信息。
- 文件由旧 Schema 写入，当时还没有该列。
- Partial Update 文件只写入部分字段。
- Dense Stats 只保存实际存在且有意义的统计列。
- 列类型或旧文件元数据无法提供可安全使用的统计。

主键字段的列统计由内部 `keyStats` 维护，`$files` 中的 `min_value_stats`、`max_value_stats` 不能替代它。Append-Only 表没有 Key/Value 拆分，这两个字段通常对应普通数据列，但同样受统计配置和 Schema 演化影响。

Min/Max 是各列独立计算的边界：

```text
min_value_stats = {amount=10, city=Beijing}
max_value_stats = {amount=500, city=Shanghai}
```

这不表示一定存在 `(amount=10, city=Beijing)` 这一行，只能说明 `amount` 和 `city` 各自的范围。

统计缺失时，Paimon 无法证明文件不含目标记录，会保守保留文件：可以多读，不能因为缺少统计而漏掉结果。

### `min_key`、`max_key` 的边界

`min_key`、`max_key` 是文件内完整主键元组的排序边界。对于复合主键 `(dt, order_id)`：

```text
min_key = [2026-09-16, 100000]
max_key = [2026-09-16, 199999]
```

它表示完整元组从 `(2026-09-16, 100000)` 排到 `(2026-09-16, 199999)`，不是每个主键字段各自独立的 Min/Max。

例如：

```text
min_key = [2026-09-15, 900000]
max_key = [2026-09-16, 100000]
```

不能据此认为 `order_id` 的独立范围是 100000～900000。复合主键按字段顺序比较：先比较 `dt`，`dt` 相同时才比较 `order_id`。

```text
min_key / max_key
→ 完整主键元组边界
→ 用于主键过滤、范围重叠判断和 LSM 合并

min_value_stats / max_value_stats
→ 各 value 列独立的统计边界
→ 用于普通字段的文件裁剪
```

### 诊断 L0 与小文件

可以按 Partition、Bucket、Level 汇总：

```sql
SELECT
    partition,
    bucket,
    level,
    COUNT(*) AS file_count,
    SUM(record_count) AS physical_record_count,
    SUM(file_size_in_bytes) / 1024 / 1024 AS total_size_mb,
    AVG(file_size_in_bytes) / 1024 / 1024 AS avg_file_size_mb
FROM `orders$files`
GROUP BY partition, bucket, level
ORDER BY partition, bucket, level;
```

如果某个 Bucket 同时出现“大量 `level=0` 文件”和“平均文件很小”，需要继续检查写入是否倾斜、Compaction 是否跟得上写入、Compaction 资源是否不足，以及 Checkpoint 是否过于频繁。

`$files` 只能说明裁剪所需的统计和文件布局是否具备。它不能证明某条 SQL 实际跳过了多少文件，也不能证明某个 File Index 被使用。确认实际效果还要查看执行计划和 Scan 指标，例如候选文件数、实际读取文件数和读取字节数。新版 Paimon 可以通过 `$file_indexes` 检查索引覆盖范围，但索引存在仍不等于查询使用了它。

## 记忆模型

```text
Writer 写出新文件
→ 新文件进入 L0
→ Snapshot 提交并发布 L0 文件
→ 查询可能在 Reader 中合并多个主键版本
→ Compaction 合并重叠文件
→ 写出 L1+ 新文件并重建统计和索引
→ 新 Snapshot 发布文件替换
```

需要分清的对象：

```text
L0/L1：数据文件的整理层级
Compaction：合并数据文件的操作
File Index：单个数据文件的查询索引
Manifest：数据文件目录及其元数据
Snapshot：一次提交后的有效文件集合
```

## 相关笔记

- [[Paimon Snapshot、Manifest、Parquet 与 Compaction 的关系]]
- [[Apache Paimon 表模型、存储组织与读取语义]]
- [[Paimon 流式读写中的 Snapshot 与 Changelog]]

## 参考资料

- [原文：Paimon 精讲（八）：一条查询是如何跳过 99% 数据的？](https://mp.weixin.qq.com/s/HJCwD-S3u96fv3uyUx4aYA)
- [Apache Paimon：System Tables](https://paimon.apache.org/docs/master/concepts/system-tables/)
- [Apache Paimon：Query Performance](https://paimon.apache.org/docs/master/append-table/query-performance/)
- [Apache Paimon：File Index](https://paimon.apache.org/docs/master/concepts/spec/fileindex/)
