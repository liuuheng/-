---
title: "Paimon 精讲（八）：查询裁剪、统计信息与 Files Table（文章批注版）"
aliases:
  - "Paimon 精讲（八）：一条查询是如何跳过 99% 数据的？裁剪链路全记录"
author: "胖泽的技术笔记"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s/HJCwD-S3u96fv3uyUx4aYA"
published_at: "2026-08-12"
reviewed_at: "2026-10-14"
tags:
  - Flink
  - Paimon
  - Paimon-精讲
---

# Paimon 精讲（八）：查询裁剪、统计信息与 Files Table（文章批注版）

> [!info] 来源、整理方式与版本边界
> 原文：[《Paimon 精讲（八）：一条查询是如何跳过 99% 数据的？裁剪链路全记录》](https://mp.weixin.qq.com/s/HJCwD-S3u96fv3uyUx4aYA)，胖泽的技术笔记，2026-08-12。正文按原文章节顺序保留技术内容、代码、表格、字段和案例；新增内容统一标为“批注”“勘误”或“版本边界”。原文没有声明准确的 Paimon/Flink 版本，内部类、默认值和源码路径不视为稳定 API。

> [!warning] 批注：“跳过 99%”是示例结果，不是索引保证
> 实际裁剪率取决于分区过滤、谓词形态、文件内数据分布、统计信息完整度、索引覆盖率和执行引擎是否完成下推。必须结合 `EXPLAIN`、扫描文件数、读取字节数和 `$files`/`$file_indexes` 验证。
> 当前文档将旧文中的 BSI 对应为 Range Bitmap，配置项为 `file-index.range-bitmap.columns`。
## 引言

在之前的文章我们从底层设计及源码层面拆解了 [File Index 的底层存储结构](https://mp.weixin.qq.com/s?__biz=MzYzNTk5NjUzOA==&mid=2247483739&idx=1&sn=69609f0cf36d86f7a205cff29223fbf2&scene=21#wechat_redirect)，回答了“File Index 到底怎么存”的问题。但存起来终究是为了用——**当查询到来时，Paimon 是如何利用这些索引信息，快速跳过不相关的文件，实现秒级响应的？**

这就引出了 Paimon 查询裁剪机制的核心三板斧：**File Index、Min/Max 统计与 Files Table**。

- **File Index** 提供了文件级别的元数据基础，是裁剪的“原材料”；
- **Min/Max 统计** 是最常用的裁剪手段，通过范围判断提前过滤掉大量无关文件；
- **Files Table** 则是一个常被忽视的调试利器，让你直接透视每个文件的统计信息，精准定位裁剪是否生效。

本文从这三者的协作关系出发，深度解析 Paimon 查询裁剪的完整链路：统计信息如何维护、裁剪条件如何触发、Files Table 怎么用，以及实际场景中的效果分析。读完你就能理解，为什么同样的 SQL，有的跑得飞快，有的却慢如蜗牛。

---

## 一、查询裁剪全链路概述

Paimon 的查询裁剪分为两个阶段，两阶段相互衔接，层层过滤：


```
Scan 阶段（读 Manifest，无额外 IO）
  └── Min/Max 统计裁剪（文件级）
  └── File Index 裁剪（仅用 embeddedIndex，文件级）
         ↓ 通过
Read 阶段（打开数据文件）
  └── File Index 裁剪（读 .index 文件，文件级 + 行级）
         ↓ 通过
  └── 实际读取数据行

```


---

## 二、File Index 查询裁剪

## 2 .1 索引存储结构

索引写入时，`DataFileIndexWriter.close()` 根据大小决定存储位置：


```
大小 <= file-index.in-manifest-threshold（默认 500B）
  → embeddedIndex 字段，嵌入 DataFileMeta，随 Manifest 存储（零额外 IO）

大小 > 阈值
  → 独立 .index 文件（如 data-xxx.orc.index）
    路径记录在 DataFileMeta.extraFiles()

```


**索引文件格式**（`FileIndexFormat`）：


```
HEAD:
  magic (8B) | version (4B) | headLength (4B)
  foreach column:
    columnName | indexSize
    foreach indexType: indexName | startPos | length

BODY:
  各列各类型索引的序列化字节数组，按顺序拼接

```


## 2 .2 索引类型与裁剪能力

| 索引类型 | 支持操作 | 返回粒度 | 适用场景 |
| --- | --- | --- | --- |
| BloomFilter |`=` | 文件级 SKIP/REMAIN | 高基数等值查询 |
| Bitmap |`=``!=``IN``NOT IN``IS NULL` | **行级**  RoaringBitmap | 低基数枚举列 |
| BSI (BitSliceIndex) | 全范围`<``<=``>``>=``BETWEEN` | **行级**  RoaringBitmap | 数值/时间范围查询 |
| RangeBitmap | 全范围 +`TopN` | **行级**  RoaringBitmap | 范围 + TopN 加速 |

配置示例：


```
'file-index.bloom-filter.columns' = 'col1,col2'
'file-index.bitmap.columns'       = 'status'
'file-index.bsi.columns'          = 'amount'

```


## 2 .3 Scan 阶段裁剪（仅 embeddedIndex）

> [!note] 补充批注：“零额外 IO”只描述已随元数据读取的 embeddedIndex
> Scan 阶段使用内嵌索引不必额外打开 sidecar 索引文件，但查询仍要读取 Snapshot、Manifest 以及裁剪后保留的数据文件。它不是整条查询链路零 IO。


```
AbstractFileStoreScan.plan()
  └── readManifestEntries()
        └── filterByStats(ManifestEntry entry)
              ├── Min/Max 统计裁剪
              └── testFileIndex(file.embeddedIndex(), entry)
                    └── FileIndexPredicate(embeddedBytes, rowType).evaluate(predicate)
                          → SKIP   → 文件从 plan 中移除
                          → REMAIN → 保留，进入 Read 阶段

```


Scan 阶段**只读 Manifest 中已有的 embeddedIndex**，不产生额外文件 IO。

## 2 .4 Read 阶段裁剪（含独立 .index 文件）


```
RawFileSplitRead.createReader(DataSplit)
  └── FileIndexEvaluator.evaluate(file, ...)
        ├── file.embeddedIndex() != null → 用内存 bytes
        └── 否则 → 读取 extraFiles() 中以 ".index" 结尾的独立索引文件
              └── FileIndexPredicate.evaluate(predicate)
                    ├── SKIP              → EmptyFileRecordReader（整文件跳过）
                    └── BitmapIndexResult → 行级精确过滤
                          ├── selection = RoaringBitmap32（命中行号集合）
                          ├── 传给 Parquet/ORC reader（列式 IO 层跳过未命中 row group）
                          └── ApplyBitmapIndexRecordReader 迭代时检查 bitmap.contains(position)

```


## 2 .5`FileIndexPredicate.evaluate()` 核心逻辑
`FileIndexReader` 实现`FunctionVisitor<FileIndexResult>`，各方法默认返回`REMAIN`，子类只重写所支持的操作符：


```
// 1. 提取谓词涉及的列名
Set<String> requiredFields = getRequiredNames(predicate);

// 2. 按列加载对应 FileIndexReader
Map<String, Collection<FileIndexReader>> readers = loadReaders(requiredFields);

// 3. 访问谓词树（Visitor 模式）
FileIndexResult result = new FileIndexPredicateTest(readers).test(predicate);
//   LeafPredicate(col, op, val) → 对该列所有 reader 调用 visitEqual/visitIn/... → AND 组合
//   CompoundPredicate(AND)     → 子结果 AND（短路求值）
//   CompoundPredicate(OR)      → 子结果 OR

```


`FileIndexResult` 只有两个单例：`REMAIN`（保留文件）和`SKIP`（跳过文件）；Bitmap 系索引返回`BitmapIndexResult`（持有`RoaringBitmap32`）用于行级过滤。

## 2 .6 写入路径（索引构建）


```
DataWriter.write(row)
  ├── 数据写入 ORC/Parquet 文件
  └── DataFileIndexWriter.write(row)
        └── 各 IndexMaintainer → FileIndexWriter.writeRecord(value)

DataFileIndexWriter.close()
  ├── 序列化为 HEAD + BODY 格式（FileIndexFormat）
  └── 大小判断：
        大 → 写独立 .index 文件，文件名存入 DataFileMeta.extraFiles()
        小 → 保留为 embeddedIndexBytes，存入 DataFileMeta.embeddedIndex()

```


## 2 .7 关键源码位置

| 类 | 路径 |
| --- | --- |
|`FileIndexFormat` |`paimon-common/.../fileindex/FileIndexFormat.java` |
|`FileIndexPredicate` |`paimon-common/.../fileindex/FileIndexPredicate.java` |
|`FileIndexReader` |`paimon-common/.../fileindex/FileIndexReader.java` |
|`DataFileIndexWriter` |`paimon-core/.../io/DataFileIndexWriter.java` |
|`FileIndexEvaluator` |`paimon-core/.../io/FileIndexEvaluator.java` |
|`RawFileSplitRead` |`paimon-core/.../operation/RawFileSplitRead.java` |
|`BitmapFileIndex` |`paimon-common/.../fileindex/bitmap/BitmapFileIndex.java` |
|`BitSliceIndexBitmapFileIndex` |`paimon-common/.../fileindex/bsi/BitSliceIndexBitmapFileIndex.java` |

---

## 三、Min/Max 统计在查询裁剪中的作用


> [!note] 批注：Min/Max 的有效性来自文件内值域足够窄
> 如果每个文件都混有很宽的数据范围，所有文件的 Min/Max 都可能与查询条件相交，统计裁剪就会失效。排序、Clustering 或合理 Bucket Key 可以缩小文件值域，但是否减少扫描仍要看最终计划。
## 3 .1 统计数据结构

**`SimpleColStats`**（写入阶段中间态，`paimon-common`）：


```
public class SimpleColStats {
    private final Object min;
    private final Object max;
    private final Long   nullCount;
}

```


**`SimpleStats`**（持久化格式，存储于`DataFileMeta`，`paimon-core`）：


```
_MIN_VALUES    BYTES          → BinaryRow，存储所有列的最小值
_MAX_VALUES    BYTES          → BinaryRow，存储所有列的最大值
_NULL_COUNTS   ARRAY(BIGINT)  → 各列 null 值数量

```


所有列的 min（或 max）被压缩编码为一个`BinaryRow`，读取时按列 index 定位，极省空间。

## 3 .2 统计信息的采集方式

写入时有两种采集路径，通过`SimpleStatsProducer` 统一抽象：


```
Parquet 格式 → ParquetSimpleStatsExtractor
  直接从 Parquet Footer 元数据读取，无需扫数据行（效率最高）

其他格式 → SimpleStatsCollector
  逐行 collect(row)，维护每列的 min/max/nullCount 状态

```


**主键表**写入时，整行统计按字段布局拆分为 keyStats 和 valueStats：


```
rowStats 布局: [key_fields... | sequenceNumber | valueKind | value_fields...]
                ↓ 按偏移截取
keyStats   ← copyOfRange(rowStats, 0, numKeyFields)
valueStats ← copyOfRange(rowStats, numKeyFields + 2, end)

```


**Dense Mode**（`statsDenseStore=true`）：`valueStats` 只存有意义的列，`valueStatsCols` 记录实际包含的列名列表，减少 Manifest 存储空间。

## 3 .3`filterByStats()` 调用链
**AppendOnly 表**（`AppendOnlyFileStoreScan`）：


```
SimpleStatsEvolution.Result stats = evolution.evolution(
    file.valueStats(), file.rowCount(), file.valueStatsCols());

boolean result = predicate.test(
    file.rowCount(), stats.minValues(), stats.maxValues(), stats.nullCounts());

// min/max 通过后，再追加 File Index 裁剪
if (result) result = testFileIndex(file.embeddedIndex(), entry);

```


**主键表**（`KeyValueFileStoreScan`）：


```
// 1. keyFilter 裁剪（主键谓词，始终执行）
predicate.test(rowCount, keyStats.minValues(), keyStats.maxValues(), keyStats.nullCounts());

// 2. valueFilter 裁剪（受 scanMode 控制）
//    L0 文件 key 有重叠，value 裁剪只能在 wholeBucket 粒度进行
//    L1+ 文件 key 无重叠，可逐文件裁剪

```


## 3 .4 谓词评估：`Predicate.test(rowCount, minValues, maxValues, nullCounts)``LeafPredicate` 对每一列的 min/max 做比较，各函数语义如下：

| 谓词 | Stats 判断条件 | 含义 |
| --- | --- | --- |
|`col = 5` |`5 >= min && 5 <= max` | 值 5 在文件范围内才可能存在 |
|`col > 5` |`max > 5` | 文件最大值 > 5，才可能有满足的行 |
|`col < 5` |`min < 5` | 文件最小值 < 5，才可能有满足的行 |
|`col >= 5` |`max >= 5` | 文件最大值 >= 5 |
|`col IS NULL` |`nullCount > 0` | 有 null 才可能通过 |
|`col IS NOT NULL` |`rowCount != nullCount` | 不全为 null |

当 min 或 max 为 null（该列无统计信息）时，保守返回`true`，不裁剪。

`CompoundPredicate(AND)` 对子谓词短路 AND，`OR` 任意一个通过即保留文件。

## 3 .5 Schema 演化的处理

当文件的`schemaId` 与当前表不一致时（加减列、改类型），`SimpleStatsEvolution` 做映射对齐：

-`indexMapping`：旧列索引 → 新 schema 列索引（新增列映射 -1）
- 新增列：`nullCount` 填`rowCount`（视为全空，不误裁剪）
- 类型变更列：`castFieldGetters` 做类型转换
- 不安全的 filter：`filterUnsafeFilter()` 过滤掉，避免语义错误

`SimpleStatsEvolutions` 通过`ConcurrentHashMap<schemaId, SimpleStatsEvolution>` 缓存各版本的映射，避免重复计算。

## 3 .6 DataFileMeta 中四个统计字段的区别

| 字段 | 类型 | 含义 |
| --- | --- | --- |
|`minKey`  /`maxKey` |`BinaryRow` | 主键的精确最小/最大值（完整主键 tuple），用于 LSM 合并排序，判断文件间 key 是否重叠 |
|`keyStats` |`SimpleStats` | 主键各列的 min/max/nullCount，用于主键谓词的文件裁剪 |
|`valueStats` |`SimpleStats` | value 列（AppendOnly 为全行）的 min/max/nullCount，用于 value 谓词的文件裁剪 |

`minKey/maxKey` 是一个完整的主键 tuple，`keyStats` 是各主键列的独立统计区间——两者互补。

## 3 .7 Min/Max 与 File Index 的分工


```
filterByStats（min/max）       → 第一道过滤，完全依赖 Manifest 内存数据，零额外 IO
  ↓ 通过
testFileIndex（Bloom/Bitmap/BSI） → 第二道过滤，更精确
  ↓ 通过
实际读取文件（Read 阶段）

```


| 维度 | Min/Max | File Index |
| --- | --- | --- |
| 数据来源 | Manifest（零额外 IO） | Manifest embedded 或 .index 文件 |
| 裁剪粒度 | 文件级 | 文件级（Bloom）或行级（Bitmap/BSI） |
| 适用查询 | 范围查询、等值查询 | 等值（Bloom）、范围（BSI）、精确行（Bitmap） |
| 局限性 | 数据分布不均时误判率高 | 需额外写入索引文件，占用存储 |

两者为**互补关系**：min/max 成本极低，是第一道宽口过滤；File Index 在 min/max 无法有效过滤时（如`col=5` 但文件 min=1/max=100）提供更精确的裁剪。

## 3 .8 关键源码位置

| 类 | 路径 |
| --- | --- |
|`SimpleColStats` |`paimon-common/.../format/SimpleColStats.java` |
|`SimpleStats` |`paimon-core/.../stats/SimpleStats.java` |
|`SimpleStatsConverter` |`paimon-core/.../stats/SimpleStatsConverter.java` |
|`SimpleStatsEvolution` |`paimon-core/.../stats/SimpleStatsEvolution.java` |
|`SimpleStatsEvolutions` |`paimon-core/.../stats/SimpleStatsEvolutions.java` |
|`Predicate` |`paimon-common/.../predicate/Predicate.java` |
|`LeafPredicate` |`paimon-common/.../predicate/LeafPredicate.java` |
|`KeyValueDataFileWriter` |`paimon-core/.../io/KeyValueDataFileWriter.java` |
|`AppendOnlyFileStoreScan` |`paimon-core/.../operation/AppendOnlyFileStoreScan.java` |
|`KeyValueFileStoreScan` |`paimon-core/.../operation/KeyValueFileStoreScan.java` |

---

## 四、Files Table 系统表字段详解


> [!warning] 边界：Files Table 展示物理文件元数据
> `$files` 中的记录数、Level、统计信息和序列范围描述文件，不等于主键表 Merge-Read 后的逻辑行数。Compaction、Delete 记录和同一 Key 的多个版本都会让物理计数与 `SELECT COUNT(*)` 不同。
`$files` 系统表暴露当前快照（或指定快照）中所有数据文件的元数据，每行对应一个`DataFileMeta`。

**查询示例：**


```
SELECT * FROM my_table$files;

-- 查询指定快照的文件列表
SELECT * FROM my_table$files /*+ OPTIONS('scan.snapshot-id'='3') */;

```


## 4 .1 基础定位字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
|`partition` | STRING | 文件所属分区值（字符串化），无分区表为 null |
|`bucket` | INT | 文件所属桶编号 |
|`file_path` | STRING | 文件完整路径；优先使用`externalPath`，否则拼接`bucketPath + "/" + fileName` |
|`file_format` | STRING | 文件格式，如`orc`、`parquet`，从文件名后缀提取 |

## 4 .2 Schema 与 LSM 结构字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
|`schema_id` | BIGINT | 写入该文件时所用的 Schema 版本 ID。若与当前表 schema_id 不一致，说明发生了 Schema 演化，读取时需通过`SimpleStatsEvolution` 做统计信息的列映射对齐 |
|`level` | INT | LSM 层级编号。AppendOnly 表固定为 0；主键表 L0 文件之间 key 有重叠，L1+ 文件之间 key 无重叠，直接影响 Compaction 策略和 value 谓词能否逐文件下推 |

## 4 .3 数据量统计字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
|`record_count` | BIGINT | 文件总行数，包含 add 行和 delete 行（`= addRowCount + deleteRowCount`） |
|`file_size_in_bytes` | BIGINT | 文件字节大小 |
|`delete_row_count` | BIGINT | 文件中的删除行数；旧版本文件该字段为 null（向后兼容）。`add_row_count = record_count - delete_row_count` |

## 4 .4 主键范围字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
|`min_key` | STRING | 文件中主键的最小值（字符串化的 BinaryRow）；AppendOnly 表无主键，为 null |
|`max_key` | STRING | 文件中主键的最大值；AppendOnly 表为 null |

这两个字段是精确的主键 tuple 值，用于判断两个文件的主键范围是否重叠，是 LSM 合并排序的核心依据。与`min/max_value_stats` 的区别在于：这里是完整主键的边界 tuple，后者是各列独立的统计区间。

## 4 .5 列级统计字段（用于查询裁剪）

| 字段 | 类型 | 说明 |
| --- | --- | --- |
|`null_value_counts` | STRING | 各列空值数量的 Map，如`{col1=0, col2=5}`，用于`IS NULL / IS NOT NULL` 谓词裁剪 |
|`min_value_stats` | STRING | 各列最小值的 Map，对应`DataFileMeta.valueStats.minValues` |
|`max_value_stats` | STRING | 各列最大值的 Map，对应`DataFileMeta.valueStats.maxValues` |

这三个字段在 Scan 阶段被`Predicate.test(rowCount, minValues, maxValues, nullCounts)` 消费，决定是否跳过该文件。

> 注：此处展示的是 value 列统计；主键列统计在内部通过`keyStats` 字段单独维护，不在 Files Table 直接展示。

## 4 .6 序列号字段（MVCC 与变更顺序）

| 字段 | 类型 | 说明 |
| --- | --- | --- |
|`min_sequence_number` | BIGINT | 文件内最小序列号 |
|`max_sequence_number` | BIGINT | 文件内最大序列号 |

序列号单调递增，每次写入操作分配。主键表用于判断同一 key 的多个版本哪个更新（序列号大的覆盖小的），也用于确定 changelog 的变更顺序。AppendOnly 表此字段为 null。

## 4 .7 文件来源与生命周期字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
|`creation_time` | TIMESTAMP | 文件创建时间戳 |
|`file_source` | STRING | 文件来源：`APPEND`（用户写入产生）或`COMPACT`（Compaction 合并产生）。可用于区分数据文件产生方式，辅助诊断 Compaction 效果 |

## 4 .8 高级功能字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
|`first_row_id` | BIGINT | 行追踪（Row Tracking）场景下该文件第一行的全局 Row ID；未开启行追踪的表为 null |
|`write_cols` | ARRAY<STRING> | Partial Update（列级更新）场景下本次实际写入的列名列表；null 表示写了全部列。读取时据此决定是否需要与其他文件的同 key 数据做列合并 |

## 4 .9 字段与内部存储对应关系


```
Files Table 字段            DataFileMeta 内部字段
──────────────────────────────────────────────────
partition               ← DataSplit.partition()
bucket                  ← DataSplit.bucket()
file_path               ← externalPath() 或 bucketPath + fileName()
file_format             ← DataFilePathFactory.formatIdentifier(fileName())
schema_id               ← schemaId()
level                   ← level()
record_count            ← rowCount()
file_size_in_bytes      ← fileSize()
min_key / max_key       ← minKey() / maxKey()
null_value_counts       ← valueStats.nullCounts
min_value_stats         ← valueStats.minValues
max_value_stats         ← valueStats.maxValues
min_sequence_number     ← minSequenceNumber()
max_sequence_number     ← maxSequenceNumber()
creation_time           ← creationTime()
delete_row_count        ← deleteRowCount()
file_source             ← fileSource()  （0=APPEND, 1=COMPACT）
first_row_id            ← firstRowId()
write_cols              ← writeCols()

```

>`embeddedIndex` 和`extraFiles`（索引相关字段）不直接暴露在 Files Table 中，但在查询裁剪（File Index 过滤）中发挥作用。

## 4 .10 关键源码位置

| 类 | 路径 |
| --- | --- |
|`FilesTable` |`paimon-core/.../table/system/FilesTable.java` |
|`DataFileMeta` |`paimon-core/.../io/DataFileMeta.java` |

---

## 五、完整查询裁剪数据流总结


```
写入时：
  DataWriter.write(row)
    ├── 数据写入 ORC/Parquet 文件
    ├── DataFileIndexWriter.write(row) → 各 FileIndexWriter.writeRecord(value)
    └── StatsCollectingSingleFileWriter → 采集 min/max/nullCount

  DataFileIndexWriter.close() + RowDataFileWriter.close()
    ├── 索引大小判断 → embeddedIndex 或 独立 .index 文件（extraFiles）
    └── 统计信息 → DataFileMeta { keyStats, valueStats, minKey, maxKey }
              → 写入 Manifest

查询时：
  Scan 阶段（读 Manifest，零额外 IO）
    └── filterByStats()
          ├── Min/Max 统计裁剪 → Predicate.test(rowCount, min, max, nullCounts)
          └── File Index 裁剪 → FileIndexPredicate(embeddedIndex).evaluate(predicate)

  Read 阶段（打开文件）
    └── FileIndexEvaluator.evaluate(file)
          ├── 读 embeddedIndex 或 独立 .index 文件
          ├── SKIP → EmptyFileRecordReader（整文件跳过）
          └── BitmapIndexResult → 行级精确过滤（RoaringBitmap32）
                └── ApplyBitmapIndexRecordReader.next() → bitmap.contains(position)

```


---

## 六、Scan 阶段裁剪全流程详解

> 本节对"读 Manifest（零额外 IO），用 Min/Max 统计 + embeddedIndex 做文件级裁剪"进行完整展开。

## 6 .1 为什么是"零额外 IO"

Paimon 的文件元数据全部持久化在 **Manifest 文件**中。每个`ManifestEntry` 包含一个完整的`DataFileMeta` 对象，而`DataFileMeta` 中已经内嵌了：

-`keyStats` /`valueStats`：Min/Max/NullCount 统计信息（`SimpleStats`，BinaryRow 编码）
-`embeddedIndex`：小于阈值（默认 500B）的 File Index 字节数组

因此 Scan 阶段**只需读取已加载到内存的 Manifest 数据**，不需要打开任何数据文件或独立`.index` 文件。

## 6 .2 调用链全景


```
AbstractFileStoreScan.plan()
  └── readManifestEntries()          ← 加载所有 ManifestEntry 到内存
        └── filterByStats(entry)     ← 对每个文件依次执行两道过滤
              │
              ├── 第一道：Min/Max 统计裁剪
              │     └── predicate.test(rowCount, minValues, maxValues, nullCounts)
              │           ↓ 返回 false → 文件直接从 plan 中移除（SKIP）
              │           ↓ 返回 true  → 进入第二道
              │
              └── 第二道：embeddedIndex 裁剪（仅当 embeddedIndex 不为 null 时执行）
                    └── testFileIndex(file.embeddedIndex(), entry)
                          └── FileIndexPredicate(embeddedBytes, rowType).evaluate(predicate)
                                ↓ SKIP   → 文件移除
                                ↓ REMAIN → 保留，进入 Read 阶段

```


AppendOnly 表与主键表的差异体现在第一道过滤的入参上（见 6.3 节）。

## 6 .3 第一道过滤：Min/Max 统计裁剪


> [!warning] 版本边界：L0/L1+ 的裁剪规则属于正确性敏感的内部实现
> 原文关于 L0 只做 Key Filter、L1+ 才能安全使用 Value Filter 的说明应绑定目标版本源码与实际执行计划。不能为了提高裁剪率手工绕过保守规则，否则可能丢失合并最终状态所需的旧版本文件。
## AppendOnly 表（AppendOnlyFileStoreScan）


```
SimpleStatsEvolution.Result stats = evolution.evolution(
    file.valueStats(), file.rowCount(), file.valueStatsCols());

boolean result = predicate.test(
    file.rowCount(),
    stats.minValues(),
    stats.maxValues(),
    stats.nullCounts()
);

```


`evolution()` 先做 Schema 演化列映射对齐（见 6.4 节），再把统计信息传给谓词评估。

## 主键表（KeyValueFileStoreScan）

主键表有两套独立统计信息，裁剪逻辑分两步：


```
第一步：keyFilter 裁剪（主键谓词，每个文件都执行）
  predicate.test(rowCount, keyStats.minValues, keyStats.maxValues, keyStats.nullCounts)

第二步：valueFilter 裁剪（受 scanMode 和 LSM level 控制）
  L0 文件：key 范围在文件间有重叠
    → value 裁剪只在 wholeBucket 粒度，不逐文件下推
  L1+ 文件：key 范围无重叠
    → 可逐文件做 value 谓词裁剪

```


L0 文件不能逐文件做 value 裁剪的原因：L0 是刚写入尚未合并的文件，同一个 key 可能分布在多个 L0 文件中，单文件 value 统计无法代表该 key 的最终值，贸然裁剪会导致数据错误。

## 6 .4 Schema 演化时的列映射对齐（SimpleStatsEvolution）

当文件的`schemaId` 与当前表不一致时（加列、删列、改类型），`evolution()` 做如下处理：


```
旧文件 schema:  [col_a, col_b]          schemaId=1
当前表 schema:  [col_a, col_b, col_c]   schemaId=2

indexMapping: [0→0, 1→1, -1]
  col_c 在旧文件中不存在，映射 -1

col_c 的保守处理：
  nullCounts[2] = rowCount   ← 视为全空，谓词 "col_c = xxx" 不裁剪
  minValues[2]  = null
  maxValues[2]  = null

```


原则：**宁可不裁剪，也不误裁剪**。`SimpleStatsEvolutions` 通过`ConcurrentHashMap<schemaId, SimpleStatsEvolution>` 缓存各版本映射，避免重复计算。

## 6 .5 谓词评估（LeafPredicate.test）

每个`LeafPredicate` 对应一个`col op value` 条件，基于 min/max/nullCount 的判断逻辑：

| 谓词 | 判断条件 | 语义 |
| --- | --- | --- |
|`col = 5` |`min <= 5 <= max` | 值 5 在文件范围内才可能存在 |
|`col > 5` |`max > 5` | 文件最大值 > 5，才可能有满足的行 |
|`col < 5` |`min < 5` | 文件最小值 < 5，才可能有满足的行 |
|`col >= 5` |`max >= 5` | 文件最大值 >= 5 |
|`col <= 5` |`min <= 5` | 文件最小值 <= 5 |
|`col IS NULL` |`nullCount > 0` | 有 null 才可能通过 |
|`col IS NOT NULL` |`rowCount != nullCount` | 不全为 null |
| min 或 max 为 null | 返回`true` | 无统计信息，保守保留 |

`CompoundPredicate(AND)` 短路求值，任一子谓词返回 false 即跳过文件；`OR` 任一通过即保留文件。

## 6 .6 第二道过滤：embeddedIndex 裁剪

## embeddedIndex 的来源

写入时，`DataFileIndexWriter.close()` 根据索引大小决定存储位置：


```
索引字节大小 <= file-index.in-manifest-threshold（默认 500B）
  → 存入 DataFileMeta.embeddedIndex（字节数组）
  → 随 Manifest 持久化，Scan 阶段读 Manifest 时已在内存
  → Scan 阶段可直接使用，零额外 IO

索引字节大小 > 阈值
  → 写独立 .index 文件（如 data-xxx.orc.index）
  → 路径记录在 DataFileMeta.extraFiles()
  → Scan 阶段不处理，留到 Read 阶段打开文件时读取

```


## FileIndexPredicate.evaluate() 执行过程


```
// 1. 从谓词中提取涉及的列名
Set<String> requiredFields = getRequiredNames(predicate);

// 2. 从 embeddedBytes 中按列加载对应 FileIndexReader
//    （FileIndexFormat 的 HEAD 部分记录了每列每种索引的起始偏移）
Map<String, Collection<FileIndexReader>> readers = loadReaders(requiredFields);

// 3. 用 Visitor 模式遍历谓词树
FileIndexResult result = new FileIndexPredicateTest(readers).test(predicate);
//   LeafPredicate → 对该列所有 reader 调用 visitEqual/visitIn/... → AND 组合
//   CompoundPredicate(AND) → 子结果短路 AND
//   CompoundPredicate(OR)  → 子结果 OR

```


各索引类型在 Scan 阶段的行为：

| 索引类型 | Scan 阶段返回值 | 说明 |
| --- | --- | --- |
| BloomFilter | SKIP 或 REMAIN | 等值查询，命中则 REMAIN，概率性排除则 SKIP |
| Bitmap | SKIP 或 REMAIN | Scan 阶段只做文件级判断，行级 bitmap 留到 Read 阶段 |
| BSI | SKIP 或 REMAIN | 范围查询判断，结构上确认无交集则 SKIP |

> **注意**：Scan 阶段即使是 Bitmap/BSI 这类行级索引，也只返回文件级 SKIP/REMAIN，不返回`BitmapIndexResult`。行级精确过滤是 Read 阶段的职责。

## 6 .7 两道过滤的分工与互补关系


```
原始文件列表（来自 Manifest，全在内存）
        │
        ▼
第一道：Min/Max 统计裁剪（成本极低，基于列的区间判断）
  优势：对范围查询、有序数据非常有效
  局限：数据分布不均时效果差（如 col=5 但文件 min=1/max=100000）
        │
        ▼
第二道：embeddedIndex 裁剪（更精确，基于实际索引结构）
  优势：BloomFilter 对高基数等值查询可精确排除大量文件
  局限：仅适用于有 embeddedIndex 的文件（小索引才嵌入 Manifest）
        │
        ▼
最终 DataSplit 列表（提交给 Read 阶段）

```


两者形成**互补关系**，串联使用：Min/Max 做低成本的宽口过滤，embeddedIndex 在 Min/Max 无法有效过滤时（数据倾斜、范围宽泛）进一步精确裁剪，共同减少进入 Read 阶段的文件数量。

---

## 七、Scan 阶段裁剪具体案例

> [!warning] 补充批注：谓词写法会决定裁剪能否发生
> 对列做 Cast、函数包装或使用无法下推的 UDF，可能使优化器无法把条件转换为统计信息或索引谓词。发现扫描文件数没有下降时，应先改写为原始列上的等值或范围条件，再通过执行计划验证。

> 本节通过四个场景，逐步演示 Scan 阶段两道过滤的完整执行过程。

## 案例一：Min/Max 成功裁剪（范围查询 + 均匀分布）

**表定义（AppendOnly）：**


```
CREATE TABLE orders (
  order_id   BIGINT,
  amount     DOUBLE,
  status     STRING
) WITH (
  'file-index.bloom-filter.columns' = 'order_id'
);

```


**Manifest 中的文件元数据（写入时已采集）：**


```
文件 F1：rowCount=10000, amount.min=1.0,    amount.max=100.0,   nullCount=0
文件 F2：rowCount=10000, amount.min=101.0,  amount.max=200.0,   nullCount=0
文件 F3：rowCount=10000, amount.min=201.0,  amount.max=300.0,   nullCount=0
文件 F4：rowCount=10000, amount.min=301.0,  amount.max=400.0,   nullCount=0

```


**查询：**


```
SELECT * FROM orders WHERE amount > 250.0 AND amount < 350.0;

```


**谓词树：**


```
CompoundPredicate(AND)
  ├── LeafPredicate(amount, GREATER_THAN, 250.0)
  └── LeafPredicate(amount, LESS_THAN,    350.0)

```


**filterByStats() 逐文件执行过程：**


```
F1：amount > 250.0 → max=100.0 > 250.0 ? 否 → false，短路，SKIP ✗
F2：amount > 250.0 → max=200.0 > 250.0 ? 否 → false，短路，SKIP ✗
F3：amount > 250.0 → max=300.0 > 250.0 ? 是 → 继续
    amount < 350.0 → min=201.0 < 350.0 ? 是 → true，REMAIN ✓
F4：amount > 250.0 → max=400.0 > 250.0 ? 是 → 继续
    amount < 350.0 → min=301.0 < 350.0 ? 是 → true，REMAIN ✓

```


**结果：F1、F2 在 Scan 阶段被裁剪，Read 阶段只打开 F3、F4，节省 50% 文件 IO。**

---

## 案例二：Min/Max 失效 + BloomFilter 补充裁剪（数据倾斜场景）

**背景：**`order_id` 取值稀疏（1～10^9 均匀分布），每个文件 min/max 范围极宽，Min/Max 无法有效过滤等值查询。

**Manifest 中的文件元数据：**


```
文件 F1：order_id.min=1,         order_id.max=1000000000, embeddedIndex=[BloomFilter(order_id)]
文件 F2：order_id.min=2,         order_id.max=999999999,  embeddedIndex=[BloomFilter(order_id)]
文件 F3：order_id.min=100,       order_id.max=999999800,  embeddedIndex=[BloomFilter(order_id)]

```


（每个文件实际写入了 10000 个稀疏 order_id，min/max 几乎覆盖全域）

**查询：**


```
SELECT * FROM orders WHERE order_id = 42857634;

```


**第一道：Min/Max 统计裁剪**


```
F1：42857634 >= 1 && 42857634 <= 1000000000 → true，REMAIN（无法排除）
F2：42857634 >= 2 && 42857634 <= 999999999  → true，REMAIN（无法排除）
F3：42857634 >= 100 && 42857634 <= 999999800 → true，REMAIN（无法排除）

```


三个文件全部通过，Min/Max 完全失效。

**第二道：embeddedIndex（BloomFilter）裁剪**


```
F1.BloomFilter.visitEqual(order_id, 42857634)
  → 查询 Bloom 位图（k 个 hash 函数对应的 bit 位）
  → 全部命中 → REMAIN ✓（42857634 实际存在于 F1）

F2.BloomFilter.visitEqual(order_id, 42857634)
  → 某个 hash 位未命中 → SKIP ✗（42857634 一定不在 F2）

F3.BloomFilter.visitEqual(order_id, 42857634)
  → 某个 hash 位未命中 → SKIP ✗（42857634 一定不在 F3）

```


**结果：BloomFilter 将 F2、F3 裁剪，Read 阶段只打开 F1。**

> BloomFilter 没有假阴性（命中则一定可能存在），但有假阳性（默认误判率约 1%）。因此 SKIP 是精确的，REMAIN 可能是误判，但这是可接受的代价。

---

## 案例三：Schema 演化后的保守裁剪

**背景：** 表原来有两列，后来新增了`city` 列。仓库中既有旧文件（无`city` 数据）也有新文件（有`city` 数据）。

**Schema 历史：**


```
schemaId=1：[order_id BIGINT, amount DOUBLE]
schemaId=2：[order_id BIGINT, amount DOUBLE, city STRING]   ← 当前表 schema

```


**Manifest 中的文件元数据：**


```
文件 F1（旧）：schemaId=1, valueStats.minValues=[1,    1.0],      nullCounts=[0, 0]
文件 F2（旧）：schemaId=1, valueStats.minValues=[5001, 50.0],     nullCounts=[0, 0]
文件 F3（新）：schemaId=2, valueStats.minValues=[8001, 80.0, "Beijing"],  nullCounts=[0, 0, 0]
文件 F4（新）：schemaId=2, valueStats.minValues=[9001, 90.0, "Shanghai"], nullCounts=[0, 0, 0]

```


**查询：**


```
SELECT * FROM orders WHERE city = 'Beijing';

```


**filterByStats() 执行过程：**


```
处理 F1（schemaId=1）：
  evolution.evolution(file.valueStats(), rowCount=10000, valueStatsCols=null)
    indexMapping = [0→0, 1→1, -1]   ← city 在旧 schema 中不存在
    city 列处理：
      minValues[2] = null
      maxValues[2] = null
      nullCounts[2] = 10000         ← rowCount，视为全 null

  predicate.test(..., city='Beijing')
    LeafPredicate(city, EQUAL, 'Beijing')
      min=null → 无统计信息，保守返回 true，REMAIN ✓

处理 F2（schemaId=1）：同 F1，city 无统计，保守 REMAIN ✓

处理 F3（schemaId=2，city.min='Beijing', city.max='Beijing'）：
  'Beijing' >= 'Beijing' && 'Beijing' <= 'Beijing' → true，REMAIN ✓

处理 F4（schemaId=2，city.min='Shanghai', city.max='Shanghai'）：
  'Beijing' >= 'Shanghai' ? 否（字典序）→ false，SKIP ✗

```


**结果：F4 被裁剪，F1、F2、F3 进入 Read 阶段。**

> F1、F2 是旧文件，实际上不含`city` 列数据，Read 阶段读出来后`city` 按默认值（null）处理。Scan 阶段对它们保守 REMAIN，是正确的——既不漏数据，也不引入错误。

---

## 案例四：主键表 L0 vs L1+ 的 value 裁剪差异

**背景：** 主键表，同一用户`user_id=1001` 的`score` 字段在多次写入后有多个版本，尚未合并（L0 状态）。

**Manifest 中的文件元数据（同一 bucket）：**


```
L0 文件 F1：keyStats=[user_id.min=1000, user_id.max=2000]
            valueStats=[score.min=60,  score.max=80]    ← user_id=1001 的 score=80（旧）

L0 文件 F2：keyStats=[user_id.min=500,  user_id.max=1500]
            valueStats=[score.min=90,  score.max=95]    ← user_id=1001 的 score=95（新）

L1 文件 F3：keyStats=[user_id.min=3000, user_id.max=5000]
            valueStats=[score.min=70,  score.max=88]

```


**查询：**


```
SELECT * FROM pk_table WHERE user_id = 1001 AND score > 85;

```


**谓词拆分：**


```
keyFilter:   LeafPredicate(user_id, EQUAL, 1001)
valueFilter: LeafPredicate(score, GREATER_THAN, 85)

```


**filterByStats() 执行过程：**


```
处理 L0 F1：
  keyFilter:   1001 >= 1000 && 1001 <= 2000 → true
  valueFilter（L0，不逐文件下推 value 裁剪）→ 跳过 value 裁剪，REMAIN ✓

处理 L0 F2：
  keyFilter:   1001 >= 500 && 1001 <= 1500 → true
  valueFilter（L0，不逐文件下推）→ REMAIN ✓

处理 L1 F3：
  keyFilter:   1001 >= 3000 ? 否 → false，SKIP ✗

```


**结果：F3 被 keyFilter 裁剪，F1、F2 进入 Read 阶段。**

**为什么 L0 不用 valueFilter 裁剪？**


```
user_id=1001 的数据分布在 F1（score=80，旧版本）和 F2（score=95，新版本）。

假设错误地对 F1 做 value 裁剪：
  F1.valueStats: score.max=80 > 85 ? 否 → SKIP ✗（被裁剪！）

但 F1 中包含 user_id=1001 的旧版本 score=80，Read 阶段需要读取 F1 和 F2
才能通过序列号（sequenceNumber）判断哪个版本更新，然后合并出最终值 score=95。

如果 F1 被错误裁剪，Merge 过程缺少旧版本，可能输出错误的中间态结果。

```


因此 Paimon 对 L0 文件**只做 keyFilter 裁剪，不做 valueFilter 裁剪**，保证 Merge 正确性。L1+ 文件因为 key 不重叠，每个文件的 value 统计是独立且准确的，可以安全地进行 value 裁剪。

## 本篇结论

回到开头的问题：File Index 存好了，查询时怎么用？

答案就是 Paimon 查询裁剪机制的三板斧——**File Index 提供元数据基础，Min/Max 统计充当裁剪主力，Files Table 赋予你透视能力**。

| 核心要素 | 角色 |
| --- | --- |
| **File Index** | 文件级统计元数据的载体，是裁剪的“弹药库” |
| **Min/Max 统计** | 最常用的裁剪触发器，通过范围判断快速跳过无关文件 |
| **Files Table** | 调试利器，让你直接查看每个文件的统计信息，验证裁剪是否生效 |

理解这套机制，你就能解释为什么有些查询秒级返回，有些却要扫半天——大概率是裁剪失效了。而失效的原因，可能是统计信息缺失、文件粒度过粗，或者查询条件无法命中 Min/Max 范围。

之前我们拆解了 File Index 的底层存储，这一篇讲清了它的上层应用。存储与查询，两张拼图合在一起，才算完整理解了 Paimon 在读取侧的加速之道。

## 复核资料

- [Append Table Query Performance](https://paimon.apache.org/docs/master/append-table/query-performance/)
- [File Index Specification](https://paimon.apache.org/docs/master/concepts/spec/fileindex/)
- [System Tables](https://paimon.apache.org/docs/master/concepts/system-tables/)

## 系列导航

- 上一篇：[[Paimon 精讲（七）：动态 Bucket 的单写限制与索引一致性（文章批注版）]]
- 下一篇：[[Paimon 精讲（九）：Append 表的 Bucket Key 与文件索引（文章批注版）]]
