---
title: "Paimon 精讲（六）：File Index 的存储与读写链路（文章批注版）"
aliases:
  - "Paimon 精讲（六）：FileIndex 底层怎么存？源码级架构全拆解"
author: "胖泽的技术笔记"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s/fE2ayVj9OGl_aUxVKQchrw"
published_at: "2026-08-02"
reviewed_at: "2026-10-14"
tags:
  - Flink
  - Paimon
  - Paimon-精讲
---

# Paimon 精讲（六）：File Index 的存储与读写链路（文章批注版）

> [!info] 来源、整理方式与版本边界
> 原文：[《Paimon 精讲（六）：FileIndex 底层怎么存？源码级架构全拆解》](https://mp.weixin.qq.com/s/fE2ayVj9OGl_aUxVKQchrw)，胖泽的技术笔记，2026-08-02。正文按原文章节顺序保留技术内容、代码、表格、字段和案例；新增内容统一标为“批注”“勘误”或“版本边界”。原文没有声明准确的 Paimon/Flink 版本，内部类、默认值和源码路径不视为稳定 API。

> [!warning] 批注：索引名称和选项存在版本变化
> 原文的 BSI 与 `file-index.bsi.columns` 是旧版写法；当前文档使用 Range Bitmap 和 `file-index.range-bitmap.columns`。Bloom Filter、Bitmap、Range Bitmap 支持的类型、谓词以及嵌入阈值也应以部署版本为准。
> 修改索引配置只影响之后新写或重写的数据文件，不会自动给历史文件补索引；需要用 `rewrite_file_index` 构建旧文件索引，并通过 `$file_indexes` 与 `$files` 核对覆盖范围。
## 引言

用过 Paimon 的同学都知道，查询快不快，很大程度上取决于**文件裁剪**做得好不好。而文件裁剪的核心，就是 File Index。

简单来说，File Index 是 Paimon 在每个文件上维护的一组元数据，记录了文件内数据的统计信息（如 min/max、null count、bloom filter 等）。查询时，Paimon 通过这些索引提前过滤掉不相关的文件，避免全量扫描。

但 File Index 远不止“存几个统计值”那么简单。本文从底层架构出发，拆解 Paimon File Index 的设计原理、索引类型、维护机制与性能权衡。读完你不仅能理解它的工作机制，还能在实际使用中做出更合理的索引配置决策。

---

## 一、是什么

File Index 是一种**数据文件级别的二级索引**机制，可让 Paimon 在不读取文件体的情况下跳过整个数据文件，甚至跳过文件内的特定行（Bitmap 索引）。它与 Paimon 现有的 min/max 统计（`SimpleStats`）互补，作用于其之上。

**一句话理解**：不开箱子，就能判断这个箱子里有没有你要找的东西。

核心代码包：

-`paimon-common/.../fileindex/` — 所有核心抽象与实现
-`paimon-core/.../io/` — 写路径集成（`DataFileIndexWriter` 等）
-`paimon-core/.../operation/` — 读路径集成（Scan、SplitRead）
-`paimon-flink/.../` — Flink 特定工具（`rewrite_file_index` 过程）

---

## 二、三种索引类型


> [!note] 批注：索引只负责排除不可能命中的数据
> Bloom Filter 允许假阳性，Bitmap/Range Bitmap 也只在支持的谓词上提供过滤结果。索引命中代表“仍需读取”，索引排除才代表“可以跳过”；最终 SQL 条件仍要在读取阶段逐行验证。
### Bloom Filter（布隆过滤器）

箱子外面贴了一张便签，写着"这个箱子里绝对没有苹果"或"可能有苹果（不确定）"。

- 说"没有"就一定没有，可以跳过这个文件
- 说"可能有"，还是得打开看看（有一定误判率，默认 fpp=0.1）
- 只支持等值查询：`WHERE col = 'value'`
- 不支持 BOOLEAN 类型和范围谓词
- 配置项：`file-index.bloom-filter.columns = col1, col2`

### Bitmap 索引

箱子外面贴了一张详细清单，精确记录"第 3、7、15 行是苹果，第 2、9 行是香蕉"。

- 可以精确跳过不匹配的**行**，不只是整个文件
- 支持：`=`,`!=`,`IN`,`NOT IN`,`IS NULL`,`IS NOT NULL`
- 适合低基数列（status、type 等）
- 不支持 DECIMAL、BINARY、ARRAY、MAP、ROW 类型
- 配置项：`file-index.bitmap.columns = col1, col2`

### BSI 索引（Bit Slice Index Bitmap）

在 Bitmap 基础上更进一步，清单还支持"第几行的价格大于 50"这种范围查询。

- 在 Bitmap 全部能力之上，额外支持：`<`,`<=`,`>`,`>=`
- 适合数值类型字段的范围查询
- 支持类型：整数类型、DATE、TIME、TIMESTAMP、DECIMAL
- 配置项：`file-index.bsi.columns = col1, col2`

---

## 三、索引存在哪里


> [!note] 批注：嵌入和独立索引都属于文件级辅助数据
> 小索引可以嵌入 Manifest 元数据，大索引保存在数据文件旁的独立索引文件。它们不替代 Snapshot/Manifest 对有效数据文件的管理，也不是跨全表维护的一棵全局二级索引。
每个数据文件（如`data-abc.orc`）写完后，会生成对应的索引文件`data-abc.orc.index`，路径由`DataFilePathFactory.dataFileToFileIndexPath()` 生成（在数据文件路径后追加`.index`）。

**索引文件格式（HEAD + BODY）：**


```
[MAGIC(8B)] [VERSION(4B)] [HEAD_LENGTH(4B)]
[column_count(4B)]
  per column: [column_name(UTF)] [index_count(4B)]
    per index: [index_type(UTF)] [start_pos(4B)] [length(4B)]
[redundant_length(4B)]
[BODY: 各列索引的序列化字节拼接]

```


**两种存储策略（由`file-index.in-manifest-threshold` 控制，默认 500B）：**

| 情况 | 存储方式 | 优势 |
| --- | --- | --- |
| 索引数据 < 500B | 嵌入`DataFileMeta.embeddedIndex()` 字节 | 查询时直接从内存读，无 I/O |
| 索引数据 >= 500B | 写入独立`.index` 文件，文件名存入`DataFileMeta.extraFiles()` | 避免 Manifest 膨胀 |

---

## 四、写路径：索引如何生成

写数据和建索引**同步进行**，不需要额外操作。


```
CoreOptions.indexColumnsOptions()
    → FileIndexOptions（解析 file-index.* 配置）
        → DataFileIndexWriter.create(fileIO, indexFilePath, rowType, options)

```


**写入流程：**

1.`RowDataFileWriter`（append-only 表）或`KeyValueDataFileWriter`（主键表，仅对 value 字段建索引）持有`DataFileIndexWriter`
2. 每写一行，调用`dataFileIndexWriter.write(row)`，按配置提取各字段值，传给对应`FileIndexWriter.writeRecord(value)`
3. 文件关闭时，`dataFileIndexWriter.close()`：

- 所有列的索引序列化为`Map<columnName, Map<indexType, byte[]>>`
- 通过`FileIndexFormat.Writer` 写出完整索引数据
- 按阈值决定嵌入到 DataFileMeta 还是写独立文件

---

## 五、读路径：索引如何生效

> [!note] 补充批注：索引裁剪分为规划期和读取期
> 内嵌在元数据中的索引可在 Scan 阶段排除候选文件；独立 sidecar 索引通常在读取阶段进一步过滤。索引只减少候选文件或数据块，最终行仍要执行原始谓词，不能把“命中索引”理解为跳过条件计算。

索引在**两个层次**介入查询：

### 层次 1：Scan 阶段（Manifest 扫描，无实际文件 I/O）

`AppendOnlyFileStoreScan` 和`KeyValueFileStoreScan` 在清单扫描时直接检查**嵌入式索引**：


```
return filter.test(minValues, maxValues, ...)
    && (!fileIndexReadEnabled || testFileIndex(entry.file().embeddedIndex(), entry));

```


- 只使用嵌入式索引（`embeddedIndex`），不读外部`.index` 文件
- 通过`file-index.read.enabled`（默认`true`）控制是否启用
- 结果：被过滤掉的文件不会分发给计算节点，彻底避免 I/O

### 层次 2：Read 阶段（Split 读取，实际文件读取时）

`RawFileSplitRead` 通过`FileIndexEvaluator.evaluate()` 评估索引：


```
FileIndexResult result = FileIndexEvaluator.evaluate(
    fileIO, dataSchema, dataFilters, dataFilePathFactory, file);

if (!result.remain()) {
    return new EmptyFileRecordReader<>();  // 跳过整个文件
}

```


- 优先检查`embeddedIndex`，若无则从`extraFiles` 找`.index` 文件读取
- **Bloom Filter / BSI 结果**：整个文件跳过或保留
- **Bitmap 结果（行级过滤）**：
```
if (result instanceof BitmapIndexResult) {
    RoaringBitmap32 selection = ((BitmapIndexResult) result).get();
    return new ApplyBitmapIndexRecordReader(fileRecordReader, bitmapResult);
}

```
`ApplyBitmapIndexFileRecordIterator` 只返回 bitmap 中命中的行，其余行跳过。

---

## 六、谓词分发机制

采用**访问者模式**（Visitor Pattern）：


```
LeafPredicate（如 name = '苹果'）
    → function().visit(fileIndexReader, fieldRef, literals)
    → fileIndexReader.visitEqual(fieldRef, "苹果")
    → 具体实现：BloomFilterFileIndex.Reader 或 BitmapFileIndex.Reader

```


复合谓词（AND/OR）递归组合子结果：

-`FileIndexResult.and()`：遇 SKIP 即短路
-`FileIndexResult.or()`：遇 REMAIN 即短路

---

## 七、整体查询流程

> [!note] 补充批注：配置了索引不等于查询实际使用索引
> 验证时应结合 `EXPLAIN`、`$files`、`$file_indexes` 和实际扫描字节数，确认谓词是否下推、索引文件是否存在、候选文件数是否下降。只检查建表参数无法证明裁剪已经发生。


```
用户查询: SELECT * FROM t WHERE name = '苹果'
          │
          ▼
  【Scan 阶段】扫描 Manifest 清单（无文件 I/O）
  文件1: 嵌入索引 → 布隆过滤器说"没苹果" → 跳过 ✗
  文件2: 嵌入索引 → 布隆过滤器说"可能有" → 保留 ✓
  文件3: 嵌入索引 → 布隆过滤器说"可能有" → 保留 ✓
          │
          ▼
  【Read 阶段】计算节点实际读文件
  文件2: 读 .index 文件 → Bitmap 说"第5、12行是苹果"
         → 只读第5、12行，其他行跳过 ✓
  文件3: 读 .index 文件 → 整个文件没有 → 跳过 ✗
          │
          ▼
  只返回文件2的第5、12行数据

```


---

## 八、追加索引（Rewrite File Index）


> [!warning] 操作边界
> `rewrite_file_index` 会读取已有数据以构建索引，但不等于重写全部数据文件。执行前要限定目标分区、评估 I/O，并在完成后查询 `$file_indexes`，不能只依据表参数认定历史文件已经具备索引。
对于原本没有建索引的已有表，可通过`rewrite_file_index` Flink 过程追加，无需重新摄取数据：


```
CALL sys.rewrite_file_index('db_name.table_name');

```


**执行流程（`RewriteFileIndexSink.FileIndexProcessor`）：**

1. 读取所有`ManifestEntry` 的数据文件元信息
2. 加载已有`.index` 文件，移除过期/不再配置的列索引
3. 重新读取数据文件，逐行喂给新的`DataFileIndexWriter`
4. 合并新旧索引字节，按阈值决定嵌入或独立文件
5. 以`CompactIncrement` 形式原子提交（旧元数据为`compactBefore`，新元数据为`compactAfter`）

---

## 九、关键类速查表

| 类 | 路径 | 作用 |
| --- | --- | --- |
|`FileIndexer` |`paimon-common/fileindex` | 顶层 SPI 接口，索引 Writer+Reader 工厂 |
|`FileIndexWriter` |`paimon-common/fileindex` | 写入期按行累积索引状态，序列化为`byte[]` |
|`FileIndexReader` |`paimon-common/fileindex` | 读取期按谓词求值（extends`FunctionVisitor`） |
|`FileIndexResult` |`paimon-common/fileindex` | REMAIN/SKIP/BitmapResult，支持 and()/or() |
|`FileIndexFormat` |`paimon-common/fileindex` | 索引文件二进制格式序列化/反序列化 |
|`FileIndexPredicate` |`paimon-common/fileindex` | 谓词评估主入口，dispatch 到各列 Reader |
|`FileIndexOptions` |`paimon-common/fileindex` | 解析`file-index.*` 配置 |
|`BloomFilterFileIndex` |`.../fileindex/bloomfilter` | 布隆过滤器实现 |
|`BitmapFileIndex` |`.../fileindex/bitmap` | Bitmap 实现，返回精确行位置 |
|`BitmapIndexResult` |`.../fileindex/bitmap` | 携带`RoaringBitmap32` 的行级结果 |
|`ApplyBitmapIndexRecordReader` |`.../fileindex/bitmap` | 行级过滤包装 Reader |
|`BitSliceIndexBitmapFileIndex` |`.../fileindex/bsi` | BSI 实现，支持范围查询 |
|`DataFileIndexWriter` |`paimon-core/io` | 多列索引写入协调器 |
|`FileIndexEvaluator` |`paimon-core/io` | 读取时评估嵌入或外部索引 |
|`AppendOnlyFileStoreScan` |`paimon-core/operation` | Scan 阶段文件跳过（append 表） |
|`KeyValueFileStoreScan` |`paimon-core/operation` | Scan 阶段文件跳过（主键表） |
|`RawFileSplitRead` |`paimon-core/operation` | Read 阶段文件/行级过滤 |
|`RewriteFileIndexProcedure` |`paimon-flink/flink-common` |`rewrite_file_index`  Flink SQL 过程 |

---

## 十、配置示例


```
CREATE TABLE orders (
    order_id BIGINT,
    user_id BIGINT,
    status STRING,
    amount DECIMAL(10, 2),
    create_time TIMESTAMP
) WITH (
    -- 对 user_id 建布隆过滤器（等值查询加速）
    'file-index.bloom-filter.columns' = 'user_id',
    'file-index.bloom-filter.user_id.items' = '1000000',
    'file-index.bloom-filter.user_id.fpp' = '0.05',

    -- 对 status 建 Bitmap（低基数枚举列）
    'file-index.bitmap.columns' = 'status',

    -- 对 amount 建 BSI（范围查询加速）
    'file-index.bsi.columns' = 'amount',

    -- 索引嵌入 Manifest 的大小阈值
    'file-index.in-manifest-threshold' = '500B',

    -- 是否启用 File Index 读取
    'file-index.read.enabled' = 'true'
);

```


---

## 本篇结论

回到开头的问题：Paimon 查询快不快，核心取决于文件裁剪做得好不好。而文件裁剪的根基，就是 File Index。

File Index 的本质可以概括为一句话：**用少量的元数据，换取大量的 IO 节省。**

| 索引类型 | 核心能力 | 适用场景 |
| --- | --- | --- |
| 统计索引（min/max） | 快速排除范围外的文件 | 时间范围、数值范围过滤 |
| 布隆过滤器 | 精确判断值是否存在 | 高基数列的点查 |
| 位图索引 | 高效处理枚举值过滤 | 低基数列的等值查询 |

但 File Index 并非免费午餐：

- 写入和 compaction 时需要维护索引，带来额外的 CPU 和内存开销
- 索引文件本身也占用存储空间
- 在小文件过多的场景下，索引的裁剪效果会被削弱

**用好 File Index 的关键**，不在于开启多少种索引，而在于理解它的维护机制和代价，根据实际查询模式做有针对性的配置。

## 复核资料

- [Append Table Query Performance](https://paimon.apache.org/docs/master/append-table/query-performance/)
- [File Index Specification](https://paimon.apache.org/docs/master/concepts/spec/fileindex/)
- [System Tables](https://paimon.apache.org/docs/master/concepts/system-tables/)

## 系列导航

- 上一篇：[[Paimon 精讲（五）：Deletion Vector 与 Merge On Write（文章批注版）]]
- 下一篇：[[Paimon 精讲（七）：动态 Bucket 的单写限制与索引一致性（文章批注版）]]
