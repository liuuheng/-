---
title: "Doris 高阶篇01：存储引擎源码解析"
author: "外星人的知识杂谈"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484393&idx=1&sn=0ca001cbe3a98ad557f507b05df36669&chksm=c1bf09f4f6c880e2836849bfe6166385b526c337e8ea5f4809b027b235c2d0d4d45772b4e05b#wechat_redirect"
published_at: "2026-09-05T21:42:12+08:00"
annotated_at: "2026-11-18"
cssclasses:
  - wechat-article
  - doris-intro-annotated
tags:
  - doris
  - 文章批注
---

# Doris 高阶篇01：存储引擎源码解析

<div class="article-meta">外星人的知识杂谈 · 2026-09-05</div>

[原文链接](https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484393&idx=1&sn=0ca001cbe3a98ad557f507b05df36669&chksm=c1bf09f4f6c880e2836849bfe6166385b526c337e8ea5f4809b027b235c2d0d4d45772b4e05b#wechat_redirect)

<!-- 正文按原文顺序保留；批注和核对说明为新增。装饰封面和独立的篇末预告已去除。 -->

## 定位与阅读前提

本文深入解析 Apache Doris BE 端存储引擎的核心实现，重点涵盖 Segment V2 文件格式、列式写入/读取链路、Page 编码与压缩机制、MemTable flush 流程以及 Rowset 生命周期管理。

**阅读前提：**

- 理解 LSM-Tree 变种存储架构基本思路
- 了解列式存储的基本概念（列独立编码、Page 分页）
- 熟悉 C++ 虚函数、智能指针等语言特性
- 建议先阅读 Doris 官方文档中关于数据模型（Duplicate/Aggregate/Unique）的说明

## 背景动机

Doris 的存储引擎经历了从 Segment V1 到 Segment V2 的演进。V2 格式带来了更灵活的编码策略、更高效的索引结构和更紧凑的文件布局。理解存储引擎的源码实现，有助于：

1. 分析写入性能瓶颈（编码效率、flush 频率）
2. 理解查询下推优化的物理基础（ZoneMap、BloomFilter、Ordinal Index）
3. 为 Compaction 调优提供底层认知

## 一、Segment V2 文件格式

### 1.1 整体文件结构

一个 Segment 文件的物理布局如下：


```text
+-------------------------------------------+
|           Magic Number (4 bytes)          |   "D","O","R","1"
+-------------------------------------------+
|           Column Data Pages               |   每列的数据按 Page 组织
|           (Column 0 Pages)                |
|           (Column 1 Pages)                |
|           ...                             |
+-------------------------------------------+
|         Column Index Pages                |   Ordinal Index / ZoneMap / BF
+-------------------------------------------+
|         Short Key Index / PK Index        |
+-------------------------------------------+
|         Footer (SegmentFooterPB)          |   Protobuf 编码的元信息
+-------------------------------------------+
|         Footer Length (4 bytes)           |
+-------------------------------------------+
|         Checksum (4 bytes)                |
+-------------------------------------------+

```


核心 protobuf 定义位于`gen_cpp/segment_v2.pb.h`（由`gensrc/proto/segment_v2.proto` 生成）。

> [!warning] 核对说明｜V2 魔数在文件尾
>
> 按 Doris 4.1 的 [`Segment::_parse_footer`](https://github.com/apache/doris/blob/branch-4.1/be/src/storage/segment/segment.cpp) 和 [`k_segment_magic`](https://github.com/apache/doris/blob/branch-4.1/be/src/storage/segment/segment_writer.cpp)，最后 12 字节依次是元信息长度、元信息 CRC32C 和魔数 `D0R1`（第二个字符是数字 `0`）。读取端先从文件尾定位 `SegmentFooterPB` 并校验，再根据元信息找列和索引。原图及下方注释保留原貌；检查实际文件时以这里的尾部布局为准。

### 1.2 关键常量


```cpp
// be/src/storage/segment/segment_writer.h
extern const char* k_segment_magic;          // "DOR1"
extern const uint32_t k_segment_magic_length; // 4

const uint32_t MAX_SEGMENT_SIZE = OLAP_MAX_COLUMN_SEGMENT_FILE_SIZE 

                                  * OLAP_COLUMN_FILE_SEGMENT_SIZE_SCALE;

```


## 二、SegmentWriter：写入总控

### 2.1 类结构


```cpp
// be/src/storage/segment/segment_writer.h
class SegmentWriter {
public:
    explicit SegmentWriter(io::FileWriter* file_writer, uint32_t segment_id,
                           TabletSchemaSPtr tablet_schema, BaseTabletSPtr tablet,
                           DataDir* data_dir, const SegmentWriterOptions& opts,
                           IndexFileWriter* inverted_file_writer);

    Status init();
    Status init(const std::vector<uint32_t>& col_ids, bool has_key);  // vertical compaction
    Status append_block(const Block* block, size_t row_pos, size_t num_rows);
    Status finalize(uint64_t* segment_file_size, uint64_t* index_size, ...);

private:
    Status _create_column_writer(uint32_t cid, const TabletColumn& column, ...);
    Status _write_data();
    Status _write_ordinal_index();
    Status _write_zone_map();
    Status _write_bloom_filter_index();
    Status _write_short_key_index();
    Status _write_primary_key_index();
    Status _write_footer();
};

```


### 2.2 写入流程


```text
append_block()
  |
  v
将 Block 中的每一列数据分发给对应的 ColumnWriter
  |
  v
finalize()
  |-> finalize_columns_data()    // 各列 flush 最后一个 Page
  |-> finalize_columns_index()   // 写入各列的索引
  |-> finalize_footer()          // 写入 SegmentFooterPB

```


`SegmentWriterOptions` 控制关键写入参数：


```cpp
struct SegmentWriterOptions {
    uint32_t num_rows_per_block = 1024;
    uint32_t max_rows_per_segment = UINT32_MAX;
    bool enable_unique_key_merge_on_write = false;
    CompressionTypePB compression_type = UNKNOWN_COMPRESSION;
};

```

> [!note] 批注｜Segment 完成与数据可见分属两层
>
> `append_block` 将一批行按列交给写入器。`finalize` 补齐尾页、索引和 Footer，使文件可被独立读取；完成一次 `finalize` 只说明一个 Segment 写完。对导入查询可见，还要等所属 Rowset 的事务成功发布。压测时可分别观察 Flush 写文件和事务发布的耗时。


## 三、ColumnWriter：列式编码引擎

### 3.1 核心接口


```cpp
// be/src/storage/segment/column_writer.h
struct ColumnWriterOptions {
    ColumnMetaPB* meta = nullptr;
    size_t data_page_size = STORAGE_PAGE_SIZE_DEFAULT_VALUE;   // 默认 64KB
    size_t dict_page_size = STORAGE_DICT_PAGE_SIZE_DEFAULT_VALUE;
    double compression_min_space_saving = 0.1;
    bool need_zone_map = false;
    bool need_bloom_filter = false;
    bool need_inverted_index = false;
    CompressionTypePB compression_type = UNKNOWN_COMPRESSION;
    TabletStorageFormatPB storage_format = TABLET_STORAGE_FORMAT_V2;
};

```


ColumnWriter 内部维护一个 PageBuilder 实例，负责将数据编码为 Page。当 Page 满时自动 flush 到文件，并更新 Ordinal Index。

### 3.2 编码策略选择

Doris 支持多种编码方式，由`EncodingTypePB` 枚举定义：

| 编码类型 | 适用场景 | 原理 |
| --- | --- | --- |
| DICT\_ENCODING | 低基数字符串 | 构建字典，值用字典 ID 替代 |
| RLE | 连续重复值 | Run-Length Encoding |
| BIT\_SHUFFLE | 整型列 | 按 bit 位重排后压缩率更高 |
| PLAIN\_ENCODING | 通用 | 无编码直接存储 |
| PREFIX\_ENCODING | 有序字符串 | 前缀压缩 |
| FOR\_ENCODING | 递增整型 | Frame of Reference |

编码选择逻辑在`resolve_default_encoding()` 中根据`storage_format`（V2/V3）和数据类型自动决定。

> [!warning] 核对说明｜默认编码要绑定存储格式和类型
>
> 编码改变值的表示，例如字典编码用整数 ID 指向字符串；压缩再处理编码后的字节。Doris 4.1 支持的 [V3 格式](https://doris.apache.org/docs/4.x/table-design/storage-format/)调整了数值类型的默认编码，还把列元信息从大 Footer 中拆出来以便按需读取。定位某个 Segment 应读该列的元信息，不能从 SQL 类型或表中新设的默认值反推旧文件的编码。

## 四、PageBuilder：数据分页

### 4.1 抽象接口


```cpp
// be/src/storage/segment/page_builder.h
class PageBuilder {
public:
    virtual Status init() = 0;
    virtual bool is_page_full() = 0;
    virtual Status add(const uint8_t* vals, size_t* count) = 0;
    virtual Status finish(OwnedSlice* owned_slice) = 0;
    virtual Status get_dictionary_page(OwnedSlice* dictionary_page);
    virtual void reset() = 0;
    virtual size_t count() const = 0;
    virtual uint64_t size() const = 0;
};

```


PageBuilder 管理单个 Data Page 的生命周期：

1. `add()`
   — 向 Page 追加数据，返回实际写入条数
2. `is_page_full()`
   — 判断 Page 是否已满（达到`data_page_size`）
3. `finish()`
   — 编码完成，输出编码后的字节流
4. `reset()`
   — 重置状态，开始构建下一个 Page

> [!note] 批注｜Page 的边界由写入器决定
>
> `add` 接收一批值并返回实际接受数量，剩余值仍要交给后续 Page；不同列的编码后字节数不同，Page 覆盖的行范围也不必相同。Ordinal Index（行序号索引）解决的是已知行号后如何找到对应的列页。

### 4.2 主要实现类

- `BinaryDictPageBuilder`

   — 字典编码 Page
- `BitshufflePageBuilder<T>`

   — Bit-Shuffle 编码
- `RlePageBuilder<T>`

   — RLE 编码
- `PlainPageBuilder<T>`

   — Plain 编码
- `FrameOfReferencePageBuilder<T>`

   — FOR 编码

## 五、压缩机制

数据在 Page 编码之后，会经过压缩层处理：


```cpp
// be/src/storage/segment/column_writer.h 中引用
class BlockCompressionCodec;

```


支持的压缩算法：

- **LZ4**

   — 默认选择，压缩/解压速度快
- **LZ4F**

   — LZ4 Frame 格式
- **ZSTD**

   — 更高压缩比，适合冷数据
- **Snappy**

   — 平衡型
- **ZLIB**

   — 兼容性好

压缩决策逻辑：只有当压缩后节省空间超过`compression_min_space_saving`（默认 10%）时，才使用压缩版本；否则回退到未压缩存储。

> [!warning] 核对说明｜区分 Page 压缩判定和表的默认算法
>
> 正文列出的算法和节省比例用于解释页级判定。不要把 LZ4 当作所有版本的新表默认值：[Doris 4.x 建表文档](https://doris.apache.org/zh-CN/docs/4.x/sql-manual/sql-statements/table-and-view/table/CREATE-TABLE/)标注自 4.0.3 起默认改为 `ZSTD`。核查时以目标版本、`SHOW CREATE TABLE` 和当前 Segment 元信息为准。

## 六、MemTable Flush 流程

### 6.1 MemTable 结构


```cpp
// be/src/load/memtable/memtable.h
enum MemType { ACTIVE = 0, WRITE_FINISHED = 1, FLUSH = 2 };

struct RowInBlock {
    size_t _row_pos;
    char* _agg_mem = nullptr;      // 聚合状态内存
    size_t* _agg_state_offset;
    bool _has_init_agg;
};

```


MemTable 是写入链路的内存缓冲区。数据导入（StreamLoad/Insert）时，先写入 MemTable，达到阈值后触发 flush。

### 6.2 Flush 流程


```text
MemTable (ACTIVE)
    |
    | (达到内存阈值)
    v
MemTable (WRITE_FINISHED) —— 进入 flush 队列
    |
    v
MemTableFlushExecutor 调度 flush 任务
    |
    v
+------- Flush 核心步骤 --------+
|  1. Sort: 按 key 排序         |
|  2. Aggregate: 预聚合(AGG表)  |
|  3. 创建 SegmentWriter        |
|  4. append_block 写入数据     |
|  5. finalize 生成 Segment     |
+-------------------------------+
    |
    v
输出 Rowset (包含一个或多个 Segment)

```


关键类：

- `MemTable`

   — 内存表，管理 Block 和排序
- `MemTableWriter`

   — 协调写入与 flush
- `MemTableFlushExecutor`

   — flush 线程池调度
- `MemTableMemoryLimiter`

   — 全局内存控制

> [!note] 批注｜一次 Flush 不等于提交一个 Rowset
>
> Flush 把当前 MemTable 的数据排好序并交给 Rowset 的写入链路，一次导入在一个 Tablet 上可以先后产生多个 Segment，再由 Rowset 收拢。小批次过多会增加 Segment 打开和后续 Compaction 的成本；排查时把 Flush 次数、每个 Rowset 的 Segment 数和导入可见延迟一起看。

### 6.3 排序实现

MemTable 内部对`RowInBlock` 按 key 列排序。对于 Aggregate 模型，排序后还会执行预聚合（将相同 key 的 value 列合并），减少写入的数据量。

## 七、SegmentIterator：读取链路


```cpp
// be/src/storage/segment/segment_iterator.h
class SegmentIterator;

```


读取时，查询引擎通过 SegmentIterator 访问数据：


```text
SegmentIterator
    |-> 根据查询条件利用索引裁剪（ZoneMap/BF/Ordinal）
    |-> 定位到目标 Page 范围
    |-> 逐 Page 解压 + 解码
    |-> 返回 Column 数据

```


Lazy Materialization 优化：先读取谓词列做过滤，得到行号集合后再读取其他列，减少 I/O。

> [!note] 批注｜过滤索引与行号定位索引的职责不同
>
> Zone Map 通过最小值、最大值等信息排除不可能命中的范围；Bloom Filter 可以确认某个值不存在，但命中仍需验证。Ordinal Index 则根据行号定位其他列的 Page。先把谓词列筛成行号集合，再按需读取输出列，才是延迟物化节省宽表 I/O 的原因。

## 八、Rowset 生命周期

Rowset 是 Tablet 中数据版本管理的基本单位：


```text
创建 (写入/导入)
    |
    v
COMMITTED (不可变)
    |
    v
VISIBLE (对查询可见)
    |
    v
参与 Compaction (与其他 Rowset 合并)
    |
    v
被新 Rowset 替代 -> 标记删除 -> GC 回收

```

> [!note] 批注｜发布与回收是不同动作
>
> 已提交的 Rowset 可以尚未对普通查询可见，成功发布后才进入可读版本。Compaction 用新 Rowset 替代输入版本范围；正在读旧快照的任务可能还持有旧文件，替代完成也不等于磁盘空间立刻释放。


## 源码路径速查表

| 模块 | 路径 |
| --- | --- |
| SegmentWriter |`be/src/storage/segment/segment_writer.h/.cpp` |
| ColumnWriter |`be/src/storage/segment/column_writer.h/.cpp` |
| PageBuilder 接口 |`be/src/storage/segment/page_builder.h` |
| VerticalSegmentWriter |`be/src/storage/segment/vertical_segment_writer.h` |
| SegmentIterator |`be/src/storage/segment/segment_iterator.h/.cpp` |
| MemTable |`be/src/load/memtable/memtable.h/.cpp` |
| MemTableWriter |`be/src/load/memtable/memtable_writer.h/.cpp` |
| MemTableFlushExecutor |`be/src/load/memtable/memtable_flush_executor.h/.cpp` |
| Segment V2 Proto |`gensrc/proto/segment_v2.proto` |
| Variant ColumnWriter |`be/src/storage/segment/variant/variant_column_writer_impl.h` |

> [!note] 批注｜用集群对应的 tag 核对接口
>
> 文中签名和路径用于定位模块，不作为固定接口。排查实际集群时取运行版本对应的代码，从 `MemTableWriter` 追踪写入，从 `Segment::_parse_footer` 和 `SegmentIterator` 追踪读取；源码重构后可按类名旁查历史路径。

## 核心数据结构/类索引

| 类名 | 作用 |
| --- | --- |
|`SegmentWriter` | Segment 文件写入总控 |
|`SegmentWriterOptions` | 写入配置（行数上限、压缩类型等） |
|`ColumnWriter` | 单列写入器（编码+索引） |
|`ColumnWriterOptions` | 列写入配置（Page 大小、索引需求） |
|`PageBuilder` | 数据 Page 构建抽象基类 |
|`SegmentIterator` | Segment 读取迭代器 |
|`MemTable` | 内存写入缓冲区 |
|`RowInBlock` | MemTable 中的行引用 |
|`SegmentFooterPB` | Segment 文件尾部元信息 |
|`ColumnMetaPB` | 列元信息（编码/压缩/索引信息） |

## 小结

Doris 存储引擎的写入链路从 MemTable 开始，经过排序、预聚合后，通过 SegmentWriter 驱动各列的 ColumnWriter 完成编码和压缩，最终生成不可变的 Segment 文件。读取链路则通过 SegmentIterator 配合多级索引实现高效的数据定位和 Lazy Materialization。理解这条链路是后续分析 Compaction、查询优化等高阶主题的基础。

