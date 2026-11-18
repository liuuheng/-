---
title: "Doris 高阶篇02：数据类型底层实现"
author: "外星人的知识杂谈"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484394&idx=1&sn=0372af6ca1e071e7b1658dd406fb9d19&chksm=c1bf09f7f6c880e1c3877c4c66af25f79a707cba1c6c3172258cb2985fc54a57c23e92cbda95#wechat_redirect"
published_at: "2026-09-06T22:44:41+08:00"
annotated_at: "2026-11-18"
cssclasses:
  - wechat-article
  - doris-intro-annotated
tags:
  - doris
  - 文章批注
---

# Doris 高阶篇02：数据类型底层实现

<div class="article-meta">外星人的知识杂谈 · 2026-09-06</div>

[原文链接](https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484394&idx=1&sn=0372af6ca1e071e7b1658dd406fb9d19&chksm=c1bf09f7f6c880e1c3877c4c66af25f79a707cba1c6c3172258cb2985fc54a57c23e92cbda95#wechat_redirect)

<!-- 正文按原文顺序保留；批注和核对说明为新增。装饰封面和独立的篇末预告已去除。 -->

## 定位与阅读前提

本文深入分析 Doris 中几种复杂数据类型的底层实现：Roaring Bitmap、HyperLogLog、VARIANT、QUANTILE\_STATE 以及嵌套类型（ARRAY/MAP/STRUCT）。理解这些类型的内存布局和序列化格式，对于性能调优和正确使用这些类型至关重要。

**阅读前提：**

- 了解基数估算（cardinality estimation）的基本概念
- 了解位图（bitmap）在集合运算中的应用
- 熟悉 JSON 半结构化数据的使用场景
- 建议先阅读前一篇存储引擎解析

## 背景动机

在 OLAP 场景中，精确去重（COUNT DISTINCT）的成本极高。Doris 提供了 BITMAP 类型实现精确去重、HLL 类型实现近似去重，大幅降低计算和存储开销。同时，VARIANT 类型为半结构化数据分析提供了灵活的列式存储方案。

## 一、Roaring Bitmap 实现

### 1.1 BitmapValue 类体系


```cpp
// be/src/core/value/bitmap_value.h
struct BitmapTypeCode {
    enum type {
        EMPTY = 0,       // 空 bitmap
        SINGLE32 = 1,    // 单个 32 位元素
        BITMAP32 = 2,    // 32 位 Roaring Bitmap
        SINGLE64 = 3,    // 单个 64 位元素
        BITMAP64 = 4,    // 64 位 Roaring Bitmap
        SET = 5,         // 小集合 V1
        SET_V2 = 10,     // 小集合 V2
        BITMAP32_V2 = 12,
        BITMAP64_V2 = 13,
    };
};

```


BitmapValue 是 Doris 中 Bitmap 列的内存表示。它采用**自适应存储策略：**


```text
元素数量:  0      1        少量(<=32)    大量
存储形式:  EMPTY  SINGLE   SET           ROARING BITMAP

```

> [!note] 批注｜精确去重要求整数 ID 一一对应
>
> 位图能精确地对其存储的整数求并集和基数。若先把字符串用户 ID 哈希成整数，哈希碰撞仍会让业务去重偏小；需要严格精确时，应使用无冲突的 ID 映射。小集合和 Roaring 之间的转换只改变存储表示，不改变元素集合。


### 1.2 序列化格式

序列化时以 TypeCode 开头，后跟 Payload：


```text
+----------+-----------------------------------+
| TypeCode | Payload                           |
| (1 byte) | (变长)                            |
+----------+-----------------------------------+

```


- `EMPTY`

  : 无 Payload
- `SINGLE32`

  : 4 字节 UInt32 小端序
- `SINGLE64`

  : 8 字节 UInt64 小端序
- `BITMAP32`

  : 标准 Roaring Bitmap 格式
- `BITMAP64`

  :`NumRoaring(varint64) + {MapKey(4B), RoaringBitmap}*N`

> [!note] 批注｜解析时先读类型码
>
> `SINGLE32`、`SINGLE64` 的负载长度固定，Roaring 编码的负载长度和布局随类型码变化。单凭元素总数无法决定用哪个解码器；需要互读时应按目标版本的 [`BitmapValue`](https://github.com/apache/doris/blob/branch-4.1/be/src/core/value/bitmap_value.h) 实际分支测试，其中 V1 与 V2 的 Roaring 类型码不同。

### 1.3 Roaring Bitmap 内部容器结构

Doris 使用 CRoaring 库（`be/src/clucene/src/ext/roaring/roaring.h`），其核心思想是将 32 位整数空间按高 16 位分桶，每个桶内用一个 Container 存储低 16 位：


```text
32位整数: [高16位 | 低16位]
           |         |
           |         +-> Container 内索引
           +-----------> 桶索引 (最多 65536 个桶)

```


三种 Container 类型：

| 容器类型 | 条件 | 内存占用 | 操作复杂度 |
| --- | --- | --- | --- |
| **ArrayContainer** | 元素 < 4096 | 2B \* n | O(n) 并集 |
| **BitmapContainer** | 元素 >= 4096 | 固定 8KB | O(1) 查找 |
| **RunContainer** | 连续区间多 | 4B \* runs | 视 runs 数 |

容器间的自动转换规则：

- ArrayContainer 元素超过 4096 → 转为 BitmapContainer
- BitmapContainer 元素降到 4096 以下 → 转为 ArrayContainer
- RunContainer 在序列化时按空间最优选择

> [!note] 批注｜容器阈值是对单桶而言
>
> 一个高 16 位桶的低位空间最多容纳 65,536 个值；整张位图可以同时包含稀疏的数组容器和稠密的位图容器。两个数组容器做并集时要遍历双方的有序值，成本随两者总长度增长；整个 Bitmap 的内存无法仅凭总基数推算，还要看值怎样分布到桶。

### 1.4 64 位扩展

标准 Roaring Bitmap 只支持 32 位整数。Doris 通过`map<uint32_t, RoaringBitmap>` 实现 64 位支持：高 32 位作为 map key，低 32 位存入对应的 32 位 Roaring Bitmap。

## 二、HyperLogLog 实现

### 2.1 核心类


```cpp
// be/src/core/value/hll.h
class HyperLogLog {
public:
    HyperLogLog() = default;
    explicit HyperLogLog(uint64_t hash_value);
    explicit HyperLogLog(const Slice& src);

    void update(uint64_t hash_value);
    void merge(const HyperLogLog& other);
    int64_t estimate_cardinality() const;
    size_t serialize(uint8_t* dst) const;
    bool deserialize(const Slice& src);

private:
    HllDataType _type = HLL_DATA_EMPTY;
    phmap::flat_hash_set<uint64_t> _hash_set;  // EXPLICIT 模式
    uint8_t* _registers = nullptr;              // SPARSE/FULL 模式
};

```


### 2.2 四阶段演进模型

Doris 的 HLL 实现采用渐进式存储，数据量从小到大依次经历四个阶段：


```text
EMPTY → EXPLICIT → SPARSE → FULL
（不可逆，只能单向转换）

```


关键常量：


```cpp
const int HLL_COLUMN_PRECISION = 14;            // 精度: 2^14 = 16384 个桶
const int HLL_EXPLICIT_INT64_NUM = 160;         // EXPLICIT 模式最大元素数
const int HLL_SPARSE_THRESHOLD = 4096;          // 非零桶数阈值
const uint16_t HLL_REGISTERS_COUNT = 16 * 1024; // 桶总数 16384

```

> [!warning] 核对说明｜SPARSE 与 FULL 主要是序列化差别
>
> Doris 4.1 的 [`HyperLogLog::update` 和 `serialize`](https://github.com/apache/doris/blob/branch-4.1/be/src/core/value/hll.cpp) 显示，超过显式集合路径后，内存直接使用完整的寄存器数组。写出字节时才按非零寄存器数量决定用 SPARSE 记录索引和值，还是用 FULL 写整段数组。因此原表的字节数不等于聚合算子里每个 HLL 状态的内存。


### 2.3 各模式详解

**EXPLICIT 模式：**

- 直接存储 hash 值（最多 160 个）
- 最大空间：1 + 1 + 160\*8 = 1282 字节
- 适用于极低基数场景

**SPARSE 模式：**

- 只存储非零 register（桶）
- 格式：`type(1B) + count(4B) + {index(2B), value(1B)}*N`
- 最大空间：1 + 4 + 3\*4096 = 12293 字节
- 非零桶不超过 4096 个时使用

**FULL 模式：**

- 存储所有 16384 个 register
- 格式：`type(1B) + registers(16384B)`
- 固定 16385 字节
- 大基数时使用

### 2.4 基数估算算法

采用 HyperLogLog++ 改进算法：

1. 对输入值进行 hash
2. 取 hash 的高 14 位定位桶号
3. 取低 50 位计算 leading zeros + 1 作为桶值
4. 估算时使用调和平均数 + 小基数偏差修正

> [!warning] 核对说明｜Doris 4.1 的实际位运算
>
> [`HyperLogLog::_update_registers`](https://github.com/apache/doris/blob/branch-4.1/be/src/core/value/hll.h) 用 `hash_value % 16384` 选桶，也就是取低 14 位。再右移 14 位，在剩余的位上数尾部连续零并加一，保留该寄存器见过的最大值。原文把高低位与零的方向写反了。合并两个状态时，同序号寄存器取较大值，因此不能把两个分片的基数估算结果直接相加来得到全局去重。

## 三、VARIANT 类型实现

### 3.1 ColumnVariant 内存结构


```cpp
// be/src/core/column/column_variant.h
class ColumnVariant final : public COWHelper<IColumn, ColumnVariant> {
    // 子列树结构
    // 每个路径对应一个子列，类型可以动态推断
};

```


VARIANT 类型的核心设计思想：**写入时 schema inference，读取时 subcolumn extraction。**

### 3.2 写入时的 Schema 推断


```text
输入 JSON 数据
    |
    v
解析 JSON，提取所有路径 (path_in_data)
    |
    v
按路径归类，推断每个路径的类型
    |
    v
高频路径 → 提取为独立子列 (typed subcolumn)
低频路径 → 存入 sparse column (JSONB 格式)
    |
    v
各子列独立编码写入 Segment

```


关键源码路径：

- `be/src/storage/segment/variant/variant_column_writer_impl.h`

   — 写入实现
- `be/src/storage/segment/variant/variant_statistics.h`

   — 统计信息
- `be/src/storage/segment/variant/nested_group_provider.h`

   — 嵌套分组

> [!warning] 核对说明｜推断子列与声明类型路径要分开
>
> [Doris 4.x VARIANT 文档](https://doris.apache.org/docs/4.x/sql-manual/basic-element/sql-data-types/semi-structured/VARIANT/)区分自动提取的子列和通过 Schema Template 声明类型的路径。自动提取还受 `variant_max_subcolumns_count` 等属性约束。查询能否只读独立子列取决于目标路径的实际存储方式；不能仅凭 SQL 写成 `v['key1']` 推断一定不解析稀疏数据。

### 3.3 Sparse Column 回退

当一个子路径出现频率低于阈值（或类型不一致），该路径的值会被存入 sparse column。sparse column 使用 JSONB 格式存储，读取时需要动态解析。


```cpp
// be/src/storage/segment/variant/sparse_column_merge_iterator.h
class SparseColumnMergeIterator;

```


### 3.4 读取时的子列提取


```cpp
// be/src/storage/segment/variant/variant_column_reader.h
class VariantColumnReader;

// be/src/storage/segment/variant/binary_column_extract_iterator.h
class BinaryColumnExtractIterator;

```


查询`SELECT v['key1']` 时，直接读取`key1` 对应的子列，无需解析整个 JSON。

## 四、QUANTILE\_STATE 与 T-Digest

QUANTILE\_STATE 类型用于近似分位数计算，底层基于 T-Digest 算法：

### 4.1 T-Digest 原理

- 将数据分布用一组质心（centroid）来近似表示
- 每个质心包含：均值（mean）和权重（weight）
- 在尾部（分位数接近 0 或 1）保留更高精度
- 支持增量 merge，适合分布式聚合

### 4.2 使用模式


```sql
-- 创建表时指定类型
CREATE TABLE t (
    id INT,
    latency QUANTILE_STATE QUANTILE_UNION
) ...;

-- 查询分位数
SELECT QUANTILE_PERCENT(QUANTILE_UNION(latency), 0.99) FROM t;

```

> [!note] 批注｜跨批次保留分位数状态
>
> `QUANTILE_UNION` 合并的是分布摘要，`QUANTILE_PERCENT` 再从合并后的状态取分位数。将每批的 P99 求平均会丢失分布信息，尤其在批次大小和尾部分布不同的时候。原例是表结构示意，`...` 需补齐表模型等语法才能执行。


## 五、嵌套类型列式存储

### 5.1 ARRAY 类型

ARRAY 列存储为两部分：

- **Offsets 列：**

  记录每行数组的起止位置
- **Elements 列：**

  所有元素平铺存储


```text
逻辑数据: [[1,2,3], [4,5], [6]]
Offsets:   [0, 3, 5, 6]
Elements:  [1, 2, 3, 4, 5, 6]

```


### 5.2 MAP 类型

MAP 列存储为三部分：

- **Offsets 列：**

  每行 map 的 entry 数量
- **Keys 列：**

  所有 key 平铺
- **Values 列：**

  所有 value 平铺


```cpp
// be/src/core/column/column_map.h
class ColumnMap;

```

> [!warning] 核对说明｜Offsets 是边界，不是逐行计数
>
> 与 ARRAY 一样，MAP 用偏移边界指向平铺的 Keys 和 Values。同一个区间中的键和值按位置配对，相邻偏移相减才得到当前行的 entry 数。连续两个边界相同只说明长度为零，是否是 `NULL` 要看空值标记。


### 5.3 STRUCT 类型

STRUCT 按字段拆分，每个字段独立存为一列，与 Parquet 的 struct 存储方式一致。

### 5.4 Nullable 处理

嵌套类型的 Nullable 通过`ColumnNullable` 包装：


```cpp
// be/src/core/column/column_nullable.h
class ColumnNullable : public COWHelper<IColumn, ColumnNullable> {
    ColumnPtr nested_column;     // 实际数据列
    ColumnPtr null_map;          // null 标记 (UInt8 向量)
};

```

> [!note] 批注｜外层为空与内部元素为空是两个层次
>
> `NULL`、`[]` 和 `[NULL]` 是不同的值。`NULL` 由外层 `ColumnNullable` 标记；空数组没有元素；`[NULL]` 有一个位置，由元素列的空值标记表达。看执行时的列结构要找到对应层级，不能用一张 null map 解释所有空值。


## 源码路径速查表

| 模块 | 路径 |
| --- | --- |
| BitmapValue |`be/src/core/value/bitmap_value.h` |
| Roaring 库 |`be/src/clucene/src/ext/roaring/roaring.h` |
| Bitmap 聚合函数 |`be/src/exprs/aggregate/aggregate_function_bitmap.h` |
| HyperLogLog |`be/src/core/value/hll.h` |
| HLL 聚合函数 |`be/src/exprs/aggregate/aggregate_function_hll_union_agg.h` |
| ColumnVariant |`be/src/core/column/column_variant.h` |
| Variant Writer |`be/src/storage/segment/variant/variant_column_writer_impl.h` |
| Variant Reader |`be/src/storage/segment/variant/variant_column_reader.h` |
| Variant Statistics |`be/src/storage/segment/variant/variant_statistics.h` |
| DataType Bitmap |`be/src/core/data_type/data_type_bitmap.h` |
| DataType HLL |`be/src/core/data_type/data_type_hll.h` |
| DataType Variant |`be/src/core/data_type/data_type_variant.h` |
| ColumnNullable |`be/src/core/column/column_nullable.h` |
| ColumnMap |`be/src/core/column/column_map.h` |

## 核心数据结构/类索引

| 类名 | 作用 |
| --- | --- |
|`BitmapValue` | Bitmap 列的内存/序列化表示 |
|`BitmapTypeCode` | Bitmap 序列化类型标识 |
|`HyperLogLog` | HLL 基数估算实现 |
|`HllDataType` | HLL 四阶段枚举 |
|`ColumnVariant` | VARIANT 列内存表示 |
|`VariantColumnReader` | VARIANT 子列读取器 |
|`ColumnNullable` | Nullable 包装列 |
|`ColumnMap` | MAP 类型列 |
|`SubcolumnTree` | VARIANT 子列树结构 |

## 小结

Doris 的复杂类型实现体现了“空间-时间-精度”三者的权衡：

- Bitmap 通过分层容器策略，在不同基数下自动选择最优存储
- HLL 通过四阶段渐进模型，小数据量时零开销，大数据量时固定空间
- VARIANT 通过写入时推断+子列提取，兼顾了灵活性和列式存储的查询性能

理解这些底层实现，有助于在实际业务中做出正确的类型选择和性能调优决策。

