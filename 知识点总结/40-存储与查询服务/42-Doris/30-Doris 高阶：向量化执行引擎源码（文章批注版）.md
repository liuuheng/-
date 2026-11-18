---
title: "Doris 高阶篇03：向量化执行引擎源码"
author: "外星人的知识杂谈"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484400&idx=1&sn=21d8331bababf7eab569fbb074812feb&chksm=c1bf09edf6c880fbb424a718d4e852ed103bd5b320aba0964e78900a41b0dce4efef64f307da#wechat_redirect"
published_at: "2026-09-12T13:57:50+08:00"
annotated_at: "2026-11-18"
cssclasses:
  - wechat-article
  - doris-intro-annotated
tags:
  - doris
  - 文章批注
---

# Doris 高阶篇03：向量化执行引擎源码

<div class="article-meta">外星人的知识杂谈 · 2026-09-12</div>

[原文链接](https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484400&idx=1&sn=21d8331bababf7eab569fbb074812feb&chksm=c1bf09edf6c880fbb424a718d4e852ed103bd5b320aba0964e78900a41b0dce4efef64f307da#wechat_redirect)

<!-- 正文按原文顺序保留；批注和核对说明为新增。装饰封面和独立的篇末预告已去除。 -->

## 定位与阅读前提

本文深入剖析 Doris BE 端向量化执行引擎的核心数据结构和算子实现模式，包括 Block/Column 体系、向量化算子的实现范式、SIMD 优化点以及 Nullable 列在向量化上下文中的处理策略。

**阅读前提：**

- 理解列式存储与行式存储的区别及各自优势
- 了解 SIMD（Single Instruction Multiple Data）的基本概念
- 熟悉 C++ 模板元编程、虚函数多态
- 建议先阅读前两篇关于存储引擎和数据类型的文章

## 背景动机

Doris 从 0.15 版本开始全面转向向量化执行引擎（参考了 ClickHouse 的设计）。相比于逐行处理的 Volcano 模型，向量化引擎通过批量处理列数据、利用 CPU Cache 局部性和 SIMD 指令集，实现了数量级的性能提升。

## 一、Block 与 Column 数据结构

### 1.1 IColumn 接口层次


```cpp
// be/src/core/column/column.h
// 注释: This file is copied from ClickHouse and modified by Doris
class IColumn : public COW<IColumn> {
public:
    using Offset = UInt32;
    using Offsets = PaddedPODArray<Offset>;

    virtual std::string get_name() const = 0;
    virtual size_t size() const = 0;
    virtual void insert_from(const IColumn& src, size_t n) = 0;
    virtual void insert_range_from(const IColumn& src, size_t start, size_t length) = 0;
    virtual void pop_back(size_t n) = 0;
    virtual MutablePtr filter(const Filter& filt, ssize_t result_size_hint) const = 0;
    virtual size_t byte_size() const = 0;
    virtual size_t allocated_bytes() const = 0;
    // ...
};

```


`IColumn` 使用 COW（Copy-On-Write）语义管理内存，核心继承体系：


```text
IColumn (抽象基类)
├── ColumnVector<T>        // 定长数值列 (Int8/16/32/64, Float32/64, ...)
├── ColumnString           // 变长字符串列
├── ColumnNullable         // Nullable 包装
├── ColumnArray            // 数组列
├── ColumnMap              // Map 列
├── ColumnVariant          // 半结构化列
├── ColumnConst            // 常量列（标量广播）
└── ColumnDecimal<T>       // Decimal 列

```

> [!note] 批注｜COW 约束列的修改
>
> 两个表达式可以持有同一列的引用；需要写入时，通过 COW（写时复制）取得可变对象，避免改到另一处仍在读取的数据。共享列不意味着每次传递 Block 都复制整列；是否需要复制取决于持有关系和修改路径。


### 1.2 ColumnVector — 定长数值列


```cpp
// be/src/core/column/column_vector.h
template <typename T>
class ColumnVector final : public COWHelper<IColumn, ColumnVector<T>> {
    using Container = PaddedPODArray<T>;
    Container data;  // 连续内存数组

public:
    const T& get_element(size_t n) const { return data[n]; }
    Container& get_data() { return data; }
    const Container& get_data() const { return data; }
};

```


`PaddedPODArray<T>` 是一个带 padding 的动态数组，特点：

- 内存对齐到 64 字节（Cache Line）
- 头尾各保留 padding，支持 SIMD 对齐访问
- 支持 resize 时不初始化（避免无用 memset）

### 1.3 ColumnString — 变长字符串列


```cpp
// be/src/core/column/column_string.h
class ColumnString final : public COWHelper<IColumn, ColumnString> {
    using Chars = PaddedPODArray<UInt8>;
    using Offsets = IColumn::Offsets;

    Chars chars;      // 所有字符串拼接的字节数组
    Offsets offsets;  // 每个字符串的结束位置
};

```


内存布局示例：


```text
字符串: ["hello", "world", "!"]
chars:   h e l l o w o r l d !
offsets: [5, 10, 11]

第 i 个字符串: chars[offsets[i-1] ... offsets[i]-1]

```

> [!note] 批注｜偏移量记录字节，不记录字符
>
> 第二个字符串从 `offsets[0]` 开始，在 `offsets[1]` 之前结束。UTF-8 汉字会占多个字节，偏移差得到字节数；相邻偏移相等表示空串，不能凭这一点判断 `NULL`。


### 1.4 ColumnNullable — Nullable 封装


```cpp
// be/src/core/column/column_nullable.h
class ColumnNullable : public COWHelper<IColumn, ColumnNullable> {
    WrappedPtr nested_column;  // 实际数据列
    WrappedPtr null_map;       // UInt8 向量，1=NULL, 0=非NULL
};

```


### 1.5 Block — 列的容器

Block 是向量化引擎中数据传递的基本单位，包含多个命名的列：


```cpp
// be/src/core/block/block.h
class Block {
    using Container = std::vector<ColumnWithTypeAndName>;
    Container data;  // {column_ptr, data_type_ptr, name}
};

```


Block 代表一批数据（通常 1024~4096 行），在算子间通过`push/pull` 模式传递。

> [!note] 批注｜Block 是算子间批次，不是磁盘 Page
>
> Block 中各列按相同的逻辑行号对齐；下游可一次过滤、投影或计算一批行。磁盘 Page 由编码和页大小决定，一次读取的 Block 可以跨 Page，也可以经过过滤而只含部分行。批大小依执行配置和算子实际输出而定。

## 二、向量化算子实现模式

### 2.1 Operator 基类


```cpp
// be/src/exec/operator/operator.h
class OperatorXBase {
public:
    virtual Status get_block(RuntimeState* state, Block* block, bool* eos) = 0;
    virtual Status push(RuntimeState* state, Block* input_block, bool eos) = 0;
    virtual Status pull(RuntimeState* state, Block* output_block, bool* eos) = 0;
    virtual bool need_more_input_data(RuntimeState* state) const = 0;
};

class DataSinkOperatorXBase {
public:
    virtual Status sink(RuntimeState* state, Block* input_block, bool eos) = 0;
};

```


### 2.2 HashJoin 向量化实现

HashJoin 分为 Build 阶段和 Probe 阶段：

**Build 阶段（Sink 端）：**


```text
JoinBuildSinkOperator::sink(block)
    |
    v
对 build 侧 Block 中的 join key 列计算 hash
    |
    v
逐行插入 HashTable
    |
    v
HashTable 结构: hash → {row_ptr, next}

```


**Probe 阶段（Source 端）：**


```text
JoinProbeOperator::pull(output_block)
    |
    v
对 probe 侧 Block 中的 join key 列计算 hash
    |
    v
批量在 HashTable 中查找匹配行
    |
    v
组装输出 Block (probe 列 + build 列)

```


关键源码：

- `be/src/exec/operator/join_build_sink_operator.h`
- `be/src/exec/operator/join_probe_operator.h`
- `be/src/exec/operator/partitioned_hash_join_probe_operator.h`
- `be/src/exec/common/join_utils.h`

> [!note] 批注｜Build 就绪之后 Probe 才能稳定查找
>
> Build 侧为连接键建立查找结构，重复键可以对应多条记录；Probe 命中哈希后还要按连接键及适用的连接谓词验证匹配。判断内存和输出量时，应分别看 Build 侧保留的数据宽度和同键匹配数，不能用 Probe 输入行数直接估算 Join 输出。

### 2.3 Aggregation 向量化实现


```text
StreamingAggregationOperator
    |
    v
输入 Block → 提取 group-by key 列
    |
    v
计算 hash，在 HashTable 中查找/插入 group
    |
    v
对每个 group 调用 aggregate function 的 update()
    |
    v
输出时遍历 HashTable，调用 finalize() 得到结果

```


关键类：

- `be/src/exec/operator/streaming_aggregation_operator.h`
- `be/src/exec/operator/aggregation_source_operator.h`
- `be/src/exec/common/agg_utils.h`
- `be/src/exprs/aggregate/aggregate_function.h`

> [!warning] 核对说明｜本地聚合可能输出中间状态
>
> `AVG` 的中间状态需要保留和、计数，分布式计划由下游合并状态后再取结果。原流程的 `update()` 和 `finalize()` 是示意名称；[Doris 4.1 的聚合接口](https://github.com/apache/doris/blob/branch-4.1/be/src/exprs/aggregate/aggregate_function.h)区分累加、序列化、合并与 `insert_result_into`。读计划时要确认该阶段在输出状态还是最终值。

### 2.4 Sort 向量化实现


```text
PartitionSortSinkOperator
    |
    v
收集所有输入 Block
    |
    v
对 sort key 列进行向量化比较排序
    |
    v
Top-N 优化: 维护堆结构，只保留前 N 行
    |
    v
输出排序后的 Block

```


关键源码：

- `be/src/exec/sort/sorter.h`
- `be/src/exec/sort/partition_sorter.h`
- `be/src/exec/sort/hybrid_sorter.h`
- `be/src/exec/operator/partition_sort_sink_operator.h`

> [!note] 批注｜局部 Top N 和全局 Top N 的限制对象不同
>
> 无分区的 Top N 针对整个排序输入，分区排序则可能需要为每个分区分别保留候选。分布式计划里的局部 Top N 还能减少网络输入，但只看单个实例的前 N 行无法得出全局顺序，仍需合并各实例的结果。

## 三、SIMD 优化点

### 3.1 Filter 操作

当 WHERE 条件产生一个 filter bitmap（`UInt8` 数组，1=保留，0=丢弃）时，利用 SIMD 加速列的过滤：


```cpp
// 伪代码：SIMD filter
for (size_t i = 0; i < size; i += 32) {
    __m256i mask = _mm256_loadu_si256(filter + i);
    // 使用 movemask 提取非零位
    uint32_t bitmask = _mm256_movemask_epi8(mask);
    // 根据 bitmask 紧凑拷贝数据
}

```

> [!warning] 核对说明｜movemask 提取最高位
>
> 原伪代码中 `filter` 字节取值为 `0` 或 `1`，直接传给 `_mm256_movemask_epi8` 得到的掩码全是零，因为两者的最高位都为零。可先将与零比较的结果转换为“非零字节为 `0xFF`”的掩码，再提取最高位；最后不足 32 行的尾批也需要单独处理。这里保留原伪代码供对照。


### 3.2 Hash 计算

对整型 key 的批量 hash 计算：


```cpp
// 对连续内存中的整型值批量计算 hash
// 利用 SIMD 指令并行处理多个值
for (size_t i = 0; i < batch_size; i += 8) {
    // AVX2: 同时处理 8 个 32-bit 整数
    __m256i keys = _mm256_loadu_si256(data + i);
    __m256i hashes = simd_murmur_hash(keys);
    _mm256_storeu_si256(result + i, hashes);
}

```


### 3.3 比较操作

字符串比较、数值比较都有 SIMD 优化版本：


```cpp
// 数值列批量比较 (ColumnVector<T>::filter)
// 编译器自动向量化 + 手动 SIMD intrinsic 双保险

```


### 3.4 NULL 处理的 SIMD 优化


```cpp
// 批量处理 null_map，利用 SIMD 判断一批数据是否全非 NULL
// 如果全非 NULL，可以走无 NULL 的快速路径
bool has_null = simd::contain_byte(null_map, size, 1);
if (!has_null) {
    // 无 NULL 快速路径：跳过所有 NULL 检查
} else {
    // 有 NULL 路径：逐个检查 null_map
}

```


## 四、Nullable 列处理策略

### 4.1 分支消除

向量化引擎中处理 Nullable 列的核心策略是**编译期分支消除：**


```cpp
template <bool nullable>
void process_column(const IColumn* column, ...) {
    if constexpr (nullable) {
        auto* nullable_col = assert_cast<const ColumnNullable*>(column);
        auto* null_map = nullable_col->get_null_map_data().data();
        auto* nested = nullable_col->get_nested_column_ptr().get();
        // 处理时检查 null_map
    } else {
        // 直接处理，无 NULL 检查开销
    }
}

```


通过模板实例化，编译器生成两个版本的函数，运行时根据列是否 Nullable 选择对应版本，避免循环内的条件分支。

### 4.2 聚合函数中的 Nullable 处理


```cpp
// 聚合函数通用模式
class AggregateFunctionNullBase {
    // 额外维护一个 flag 表示是否收到过非 NULL 值
    // 如果所有输入都是 NULL，最终输出也是 NULL
};

```

> [!warning] 核对说明｜空集返回值由聚合函数定义
>
> `SUM` 对全空输入返回 `NULL`，但 `COUNT(col)` 返回 0；`COUNT(*)` 仍统计这些行。不能把原示意类的结果规则推广到所有聚合。排查时先核对 SQL 函数的空值语义，再看实现是否在 Nullable 包装或具体函数中处理。


### 4.3 表达式求值中的短路优化

对于`a AND b`，如果 a 为 NULL/false，可以跳过 b 的计算。向量化实现中通过 Selection Vector（选中行号数组）来实现这种短路。

> [!warning] 核对说明｜不能把 NULL 当成 FALSE 短路
>
> SQL 三值逻辑下，`NULL AND FALSE` 为 `FALSE`，`NULL AND TRUE` 为 `NULL`。左侧为 `FALSE` 时右侧不影响结果，左侧为 `NULL` 时通常仍需右侧的真假信息。向量化按行做掩码或选择向量时也须保留这一区别。

## 五、内存管理

### 5.1 Arena 分配器


```cpp
// be/src/core/arena.h
class Arena;

```


用于 HashTable 中 key 的内存分配，特点：

- 批量分配，减少 malloc 系统调用
- 生命周期与算子绑定，整体释放
- 无碎片化问题

> [!note] 批注｜批量分配减少小对象管理成本
>
> Arena 按大块申请内存，可在作用域结束时整体释放，小对象通常不会逐个归还。块尾未利用空间、容量增长和对齐仍然会造成额外占用；看聚合内存时，还应区分哈希表槽位、Arena 容量以及算子实际保留的状态数据。

### 5.2 PODArray 内存策略


```cpp
// PaddedPODArray 特点
// 1. resize 时 2x 增长，减少 realloc 次数
// 2. 前后 padding 保证 SIMD 对齐访问不越界
// 3. 析构时不调用元素析构函数（POD 类型保证）

```


## 源码路径速查表

| 模块 | 路径 |
| --- | --- |
| IColumn 基类 |`be/src/core/column/column.h` |
| ColumnVector |`be/src/core/column/column_vector.h` |
| ColumnString |`be/src/core/column/column_string.h` |
| ColumnNullable |`be/src/core/column/column_nullable.h` |
| Block |`be/src/core/block/block.h` |
| Operator 基类 |`be/src/exec/operator/operator.h` |
| JoinBuild |`be/src/exec/operator/join_build_sink_operator.h` |
| JoinProbe |`be/src/exec/operator/join_probe_operator.h` |
| Aggregation |`be/src/exec/operator/streaming_aggregation_operator.h` |
| Sort |`be/src/exec/sort/sorter.h` |
| PartitionSorter |`be/src/exec/sort/partition_sorter.h` |
| HashTable Probe |`be/src/exec/operator/join/process_hash_table_probe.h` |
| AggUtils |`be/src/exec/common/agg_utils.h` |
| JoinUtils |`be/src/exec/common/join_utils.h` |

## 核心数据结构/类索引

| 类名 | 作用 |
| --- | --- |
|`IColumn` | 列接口基类，定义所有列操作 |
|`ColumnVector<T>` | 定长数值列，底层 PaddedPODArray |
|`ColumnString` | 变长字符串列（chars + offsets） |
|`ColumnNullable` | Nullable 包装（nested + null\_map） |
|`ColumnConst` | 常量列（标量广播到 N 行） |
|`Block` | 列的容器，算子间数据传递单位 |
|`OperatorXBase` | 向量化算子基类 |
|`DataSinkOperatorXBase` | Sink 算子基类 |
|`PaddedPODArray<T>` | 带 padding 的 POD 动态数组 |
|`Arena` | 块状内存分配器 |

## 小结

Doris 向量化引擎的性能优势来源于三个层面：

1. **数据结构层：**

   Column 以连续数组存储同类型数据，天然 Cache-friendly
2. **算子实现层：**

   批量处理 Block，减少虚函数调用和分支预测失败
3. **底层优化层：**

   SIMD 指令加速计算密集操作，模板特化消除 Nullable 分支

这套设计使得 Doris 在 OLAP 查询中能够充分利用现代 CPU 的流水线和向量化能力。

