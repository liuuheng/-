---
title: "Doris 高阶篇06：Compaction 与数据合并源码解析"
author: "外星人的知识杂谈"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484403&idx=1&sn=c869b51fc3ddc976e2df3649adad3306&chksm=c1bf09eef6c880f82feb3efc440e9ad64c527b480e36e7959e94f7cc0fa5fbe5f923b39c8f3d#wechat_redirect"
published_at: "2026-09-18T12:15:41+08:00"
annotated_at: "2026-11-18"
cssclasses:
  - wechat-article
  - doris-intro-annotated
tags:
  - doris
  - 文章批注
---

# Doris 高阶篇06：Compaction 与数据合并源码解析

<div class="article-meta">外星人的知识杂谈 · 2026-09-18</div>

[原文链接](https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484403&idx=1&sn=c869b51fc3ddc976e2df3649adad3306&chksm=c1bf09eef6c880f82feb3efc440e9ad64c527b480e36e7959e94f7cc0fa5fbe5f923b39c8f3d#wechat_redirect)

<!-- 正文按原文顺序保留；批注和核对说明为新增。装饰封面和独立的篇末预告已去除。 -->

## 定位与阅读前提

本文深入分析 Doris BE 端 Compaction 子系统的源码实现，涵盖调度算法、Cumulative/Base Compaction 策略、MoW 模型下的 Delete Bitmap 合并，以及并发控制机制。

**阅读前提：**

- 了解 LSM-Tree 思想及 Compaction 的必要性
- 熟悉 Doris 的 Rowset/Segment 存储结构（参见本系列第 01 篇）
- 了解 Doris 数据模型（Duplicate/Aggregate/Unique/MoW）

---

## 背景动机

Doris 采用类 LSM-Tree 的存储架构：数据先写入 MemTable，flush 后形成新的 Rowset。随着导入批次增加，Rowset 数量会不断膨胀，带来以下问题：

1. **查询性能下降：**

   每次查询需要 merge 多个 Rowset，Rowset 越多 merge 代价越大
2. **元数据膨胀：**

   Tablet 的 Rowset 列表过长增加内存占用
3. **Delete 语义实现：**

   MoW（Merge-on-Write）模型需要通过 Delete Bitmap 标记删除行，Compaction 需要重新计算 Bitmap

Compaction 子系统负责在后台持续将多个小 Rowset 合并为大 Rowset，是保障查询性能的关键后台任务。

---

## Compaction 基类架构


```text
源码路径: be/src/storage/compaction/compaction.h

```


### 类继承体系


```text
Compaction (抽象基类)
├── CompactionMixin (StorageEngine 混入，本地模式)
│   ├── BaseCompaction
│   ├── CumulativeCompaction
│   ├── FullCompaction
│   └── ColdDataCompaction
└── CloudCompactionMixin (云存储模式)
    ├── CloudBaseCompaction
    ├── CloudCumulativeCompaction
    └── CloudFullCompaction

```


### Compaction 基类核心接口


```cpp
class Compaction {
public:
    // 选择满足合并条件的输入 Rowset
    virtual Status prepare_compact() = 0;
    // 执行合并并修改 Tablet 元数据
    virtual Status execute_compact() = 0;
    // 合并类型标识
    virtual ReaderType compaction_type() const = 0;
    virtual std::string_view compaction_name() const = 0;
};

```


**四步骤流程**（注释中明确描述）：

1. Pick rowsets satisfied to compact（选择输入 Rowset）
2. Do compaction（执行合并）
3. Modify rowsets（修改元数据）
4. GC output rowset if failed（失败回滚）

> [!note] 批注｜先构建输出，再切换可读版本
>
> 合并写出新 Rowset 后，还要更新 Tablet 元数据，让后续读取使用新版本映射。若在替换前失败，旧输入仍负责提供数据，清理的是未发布的输出。若旧查询还持有输入 Rowset，替换完成也不代表旧文件立即可删。

---

## Compaction 调度机制

### 调度入口


```text
源码路径: be/src/storage/storage_engine.h
源码路径: be/src/storage/storage_engine.cpp

```


`StorageEngine` 通过后台线程`_compaction_producer_thread` 周期性扫描所有 Tablet，为满足条件的 Tablet 提交 Compaction 任务。

核心方法链：


```text
StorageEngine::_compaction_producer_callback()
  → 遍历所有 Tablet 计算 compaction_score
  → 按 score 降序排列
  → submit_compaction_task(tablet, compaction_type)
    → _submit_compaction_task()
      → 创建 BaseCompaction/CumulativeCompaction 实例
      → 提交到 compaction 线程池执行

```


### Score 计算

Compaction Score 决定了 Tablet 的合并优先级。Score 越高，越优先被调度。

**Cumulative Score：** 由`CumulativeCompactionPolicy::calc_cumulative_compaction_score()` 计算，通常等于 cumulative point 之后的 Rowset 中的 segment 总数。

**Base Score：** 通常等于 cumulative point 之前的 Rowset 数量（不含 base rowset 本身）。

> [!note] 批注｜Score 表示调度压力，不直接等于合并字节数
>
> Score 用于为候选排序，应结合策略、Rowset 的重叠情况和版本分布理解。一张表的平均 Score 可能掩盖单个高分 Tablet。判断积压可同时看该 Tablet 的版本数量、Score 随时间的变化及合并吞吐；同分任务的输入字节数也可能不同。

### 并发控制


```text
源码路径: be/src/storage/compaction/compaction_permit_limiter.h

```


关键配置参数：

| 参数 | 含义 |
|------|------|
|`compaction_task_num_per_disk` | 每块磁盘并发 compaction 任务数上限 |
|`cumulative_compaction_num_threads_per_disk` | 每磁盘 cumulative compaction 线程数 |
|`base_compaction_num_threads_per_disk` | 每磁盘 base compaction 线程数 |
|`total_permits_for_compaction_score` | 全局 compaction permit 总数 |

`CompactionPermitLimiter` 通过 permit 机制限制系统级 compaction 并发度，避免过多 compaction 任务抢占 IO/CPU 资源。

> [!note] 批注｜并发名额与 Permit 不等价
>
> 磁盘并发数决定可以同时处理多少任务；Permit 则按调度估算限制同时在途的合并工作量。提高并发可能消化小任务积压，也可能抢占导入和前台查询的 I/O。调参应同时观察积压是否收敛，以及磁盘延迟、导入吞吐和前台查询 P95。

---

## Cumulative Compaction

### 概念

Cumulative Compaction 合并的是 **cumulative point 之后**（即较新的增量 Rowset）的多个 Rowset。它是频率最高的 compaction 类型。


```text
源码路径: be/src/storage/compaction/cumulative_compaction.h
源码路径: be/src/storage/compaction/cumulative_compaction.cpp

```


### Size-Based Policy


```text
源码路径: be/src/storage/compaction/cumulative_compaction_policy.h
源码路径: be/src/storage/compaction/cumulative_compaction_policy.cpp

```


Doris 默认使用`size_based` 策略（`CUMULATIVE_SIZE_BASED_POLICY`），核心逻辑：

**pick\_input\_rowsets 算法：**

1. 从 candidate\_rowsets（cumulative point 之后的 Rowset）中选择连续版本的 Rowset
2. 遇到 Delete Rowset 则截断（不跨 Delete 合并）
3. 基于 compaction\_level 分层，同层 Rowset 优先合并
4. 合并后的 Rowset 如果达到 promotion 阈值则提升层级

**Cumulative Point 推进：**

- `update_cumulative_point()`

   在合并完成后判断输出 Rowset 是否满足 promotion 条件
- 满足条件时将 cumulative point 前移，使该 Rowset 进入 base compaction 候选范围

**关键配置：**
| 参数 | 含义 |
|------|------|
|`cumulative_size_based_promotion_size_mbytes` | 晋升到 base 层的大小阈值（默认 1024MB） |
|`cumulative_size_based_promotion_ratio` | 晋升比例阈值 |
|`cumulative_size_based_promotion_min_size_mbytes` | 晋升最小大小 |
|`cumulative_size_based_compaction_lower_size_mbytes` | 合并下界 |

> [!note] 批注｜先用版本连续性限定合并范围
>
> Compaction 输出要能替换一段连续的版本。若版本序列中间缺失，不能把缺口两边简单写成一个覆盖整段的 Rowset。Cumulative Point 区分增量段与 Base 候选，达到晋升条件后才推进；一次 Cumulative 完成不意味着输出立刻进入 Base 范围。

### Time-Series Policy


```text
源码路径: be/src/storage/compaction/cumulative_compaction_time_series_policy.h

```


针对时序数据场景的特殊策略，按时间窗口组织合并。

> [!warning] 核对说明｜不只按时间触发
>
> [Doris 4.1 时序策略实现](https://github.com/apache/doris/blob/branch-4.1/be/src/storage/compaction/cumulative_compaction_time_series_policy.cpp)会检查候选总大小、文件数量和距离上次合并的时间等条件。检查一次时序合并为什么启动，不能只看日志日期或导入数据的事件时间。

---

## Base Compaction

### 概念

Base Compaction 合并的是 **cumulative point 之前**的 Rowset（包括 base rowset），将所有历史版本合并为一个大的 base rowset。频率较低但每次合并数据量大。


```text
源码路径: be/src/storage/compaction/base_compaction.h
源码路径: be/src/storage/compaction/base_compaction.cpp

```


### pick\_rowsets\_to\_compact 逻辑


```cpp
class BaseCompaction final : public CompactionMixin {
private:
    Status pick_rowsets_to_compact();
    void _filter_input_rowset();
};

```


选择策略：

1. 获取 cumulative point 之前所有 Rowset 作为候选
2. 调用`find_longest_consecutive_version()` 找出最长连续版本序列
3. 过滤不需要合并的 Rowset（如 Duplicate Key 无 delete predicate 时可跳过）
4. 至少需要 2 个输入 Rowset 才会执行

> [!note] 批注｜候选历史范围与实际输入不能画等号
>
> Base 在相应历史范围内选择满足连续性和触发条件的 Rowset。原概念段的“所有历史版本”是目标形态，原选择流程已经给出截取与过滤逻辑；诊断时用实际输入版本范围判断本轮合并是否覆盖目标数据。

---

## Delete Bitmap Compaction（MoW 模型）

在 Merge-on-Write Unique Key 模型中，Doris 使用 Delete Bitmap 标记被更新/删除的行。Compaction 过程中需要重新计算 Delete Bitmap。

### 流程


```text
Compaction::execute_compact()
  → CompactionMixin::execute_compact_impl()
    → 合并输入 Rowset 为输出 Rowset
    → update_delete_bitmap()  // 重新计算 Delete Bitmap
    → modify_rowsets()        // 原子替换 Tablet 中的 Rowset

```


`update_delete_bitmap()` 的核心逻辑：

1. 获取 compaction 期间新增的增量 Rowset（在 compaction 执行过程中可能有新数据导入）
2. 对增量 Rowset 中的每一行，检查是否与输出 Rowset 中的行产生主键冲突
3. 如冲突，在输出 Rowset 的 Delete Bitmap 中标记对应行

关键常量：


```cpp
static constexpr int COMPACTION_DELETE_BITMAP_LOCK_ID = -1;

```

> [!warning] 核对说明｜删除位图要映射到输出的行号
>
> 删除位图标记的是某个 Rowset、Segment 中的物理行。合并改变物理布局，不能直接复用旧位图。[4.1 的 Compaction 实现](https://github.com/apache/doris/blob/branch-4.1/be/src/storage/compaction/compaction.cpp)通过 `RowIdConversion` 计算输出 Rowset 的删除位图，还需覆盖合并期间发布的新版本造成的删除。原文的逐行冲突流程可用来理解结果，源码核对要看行号映射和版本范围。


---

## 合并执行流程

### CompactionMixin::execute\_compact\_impl


```text
execute_compact_impl(permits)
  │
  ├── build_basic_info()          // 构建合并上下文
  │     ├── 检查是否可做 ordered compaction
  │     └── 初始化 RowsetWriter
  │
  ├── handle_ordered_data_compaction()  // 尝试有序合并（无需 merge）
  │     └── do_compact_ordered_rowsets()
  │
  └── (若非 ordered) 标准 merge 流程
        ├── 创建 Merger
        ├── 逐行/逐列合并
        ├── 写入输出 Rowset
        └── update_compaction_level()

```


### Vertical Compaction

当列数较多时，Doris 支持 Vertical Compaction：按列组（Column Group）分批读取和合并，大幅降低内存消耗。`Compaction` 基类中的`_is_vertical` 字段标记是否采用垂直合并。

> [!note] 批注｜值列要复用相同的行选择结果
>
> 先处理 Key 等决定保留顺序与输出行的列，再按列组读取和写入剩余列，后续列组必须使用一致的行映射，否则同一行的字段会错位。列组越小，单次驻留的列数据通常越少，也可能增加分组执行开销；可以对照峰值内存和合并耗时。

---

## 源码路径速查表

| 模块 | 路径 |
| --- | --- |
| Compaction 基类 |`be/src/storage/compaction/compaction.h/.cpp` |
| CompactionMixin |`be/src/storage/compaction/compaction.h`  (line 190+) |
| BaseCompaction |`be/src/storage/compaction/base_compaction.h/.cpp` |
| CumulativeCompaction |`be/src/storage/compaction/cumulative_compaction.h/.cpp` |
| CumulativeCompactionPolicy |`be/src/storage/compaction/cumulative_compaction_policy.h/.cpp` |
| TimeSeries Policy |`be/src/storage/compaction/cumulative_compaction_time_series_policy.h/.cpp` |
| FullCompaction |`be/src/storage/compaction/full_compaction.h/.cpp` |
| CompactionPermitLimiter |`be/src/storage/compaction/compaction_permit_limiter.h/.cpp` |
| CompactionTaskTracker |`be/src/storage/compaction_task_tracker.h/.cpp` |
| StorageEngine（调度） |`be/src/storage/storage_engine.h/.cpp` |
| Compaction Score HTTP |`be/src/service/http/action/compaction_score_action.h/.cpp` |
| Compaction 状态 HTTP |`be/src/service/http/action/compaction_action.h/.cpp` |
| Cloud Compaction |`be/src/cloud/cloud_base_compaction.h/.cpp`  等 |

---

## 核心数据结构/类索引

| 类名 | 职责 |
| --- | --- |
|`Compaction` | 抽象基类，定义 compaction 四步流程 |
|`CompactionMixin` | 本地 StorageEngine 的 compaction 实现混入 |
|`BaseCompaction` | Base Compaction 实现 |
|`CumulativeCompaction` | Cumulative Compaction 实现 |
|`FullCompaction` | 全量 Compaction（手动触发） |
|`ColdDataCompaction` | 冷数据 Compaction |
|`CumulativeCompactionPolicy` | Cumulative 策略抽象基类 |
|`SizeBasedCumulativeCompactionPolicy` | 默认 size\_based 策略 |
|`CompactionPermitLimiter` | 全局并发 permit 控制 |
|`CompactionTaskTracker` | Compaction 任务跟踪器 |
|`StorageEngine` | 调度入口，compaction producer 线程 |
|`CloudCompactionMixin` | 云模式 compaction 基类 |
|`Merger` | 实际的多路归并器 |
|`RowsetWriter` | 输出 Rowset 写入器 |

---

## 关键配置参数汇总

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
|`cumulative_compaction_policy` | size\_based | cumulative 策略选择 |
|`cumulative_size_based_promotion_size_mbytes` | 1024 | 晋升阈值（MB） |
|`compaction_task_num_per_disk` | 4 | 每磁盘并发任务上限 |
|`base_compaction_num_threads_per_disk` | 1 | base compaction 线程数/磁盘 |
|`cumulative_compaction_num_threads_per_disk` | 1 | cumulative compaction 线程数/磁盘 |
|`total_permits_for_compaction_score` | 10000 | 全局 permit 上限 |
|`enable_vertical_compaction` | true | 是否启用垂直合并 |
|`vertical_compaction_num_columns_per_group` | 5 | 垂直合并每组列数 |

> [!warning] 核对说明｜参数名和默认值以部署版本为准
>
> 原表保留。以 [Doris 4.1 的 BE `config.cpp`](https://github.com/apache/doris/blob/branch-4.1/be/src/common/config.cpp)为例，晋升大小的 BE 参数写作 `compaction_promotion_size_mbytes`，默认为 `1024`；`total_permits_for_compaction_score` 在这个分支的默认值是 `1000000`。表属性、BE 参数和运行时已改值分开查，不要直接套用原表默认值。

---

## 小结

Doris Compaction 子系统的设计特点：

1. **分层合并：**

   Cumulative（增量快速合并）+ Base（全量深度合并）两级策略
2. **Score 驱动调度：**

   通过 compaction score 优先处理碎片化严重的 Tablet
3. **Permit 并发控制：**

   避免 compaction 风暴耗尽 IO 资源
4. **MoW Delete Bitmap：**

   合并过程中正确维护 Delete Bitmap 保证数据一致性
5. **Vertical Compaction：**

   宽表场景下的内存优化

理解 Compaction 机制对于调优 Doris 的写入和查询性能至关重要：过少的 compaction 导致查询变慢（Rowset 过多），过多的 compaction 则抢占 IO 影响导入吞吐。

