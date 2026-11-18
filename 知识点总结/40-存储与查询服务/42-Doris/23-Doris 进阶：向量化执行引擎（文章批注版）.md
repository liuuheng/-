---
title: "Doris 进阶篇04：向量化执行引擎"
author: "外星人的知识杂谈"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484367&idx=1&sn=135b55443e36d4ead9ceae0f4f9dc376&chksm=c1bf09d2f6c880c4747e38bb84f8eb1b2838639f4a097cf99397cab10f74334de1e4e05e7ed5#wechat_redirect"
published_at: "2026-08-25T21:49:12+08:00"
annotated_at: "2026-11-18"
cssclasses:
  - wechat-article
  - doris-intro-annotated
tags:
  - doris
  - 文章批注
---

# Doris 进阶篇04：向量化执行引擎

<div class="article-meta">外星人的知识杂谈 · 2026-08-25</div>

[原文链接](https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484367&idx=1&sn=135b55443e36d4ead9ceae0f4f9dc376&chksm=c1bf09d2f6c880c4747e38bb84f8eb1b2838639f4a097cf99397cab10f74334de1e4e05e7ed5#wechat_redirect)

<!-- 保留原文正文、顺序与示例；批注和核对说明为新增。装饰封面与篇末导航已移除；代码未在集群运行。 -->

> **定位：** 进阶篇第 4 篇 | 理解 Doris 执行引擎的核心优化原理
>
> **阅读前提：** 入门篇第 06 篇（查询执行基础）

---

## 一、背景动机

传统数据库采用 Volcano（火山）模型执行查询：每个算子逐行处理数据，通过`next()` 接口逐行拉取。这种模型在 OLTP 场景下表现良好，但在 OLAP 场景下面临严重性能瓶颈：

- **虚函数调用开销：**

  每处理一行数据就需要一次虚函数调用
- **CPU Cache 不友好：**

  逐行处理导致频繁的 Cache Miss
- **无法利用 SIMD：**

  单行处理无法触发 CPU 的向量化指令
- **线程阻塞问题：**

  传统模型中算子间通过阻塞等待同步数据

Doris 在 1.2 版本引入向量化执行引擎，2.0 版本引入 Pipeline 执行模型，彻底解决了上述问题。

---

## 二、Volcano 模型 vs 向量化模型

### 2.1 Volcano 模型（传统）


```text
┌──────────────┐
│  Result Set  │
└──────┬───────┘
       │ next() → 1 row
┌──────┴───────┐
│   Project    │
└──────┬───────┘
       │ next() → 1 row
┌──────┴───────┐
│  Hash Join   │
└──┬───────┬───┘
   │       │ next() → 1 row
┌──┴──┐ ┌──┴──┐
│Scan │ │Scan │
└─────┘ └─────┘

```


**缺点：**

- 每次`next()` 只返回 1 行
- N 行数据 × M 个算子 = N×M 次虚函数调用
- 无法利用现代 CPU 的超标量流水线和 SIMD 指令集

### 2.2 向量化模型（Doris 当前）


```text
┌──────────────┐
│  Result Set  │
└──────┬───────┘
       │ get_block() → 4096 rows (Block)
┌──────┴───────┐
│   Project    │
└──────┬───────┘
       │ get_block() → 4096 rows (Block)
┌──────┴───────┐
│  Hash Join   │
└──┬───────┬───┘
   │       │ get_block() → 4096 rows (Block)
┌──┴──┐ ┌──┴──┐
│Scan │ │Scan │
└─────┘ └─────┘

```


**改进：**

- 每次处理一个 Block（默认 4096 行）
- 虚函数调用减少 4096 倍
- 列式 Block 布局对 CPU Cache 友好
- 可直接使用 SIMD 指令批量计算

> [!warning] 核对说明｜批大小、调用次数与提速是三件事
>
> 一批行共享一次算子边界调用，能摊薄调度开销；但实际批次随 `batch_size`、过滤、尾批和算子输出变化。用 N 除以 4096 只是满批的示意，不能用作实际调用数，更不能换算成端到端提速。批处理也不自动等于 SIMD：分支、复杂数据类型和随机访存仍可能限制收益。[向量化执行](https://doris.apache.org/docs/4.x/key-features/vectorized-execution/)

### 2.3 性能对比

| 维度 | Volcano | 向量化 |
| --- | --- | --- |
| 单次处理行数 | 1 行 | 4096 行（Block） |
| 虚函数调用 | N×M 次 | N/4096×M 次 |
| CPU Cache | 频繁 Miss | 列式连续内存，Hit 率高 |
| SIMD 利用 | 无法使用 | 可直接加速 |
| 分支预测 | 频繁 Miss | 批量处理减少分支 |
| 实测加速比 | 基准 | 3~10x |

---

## 三、Block 与列式内存布局

### 3.1 Block 结构

Block 是向量化引擎的核心数据结构，代表一批数据的列式组织：


```cpp
// 源码位置：be/src/core/block/block.h
class Block {
    std::vector<ColumnWithTypeAndName> _columns;  // 列数组
    size_t _rows = 0;                              // 行数（默认 4096）
};

```


每个 Column 是同一类型的连续内存数组：


```text
Block (4096 rows × 3 columns):
┌────────────────────────────────────────────────────────┐
│  Column<Int64>   │  Column<String>   │  Column<Double> │
│  [1,2,3,...4096] │  ["a","b",..4096] │  [1.1,2.2,...]  │
│  连续 32KB        │  偏移量+数据       │  连续 32KB       │
└────────────────────────────────────────────────────────┘

```

> [!warning] 核对说明｜伪代码表达组织方式，不是发布版类定义
>
> `Block` 持有列及其类型、名称，可把一行理解成各列同一下标的元素。定长整数、变长字符串和 Nullable 的列表示并不相同；变长字符串要访问字符缓冲区和边界，而不是有 4096 个固定宽度字符串槽。源码路径与成员字段以所用版本为准。


### 3.2 为什么是 4096 行

4096 是经验最优值，平衡了多个因素：

| 因素 | 太小的问题 | 太大的问题 |
| --- | --- | --- |
| 虚函数开销 | 摊薄不够 | - |
| L1/L2 Cache | 未填满 | 溢出 Cache |
| 内存分配 | 碎片化 | 延迟高 |
| 延迟 | - | 首行返回慢 |

INT64 列：4096 × 8 字节 = 32KB，恰好适配 L1 Cache（通常 32~64KB）。

> [!note] 批注｜缓存要算参与运算的工作集
>
> 4096 个 `INT64` 的数据是 32 KiB，但一个表达式还会访问另一输入列、输出列及 NULL 标记。它们同时占用缓存，不能仅凭单列大小判断是否命中 L1，更不能确定该批大小在所有负载上最优。

### 3.3 Nullable 列处理


```text
Column<Nullable<Int64>>:
┌──────────────────────┬──────────────────────┐
│  null_map (bitmap)   │  data (Int64 array)  │
│  [0,0,1,0,1,...]    │  [1,2,0,4,0,...]     │
│  4096 bits = 512B    │  4096 × 8B = 32KB    │
└──────────────────────┴──────────────────────┘

```


null\_map 独立存储，计算时可以通过 SIMD 批量判断 NULL，避免逐行 if 分支。

> [!warning] 核对说明｜内存 NullMap 可以每行 1 字节
>
> 原图的 512 B 是把 4096 个标志按位紧凑存放的大小。Doris 的 `ColumnNullable` 实现使用 `ColumnUInt8` 保存 NULL 标记，相同批次在这种内存表示下的数据部分约为 4096 B。磁盘上的编码方式不能拿来计算执行时的工作集。[Doris 源码示例](https://apache.googlesource.com/doris/%2B/685e91f962a068ce098280d1ca84ddb6bc782c1d/be/src/exprs/function/cast/variant_v2/cast_variant_to_scalar.cpp)

---

## 四、SIMD 加速

### 4.1 什么是 SIMD

SIMD（Single Instruction, Multiple Data）：一条指令同时处理多个数据。


```text
标量（逐个计算）：          SIMD（批量计算，AVX2 示例）：
a[0] + b[0] = c[0]        ┌─────────────────────────┐
a[1] + b[1] = c[1]        │ a[0..3] + b[0..3]       │ → 1 条指令
a[2] + b[2] = c[2]        │ = c[0..3]               │
a[3] + b[3] = c[3]        └─────────────────────────┘
→ 4 条指令                  → 1 条指令（4x 加速）

```


### 4.2 Doris 中 SIMD 的应用场景

| 场景 | SIMD 用法 | 加速效果 |
| --- | --- | --- |
| 过滤（WHERE） | 批量比较生成 selection vector | 4~8x |
| 聚合（SUM/AVG） | 批量累加 | 4~8x |
| Hash 计算 | CRC32/MurmurHash 批量计算 | 3~4x |
| 字符串比较 | SSE4.2 PCMPESTRI 指令 | 2~4x |
| NULL 判断 | 批量位运算 | 8x+ |
| 编码/解码 | 批量 Delta/RLE 解码 | 3~5x |

### 4.3 AVX2 vs AVX-512

| 指令集 | 寄存器宽度 | 单次处理 | Doris 支持 |
| --- | --- | --- | --- |
| SSE4.2 | 128 bit | 2×INT64 或 4×INT32 | 是 |
| AVX2 | 256 bit | 4×INT64 或 8×INT32 | 是（主力） |
| AVX-512 | 512 bit | 8×INT64 或 16×INT32 | 部分（视 CPU） |


```sql
-- 查看 BE 是否启用了 AVX2
-- 检查 BE 启动日志中的 CPU features
-- 或查看 /proc/cpuinfo 中的 flags

```


### 4.4 Filter 向量化示例


```cpp
WHERE age > 18 的执行过程：

传统：
for (int i = 0; i < N; i++) {
    if (age[i] > 18) emit(row[i]);  // 逐行分支判断
}

向量化 + SIMD：
// 1. 批量比较，生成 selection vector
__m256i threshold = _mm256_set1_epi32(18);
__m256i data = _mm256_loadu_si256(age_ptr);
__m256i mask = _mm256_cmpgt_epi32(data, threshold);
// mask 中 1 表示满足条件

// 2. 根据 mask 批量选择
// 无分支，CPU 流水线不会被打断

```

> [!warning] 核对说明｜比较掩码不是已经压紧的结果行
>
> `_mm256_cmpgt_epi32` 对 8 个 32 位整数分别比较，真值 lane 为全 1 位，假值为全 0 位。得到掩码后仍需构造保留位置或压紧列数据；Nullable 列还要把 NULL 标记纳入 SQL 谓词语义。硬件支持 AVX2 也不证明目标算子走了该实现，最终收益取决于编译、CPU 和数据类型。


---

## 五、Pipeline 执行模型

### 5.1 为什么需要 Pipeline

向量化解决了“单次处理多少数据”的问题，但没有解决“算子间如何协调”的问题。传统模型中：

- **阻塞问题：**

  Hash Join 的 Build 阶段会阻塞 Probe 端
- **线程模型：**

  一个 Fragment Instance 占一个线程，线程数 = Instance 数
- **资源浪费：**

  大量线程在等待 IO 或 Build 完成时空转

### 5.2 Pipeline 模型核心概念

Doris 2.0 引入的 Pipeline 执行模型将查询计划拆分为多个 Pipeline，每个 Pipeline 由多个 Operator 组成：


```text
源码位置：be/src/exec/pipeline/pipeline_task.h

核心组件：
┌─────────────────────────────────────────────────┐
│  PipelineTask                                    │
│  ├── Pipeline（一组串联的 Operator）              │
│  │   ├── SourceOperator（数据源，如 Scan）        │
│  │   ├── IntermediateOperator（中间算子）         │
│  │   └── SinkOperator（数据输出）                │
│  ├── Dependency（依赖关系）                       │
│  └── RuntimeState                               │
└─────────────────────────────────────────────────┘

```


### 5.3 Dependency 驱动调度

Pipeline 模型的核心创新是 **Dependency 驱动：**


```text
传统模型（拉取式，blocking）：
  Thread1: Scan → Filter → (BLOCKED waiting for Build) → Probe → Output

Pipeline 模型（推送式，non-blocking）：
  Pipeline1: Scan → Filter → HashJoinBuildSink
  Pipeline2: Scan → Filter → HashJoinProbeSink → Output

  Pipeline2 通过 Dependency 监听 Pipeline1 的 Build 完成事件
  Build 未完成时，Pipeline2 不会被调度（不占线程）

```

```cpp
// 源码位置：be/src/exec/pipeline/dependency.h
class Dependency {
    // 当依赖满足时，唤醒等待的 PipelineTask
    void set_ready();
    // PipelineTask 检查是否可以执行
    bool is_ready();
};

```

> [!note] 批注｜任务让出线程，但依赖本身仍然存在
>
> Probe 必须等哈希表准备就绪。Dependency 未满足时，任务不占着执行池线程等待；就绪后重新进入可运行队列。因此等待时间可以很长，线程仍可去运行其他任务，负责 IO 的线程也不等于消失。[Pipeline 执行引擎](https://doris.apache.org/docs/4.x/key-features/pipeline-execution-engine/)


### 5.4 Pipeline 如何避免线程阻塞

| 传统模型 | Pipeline 模型 |
| --- | --- |
| 线程在 Build 阶段阻塞等待 | Build/Probe 拆成两个 Pipeline，Build 完成后通知 Probe |
| 线程在网络 IO 阻塞 | 异步 IO + Dependency，IO 完成后通知任务可调度 |
| 线程在 Spill 磁盘 IO 阻塞 | 异步 Spill + Dependency |
| 线程数 = Fragment Instance 数 | 线程池固定大小，Task 在池中复用线程 |

### 5.5 执行流程示例

以`SELECT * FROM A JOIN B ON A.id = B.id WHERE A.dt = '2026-01-01'` 为例：


```text
Step 1: FE 生成执行计划，划分 Pipeline

Pipeline 0 (Build 侧):
  ScanOperator(B) → HashJoinBuildSink

Pipeline 1 (Probe 侧，依赖 Pipeline 0 完成):
  ScanOperator(A, predicate: dt='2026-01-01') → HashJoinProbe → ResultSink

Step 2: BE 调度执行

ThreadPool:
  [Thread-1] 执行 Pipeline0.Task0（扫描 B 的 Tablet-1）
  [Thread-2] 执行 Pipeline0.Task1（扫描 B 的 Tablet-2）
  ...
  Pipeline0 所有 Task 完成 → Dependency.set_ready()

  [Thread-1] 执行 Pipeline1.Task0（扫描 A + Probe）
  [Thread-2] 执行 Pipeline1.Task1（扫描 A + Probe）

```

> [!warning] 核对说明｜FE 规划跨 BE 的 Fragment，BE 再拆 Pipeline
>
> 图中的 FE “划分 Pipeline”是简写：FE 生成物理计划和 Fragment，BE 把收到的 Fragment 转成有依赖的 Pipeline 并实例化任务。A/B 的 Build 与 Probe 角色要看实际 Join 计划，不能按 SQL 左右顺序固定指定。图中的线程和 Tablet 对应关系也不是固定绑定。[Pipeline 执行原理](https://doris.apache.org/docs/4.x/query-acceleration/optimization-technology-principle/pipeline-execution-engine/)


---

## 六、并行度调优

### 6.1 关键参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
|`parallel_fragment_exec_instance_num` | 1（Session） | 单 Fragment 并行实例数 |
|`parallel_pipeline_task_num` | 0（自动） | Pipeline 并行度，0 = CPU 核数的一半 |
|`max_instance_num` | 64 | 单查询最大 Instance 数 |
|`enable_pipeline_x_engine` （2.1+已默认开启） | true | 启用优化后的 Pipeline 引擎 |

> [!warning] 核对说明｜按版本区分引擎开关和执行并行度
>
> 从 3.0 起，官方文档把 Pipeline 描述为唯一的执行引擎，不能把旧版 PipelineX 开关当成当前的调优项。目标会话先用 `SHOW VARIABLES LIKE 'parallel_pipeline_task_num'` 确认设置，再在同一负载下比较延迟和 Profile。开到更高会增加本地数据重分发与调度成本，不假定阈值对每台 BE 相同。[并行度调优](https://doris.apache.org/docs/4.x/query-acceleration/tuning/tuning-execution/parallelism-tuning/)

### 6.2 并行度对查询的影响


```sql
-- 设置并行度
SET parallel_fragment_exec_instance_num = 8;
SET parallel_pipeline_task_num = 0;  -- 自动

-- 查看实际执行计划中的并行度
EXPLAIN SELECT ...;

```


| 并行度设置 | 适合场景 | 注意事项 |
| --- | --- | --- |
| 1 | 高并发小查询 | 减少资源争抢 |
| CPU/2 | 大查询、低并发 | 默认值，通常最优 |
| CPU | 超大查询、独占模式 | 可能影响其他查询 |

### 6.3 Scan 并行度

Scan 阶段的并行度由 Tablet 数量决定（每个 PipelineTask 负责若干 Tablet）。如果表的 Bucket 数过少，Scan 并行度受限：


```text
实际 Scan 并行度 = min(parallel_pipeline_task_num, tablet_count)

```


**教训：** 一张只有 1 个 Bucket 的表，无论并行度设多高，Scan 阶段都只有 1 个线程工作。

> [!warning] 核对说明｜Tablet 数限制数据布局，不等于 Scanner 数
>
> 一个表的「1 桶」通常指每个分区有一个桶；保留多个分区时仍可涉及多个 Tablet。即使只扫一个 Tablet，4.x 的 ScanOperator 也可视执行条件把数据切给多个 Scanner。判断实际并行度时分开看保留的 Tablet 数、PipelineTask 数和运行时扫描任务，原公式不适合作统一上界。[并行扫描](https://doris.apache.org/docs/4.x/query-acceleration/optimization-technology-principle/pipeline-execution-engine/)

---

## 七、Profile 分析

### 7.1 查看执行 Profile


```sql
-- 启用 Profile
SET enable_profile = true;

-- 执行查询
SELECT ...;

-- 查看最近的 Profile
SHOW QUERY PROFILE "/";

-- 查看指定查询的详细 Profile
SHOW QUERY PROFILE "query_id";

```


### 7.2 关键指标解读


```text
Profile 中的关键指标：

EXCHANGE_OPERATOR:
  WaitForDependencyTime: 500ms    ← 等待上游数据的时间（可能是网络瓶颈）

HASH_JOIN_OPERATOR:
  BuildTime: 200ms                 ← Hash 表构建时间
  ProbeTime: 1500ms               ← Probe 探测时间
  HashTableSize: 10000000          ← Hash 表大小（影响内存）

OLAP_SCAN_OPERATOR:
  RowsRead: 50000000               ← 实际读取行数
  RowsReturned: 1000               ← 返回行数（差距大说明过滤效果好）
  BlocksOpen: 100                  ← 打开的数据块数
  CompressedBytesRead: 500MB       ← 压缩后读取量

```

> [!warning] 核对说明｜等待、执行时间与扫描口径要分开
>
> `WaitForDependencyTime` 是等依赖的时间，Exchange 等待可能来自上游计算尚未产出，也可能来自数据传输、缓冲或倾斜。应对照发送侧的输入量和执行时间，再看实例间最大值。4.x 文档常见输出计数是 `RowsProduced`，读取量和压缩字节要看实际算子及 Profile 层级，不宜把示意字段直接当通用计数器。[Query Profile](https://doris.apache.org/docs/4.x/query-acceleration/query-profile/)


---

## 八、小结

| 要点 | 内容 |
| --- | --- |
| 向量化核心 | Block（4096行）+ 列式内存布局，减少虚函数调用 |
| SIMD 加速 | AVX2/SSE4.2 批量计算，Filter/Agg/Hash 等场景 3~10x 加速 |
| Pipeline 模型 | 算子拆分为 Pipeline，Dependency 驱动调度，消除线程阻塞 |
| 并行度调优 |`parallel_pipeline_task_num`  控制，Tablet 数是并行度上限 |
| Profile 分析 | 通过 WaitForDependency/ScanRows 定位瓶颈 |
