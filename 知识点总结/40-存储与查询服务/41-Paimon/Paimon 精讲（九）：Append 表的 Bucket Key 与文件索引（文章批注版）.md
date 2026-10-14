---
title: "Paimon 精讲（九）：Append 表的 Bucket Key 与文件索引（文章批注版）"
aliases:
  - "Paimon 精讲（九）：非主键表要想查得快，关键索引要用对，那怎么用呢？"
author: "胖泽的技术笔记"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s/q_tprGyV7-aOu35DaBF26A"
published_at: "2026-08-27"
reviewed_at: "2026-10-14"
tags:
  - Flink
  - Paimon
  - Paimon-精讲
---

# Paimon 精讲（九）：Append 表的 Bucket Key 与文件索引（文章批注版）

> [!info] 来源、整理方式与版本边界
> 原文：[《Paimon 精讲（九）：非主键表要想查得快，关键索引要用对，那怎么用呢？》](https://mp.weixin.qq.com/s/q_tprGyV7-aOu35DaBF26A)，胖泽的技术笔记，2026-08-27。正文按原文章节顺序保留技术内容、代码、表格、字段和案例；新增内容统一标为“批注”“勘误”或“版本边界”。原文没有声明准确的 Paimon/Flink 版本，内部类、默认值和源码路径不视为稳定 API。

> [!warning] 批注：Append 表没有主键合并语义，但可以通过布局裁剪数据
> Fixed-Bucket Append 表只有在查询对完整 `bucket-key` 使用有限的等值或 `IN` 条件时，才能计算候选 Bucket；选中 Bucket 后仍需读取其中其他 Key 并执行行级过滤。Unaware-Bucket Append 表则没有这类 Bucket 裁剪。
> 原文给出的 Bloom Filter `<5%`、BSI `5%～15%` 写入开销没有基准环境和测试来源，只能视为经验示例，不能用于容量承诺。
## 引言

关于 Paimon 的 Bucket 和索引，之前的文章大多围绕**主键表**展开——固定 Bucket、动态 Bucket怎么选，File Index 如何存储和裁剪。但有一个场景被很多人忽略了：

**非主键表（Append-only 表）的 Bucket 和索引，该怎么玩？**

非主键表没有主键约束，数据按追加方式写入，不存在更新冲突的问题。但这并不意味着 Bucket 和索引对它不重要。恰恰相反，非主键表的 Bucket-Key 配置直接影响着写入的并行度和文件的分布形态，而索引的优化策略也与主键表有着本质区别。

很多人拿着主键表的经验去套非主键表，结果发现：Bucket 设多了小文件爆炸，设少了写入瓶颈；索引开了但查询没快多少，反而增加了写入开销。

本文专门聚焦非主键表，从 Bucket-Key 的作用机制入手，分析它与主键表的异同，并给出非主键表场景下的索引优化策略和配置建议。如果你正在使用或即将使用 Paimon 的非主键表，这篇文章能帮你少踩几个坑。

## 1 . Bucket-Key 的查询加速原理

### 核心机制：写时路由 + 读时剪枝

> [!note] 补充批注：复合 Bucket Key 需要完整约束才能精确定位 Bucket
> 当 Bucket Key 由多列组成时，查询通常要同时对每一列给出等值或可枚举的 `IN` 条件，才能计算出确定的候选 Bucket。只过滤其中一列时，往往仍需扫描多个或全部 Bucket。

**写入时**，Paimon 根据 bucket-key 的哈希值决定每行数据写入哪个 bucket：


```
bucket_id = abs(hash(bucket_key_value)) % num_buckets

```


相同 bucket-key 值的数据**始终落入同一个 bucket**。

**查询时**，当 WHERE 条件包含对 bucket-key 的等值过滤，Paimon 会：

1. 对过滤值重新计算哈希，得到目标`bucket_id`
2. 只读取该 bucket 下的文件
3. 完全跳过其他 bucket（称为 **bucket pruning**）

### 作用边界

bucket-key 只能做到 **bucket 层面**的过滤，无法直接做到 bucket 内文件层面的过滤：

| 机制 | 非主键表效果 |
| --- | --- |
| bucket pruning | 有效（等值过滤） |
| 文件级 min/max 剪枝 | 数据无排序保证，统计区间重叠大，效果有限 |

---

## 2 . Bucket 内文件级过滤的优化手段


> [!warning] 版本校准
> 当前文档使用 Bloom Filter、Bitmap 和 Range Bitmap；旧文中的 BSI 选项应改查 `file-index.range-bitmap.columns`。索引是否支持组合谓词、Top-N 和具体字段类型随版本与引擎变化，应以实际文档和 `EXPLAIN` 为准。
### 2 .1 文件索引：Bloom Filter（等值查询）


```
'file-index.bloom-filter.columns' = 'job_name,metric_name',

'file-index.bloom-filter.job_name.items' = '10000'
'file-index.bloom-filter.job_name.fpp' = '0.001'

'file-index.bloom-filter.metric_name.items' = '200000'
'file-index.bloom-filter.metric_name.fpp' = '0.01'

```


- 写入时自动构建，无需额外调度
- 仅支持等值查询（`=`、`IN`）
- 大小只由`items`（期望 distinct 数）与`fpp` 决定，**与实际基数无关**，适合**高基数**列（如`user_id`、`email`、唯一键）
- 只能做到**文件级**过滤，且存在**假阳性**（判定"可能包含"时仍读文件，但不会漏判）
- 写入性能损耗 **< 5%**，可忽略

### 2 .2 文件索引：Bitmap（低基数列等值查询）


```
'file-index.bitmap.columns' = 'job_name,metric_name'

```


- Bitmap 索引把每个列值映射到行号位图，做到**行级**过滤（不只文件级），且**精确无假阳性**
- 支持`=`、`IN`、`!=`（Bloom Filter 和 BSI 均不支持`!=`）
- 体积随基数线性增长，只适合**低基数**列（如`status`、`category`、`job_name`、`metric_name`）；高基数/唯一列（`user_id`、主键）位图稀疏、体积大，不适用
- 写入性能损耗介于 Bloom 与 BSI 之间

### 2 .3 文件索引：BSI（范围查询）


```
'file-index.bsi.columns' = 'timestamp'

```


- BSI（Bit-Slice Index）专为数值型列的范围查询设计
- 支持`>`、`<`、`>=`、`<=`、`BETWEEN`、`=`、`IN`
- 对`timestamp BIGINT` 列完全适用
- 写入性能损耗约 **5%~15%**，建议灰度验证后再加

### 2 .4 Sort Compact（范围查询替代方案）

> [!warning] 补充批注：排序 Compaction 改善裁剪，不保证结果顺序
> 排序后文件的 Min/Max 范围通常更集中，有利于范围谓词排除文件；SQL 结果仍只有显式 `ORDER BY` 才保证顺序。同时，排序 Compaction 会重写数据文件，应把额外 I/O 和写放大计入评估。


```
CALL sys.compact(
    'ods_dl.table_name',
    'order_strategy'='order',
    'order_by'='timestamp'
);

```


- 排序后文件内 timestamp 值域不重叠，min/max 裁剪生效
- 对实时写入链路**零影响**（开销转移到离线 compact 任务）
- 需要调度定期执行，维护成本较高

### 2 .5 对比总结

| 机制 | 适用查询类型 | 精度 | 写入影响 | 维护成本 |
| --- | --- | --- | --- | --- |
| Bloom Filter | 等值查询 | 文件级 | 极小 | 低（自动） |
| Bitmap | 等值 +`!=` | 行级 | 低~中 | 低（自动） |
| BSI | 范围查询 + 等值 | 行级 | 中等 | 低（自动） |
| Sort Compact | 范围查询 + 等值 | 文件级（排序后 min/max） | 无 | 高（需调度） |

---

## 3 . 是否可以对 bucket-key 字段再次构建索引


> [!note] 批注：Bucket 裁剪与文件索引可以叠加，但解决的粒度不同
> Bucket Key 先排除其他 Bucket；文件索引再排除候选 Bucket 内不可能命中的文件或数据块。是否值得重复建索引，取决于单个 Bucket 内文件数、Key 基数和实际查询选择性，不能只按字段身份判断。
**可以**，在以下场景有实际价值：

- 单个 bucket 内文件数量多（数据量大、长期未 compact）
- 每个文件包含的 bucket-key 值种类多（高并发写入混入多种值）

查询时两层过滤叠加：


```
第一层：bucket pruning   → 定位到 1 个 bucket（精确）
第二层：bloom filter     → 在该 bucket 内跳过不含该值的文件

```


如果已经做了 sort compact 按 bucket-key 排序，min/max 裁剪已足够精确，bloom filter 的额外收益较小。

---

## 4 . 实际建表案例分析

### 原始建表语句存在的问题

**场景**：存放 Flink 运行指标，600 个任务，每个任务每分钟产生 100+ 个指标。

### 问题1：严重 Bug - bucket-key 引用了不存在的列


```
'bucket-key'='job_name,metric'    -- ❌ 列名是 metric_name，不是 metric
'bucket-key'='job_name,metric_name'  -- ✅ 修正

```


### 问题2：value 字段类型不合理


```
`value` STRING   -- ❌ 范围比较需要显式 CAST，容易出错
`value_double` DOUBLE   -- ✅ 数值类指标直接用 DOUBLE

```


### 问题3：时序范围查询无文件级过滤

核心查询是按时间区间分析趋势，需要加 BSI 索引：


```
'file-index.bsi.columns' = 'timestamp'

```


### Bucket 数量评估

**数据量估算**：


```
600 任务 × 100 指标 × 1次/分钟 × 60 × 24 = 86,400,000 条/天
原始约 26 GB，zstd 压缩后约 5 GB/天

```


**每 bucket 数据量**：


```
5 GB ÷ 64 bucket ≈ 78 MB/bucket

```


Paimon 推荐每个 bucket 128MB~1GB，当前略偏小但基本合理。实际指标数超过 100 时数据量更大，`bucket=64` 可以保留，也有未来扩展余量。

| 每任务指标数 | 天数据量（压缩后） | 每bucket大小(64) | 评价 |
| --- | --- | --- | --- |
| 100 | ~5 GB | ~78 MB | 略偏小 |
| 150 | ~7.5 GB | ~117 MB | 合理 |
| 200 | ~10 GB | ~156 MB | 理想 |

### 优化后的关键参数


```
TBLPROPERTIES (
  'bucket'='64',
  'bucket-key'='job_name,metric_name',                   -- 修复列名
  -- 'file-index.bloom-filter.columns'='job_name,metric_name', -- 任务distinct之后只有600，且指标也只有100，基数较低，不需要使用bloom-filter , 使用bitmap 构建索引即可
  -- 'file-index.bloom-filter.job_name.items'='600',
  -- 'file-index.bloom-filter.job_name.fpp'='0.001',
  -- 'file-index.bloom-filter.metric_name.items'='100',
  -- 'file-index.bloom-filter.metric_name.fpp'='0.01',
  'file-index.bitmap.columns'='job_name,metric_name',    -- 低基数等值/!= 过滤（行级）
  'file-index.bsi.columns'='timestamp',                  -- 时间范围查询加速
  'compaction.max.file-num'='50',
  'compaction.min.file-num'='5',
  'num-sorted-run.stop-trigger'='30',
  'parquet.compression'='zstd',
  'partition.expiration-time'='3d',
  'partition.timestamp-formatter'='yyyyMMdd',
  'partition.timestamp-pattern'='$dt',
  'snapshot.expire.limit'='8',
  'snapshot.num-retained.min'='16',
  'snapshot.time-retained'='24h',
  'sort-spill-threshold'='10'
)

> 注：`job_name`（600 值）与 `metric_name`（100+ 值）均属中低基数列，等值过滤用 **Bitmap** 比 Bloom Filter 更精准（行级，且额外支持 `!=`）。两者对同一列同时加会冗余，低基数场景建议优先 Bitmap、可去掉对这两列的 Bloom Filter。

```


---

## 5 . 写入性能影响评估与缓解方案


> [!warning] 勘误：百分比必须用本表实测替换
> 索引构建成本与行宽、类型、基数、文件大小、Flush/Compaction 频率和 CPU 有关。上线时记录相同数据量下的写入吞吐、Checkpoint 时长、Compaction backlog、索引大小和扫描字节，才能判断索引收益。
### 索引对写入性能的影响

文件索引在**数据文件生成时同步构建**（flush 和 compaction 时）：

| 索引 | 构建开销 | 原因 |
| --- | --- | --- |
| Bloom Filter | 低（< 5%） | 逐行哈希累加，轻量 |
| Bitmap | 低~中 | 按值构建行号位图，比 bloom 重、比 BSI 轻 |
| BSI | 中等（5%~15%） | 按 bit 位切片计算，比 bloom filter 重 |

### 缓解建议

1. **分阶段上线**：先只加 Bloom Filter / Bitmap 观察稳定后再加 BSI
2. **适当增大 compaction 阈值**：减少 compaction 频率，降低索引重建压力
3. **若写入性能优先**：用 Sort Compact 替代 BSI，把 CPU 开销转移到离线任务

## 6 、总结

回到开头的问题：非主键表的 Bucket 和索引，到底该怎么玩？

答案可以概括为一句话：**别拿主键表的经验往非主键表上硬套。**

| 维度 | 主键表 | 非主键表 |
| --- | --- | --- |
| Bucket 核心作用 | 数据分桶 + 更新隔离 | 控制写入并行度与文件分布 |
| Bucket-Key 含义 | 主键或其子集 | 可自定义，影响数据分布 |
| 索引优化重点 | 支持点查和更新裁剪 | 聚焦排序和文件内统计 |
| 常见误区 | 桶数设太少导致倾斜 | 桶数设太多导致小文件 |

非主键表的 Bucket-Key 更像一个**写入分组键**——它决定了哪些数据写到同一个文件，直接影响文件大小和后续查询效率。而索引优化则应聚焦于**排序和文件内统计信息的利用**，而非主键表特有的点查能力。

理解这两点，你在配置非主键表时就不会再迷茫：**Bucket-Key 按写入分布需求设，索引按查询模式配，两者独立决策，互不绑架。**

## 复核资料

- [Bucketed Append](https://paimon.apache.org/docs/master/append-table/bucketed/)
- [Append Table Query Performance](https://paimon.apache.org/docs/master/append-table/query-performance/)
- [Append Table Overview](https://paimon.apache.org/docs/master/append-table/)

## 系列导航

- 上一篇：[[Paimon 精讲（八）：查询裁剪、统计信息与 Files Table（文章批注版）]]
- 下一篇：[[Paimon 精讲（十）：Flink Sink 并发、Bucket 路由与写入冲突（文章批注版）]]
