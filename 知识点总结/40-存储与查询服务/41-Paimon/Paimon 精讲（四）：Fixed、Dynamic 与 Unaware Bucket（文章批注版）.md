---
title: "Paimon 精讲（四）：Fixed、Dynamic 与 Unaware Bucket（文章批注版）"
aliases:
  - "Paimon 精讲（四）：三种 Bucket 模式怎么选？一文讲透"
author: "胖泽的技术笔记"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s/TWwolrziVx9tioUpvjlySg"
published_at: "2026-07-29"
reviewed_at: "2026-10-14"
tags:
  - Flink
  - Paimon
  - Paimon-精讲
---

# Paimon 精讲（四）：Fixed、Dynamic 与 Unaware Bucket（文章批注版）

> [!info] 来源、整理方式与版本边界
> 原文：[《Paimon 精讲（四）：三种 Bucket 模式怎么选？一文讲透》](https://mp.weixin.qq.com/s/TWwolrziVx9tioUpvjlySg)，胖泽的技术笔记，2026-07-29。正文按原文章节顺序保留技术内容、代码、表格、字段和案例；新增内容统一标为“批注”“勘误”或“版本边界”。原文没有声明准确的 Paimon/Flink 版本，内部类、默认值和源码路径不视为稳定 API。

> [!warning] 批注：原文“三种模式”混合了两类表的不同机制
> 主键表当前有 Fixed（正整数）、Dynamic（`-1`）和 Postpone（`-2`）；Append 表有 Fixed Bucket 与 Unaware Bucket（默认 `-1`）。Dynamic 与 Unaware 虽然都写 `-1`，前者维护主键到 Bucket 的索引，后者只是无固定 Bucket 路由，语义完全不同。
> Bucket 数不能只按写入并发的整数倍选择。官方给出的初始估算是 MOR 场景每 Bucket 约 200 MB～1 GB，再结合热点 Key、小文件、Compaction 和查询计划校准。
## 引言

用过 Paimon 的同学都知道，Bucket 是个绕不开的参数。但到底该选哪种模式，很多人心里没底。

选固定 Bucket，怕数据倾斜把某个桶撑爆；选动态 Bucket，又担心小文件太多影响性能；至于 Unaware Bucket，看起来最简单，但什么时候该用它？

**选错模式，轻则写入变慢，重则查询超时甚至 OOM。** 而且一旦表建好了，改 Bucket 模式可不是改个参数那么简单。

本文一次性讲透 Paimon 三种 Bucket 模式——**固定 Bucket、动态 Bucket、Unaware Bucket** 的工作原理、适用场景和选型建议。读完你就能根据自己的数据特征，快速做出正确选择。

---

## 一、基础概念

Paimon 表的 bucket 模式由建表参数`bucket` 决定，不同表类型与`bucket` 取值的组合对应不同模式：

| 表类型 |`bucket`  取值 | 模式 |
| --- | --- | --- |
| Primary Key 表 |`N > 0` | 固定 Bucket |
| Primary Key 表 |`-1` | 动态 Bucket |
| Append 表 |`N > 0` | 固定 Bucket |
| Append 表 |`-1` | **Unaware Bucket** |

---

## 二、固定 Bucket（`bucket = N`）

> [!note] 补充批注：固定 Bucket 的路由字段受表类型约束
> 主键表未显式设置 `bucket-key` 时，通常由主键中排除分区列后的字段参与路由；显式设置时应是主键字段的子集且不能包含分区列。配置合法不代表分布均匀，仍需用实际基数和热点情况验证 Bucket 倾斜。
### 原理

建表时指定 bucket 数量，写入时按 bucket key 的 hash 值路由到对应 bucket，bucket 编号与写入/读取 subtask 静态绑定。


```
bucket-0 → subtask-0（永久绑定）
bucket-1 → subtask-1（永久绑定）
...
bucket-N → subtask-N（永久绑定）

```


### bucket-key 说明

`bucket-key` 为**非必填项**，但 Primary Key 表与 Append 表的行为有所不同：

|  | Append 固定 Bucket | PK 固定 Bucket |
| --- | --- | --- |
| bucket-key 是否必填 | **否** | **否** （默认用主键） |
| 不填时的默认行为 | 全列 hash，均匀分布 | 全部主键列 hash |
| bucket-key 的约束 | 任意列均可 | 必须是主键的子集 |

**不指定 bucket-key（默认）**：按整行所有列的 hash 值分配 bucket，数据分布均匀，但同一逻辑关联的数据不保证落在同一 bucket。


```
'bucket' = '8'
-- 不设 bucket-key → 全列 hash，均匀打散

```


**指定 bucket-key**：按指定列的 hash 值路由，相同 bucket-key 值的行保证落在同一 bucket，产生数据局部性。


```
'bucket'     = '8'
'bucket-key' = 'business_id,model_id'
-- 相同 business_id+model_id 的数据聚集在同一 bucket

```


**何时应显式指定 bucket-key：**

- 下游消费按某个维度聚合（如按`business_id` 聚合），指定该列可减少跨 bucket shuffle
- 需要相关数据聚集存储，提升局部性查询性能
- 与流式消费侧的 key 对齐，减少 join 时的数据重分布

不关心数据局部性、只需均匀分布时，不填即可。

### 优点

- **读性能最高**：点查/范围查可精确定位 bucket，跳过无关文件
- **流式消费最稳定**：bucket 边界固定，每个 subtask 持续有数据，无空闲浪费
- **写入 overhead 低**：无需维护额外索引
- **同时支持 Primary Key 表和 Append 表**

### 缺点

- **需要提前规划容量**：bucket 数一旦设定难以调整，变更需全量重写数据
- **数据倾斜风险**：bucket key 分布不均时，部分 bucket 过大
- **运维复杂度高**：bucket 数设置不当（过少或过多）均有代价

### 适用场景

- 数据量可预估、分布相对均匀的表
- 对点查性能要求高的 Primary Key 表
- 需要高并发流式增量消费的表
- 数据规模长期稳定的核心生产表

---

## 三、动态 Bucket（Primary Key 表`bucket = -1`）

> [!warning] 批注：Dynamic Bucket 会增长，不等于自动消除数据倾斜
> Dynamic Bucket 为新 Key 分配 Bucket，并让已有 Key 回到原 Bucket。热点集中在少量既有 Key 时，增加 Bucket 不能拆开同一个 Key 的负载。它还要求同一分区只有一个写入作业，并需要为索引初始化、内存和本地磁盘预留资源。
### 原理

仅支持 Primary Key 表。Paimon 自动维护一个 key → bucket 的索引文件（index 文件），写入时通过查索引决定该 key 写入哪个 bucket，随数据量增长自动扩展 bucket 数。

两种子模式：

- 默认模式：按`dynamic-bucket.target-row-num`（默认 200 万行/bucket）自动扩展
-`cross-partition-upsert`：支持跨分区 upsert

### 优点

- **免去手动规划**：bucket 数量自动管理，运维成本低
- **数据自动均衡**：不易出现单 bucket 过大
- **快速上线**：无需容量规划，适合探索性业务

### 缺点

- **写入 overhead 更高**：每次写入需查询/维护 index 文件，高并发写入时影响明显
- **仅支持 Primary Key 表**：Append 表不可用
- **流式读取支持较弱**：bucket 拓扑动态变化，流读稳定性不如固定 Bucket
- **内存压力较大**：index 需要部分缓存

### 适用场景

- 数据量难以预估或持续增长的表
- 对写入便捷性要求高、读性能次之的场景
- 批量导入/离线分析为主的 Primary Key 表
- 快速上线、不想做容量规划的场景

---

## 四、Unaware Bucket（Append 表`bucket = -1`）

> [!warning] 勘误：读并发不必机械等于历史写并发
> Unaware Bucket 没有基于 `bucket-key` 的裁剪和 Bucket 内顺序保证，但扫描器仍可根据文件和 Split 规划并行度。原文中的 `scan.model` 应核对为当前公开的 `scan.mode`；`scan.infer-parallelism` 等选项也必须以实际版本 `$options` 和执行计划为准。
### 原理

完全抛弃 bucket 概念，每个写入任务直接写文件，无任何 bucket 路由或协调。文件组织类似普通对象存储文件堆叠，通过 **snapshot 级别的文件发现机制**支持流式读取。

### 流式消费机制（重要）

Unaware Bucket 支持流式读取，但机制与固定 Bucket 有本质区别：


```
每隔 continuous.discovery-interval
  → 扫描新 snapshot
  → 将新 snapshot 中新增文件转换为 splits
  → 将 splits 动态分配给空闲 source subtask

```


**关键特性**：每轮活跃的 source subtask 数 ≈ 该 snapshot 周期新增文件数 ≤ 写入侧并发度。

### 实际案例：并发度不匹配问题

场景：source 算子并发度配置为 32，但实际只有 2 个 subtask 有数据。

原因：写入侧只有 2 个并发 subtask，每个 snapshot 周期只产生 2 个新文件，因此只有 2 个 splits 被分配，其余 30 个 source subtask 空闲等待。


```
写入侧 2 subtask → 每轮产生 2 个文件 → 2 个 splits → 只有 2 个 source subtask 激活

```


**建议**：`scan.parallelism` 应与写入侧并发度对齐，避免资源浪费。

### 优点

- **最高写入吞吐**：无路由计算、无并发写同一 bucket 的竞争，支持近乎无限水平扩展
- **运维最简单**：完全不需要规划 bucket 数量
- **适合超大规模数据接入**：Kafka → Paimon 日志表的典型选择

### 缺点

- **流读并发度受写入侧限制**：有效并发 ≤ 写入并发，无法充分利用高并发读配置
- **查询性能差**：无数据局部性，全表扫描效率低（可通过 bitmap file index 缓解）
- **Compaction 压力大**：文件散乱，需要独立的全局 Compaction 作业
- **`write-only=true` 时必须配套 Compaction 作业**，否则文件数无限累积

### 适用场景

- 纯日志/埋点数据入湖，以批量分析为主
- 写入 TPS 极高（如每秒数十万条），吞吐优先于查询性能
- 数据生命周期短，定期清理，不依赖精细查询

---

## 五、综合对比


> [!note] 补充：Postpone Bucket 的位置
> `bucket=-2` 先把数据写入 Postpone 区域，再由 Spark 批处理或 Compaction 按分区规模分配真实 Bucket。它适合不同分区大小差异明显、无法预先使用统一 Bucket 数的场景，但写入后的可见性和维护流程取决于具体配置。
| 维度 | 固定 Bucket | 动态 Bucket | Unaware Bucket |
| --- | --- | --- | --- |
| 支持表类型 | PK + Append | 仅 PK | 仅 Append |
| 写入吞吐 | 高 | 中（index lookup overhead） | 最高 |
| 点查性能 | 最高 | 中 | 无（无主键） |
| 流读支持 | 最好（静态 bucket 绑定） | 较弱 | 支持，但受写入并发限制 |
| 流读并发利用率 | 高（bucket 数决定） | 中 | 低（≤ 写入并发） |
| 运维复杂度 | 高（需规划容量） | 低 | 最低 |
| 数据均衡性 | 依赖 bucket key 设计 | 自动均衡 | 自动均衡 |
| Compaction 方式 | 按 bucket 独立 | 按 bucket 独立 | 全局文件合并 |

---

## 六、选型决策树

> [!note] 补充批注：选型先固定数据语义
> 主键 Upsert 表再比较 Fixed、Dynamic 与 Postpone Bucket；Append 表再比较 Fixed 与 Unaware Bucket。随后检查并发写入、跨分区更新、Lookup 查询和流式消费要求，否则只按数据量选择 Bucket 模式容易得到语义不兼容的方案。


```
是否 Primary Key 表？
├── 否（Append 表）
│   ├── 写入吞吐优先 / 数据量不确定 → Unaware Bucket（bucket=-1）
│   └── 流读并发要求高 / 数据量可预估 → 固定 Bucket（bucket=N）
└── 是（Primary Key 表）
    ├── 点查优先 + 流读稳定性要求高 + 数据量可预估 → 固定 Bucket（bucket=N）
    └── 快速上线 + 运维简单 + 批处理为主 → 动态 Bucket（bucket=-1）

```


---

## 七、配置示例参考

### Unaware Bucket 高吞吐写入表典型配置


```
TBLPROPERTIES (
  'bucket'='-1',                              -- Unaware Bucket 模式
  'write-only'='true',                        -- 写节点不参与 compaction，最大化吞吐
  'write-buffer-size'='512MB',                -- 大写缓冲，减少 flush 频率
  'write-manifest-cache'='1GB',              -- manifest 缓存，减少读写开销
  'target-file-size'='512MB',                -- 大文件目标，减少小文件数量
  'compaction.min.file-num'='5',             -- 至少 5 个文件才触发 compaction
  'compaction.max.file-num'='50',            -- 每次最多合并 50 个文件
  'file-index.bitmap.columns'='col_a,col_b', -- bitmap 索引缓解无局部性的查询劣势
  'file.format'='parquet',
  'parquet.compression'='zstd'
)

```


### Unaware Bucket 流式消费注意事项


```
-- scan.parallelism 应与写入侧并发度对齐，避免大量 subtask 空转
OPTIONS(
  'scan.model'='latest',
  'continuous.discovery-interval'='5s',
  'scan.infer-parallelism'='false',   -- 关闭自动推断（无 bucket 可参考）
  'scan.parallelism'='2'              -- 手动设置，对齐写入侧并发
)

```

> **注意**：`write-only=true` 时，必须单独运行 Flink Compaction 作业，否则文件数无限累积，严重影响读取性能和 manifest 大小。

---

## 八、核心结论

- **明确 SLA 的生产核心表**：用固定 Bucket，点查和流读性能最可预期
- **快速接入或探索性 PK 表**：用动态 Bucket，省去容量规划
- **日志/埋点高吞吐入湖**：用 Unaware Bucket，写入吞吐最大，但需配套 Compaction 作业并注意消费并发与写入并发对齐

## 复核资料

- [Primary-Key Data Distribution](https://paimon.apache.org/docs/master/primary-key-table/data-distribution/)
- [Append Table](https://paimon.apache.org/docs/master/append-table/)
- [Bucketed Append](https://paimon.apache.org/docs/master/append-table/bucketed/)

## 系列导航

- 上一篇：[[Paimon 精讲（三）：Partition、Snapshot 与数据生命周期（文章批注版）]]
- 下一篇：[[Paimon 精讲（五）：Deletion Vector 与 Merge On Write（文章批注版）]]
