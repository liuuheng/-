---
title: "Flink 实时维表性能优化（上）：用对 Paimon Lookup 的完整实践"
author: "胖泽的技术笔记"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s/hcD8Em8y-bbkB6UeIDQGcA"
published_at: "2026-10-03T21:29:39+08:00"
aliases:
  - Paimon Lookup Join 路径选择与配置
tags:
  - Flink
  - Paimon
  - Lookup-Join
  - 实时维表
reviewed_at: "2026-10-13"
---

# Flink 实时维表性能优化（上）：用对 Paimon Lookup 的完整实践

> Source: wechat
> Author: 胖泽的技术笔记
> Original: https://mp.weixin.qq.com/s/hcD8Em8y-bbkB6UeIDQGcA

> [!info] 阅读方式与版本边界
> 正文保留原文章节、代码、表格、案例和结论；新增内容统一标为“批注”或“勘误”。文章没有声明准确的 Paimon/Flink 版本，内部类、参数和路径选择规则属于版本相关实现。笔记以 2026-10-13 可查的 Paimon 官方文档复核，真正上线仍以部署版本、`EXPLAIN`、TaskManager 日志和实际指标为准。

## 引言

在实时数仓建设中，**Lookup Join**（维表关联）几乎是每个业务都会用到的场景：实时流里的每一条数据，都需要去维表查一次，补充用户信息、商品属性、地区编码等维度字段。而随着 Apache Paimon 逐渐成为 Flink 生态中事实上的湖存储标准，越来越多的团队开始把 Paimon 表当作维表使用。

然而，Paimon 维表的 Lookup 在生产实践中并不总是"开箱即用"那么顺畅。你是否遇到过这些问题：

- 作业启动缓慢，维表加载动辄几分钟，甚至撑爆 checkpoint 超时？
- TaskManager 内存/磁盘被维表缓存占满，资源成本居高不下？
- 同一张大维表被多个 Job 反复加载，集群资源浪费严重？
- 运行平稳的作业周期性出现反压，却找不到根因？

这些表象背后的根源，往往指向同一个关键点：**Lookup 走了哪条缓存路径**。

Paimon 内部存在 PartialCache Remote、PartialCache Local 和 FullCache 等 Lookup 实现。触发哪条路径，取决于表结构、分桶方式、Join Key、连接器配置和版本实现。本文围绕路径选择、建表与 Hint、Query Service、监控和排障展开。

---

## 核心结论：关键在于触发 Partial Cache 路径

Paimon 有三条 Lookup 路径，性能从高到低依次为：

| 路径 | 触发条件 | 启动开销 | 内存占用 | 多 Job 共享 |
| --- | --- | --- | --- | --- |
| **PartialCache Remote（最优）** | Query Service 已启动 + join 键 = 主键 + 固定分桶 | 极低 | 极低 | 是 |
| **PartialCache Local（推荐）** | join 键 = 主键 + 固定分桶 | 极低 | 低 | 否 |
| FullCache（兜底） | 其他所有情况 | 高（全量写入 RocksDB） | 高 | 否 |

> [!warning] 勘误：这张表不能当成跨版本的固定性能排名
> Query Service 会增加一次网络调用，Local Partial 走本地文件，二者谁更快取决于网络、缓存热度、并行度和服务负载。当前官方文档只说明 Query Service 可共享查询服务、Lookup 会优先使用它，并没有承诺 Remote 在所有负载下最快。固定分桶也不是唯一条件：复合键是否覆盖完整 Bucket Key、Flink/Paimon 版本以及实际执行计划都会改变路径。

---

## 一、核心原理（通俗解释）

## 1 .1 Lookup Join 整体工作机制

Flink Lookup Join 的场景：实时流里的每条数据，都去维表查一次并补充信息。


```
实时流：[user_id=123, action=click]
维表：  [user_id=123, name=张三, city=上海]
结果：  [user_id=123, action=click, name=张三, city=上海]

```


Paimon 维表存在 HDFS/OSS 等远端文件系统，Flink 不可能每次都去远端查（太慢），所以会把维表数据**缓存到 TaskManager 本地**：


```
远端存储（HDFS/OSS）                    TaskManager 本地
┌─────────────────────┐               ┌─────────────────┐
│   Paimon 维表文件    │  启动时加载    │  本地缓存        │
│   (SST/Parquet)     │  ──────────>  │  (RocksDB 或    │
│                     │               │   本地索引文件)  │
└─────────────────────┘               └─────────────────┘
                                              ↑ 每条流数据来了，在这里查

```


维表数据也会定期（默认 10 秒）增量同步到本地缓存，保持数据最新。

> [!note] 批注：这里的“同步”需要拆成两步
> 先发现新 Snapshot/文件元数据，再更新本地可查询状态。发现周期为 10 秒，不等于业务变更一定在 10 秒内可见；写入侧还要完成 Checkpoint 和 Snapshot Commit，读取侧还要完成刷新或首次文件加载。新鲜度要从“业务变更 → Snapshot 提交 → Lookup 刷新完成 → 下一次查询命中新值”整条链路测量。

## 1 .2 FullCache vs PartialCache 底层原理

## FullCache：把整个维表搬到本地

启动时，把**全部数据**写入 TaskManager 本地的 RocksDB（一个嵌入式 KV 数据库）：


```
启动阶段（可能需要几分钟）：
远端 1000万行数据  ──全量写入──>  本地 RocksDB
                                  ├── key: user_id=1  → row
                                  ├── key: user_id=2  → row
                                  └── ...（全量）

查询阶段：来了 user_id=123  ──查 RocksDB──>  结果（毫秒级）

```


缺点：启动慢（全量写入），内存/磁盘占用大。

## PartialCache：只缓存查过的部分

启动时什么都不加载，只下载文件索引（知道哪个 key 在哪个文件里）：


```
启动阶段（秒级完成）：只下载文件元数据/索引

查询阶段：来了 user_id=123
  ├── 1. 计算 123 在哪个文件（hash 计算，O(1)）
  ├── 2. 如果文件已在本地缓存 → 直接查
  └── 3. 如果没有 → 只下载这一个文件 → 缓存 → 查询

```


类比：

- FullCache = 把图书馆所有书都搬到桌子上
- PartialCache = 知道每本书在几号书架，用时去拿，用完放桌上备用

## 1 .3 为什么固定分桶能触发 PartialCache？

核心问题：**能不能快速定位数据在哪个文件**。

Paimon 写入时，根据主键把数据分配到不同 bucket（桶），每个桶对应一组文件：


```
写入 user_id=123
  ↓  hash(123) % bucket数 = 7
  ↓  数据写入 bucket-7 对应的文件

```


**固定分桶（bucket=32）**：查询时能立刻算出目标在哪个 bucket：


```
查询 user_id=123
  ↓  hash(123) % 32 = 7  （O(1) 计算，直接定位）
  ↓  只需读 bucket-7 的文件，精确命中！

```


**动态分桶（bucket=-1）**：没有固定映射规则，不知道 key 在哪个文件，必须全量加载扫描 → 只能走 FullCache。

> [!warning] 版本边界
> “动态分桶只能 FullCache”描述的是文章对应实现的 Lookup 路径，不应推广为所有 Paimon 读取。动态分桶本身维护 Key 到 Bucket 的索引；是否能用于某种 Lookup 优化要看连接器版本。当前官方 Query Service 和 Flink 2.0 Shuffle Lookup 明确要求固定分桶，因此在这两种场景下确实不能用 `bucket=-1`。

## 1 .4 bucket 数量怎么选？

| 维表数据量 | 建议 bucket 数 |
| --- | --- |
| 100万行以内 | 8 ~ 16 |
| 100万 ~ 1000万行 | 16 ~ 64 |
| 1000万行以上 | 64 ~ 128 |

**实际规则**：bucket 数建议设为**写入并行度的整数倍**，避免数据倾斜：


```
写入并行度 = 8，bucket = 32  ✓  每个并行度均匀负责 4 个 bucket
写入并行度 = 8，bucket = 10  ✗  分配不均匀

```

> [!warning] 勘误：行数和“并行度整数倍”都不是充分判据
> Bucket 数还受每行字节数、分区数、更新频率、热点 Key、目标文件大小和 Compaction 能力影响。Bucket 数能被写入并行度整除，只能让静态任务分配更整齐，不能消除业务 Key 倾斜。官方当前给出的固定桶初始估算是每 Bucket 约 200 MB～1 GB，再用 `$buckets`、`$files` 和真实吞吐校准；文章的行数区间应当视为作者经验值。


---

## 二、建表时必须满足的条件（触发 PartialCache）

同时满足以下三点，才会走最优的`PrimaryKeyPartialLookupTable` 路径：

> [!note] 批注：先分清“完整主键”和“完整 Bucket Key”
> 原文按 `PrimaryKeyPartialLookupTable` 的源码条件写成 Join Key 覆盖完整主键。当前官方 Flink 2.0 Shuffle Lookup 的公开约束则是 Join Key 包含全部 Bucket Key。前者决定能否唯一点查逻辑行，后者决定能否把记录路由到确定的 Bucket；当 `bucket-key` 只是复合主键的子集时，两者不是同一个条件。

## 1 . 固定分桶（最关键）


```
CREATE TABLE dim_table (
  user_id BIGINT,
  ...
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'bucket' = '16',          -- 必须 > 0，固定分桶（HASH_FIXED 模式）
  'bucket-key' = 'user_id'  -- 可省略，默认用主键
);

```

>`bucket = -1`（动态分桶）或无 bucket 设置会强制走 FullCache。

## 2 . Join 键 = 完整主键


```
-- 正确：join 键 user_id 就是主键
SELECT s.*, d.name
FROM stream_table s
LEFT JOIN dim_table FOR SYSTEM_TIME AS OF s.proc_time AS d
  ON s.user_id = d.user_id;  -- join 键必须覆盖完整主键

```


## 3 . Merge Engine 用默认值 deduplicate


```
-- merge-engine 默认就是 deduplicate，不需要显式设置
-- 若改为 partial-update 或 aggregation，则 PartialCache 不可用

```

> [!warning] 版本边界
> `partial-update`、`aggregation` 需要合并旧状态才能得到最终逻辑行，不能只凭一条增量记录查询。但是否必然回退 FullCache、是否需要 Changelog，随 Paimon 版本和表模式变化。应把 Merge Engine、Changelog Producer、Deletion Vector 和实际 Lookup 类一起核对，不能只检查 `merge-engine` 一个选项。


---

## 三、完整建表模板


```
CREATE TABLE paimon_dim (
  user_id   BIGINT,
name      STRING,
  city      STRING,
  score     DOUBLE,
  dt        STRING,
  PRIMARY KEY (user_id) NOTENFORCED
) PARTITIONED BY (dt)
WITH (
-- 核心：固定分桶，触发 PartialCache
'bucket'                            = '32',

-- 刷新策略：降低全量缓存路径的刷新频率
'continuous.discovery-interval'     = '30s',

-- Partial 缓存本地文件配置
'lookup.cache-file-retention'       = '2h',
'lookup.cache-max-disk-size'        = '20gb',
'lookup.cache-max-memory-size'      = '512mb',
'lookup.cache.bloom.filter.enabled' = 'true',

-- changelog 配置（FullCache 路径需要；PartialCache 不强依赖但建议配置）
'changelog-producer'                = 'lookup'
);

```

> [!warning] 勘误：这段 DDL 不能直接执行，也不满足常规分区主键约束
> 原文的 `NOTENFORCED` 少了空格，应为 `NOT ENFORCED`；缩进不影响语义。更重要的是，表按 `dt` 分区，而主键只有 `user_id`。固定分桶的分区主键表通常要求主键包含所有分区列，官方动态分区示例使用 `PRIMARY KEY (user_id, dt)`。如果业务含义是“每天一份完整维表”，应使用 `(user_id, dt)` 并通过 `scan.partitions=max_pt()` 选择最新分区；如果表表示一份持续更新的当前状态，则先判断是否真的需要按 `dt` 分区。`changelog-producer=lookup` 也不是 Partial Lookup 的通用必选项，它会增加写入侧旧值查询和 Compaction 成本。


---

## 四、Flink SQL Hint 配置（作业侧）


```
SELECT s.*, d.name, d.city
FROM stream_table s
LEFT JOIN paimon_dim /*+ OPTIONS(
  'lookup.cache'                          = 'AUTO',
  'lookup.async'                          = 'true',
  'lookup.async-thread-number'            = '16',
  'lookup.refresh.async'                  = 'true',
  'lookup.cache.bloom.filter.enabled'     = 'true',
  -- Lookup 专用刷新间隔，优先级高于表级 continuous.discovery-interval
  'lookup.continuous.discovery-interval'  = '10s',
  -- 时间格式：yyyy-MM-dd HH:mm->yyyy-MM-dd HH:mm（秒固定为 :00）
  'lookup.refresh.time-periods-blacklist' = '2026-07-05 02:00->2026-07-05 04:00'
) */
FOR SYSTEM_TIME AS OF s.proc_time AS d
ON s.user_id = d.user_id;

```

> [!warning] 参数作用域需要按版本复核
> 当前官方配置把 `lookup.refresh.time-periods-blacklist` 描述为只用于 Full Cache，文章根据某版 `FileStoreLookupFunction.tryRefresh()` 认为 Partial 也受控。两者冲突时，不能用“源码永远优先”概括；应锁定正在运行的 Paimon JAR 版本，查看该版本源码并通过刷新日志验证。`lookup.async=true` 也只改变等待方式，不会消除远端 I/O、乱序约束或维表数据尚未提交的问题。


---

## 五、不同场景下的路径选择

| 场景 | 建议 |
| --- | --- |
| Join 键 = 主键，维表大（千万行+） | 固定分桶，走 PartialCache Local |
| Join 键 = 主键，多个 Job 共用同一张大维表 | 启动 Query Service，走 PartialCache Remote |
| Join 键 ≠ 主键（如按 name 查） | 必走 FullCache，设置`lookup.cache-rows` 调 RocksDB 缓存 |
| 需要聚合语义（如求和） | merge-engine=aggregation，只能走 FullCache，需配`changelog-producer=lookup` |
| 有 QueryService 在运行 | PartialCache 自动走 Remote 路径，不需要额外配置 |
| 数据量小 + 查询分布均匀 | 可考虑显式设`lookup.cache=FULL`，见下方分析 |

## 5 .1 数据量小时，FullCache 是否比 PartialCache 更好？

**结论：不一定，取决于查询分布和重启敏感度。**

## FullCache 在数据量小时的优势

数据量小时，FullCache 全量加载的启动开销不明显，且有一个 PartialCache 没有的优势：**启动后所有数据都在本地，查询完全不依赖远端文件系统**，要么命中 Caffeine（纯内存），要么命中 RocksDB（本地磁盘），没有网络开销。

## PartialCache 的潜在问题（单 TaskManager 场景）

**注意：此问题仅存在于单 TaskManager 或并行度极低的场景。**

PartialCache 是按需加载，首次查到某个 bucket 时需要从远端下载文件。在**单 TaskManager** 场景下，如果：

- 维表数据量小但 bucket 数量多（如 32 个）
- 查询的 joinKey **分布很广**，覆盖大多数 bucket

则最终几乎每个 bucket 都会被下载，总开销和 FullCache 相当，却多了反复判断文件是否已缓存的逻辑开销。

## 多 TaskManager 场景下 PartialCache 仍然更优

**在实际生产环境（多 TM 并行）下，即使查询分布广，PartialCache 优势依然明显。**

假设：

- 维表：32 个 bucket
- Flink 作业：4 个 TaskManager 并行
- 查询分布：均匀

**FullCache 模式：**


```
TM-1: bootstrap 全量加载 → 32 个 bucket 全在本地
TM-2: bootstrap 全量加载 → 32 个 bucket 全在本地
TM-3: bootstrap 全量加载 → 32 个 bucket 全在本地
TM-4: bootstrap 全量加载 → 32 个 bucket 全在本地

集群总资源占用 = 32 × 4 = 128 份 bucket 数据（高冗余）

```


**PartialCache 模式：**


```
TM-1: 实际查到 bucket-1, 7, 15, 22  → 只缓存这 4 个
TM-2: 实际查到 bucket-3, 9, 18, 29  → 只缓存这 4 个
TM-3: 实际查到 bucket-5, 11, 20, 31 → 只缓存这 4 个
TM-4: 实际查到 bucket-2, 8, 14, 26  → 只缓存这 4 个

集群总资源占用 = 最多 32 份（每个 bucket 只被需要它的 TM 缓存一次）
单个 TM 资源占用 = 只是实际查到的部分（远小于全量）

```


| 维度 | FullCache | PartialCache |
| --- | --- | --- |
| 单个 TM 本地数据 | 全量（32 个 bucket） | 部分（实际查到的 bucket） |
| 集群总数据量 | 32 × TM数量（高冗余） | 最多 32（理想情况下不重复） |
| 启动开销 | 每个 TM 都要全量加载 | 每个 TM 按需加载 |
| 内存/磁盘压力 | 每个 TM 都很高 | 分散到各 TM，单个压力低 |

**结论：在多 TM 场景下，PartialCache 的按需加载使得每个 bucket 只被实际需要它的 TM 下载和缓存，避免了 FullCache 的全量冗余复制，集群总资源占用远低于 FullCache。**

> [!warning] 勘误：普通 Partial Cache 不保证“一个 Bucket 在集群内只缓存一份”
> 如果多个 Lookup Subtask 都收到属于同一 Bucket 的 Key，它们可以各自缓存相同文件。只有目标版本支持并启用了 Bucket-aware Shuffle，并确认相同 Bucket 被稳定路由到指定 Subtask，才能按文章的理想模型估算副本数。当前官方 Shuffle Lookup 还要求 Flink 2.0+、固定分桶、Join Key 包含完整 Bucket Key，并通过 `LOOKUP('shuffle'='true')` 显式开启。

## 选择 FullCache 的条件

| 条件 | 说明 |
| --- | --- |
| 数据量小（百万行以内） | 全量加载 RocksDB 速度快，启动慢问题不明显 |
| 查询 joinKey 分布广（无热点） | PartialCache 最终也会下载大部分文件，优势消失 |
| 内存充裕，`lookup.cache-rows` 设得足够大 | 数据全在 Caffeine 内存层，查询极快 |
| join 键 ≠ 主键 / 需要聚合语义 | 本身只能走 FullCache，无需纠结 |

## 坚持 PartialCache 的条件

| 条件 | 说明 |
| --- | --- |
| **查询 joinKey 集中**（热点明显） | 只有少数 bucket 被频繁访问，大部分文件无需下载 |
| 维表更新频繁 | FullCache 每次刷新要把 changelog apply 到 RocksDB，PartialCache 只需更新文件元数据 |
| 重启容忍度低 | PartialCache 启动秒级，FullCache 即使数据量小也需要全量写 RocksDB |

## 配置方式


```
-- 显式强制走 FullCache，并调大内存缓存行数
LEFT JOIN paimon_dim /*+ OPTIONS(
  'lookup.cache'      = 'FULL',
  'lookup.cache-rows' = '500000'   -- 尽量让数据留在 Caffeine 内存层
) */
FOR SYSTEM_TIME AS OF s.proc_time AS d
ON s.user_id = d.user_id;

```


## 5 .2 PartialCache vs FullCache 缓存方式对比

**两者的缓存方式完全不同：**

| 路径 | 缓存结构 | 说明 |
| --- | --- | --- |
| **FullCache** | Caffeine LRU + RocksDB | 两层缓存：Caffeine内存 LRU → 本地 RocksDB，存储完整数据行 |
| **PartialCache Local** | Caffeine + LookupFile | Caffeine 管理 LookupFile 生命周期，LookupFile 是轻量级 KV 索引文件（SORT 或 HASH 格式） |
| **PartialCache Remote** | 无本地缓存 | 每次查询通过 TCP 调用 Query Service |
**Caffeine 不是类似 RocksDB 的东西，它俩完全不是一个层级。**
- **Caffeine**​ = 纯内存的 Java 对象缓存（类似升级版 HashMap，带自动淘汰）
- **RocksDB**​ = 嵌入式 KV 数据库（数据落本地磁盘，有 WAL、LSM 树、持久化能力）

## FullCache 的两层缓存


```
查询 key
  ↓
Caffeine LRU（堆内，按 lookup.cache-rows 行数限制）
  ├── 命中 → 直接返回
  └── 未命中 → 穿透到 RocksDB（本地磁盘，存储完整数据行）
              └── 回填 Caffeine

```


使用`RocksDBValueState` /`RocksDBSetState` /`RocksDBListState` 管理数据。

## PartialCache Local 的缓存


```
查询 key
  ↓
lookupFileCache（Caffeine，管理 LookupFile 对象）
  ├── 命中 → 在 LookupFile 上点查（本地 KV 索引文件）
  └── 未命中 → 从远端下载 SST → 重新编码为 LookupFile → 放入 cache

```


LookupFile **不是 RocksDB**，而是 Paimon 自己实现的轻量级索引格式：

-`lookup.local-file-type=SORT`（默认）→ 有序 KV 索引 + BloomFilter
-`lookup.local-file-type=HASH` → 哈希 KV 索引

这些格式专为点查优化，比 RocksDB 更轻量。

> [!note] 批注：不要把 Data File、远端 Lookup File 和本地缓存文件混成一个对象
> * 不同版本可能从原始 Data File 构建本地索引，也可能直接复用表存储中的 Lookup File。文章用“SST”统称候选文件便于讲解，但 Paimon 数据文件可以是 Parquet、ORC 或 Avro，Lookup 文件是另一套点查结构。排查网络流量时要从实际日志确认下载的是数据文件还是已生成的 Lookup 文件。
> * 本地磁盘 LookupFile 比 HDFS 快 10~100 倍（省网络、省协议），与数据库查询同处毫秒级延迟；但数据库赢在实时性和共享能力，本地磁盘赢在吞吐量和零运维。选哪种方案不看"谁绝对更快"，看你的维表更新频率、查询分布和架构约束。
> * 热查延迟相近，但 FullCache 的"重"体现在全量加载、全量冗余、刷新写放大；PartialCache 的"轻"体现在按需加载、无写路径、无后台 Compaction——这些差异在维表大、TM 多、更新频繁时会直接决定作业能不能跑稳。

## 核心区别

- **FullCache**：把数据存在 RocksDB 里（通用 KV 数据库，功能强大但相对重）
- **PartialCache**：把数据存在专用的 LookupFile 索引文件里（轻量级，只为点查服务）

---

## 六、全部性能相关配置参数

## 6 .1 Flink 连接器参数（FlinkConnectorOptions）

| 参数 key | 默认值 | 说明 |
| --- | --- | --- |
|`lookup.cache` |`AUTO` |`AUTO` ：优先 Partial；`FULL`：强制全量 |
|`lookup.async` |`false` | 是否开启异步 lookup（减少主流阻塞） |
|`lookup.async-thread-number` |`16` | 异步线程数 |
|`lookup.bootstrap-parallelism` |`4` | 全量 bootstrap 时并行读 split 的线程数 |
|`lookup.refresh.async` |`false` | 增量刷新是否异步（全量缓存路径） |
|`lookup.refresh.async.pending-snapshot-count` |`5` | 异步刷新积压超过此阈值改为同步 |
|`lookup.refresh.time-periods-blacklist` | 无 | 禁止刷新的时间段，格式`yyyy-MM-dd HH:mm->yyyy-MM-dd HH:mm,...`（源码实现对 FullCache 和 PartialCache 均生效，参数描述中注释"仅 FullCache"与代码行为不符，以代码为准） |
|`lookup.dynamic-partition.refresh-interval` |`1h` |`scan.partitions`  动态分区探测间隔 |

## 6 .2 表级 Lookup Cache 参数（Table CoreOptions）

| 参数 key | 默认值 | 说明 |
| --- | --- | --- |
|`lookup.continuous.discovery-interval` | 无（回退到`continuous.discovery-interval`） | **Lookup 专用** 刷新间隔，通过 SQL Hint 设置，优先级高于表级参数 |
|`continuous.discovery-interval` |`10s` | 表级元数据刷新间隔，Lookup 优先使用上面的专用参数 |
|`lookup.local-file-type` |`SORT` | 本地索引文件格式：`SORT`（有序 SST）或`HASH`（哈希文件） |
|`lookup.hash-load-factor` |`0.75` | HASH 模式的负载因子 |
|`lookup.cache-file-retention` |`1h` | Partial 缓存本地文件保留时间 |
|`lookup.cache-max-disk-size` | 无限制 | 本地缓存文件最大总磁盘占用 |
|`lookup.cache-spill-compression` |`zstd` | 本地文件压缩格式 |
|`lookup.cache-max-memory-size` |`256mb` | 内存缓存上限 |
|`lookup.cache.bloom.filter.enabled` |`true` | 是否为本地文件生成 Bloom Filter |
|`lookup.cache.bloom.filter.fpp` |`0.05` | Bloom Filter 误判率 |

## 6 .3 RocksDB 参数（FullCache 路径专用）

| 参数 key | 默认值 | 说明 |
| --- | --- | --- |
|`lookup.cache-rows` |`10000` | RocksDB 上层 Caffeine LRU 缓存的最大行数 |
|`rocksdb.thread.num` |`2` | RocksDB 后台线程数 |
|`rocksdb.block.cache-size` |`128mb` | RocksDB Block Cache |
|`rocksdb.writebuffer.size` |`64mb` | Write Buffer |
|`rocksdb.compression.type` |`LZ4_COMPRESSION` | 压缩类型 |
|`rocksdb.use-bloom-filter` |`false` | RocksDB SST 的 Bloom Filter |

---

## 七、Query Service（多 Job 共享维表的最优方案）

## 7 .1 什么是 Query Service？

PartialCache Local 模式下，每个 Flink Job 的 Lookup 算子都在自己本地维护一份索引。当**多个 Job 共用同一张维表**时，会出现重复下载、重复维护的问题。

Query Service 将维表索引抽离成一个**独立的常驻 Flink Streaming Job**，所有 Lookup Join 算子通过网络查询它，共享一份数据：


```
                ┌─────────────────────────┐
                │   Query Service（独立）   │
                │  - 维护维表索引           │
                │  - 监听文件变化           │
                │  - 对外提供 TCP 查询接口  │
                └──────────┬──────────────┘
                           │ 网络查询
          ┌────────────────┼────────────────┐
          ↓                ↓                ↓
      Job A 的           Job B 的         Job C 的
    Lookup 算子        Lookup 算子      Lookup 算子

```


## 7 .2 什么情况下建议使用 Query Service？

| 场景 | 是否推荐 |
| --- | --- |
| **多个 Flink Job 共用同一张大维表** | 强烈推荐，节省资源 |
| 维表非常大，不想每个算子各自缓存 | 推荐，集中维护一份索引 |
| 对维表**实时性要求高** | 推荐，Server 端实时监听文件变化 |
| 希望 lookup 计算与主 Job 资源隔离 | 推荐 |
| 单个 Job 使用维表，规模适中 | 不推荐，用 PartialCache Local 更简单 |

## 7 .3 使用 Query Service 的前提条件

以下条件**必须同时满足**：

| 条件 | 说明 |
| --- | --- |
| 有主键的表 | Append-only 表不支持 |
| 固定分桶（`bucket > 0`） | 动态分桶不支持 |
| Join 键 = 主键 | 才能精确路由到对应 Server |
|`merge-engine = deduplicate` （默认） | 或开启`changelog-producer=lookup` /`deletion-vectors.enabled=true` |
| Flink Streaming 模式 | Batch 模式不支持 |

## 7 .4 如何启动 Query Service？

Query Service 是一个独立的 Flink Streaming Job，需要单独提交：


```
-- 方式一：Flink SQL Procedure（推荐）
CALL sys.query_service(`table` => 'mydb.dim_table', parallelism => 4);

```

```
# 方式二：命令行提交
flink run \
  -c org.apache.paimon.flink.action.FlinkActions \
  paimon-flink.jar \
  query-service \
  --warehouse hdfs://xxx/warehouse \
  --database mydb \
  --table dim_table \
  --parallelism 4

```


启动后，它会把自己的地址写入维表目录的服务注册文件：


```
{warehouse}/mydb/dim_table/service/service-primary-key-lookup

```


Lookup Join 算子启动时检测到此文件，**自动切换到 Remote 模式**，无需任何额外配置。

## 7 .5 Query Service 内部架构


```
QueryFileMonitor（parallelism=1）
  监听维表文件变化，按 (partition, bucket) % N 路由
        ↓
QueryExecutorOperator（parallelism=N）
  每个实例持有 LocalTableQuery（内存索引）
  启动 KvQueryServer（Netty TCP Server）监听查询请求
        ↓
QueryAddressRegister（parallelism=1, Sink）
  收集所有 Server 地址，写入 DFS 服务注册文件

```


## 7 .6 Query Service vs PartialCache Local 对比

| 维度 | Query Service（Remote） | PartialCache Local |
| --- | --- | --- |
| 数据位置 | 独立 Service 的 TaskManager | 主 Job 的 TaskManager 本地 |
| 多 Job 共享 | 是，一份数据服务多个 Job | 否，每个 Job 独立维护 |
| 网络开销 | 有（每次查询一次 TCP 往返） | 无（纯本地 IO） |
| 数据实时性 | 实时文件监听，更及时 | 轮询（默认 10s） |
| 资源隔离 | 与主 Job 完全分离 | 共享主 Job 资源 |
| 运维复杂度 | 高（需额外维护一个 Job） | 低（无需额外部署） |
| 适合场景 | 多 Job 共享大维表 | 单 Job，维表中等规模 |

## 7 .7 Query Service 是否是通用最优方案？

**结论：不考虑运维成本的话，Query Service 就是通用最优解。**

从纯性能角度，Query Service 完全覆盖了 PartialCache Local 的所有优点，还额外多出数据实时性更好、支持多 Job 共享等优势。唯一的代价是每次查询多一次 TCP 网络往返，但在大多数场景下这个延迟差异在几毫秒以内，完全可以接受。

Paimon 的路径选择逻辑也印证了这一点：`lookup.cache=AUTO` 时，代码会**优先检测 Query Service 是否存在**，存在就走 Remote，否则才降级到 Local——官方设计上同样认为 Remote 是更优路径。

**决策建议：**

- 能接受多维护一个 Job → 直接上 Query Service，不需要纠结用哪种缓存模式
- 希望架构尽量简单 → 单 Job 场景用 PartialCache Local 就足够，无需额外部署

> [!warning] 勘误：Query Service 不是不计运维成本后的通用最优解
> Remote 每次查询多一次 RPC，还可能受服务热点、网络抖动、服务并行度和注册信息状态影响。Local Partial 在单 Job、低延迟、本地缓存命中高时可能更快。官方只说明 Query Service 可被多个 Lookup 共享、可用时 Lookup 会优先使用，不等于官方承诺它在所有负载下性能最优。

---

## 八、监控与可观测性

## 8 .1 确认当前走哪条 Lookup 路径

**方法一：TaskManager 启动日志**

| 日志关键字 | 说明 |
| --- | --- |
|`LocalQueryExecutor` | PartialCache Local 路径 |
|`RemoteQueryExecutor` | PartialCache Remote（Query Service）路径 |
|`Cannot create PrimaryKeyPartialLookupTable because bucket mode isn't HASH_FIXED. Will create FullCacheLookupTable.` | 降级到 FullCache，需检查 bucket 配置 |
|`FullCacheLookupTable`  + 无上述警告 | join 键 ≠ 主键，正常走 FullCache |

**方法二：Flink Web UI**

进入对应 TaskManager → Metrics，搜索`lookup` 相关指标，可观察到缓存类型和命中情况。

## 8 .2 核心 Metrics 指标

## Paimon 侧 Metrics

| 指标名 | 含义 | 异常信号 |
| --- | --- | --- |
|`paimon.lookup.cache.hitCount` | 缓存命中次数 | - |
|`paimon.lookup.cache.missCount` | 缓存未命中次数 | miss 率持续 > 50% 说明缓存不够用 |
|`paimon.lookup.cache.loadCount` | LookupFile 构建（下载）次数 | 持续增长说明文件被频繁淘汰再重建 |
|`paimon.lookup.cache.evictionCount` | Caffeine 淘汰次数 | 频繁淘汰需扩大内存/磁盘配置 |
|`paimon.lookup.refreshCount` | 刷新触发次数 | 可验证刷新间隔是否符合预期 |

> [!warning] 勘误：这些 Paimon 指标名不是当前官方文档公开的稳定短名称
> 当前官方 Metrics 文档为 Local Partial Lookup 列出的是 `partialLookupCount` 和 `partialLookupRemoteAccessCount`。后者表示“某次 Lookup 至少有一个文件需要从表存储创建”，按调用次数计，不是对象存储 I/O 次数。文章中的 `hitCount`、`missCount` 等名称可能来自特定版本或内部 Cache，必须先在目标版本 Web UI/Reporter 中确认存在，再写告警。

## Flink 侧 Metrics

| 指标名 | 含义 | 排查用途 |
| --- | --- | --- |
|`numRecordsInPerSecond` （Lookup 算子） | 每秒处理记录数 | 下降说明 Lookup 成为瓶颈 |
|`backPressuredTimeMsPerSecond` | 每秒被反压时间 | > 200ms 需排查原因 |
|`idleTimeMsPerSecond` | 每秒空闲时间 | 偏高说明上游数据慢，非 Lookup 问题 |

## 8 .3 本地磁盘缓存积压排查


```
# 查看 Paimon Lookup 本地缓存文件目录（路径在 TaskManager 工作目录下）
# 默认位置：${taskmanager.tmp.dirs}/paimon-lookup/

# 检查磁盘占用是否超出预期
du -sh ${TM_TMP_DIR}/paimon-lookup/

# 若磁盘快满，检查以下配置是否生效
# lookup.cache-max-disk-size  （磁盘上限）
# lookup.cache-file-retention （文件保留时间）

```


**积压原因排查：**

| 现象 | 可能原因 | 解决方案 |
| --- | --- | --- |
| 磁盘持续增长，不释放 |`lookup.cache-file-retention`  过长 | 缩短保留时间 |
| 磁盘超过`lookup.cache-max-disk-size` 但未清理 | Caffeine evict 回调延迟 | 重启 TM 或降低`lookup.cache-max-disk-size` 触发强制清理 |
| 磁盘占用远超维表大小 | 多次 compaction 后旧 LookupFile 未及时失效 | 检查`DropFileCallback` 是否正常触发（升级 Paimon 版本） |

## 8 .4 端到端数据延迟监控建议


```
推荐监控链路：
  Paimon 写入时间戳（snapshot commit time）
      ↓
  Lookup 算子最后一次 refresh 时间
      ↓
  流数据 event time

告警规则：
  维表滞后 = 当前时间 - 最后一次成功 refresh 时间
  若滞后 > 2 × continuous.discovery-interval，触发告警

```


---

## 九、常见问题 Q&A

## Q1：Lookup Paimon 表缓存更新时间是多少？

缓存更新涉及两个不同维度的参数，需分开理解：

| 参数 | 默认值 | 作用 |
| --- | --- | --- |
|`lookup.continuous.discovery-interval` | 无（回退到下方） | **Lookup 专用** 刷新间隔，通过 SQL Hint 设置，优先级最高 |
|`continuous.discovery-interval` |`10s` | 表级元数据刷新间隔，Lookup 无专用参数时生效 |
|`lookup.cache-file-retention` |`1h` | PartialCache 本地 SST 文件保留时长 |
|`lookup.dynamic-partition.refresh-interval` |`1h` |`scan.partitions = max_pt()`  动态分区探测间隔 |

> **实践建议**：需要为 Lookup 单独设置刷新频率时，优先用 SQL Hint 中的`lookup.continuous.discovery-interval`避免影响同一张表的其他读写作业的`continuous.discovery-interval` 配置。

---

## Q2：既然元数据每 10s 刷新一次，为什么本地缓存文件还要保留 1 小时？

**两个参数管的是完全不同的事：**

-`continuous.discovery-interval = 10s`：每 10 秒向 Paimon 拉取一次最新 snapshot 的`DataFileMeta`（文件描述信息：位置、大小、min/max key），**全程无文件 IO**，极轻量。目的是让 Flink 感知到最新数据在哪里。
-`lookup.cache-file-retention = 1h`：PartialCache 模式下，当某条 key 被真正查询时，才会把对应的 SST 文件下载到本地磁盘。`1h` 是指超过 1 小时没被访问的本地文件才会被清理。目的是避免热点文件被反复重复下载。

**类比理解：**

| 动作 | 对应参数 |
| --- | --- |
| 图书馆每 10s 更新一次**书目索引**（哪些书在哪个书架） |`continuous.discovery-interval` |
| 你借走的书，**1 小时内没还才催还** |`lookup.cache-file-retention` |

更新书目索引 ≠ 把所有书重新搬一遍。

**结论：**

- **10s** 保证元数据视图足够新（知道最新数据在哪）
- **1h** 保证已下载的本地文件不被过早清理（避免重复下载，复用热点数据）

---

## Q3：`lookup.refresh.async.pending-snapshot-count` 的作用是什么？**

**前置背景**：该参数只在 **FullCache +`lookup.refresh.async=true`** 时生效。FullCache 刷新需要把新 snapshot 的 changelog apply 到本地 RocksDB，开启异步刷新后由后台线程执行，不阻塞主查询线程。

**参数含义**（默认值`5`）：后台异步刷新线程积压的待处理 snapshot 数量超过此阈值时，自动降级为同步刷新。

**为什么需要这个保护机制？**

正常情况下后台能跟上写入速度，积压可控。但若上游写入速度远快于刷新速度，积压持续增长会导致：

1. **数据严重滞后**：查询到的是很旧的数据
2. **资源持续膨胀**：积压的 snapshot 元数据堆积占用内存

**触发后行为：**


```
积压 ≤ 5  →  异步刷新（不阻塞查询，性能优先）
积压 > 5  →  同步刷新（阻塞查询，数据新鲜度优先）

```


**调优建议：**

| 场景 | 建议 |
| --- | --- |
| 维表更新频繁、写入速度快 | 适当调小（如`3`），更早触发同步保护 |
| 维表更新慢、查询延迟敏感 | 可适当调大（如`10`），减少同步阻塞频率 |
| 不希望查询被刷新阻塞 | 直接关闭`lookup.refresh.async`，使用同步刷新 |

> [!warning] 勘误：最后一行建议逻辑相反
> 关闭 `lookup.refresh.async` 就是让刷新同步执行，更可能阻塞查询，不会减少阻塞。若目标是降低阻塞，应先保留异步刷新，调整线程、积压阈值和单次刷新工作量，并对维表最大允许滞后设置告警。该参数是否只在 FullCache 生效也要按部署版本复核。

**一句话总结**：这是异步刷新的**熔断保护阀**，防止后台刷新跟不上写入速度时数据无限滞后。

---

## 十、总结

上篇我们集中解决一个问题：**如何把 Paimon 维表的 Lookup 用对、配好**。把全文核心结论浓缩如下，方便你在落地时快速回顾。

## 一句话记住核心

**性能的关键，在于让 Lookup 触发 PartialCache 路径，而不是默认走FullCache。**

三条路径的性能与适用关系：

| 路径 | 触发条件 | 适用场景 |
| --- | --- | --- |
| PartialCache Remote | Query Service 已启动 + join 键 = 主键 + 固定分桶 | 多 Job 共用同一张大维表（最优） |
| PartialCache Local | join 键 = 主键 + 固定分桶 | 单 Job，维表中等规模（推荐默认） |
| FullCache | 其他所有情况 | join 键 ≠ 主键、需聚合语义、或维表极小 |

## 落地的三步动作

1. **建表阶段**：设置`bucket = N`（N > 0 固定分桶），主键清晰，`merge-engine` 保持默认`deduplicate`——这是触发 PartialCache 的三个前置条件，缺一不可。
2. **作业阶段**：通过 SQL Hint 开启`lookup.cache=AUTO`、异步查询（`lookup.async=true`）和合理的刷新间隔，让运行期吞吐和数据新鲜度达到平衡。
3. **共享场景**：多个 Job 共用同一张大维表时，启动 Query Service，自动升级到 Remote 路径，用一份索引服务所有 Job，从根上消除重复加载的资源浪费。

## 几个容易被忽视的认知

- **PartialCache 比 FullCache 不止"省内存"，还"启动快、刷新轻、滞后短"**：因为它的 refresh 只拉文件元数据、无文件 IO，查询时才按需下载对应数据文件。
- **FullCache 不是不能用，而是要看场景**：维表极小、查询分布广、内存充裕时，FullCache 把数据全放 Caffeine 内存层反而更稳；但分区切换会触发全量 bootstrap，重启风险高。

> [!note] 批注：把总结转换成上线判断
> 不能只追求“触发 PartialCache”。先确认查询语义正确，再比较路径：Join Key 是否能唯一定位逻辑行；表的 Merge Engine 是否需要合并旧状态；维表新鲜度允许多少秒；重启时允许多久恢复；本地盘、网络和 Query Service 是否有容量。路径只是实现选择，这些约束才决定最终方案。

## 复核资料

- [Apache Paimon：Lookup Joins](https://paimon.apache.org/docs/master/flink/sql-lookup/)
- [Apache Paimon：Configurations](https://paimon.apache.org/docs/master/maintenance/configurations/)
- [Apache Paimon：Metrics](https://paimon.apache.org/docs/master/maintenance/metrics/)
- [Apache Paimon：Data Distribution](https://paimon.apache.org/docs/master/primary-key-table/data-distribution/)
- 下篇：[[04-Flink 实时维表性能优化（下）：用对 Paimon Lookup 的完整实践（文章批注版）]]
- 相关笔记：[[18-Paimon 系统表、监控指标与故障排查]]、[[13-Paimon 主键表建模与稳定性检查清单]]
