---
title: "Flink 实时维表性能优化（下）：用对 Paimon Lookup 的完整实践"
author: "胖泽的技术笔记"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s/lAgcMsBrX-O_HCDKjGdzGg"
published_at: "2026-10-10T18:27:18+08:00"
aliases:
  - Paimon Lookup Join 缓存实现与容错
tags:
  - Flink
  - Paimon
  - Lookup-Join
  - 实时维表
reviewed_at: "2026-10-13"
---

# Flink 实时维表性能优化（下）：用对 Paimon Lookup 的完整实践

> Source: wechat
> Author: 胖泽的技术笔记
> Original: https://mp.weixin.qq.com/s/lAgcMsBrX-O_HCDKjGdzGg

> [!info] 阅读方式与版本边界
> 正文保留原文章节、代码、表格、案例和结论；新增内容统一标为“批注”或“勘误”。源码目录、类名和内部状态结构没有跨版本兼容承诺。下文涉及 Query Service、Deletion Vector、动态分区和 Changelog 的结论，必须结合运行中的 Paimon/Flink 版本验证。

## 引言

这篇文章继续回答 Paimon Lookup 上线后的机制问题：

- 同样是 FullCache，为什么 join 键等于主键和不等主键时，存储结构和查询路径完全不同？
- PartialCache 凭什么启动快、内存低？LookupFile 到底是什么，又是怎么按需构建、被 Caffeine 管理生命周期的？
- Lookup Join 是"非对齐"的，那维表数据究竟滞后多少？能不能量化、能不能缓解？
- TaskManager 重启、Query Service 宕机、Compaction 触发后，缓存会经历什么？周期性反压的根因怎么定位？
- 多分区维表跨零点切换时，`max_pt()` /`max_two_pt()` 是怎么平滑过渡的？

正文依次分析 FullCache 的三个子类、PartialCache 的按需加载链路、数据新鲜度、异常恢复、Changelog、Compaction 和动态分区。

---

## 一、Paimon Lookup 代码结构（参考）


```

> [!warning] 版本边界：这棵源码目录只是一版实现快照
> 阅读源码时应在目标 Paimon JAR 对应的 Tag/Commit 上搜索类名。类被移动、重命名或职责被拆分后，不能因为当前仓库里找不到同一路径就判断功能消失；也不能把内部类当成配置契约。对使用者稳定的证据是 SQL、表选项、执行计划、日志和指标。
paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/lookup/
├── FileStoreLookupFunction.java        # 核心入口：协调 cache 模式选择与刷新
├── FullCacheLookupTable.java           # 全量缓存基类（RocksDB 存储）
├── PrimaryKeyLookupTable.java          # PK 表 + join 键 = PK → RocksDBValueState
├── SecondaryIndexLookupTable.java      # PK 表 + join 键 ≠ PK → 二级索引
├── NoPrimaryKeyLookupTable.java        # Append 表 → RocksDBListState
└── PrimaryKeyPartialLookupTable.java   # Partial 缓存（Local 或 Remote）

paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/service/
├── QueryService.java                   # Query Service 构建入口
├── QueryFileMonitor.java               # 监听文件变化
├── QueryExecutorOperator.java          # 持有索引 + 启动 Netty Server
└── QueryAddressRegister.java           # 注册 Server 地址到 DFS

paimon-core/src/main/java/org/apache/paimon/mergetree/
├── LookupLevels.java                   # Partial 缓存核心：按 key 在 LSM 各层查
└── LookupFile.java                     # 本地缓存文件（Caffeine 管理生命周期）

```


---

## 二、FullCache 三个子类详解

## 2 .1 继承关系


```
FullCacheLookupTable（抽象基类）
  ├── PrimaryKeyLookupTable          → join 键 = 主键
  │     └── SecondaryIndexLookupTable → join 键 ≠ 主键（继承自上者）
  └── NoPrimaryKeyLookupTable        → 无主键 append-only 表

```


三个子类都使用**两层缓存**：Caffeine LRU（堆内）+ RocksDB（磁盘），但底层存储结构和查询逻辑各不相同。

## 2 .2 存储结构对比

| 子类 | RocksDB State 类型 | RocksDB 存储格式 | 适用场景 |
| --- | --- | --- | --- |
| PrimaryKeyLookupTable |`RocksDBValueState` |`primaryKey → row` | join 键 = 主键 |
| SecondaryIndexLookupTable |`RocksDBValueState`  +`RocksDBSetState` |`primaryKey → row`  +`joinKey → Set<primaryKey>` | join 键 ≠ 主键 |
| NoPrimaryKeyLookupTable |`RocksDBListState` |`joinKey → List<row>` | 无主键 append 表 |

## 2 .3 两层缓存的核心设计

所有子类共用`RocksDBState` 基类的两层缓存逻辑：


```
查询 key
  ↓
Caffeine LRU（堆内，softValues，按行数限制）
  ├── 命中 → 直接返回（不访问 RocksDB）
  └── 未命中 → 穿透到 RocksDB
              ├── 找到 → 回填 Caffeine，返回结果
              └── 未找到 → 回填 null 标记（防止重复穿透），返回空

```


关键设计：**null 也会被缓存**（`Reference(null)` 标记），下次查同一个不存在的 key 时直接命中 Caffeine 返回空，不再穿透 RocksDB，避免缓存穿透问题。

> [!note] 批注：负缓存会换来“短时间看不到刚写入值”的风险吗
> 只有刷新逻辑在新记录到达时同步更新或失效负缓存，结果才会变新。文章随后写到 `put` 会更新 Caffeine、`delete` 会写入空标记，这正是正确性的另一半。排查“数据已写入但 Lookup 仍为空”时，要同时检查 Snapshot 是否提交、刷新是否完成，以及该 Key 的负缓存是否被正确更新，不能只看 RocksDB 中有没有数据。

## 2 .4 PrimaryKeyLookupTable Lookup 流程

结构最简单，一次 RocksDB 点查即可：


```
查询 joinKey（= primaryKey）
  ↓
keyRearrange（若 joinKey 字段顺序与表主键定义顺序不同，先重排）
  ↓
tableState.get(primaryKey)
  ├── Caffeine 命中 → 返回 row（至多 1 条）
  └── Caffeine 未命中 → RocksDB point get → 回填 Caffeine → 返回

```


**刷新时（refresh）：**

-`INSERT / UPDATE_AFTER` →`put(pk, newRow)`，同步更新 Caffeine
-`DELETE / UPDATE_BEFORE` →`delete(pk)`，Caffeine 中标记为`ref(null)`（非 invalidate，防穿透）

## 2 .5 SecondaryIndexLookupTable Lookup 流程

需要两次查找，用两个 RocksDB ColumnFamily 维护双索引：


```
查询 joinKey（≠ primaryKey）
  ↓
第一步：indexState.get(joinKey)
  ├── Caffeine 命中 → 得到 List<primaryKey>
  └── 未命中 → RocksDB 前缀扫描（joinKey 前缀）→ 回填 Caffeine
  ↓
第二步：for each primaryKey:
    tableState.get(primaryKey)
    ├── Caffeine 命中 → 得到 row
    └── 未命中 → RocksDB point get → 回填 Caffeine
  ↓
返回 List<row>（可能多条）

```


`RocksDBSetState` 存储格式巧妙：把`joinKey + primaryKey` 拼接作为 RocksDB 的 key，value 为空。利用 RocksDB 字典序排列特性，同一 joinKey 前缀的所有 primaryKey 在磁盘上物理相邻，一次前缀扫描即可批量读出整个 Set。

**刷新时最复杂**，UPDATE 可能导致 joinKey 字段本身的值发生变化，必须同时维护两个索引的一致性：


```
收到 UPDATE_AFTER（joinKey 从 "A" 变为 "B"）：
  1. 查出旧行（旧 joinKey = "A"）
  2. indexState.retract("A", pk)   ← 先撤销旧的二级索引
  3. tableState.put(pk, newRow)    ← 更新数据行
  4. indexState.add("B", pk)       ← 建立新的二级索引

```

> [!warning] 批注：只有 `UPDATE_AFTER` 本身未必知道旧 Join Key
> 要撤销 `A → pk`，刷新端必须能拿到旧行：来自完整的 `UPDATE_BEFORE`、本地 `primaryKey → oldRow` 状态，或其他可重建旧值的路径。如果输入只有新值且本地旧状态不完整，二级索引会残留。文章的四步流程正确描述了目标状态，但生产验证还要确认“旧行从哪里来”。


## 2 .6 NoPrimaryKeyLookupTable Lookup 流程

无主键，同一 joinKey 对应多行，全部以 List 形式存储：


```
查询 joinKey
  ↓
state.get(joinKey)
  ├── Caffeine 命中 → 返回 List<row>（可能很多条）
  └── 未命中 → RocksDB get → ListDelimitedSerializer 反序列化 → 回填 Caffeine → 返回

```


**刷新时只支持 INSERT / UPDATE\_AFTER**，收到 DELETE/UPDATE\_BEFORE 直接抛异常（append-only 表不支持删除语义）。每次追加用 RocksDB 的`merge()` 原子操作，不需要先读旧值再写，写入性能高。

注意：Caffeine invalidate 后，下次查询需要把该 joinKey 下**所有行**从 RocksDB 重新读出反序列化，joinKey 对应行数多时代价较高。

> [!note] 批注：无主键维表最怕低基数 Join Key
> 如果按 `city`、`category` 这类低基数字段关联，一个 Key 可能对应大量行；单次反序列化、返回结果膨胀和下游 Join 放大都会成为瓶颈。此时问题不只是缓存大小，还要确认业务上是否真的允许一条事实记录匹配多条维度记录。

## 2 .7 三个子类综合对比

| 维度 | PrimaryKeyLookupTable | SecondaryIndexLookupTable | NoPrimaryKeyLookupTable |
| --- | --- | --- | --- |
| 查询次数 | 1 次点查 | 1 次前缀扫 + N 次点查 | 1 次 get |
| 返回行数 | 最多 1 条 | N 条 | N 条 |
| RocksDB CF 数量 | 1 个 | 2 个（各分一半 LRU 缓存） | 1 个 |
| UPDATE 处理 | 直接 put 覆盖 | 需撤销旧索引再建新索引 | 不支持 DELETE |
| null 穿透防护 | 有（`ref(null)`） | 有 | 有（emptyList 缓存） |
| 缓存更新方式 | 精确回填/标记 | add/retract 后 invalidate | merge 后 invalidate |
| 启动开销 | 全量写 1 个 CF | 全量写 2 个 CF | 全量写 1 个 CF（需聚合同 joinKey 的行） |

---

## 三、PartialCache 按需加载机制详解

## 3 .1 核心设计：元数据全量，文件按需

PartialCache 严格区分两个阶段：

- **refresh() 阶段**：只拉取文件元数据（DataFileMeta），完全没有文件 IO
- **get() 查询阶段**：第一次访问某个文件时才真正下载并构建本地索引

> [!warning] 表述边界：“没有文件 IO”应改为“不读取完整数据文件内容”
> 获取 Snapshot、Manifest、DataFileMeta 本身仍可能访问远端元数据文件或 Catalog，只是数据量通常远小于扫描数据文件。判断刷新成本时应观察 Manifest 数量、对象存储请求和 Scan Planning 耗时，不能把元数据读取当成零 I/O。

## 3 .2 refresh() 只做了什么？


```
refresh()
  ↓
StreamTableScan.plan()  ← 从 snapshot 拉取增量变化（纯元数据，极轻量）
  ↓
对每个变化的 split：
  before = [旧文件元数据列表]
  after  = [新文件元数据列表]
  ↓
Levels.update(before, after)
  ├── 把新的 DataFileMeta 加入内存层级结构
  ├── 把旧的 DataFileMeta 从内存中移除
  └── 对被删除的文件触发 DropFileCallback
        → lookupFileCache.invalidate(fileName)
        → 删除对应本地索引文件（如果存在）

```


`DataFileMeta` 只是描述文件位置、大小、min/max key 等信息的 Java 对象，**不包含文件内容**，整个 refresh 过程无任何网络 IO。

## 3 .3 get() 查询时的完整流程


```
get(key)
  ↓
① 根据 hash(key) % bucket数，定位 bucket
  ↓
② LookupLevels 按层级 + key range 定位候选 SST 文件（纯内存）
   L0：遍历所有文件，找 minKey ≤ key ≤ maxKey 的文件（文件间可重叠）
   L1+：二分查找定位唯一候选文件（文件间无重叠）
  ↓
③ lookupFileCache.getIfPresent(fileName)   ← Caffeine 查询
   ├── 命中 → 直接在本地索引文件上点查（本地磁盘 IO）
   └── 未命中 → createLookupFile()  ← 首次访问才触发
                  │
                  ├── 从远端（HDFS/OSS）读取整个 SST 内容（网络 IO）
                  ├── 重新编码为本地 KV 索引格式（含 BloomFilter）
                  ├── 写入 TaskManager 本地磁盘
                  └── 放入 Caffeine Cache
  ↓
④ 返回 value，若是 DELETE 标记则返回 null

```

> [!note] 批注：一次 Key 查询可能命中多个物理版本
> L0 文件范围可以重叠；不同 Level/Sorted Run 也可能保存同一主键的多个版本。`LookupLevels` 的职责不只是“找到一个文件”，还要按 Sequence Number、RowKind 和 Merge Engine 合并候选记录，得到一个逻辑结果。固定 Bucket 只缩小候选范围，不代表一个 Key 永远只读一个文件。


## 3 .4 LookupFile 是什么？

LookupFile **不是**原始 SST 文件，而是重新构建的**本地 KV 索引文件**：

| 维度 | 原始 SST | LookupFile（本地索引） |
| --- | --- | --- |
| 位置 | 远端 HDFS/OSS | TaskManager 本地磁盘 |
| 格式 | 列存格式（ORC/Parquet） | KV 索引格式（SORT 或 HASH） |
| 内容 | 完整行数据（所有列） | key → (value + sequenceNumber + rowKind) 紧凑编码 |
| 访问方式 | 顺序/列式扫描 | 随机 key 点查 |
| 生命周期 | 受 snapshot 管理 | 受 Caffeine 管理（超时/超量自动删除） |

每个 SST 文件只在**第一次被查询命中时**才构建对应的 LookupFile，之后复用。

> [!warning] 版本边界：原始文件不一定是“SST/ORC/Parquet → 本地重编码”这一条路径
> Paimon 的数据文件格式与 Lookup File 是两套概念；部分版本或表模式会在表存储中生成可远程复用的 Lookup 文件。本地未命中时可能下载既有 Lookup File，也可能读取数据文件后构建。文章的流程用于理解“按需付出成本”，不要据此硬编码文件后缀、目录或网络字节估算。

## 3 .5 Caffeine 管理 LookupFile 的生命周期


```
LookupFile 被创建
  → 放入 Caffeine Cache（key = SST 文件名）
  → 按以下规则自动 evict：

  ┌─ 超过空闲时间（lookup.cache-file-retention，默认 1h）
  ├─ 超过磁盘总量限制（lookup.cache-max-disk-size）
  └─ SST 被 compaction 合并删除（DropFileCallback 触发 invalidate）

evict 时执行：
  1. 关闭 LookupStoreReader（释放文件句柄）
  2. 删除本地磁盘上的索引文件

```


compaction 后旧 SST 被替换，`DropFileCallback` 会立即 invalidate 对应 LookupFile，**保证不会查到过期数据**。

> [!warning] “立即”是进程内回调视角，不是端到端可见性保证
> Lookup 算子先要发现包含 Compaction 结果的新 Snapshot，之后才更新文件集合并触发旧文件失效。发现周期内仍可能使用旧视图；正确性依赖 Snapshot 切换和版本合并规则，而不是文件一生成就通知所有 Job。这里应区分“旧 Snapshot 的一致结果”和“最新业务值已经可见”。

## 3 .6 全流程总结图


```
维表文件（远端 HDFS/OSS）
  SST-1  SST-2  SST-3  SST-4  SST-5  SST-6
    ↑      ↑                    ↑
    |      |                    |
    |      | 被查询到，构建本地索引 |
    |      |                    |
  LookupFile-1  LookupFile-2   （SST-5 从未被查，不下载）
  （本地磁盘）   （本地磁盘）

refresh() 维护的：所有 SST 的 DataFileMeta（纯内存，极轻量）
get() 触发的：只有被查到的 SST 才构建 LookupFile

```


**PartialCache 启动快的根本原因**：refresh 只拉元数据，无文件 IO。**内存/磁盘占用低的根本原因**：LookupFile 按需构建，从未被查到的 SST 永远不会被下载。

---

## 四、数据一致性与延迟语义

## 4 .1 Lookup Join 的非对齐本质

Flink Lookup Join 是**非对齐 Join**：流数据到达时，用的是"当前时刻"的维表快照，而非与流数据在业务时间上严格对齐的维表状态。


```
业务时间线：
  t=10  用户 123 的 city 从"上海"改为"北京"（写入 Paimon）
  t=12  流数据 [user_id=123, action=buy] 到达 Flink

理想（对齐）：查到 city=北京
实际（非对齐）：
  ├── 若维表尚未刷新 → 查到 city=上海（旧值）
  └── 若维表已刷新   → 查到 city=北京（新值）

```


**这不是 Bug，是 Lookup Join 的设计取舍**：用低延迟、低成本换取强一致性的放弃。

> [!note] 批注：Processing-time Lookup 与 Event-time Temporal Join 的问题不同
> `FOR SYSTEM_TIME AS OF proc_time` 查询的是处理时刻可见的维表状态，不会按事实记录的 Event Time 回到历史维度版本。即使刷新零延迟，晚到事件也可能关联到“现在的维度值”。如果业务要求订单发生时的用户等级，需要保存版本有效期并按 Event Time 关联，不能只缩短发现周期。

## 4 .2 维表滞后的量化

滞后时间 = 上游写入 Paimon 的时间 → Flink Lookup 感知到新数据的时间

| 路径 | 滞后来源 | 典型滞后 |
| --- | --- | --- |
| FullCache |`continuous.discovery-interval`  + RocksDB apply 时间 | 10s ~ 数分钟（取决于数据量） |
| PartialCache Local |`continuous.discovery-interval` （只更新元数据） | 10s ~ 30s |
| PartialCache Remote（Query Service） | Query Service 实时监听文件变化 | 秒级（接近实时） |

> [!warning] 这些时间是作者经验区间，不是产品 SLA
> 实际滞后至少包含写入 Checkpoint、Snapshot Commit、Lookup 发现、刷新处理和下一次查询。Full/Local/Remote 的大小关系也会受维表大小、服务负载和网络影响。上线时应记录 `业务变更时间 → snapshot commit_time → Lookup refresh 完成时间 → 首次命中新值时间`，用 P95/P99 而不是表中的固定秒数定告警。

PartialCache 的滞后比 FullCache **更短**，原因：

- FullCache 刷新 = 拉元数据 + 把 changelog apply 到 RocksDB（有写入开销）
- PartialCache 刷新 = 只拉元数据（极轻量），下次查询时自动读最新文件

## 4 .3 刷新黑名单期间的查询行为

`lookup.refresh.time-periods-blacklist` 配置后：

- 黑名单时段内，**不触发新的刷新**（元数据不更新）
- 查询照常进行，返回的是**黑名单开始前最后一次刷新的数据**（旧快照）
- 不会报错，也不会阻塞
- 黑名单结束后，自动恢复正常刷新

**时间格式**：`yyyy-MM-dd HH:mm->yyyy-MM-dd HH:mm`（秒位固定补`:00`，多段用逗号分隔）


```
-- 正确格式示例
'lookup.refresh.time-periods-blacklist' = '2026-07-05 02:00->2026-07-05 04:00'
-- 多段
'lookup.refresh.time-periods-blacklist' = '2026-07-05 02:00->2026-07-05 04:00,2026-07-05 14:00->2026-07-05 15:00'

```

> **注意**：参数描述中注释"Only used when lookup table is FULL cache mode"，但源码实现（`FileStoreLookupFunction.tryRefresh()`）对 FullCache 和 PartialCache 均先检查黑名单，以代码行为为准——**两种路径均受黑名单控制**。

**适用场景**：凌晨大批量写入 Paimon 期间，避免频繁刷新导致抖动，用旧数据换稳定性。

> [!warning] 使用黑名单前必须定义允许返回旧值的最长时间
> 黑名单不是“避开抖动但语义不变”，而是主动冻结维表视图。若订单风控、价格、库存等字段不允许旧值，就不应靠黑名单解决性能问题。并且当前官方配置说明把该参数限定在 Full Cache；文章所述 Partial 也生效是特定源码版本观察，需要在部署版本验证。

## 4 .4 PartialCache vs FullCache 数据新鲜度的本质差异

| 维度 | FullCache | PartialCache |
| --- | --- | --- |
| 感知粒度 | **行级别** ：通过 changelog 逐行 apply 变更 | **文件级别** ：感知到文件元数据变化即可 |
| 刷新代价 | 高（需写入 RocksDB） | 极低（只更新内存中的 DataFileMeta） |
| 数据新鲜度 | 刷新后立即可查到最新行 | 刷新后下次查询时按需读最新文件 |
| 删除语义 | 支持（DELETE changelog 会清除 RocksDB 中的行） | 支持（通过 rowKind 标记过滤） |

**结论**：两者在稳态下数据新鲜度相近（都受`continuous.discovery-interval` 控制），但 PartialCache 的刷新代价更小，在写入频繁时滞后反而更短。

## 4 .5 如何减少维表滞后的业务影响

| 方案 | 适用场景 |
| --- | --- |
| 缩短`continuous.discovery-interval`（如`3s`） | 对新鲜度要求高，且刷新开销可接受 |
| 使用 Query Service（Remote 路径） | 多 Job 共用维表，要求秒级新鲜度 |
| 业务侧容忍设计：对延迟敏感字段走实时流而非维表 | 维表只补充低频变化的属性（如用户等级、城市） |
| 将高频变化字段从维表拆出，改用双流 Join | 从根本上消除非对齐问题，但资源开销更大 |

---

## 五、异常容错与重启行为

## 5 .1 TaskManager 重启后的缓存恢复

Lookup 缓存**不持久化**，TaskManager 重启后缓存全部丢失，需要重新初始化：

> [!note] 批注：“重启”要区分 JVM、容器和节点
> Caffeine 对象和打开的 Reader 一定会丢失；本地磁盘文件是否还在，取决于 TaskManager 临时目录、Pod/节点迁移和清理策略。即使文件残留，新进程也未必直接复用。容量评估应按“缓存可随 Task 丢失”设计，但排障时不能只凭目录还在就认定热缓存已恢复。

| 路径 | 重启后行为 | 恢复耗时 |
| --- | --- | --- |
| **FullCache** | 重新全量 bootstrap：读取最新 snapshot，全量写入 RocksDB | 分钟级（取决于维表大小） |
| **PartialCache Local** | 只重新拉取文件元数据，本地 LookupFile 全部丢失，后续查询按需重建 | 秒级启动，查询预热期有额外延迟 |
| **PartialCache Remote** | 主 Job 重启不影响 Query Service，重连后立即可查 | 秒级（仅重连开销） |

**FullCache 重启的风险**：


```
维表 1 亿行，bootstrap 期间：
  ├── 主流数据堆积在 Source（Kafka offset 不推进）
  ├── checkpoint 无法完成（Lookup 算子未 ready）
  └── 若 bootstrap 超过 checkpoint 超时时间 → 作业反复重启

```


**缓解方案**：


```
-- 增大 bootstrap 并行度，加速全量加载
'lookup.bootstrap-parallelism' = '16'

-- 或直接改用 PartialCache，彻底规避重启慢问题
'lookup.cache' = 'AUTO'   -- 配合固定分桶自动走 PartialCache

```


## 5 .2 Lookup 算子 Backpressure 根因分析

Lookup 算子出现 backpressure 时，按以下顺序排查：

## 原因一：网络 IO（PartialCache 首次加载文件）


```
症状：作业启动初期或冷 key 突增时出现短暂 backpressure
根因：PartialCache 首次访问某个 SST 文件时需要从 HDFS/OSS 下载
排查：观察 TaskManager 网络带宽是否在启动/预热阶段飙升
解决：
  ├── 预热期适当降低流量（上游限速）
  └── 开启 lookup.async=true，异步 IO 不阻塞主线程

```


## 原因二：RocksDB GC / Compaction（FullCache）


```
症状：运行一段时间后周期性 backpressure，伴随 CPU 升高
根因：RocksDB 后台 compaction 与查询竞争 IO
排查：观察 TaskManager CPU 和磁盘 IO 是否周期性升高
解决：
  ├── 增大 rocksdb.thread.num（后台线程数）
  ├── 增大 rocksdb.block.cache-size（减少磁盘读）
  └── 考虑切换到 PartialCache，彻底避免 RocksDB

```


## 原因三：Caffeine 大量淘汰（PartialCache）


```
症状：查询 key 分布宽，缓存频繁 miss，大量文件反复下载
根因：lookup.cache-max-memory-size 设置过小，LookupFile 频繁被淘汰
排查：观察 TaskManager 磁盘 IO 是否持续偏高（而非只在启动时）
解决：
  ├── 增大 lookup.cache-max-memory-size（如 1gb）
  └── 增大 lookup.cache-max-disk-size

```


## 原因四：同步刷新阻塞（FullCache + lookup.refresh.async=false）


```
症状：每隔 continuous.discovery-interval 出现规律性短暂卡顿
根因：同步刷新期间，主线程等待 changelog apply 到 RocksDB 完成
排查：backpressure 间隔与 continuous.discovery-interval 一致
解决：开启 lookup.refresh.async=true

```


## 5 .3 Query Service 宕机后的降级行为

Query Service 是独立 Flink Job，宕机后主 Job 的行为：


```
Query Service 宕机
  ↓
主 Job Lookup 算子发起 TCP 查询 → 连接失败
  ↓
抛出异常 → 触发 Flink 算子级重启（取决于重启策略）
  ↓
重启后重新读取服务注册文件
  ├── Query Service 已恢复 → 重连，继续走 Remote 路径
  └── Query Service 未恢复 → 注册文件不存在 → 降级到 PartialCache Local

```

> [!warning] 勘误：不要把“自动降级到 Local”当成已保证的容错契约
> 官方文档只说明 Query Service 可用时 Lookup 优先使用它，没有承诺运行中的 Remote 请求失败会无损切 Local。服务注册文件可能在服务宕机后仍存在，客户端重启也可能继续尝试失效地址；切换路径还可能需要重新创建 LookupFunction。生产方案应按“Query Service 故障会导致 Lookup 失败并触发作业恢复”设计，除非已在目标版本做过故障注入并确认降级行为。


**注意**：降级到 Local 路径后，需要重新初始化本地索引（秒级），期间有短暂延迟。

**生产建议**：


```
├── 为 Query Service 配置自动重启策略（fixed-delay 或 exponential-delay）
├── 监控 Query Service Job 状态，异常时及时告警
└── 评估业务对短暂降级的容忍度，必要时预留 Local 路径的磁盘资源

```

> [!note] 批注：故障演练至少覆盖三种状态
> 仅停止 Query Service Job、停止后保留注册文件、停止后删除注册文件。分别观察主 Job 是继续重试、失败重启还是重新选择 Local，并记录恢复时间和是否出现重复/漏关联。没有这组证据，不应把 Local 资源预留等同于已经具备自动降级能力。


---

## 六、Changelog Producer 与 Lookup 的关系

## 6 .1 为什么 FullCache 需要 changelog-producer

FullCache 的刷新机制是**增量 apply changelog**：每次刷新时，读取上次 snapshot 到最新 snapshot 之间的变更记录（INSERT / UPDATE\_BEFORE / UPDATE\_AFTER / DELETE），逐行写入 RocksDB。

这要求 Paimon 表必须能**产出完整的行级 changelog**。不同`changelog-producer` 配置的能力如下：

| changelog-producer | 能否产出 DELETE / UPDATE\_BEFORE | FullCache 可用性 |
| --- | --- | --- |
|`none` （默认） | 否（只有 INSERT） | **有风险** ：删除/更新的旧值无法同步到 RocksDB，数据可能永久错误 |
|`input` | 取决于上游是否携带 | 部分可用，依赖上游 |
|`lookup` | 是（Paimon 自动补全） | **推荐** ：完整 changelog，刷新准确 |
|`full-compaction` | 是（compaction 时产出） | 可用，但 changelog 有延迟（compaction 触发才产出） |

> [!warning] 版本边界：`none` 不等于物理文件里“只有 INSERT”
> `changelog-producer` 控制 Paimon 向流式消费者提供怎样的变更日志，不改变主键表内部仍可能保存更新和删除语义。某些版本的 Full Cache 可通过 Snapshot 差异重建变化，而不是完全依赖专用 Changelog 文件。正确判断是：目标版本的 Full Cache 刷新能否从当前表配置取得完整的插入、更新和删除，而不是看到 `none` 就直接判定永久错误。

## 6 .2 changelog-producer=lookup 的工作原理

`lookup` 模式下，Paimon 写入时会**主动查询当前值**来补全 changelog：


```
写入 UPDATE user_id=123, city=北京
  ↓
Paimon 先查当前值：city=上海
  ↓
补全产出：
  UPDATE_BEFORE: [123, 上海]
  UPDATE_AFTER:  [123, 北京]

```


这样 FullCache 刷新时就能收到完整的行级变更，准确维护 RocksDB 状态。

**代价**：每次写入多一次读操作，写入吞吐略有下降（通常 10%~20%）。

> [!warning] “10%～20%”没有跨环境证据
> Lookup Compaction 会读旧值并维护本地/远端缓存，实际影响取决于更新比例、缓存命中、本地盘、对象存储、Checkpoint 和 Compaction 并行度。这个数字只能保留为作者经验，容量规划必须用自己的写入回放测试。

## 6 .3 PartialCache 是否依赖 changelog-producer

**PartialCache 不依赖 changelog**，原因在于它的刷新机制完全不同：


```
PartialCache 刷新：只感知文件元数据变化（DataFileMeta 增删）
查询时：直接读最新 SST 文件内容，文件本身已包含最终状态

```


因此 PartialCache **对`changelog-producer` 没有要求**，即使设为`none` 也能正确查到最新数据。

## 6 .4 deletion-vectors.enabled 在 Lookup 场景下的作用

`deletion-vectors.enabled=true` 是 Paimon 的一种优化模式：用**删除向量文件**标记被删除的行，而非通过 compaction 物理合并来消除旧版本。

| 维度 | 未开启 deletion-vectors | 开启 deletion-vectors |
| --- | --- | --- |
| 删除/更新的处理 | 写入新版本，compaction 时合并消除旧版本 | 写入删除向量文件标记旧行无效 |
| compaction 频率 | 较高（需要合并旧版本） | 较低（只需合并删除向量） |
| Lookup 查询 | 可能读到多版本，需 merge | 读时过滤删除向量，性能更好 |
| FullCache 支持 | 需配合 changelog-producer | **可替代 changelog-producer** ：满足 FullCache 刷新需要的完整语义 |

**结论**：`deletion-vectors.enabled=true` 是`changelog-producer=lookup` 的现代替代方案，写入性能更好，且同样满足 FullCache 的刷新语义要求。Paimon 0.8+ 推荐使用。

> [!warning] 勘误：Deletion Vector 不能笼统地视为 Changelog Producer 的替代品
> Deletion Vector 是表的物理存储/读取模式，用位图标记旧文件中的失效行；Changelog Producer 决定流式消费者能收到哪些行级变化。二者解决的问题不同，可以存在兼容约束，但不能推出“开启 DV 就一定向 FullCache 提供完整 `UPDATE_BEFORE/DELETE`”。应分别验证表读取正确性和 Full Cache 刷新输入。官方也没有把 DV 描述为所有 Lookup 场景的通用替代方案。


```
-- 推荐配置（二选一即可）
-- 方案一：老版本兼容
'changelog-producer' = 'lookup'

-- 方案二：Paimon 0.8+ 推荐
'deletion-vectors.enabled' = 'true'

```

> [!note] 批注：二选一代码是文章观点，不是通用配置模板
> 选择 DV 还要检查 Merge Engine、表模式、Compaction、目标引擎版本和 Time Travel 行为；选择 `changelog-producer=lookup` 则要预算写入侧 Lookup Compaction 成本。不能只按“Paimon 0.8+”这一条条件切换。


---

## 七、Compaction 与 Lookup 的交互

## 7 .1 Compaction 对 PartialCache 的影响

Compaction 会合并多个 SST 文件生成新文件，同时删除旧文件。对 PartialCache 的影响链路：


```
Compaction 触发
  ↓
旧 SST 文件被删除，新 SST 文件生成
  ↓
PartialCache refresh() 感知到文件元数据变化
  ├── 新文件的 DataFileMeta 加入内存层级结构
  └── 旧文件触发 DropFileCallback
        → lookupFileCache.invalidate(旧文件名)
        → 对应本地 LookupFile 被删除
  ↓
下次查询命中被 compaction 覆盖的 key
  → 需重新下载新 SST 文件，重建 LookupFile

```


**结论**：Compaction 会造成 PartialCache 的**局部缓存失效**，下次访问受影响 key 时有一次重新下载的开销。

> [!note] 批注：抖动大小由“失效工作集”决定，不由 Compaction 是否发生单独决定
> 如果 Compaction 只重写冷 Key 文件，业务几乎感知不到；如果一次重写覆盖大部分热点范围，随后的并发查询会集中重建。应对齐 Compaction 输出文件、`partialLookupRemoteAccessCount`、对象存储流量和 Lookup P99，确认是否真是缓存重建导致的反压。

## 7 .2 Compaction 引发的 Lookup 抖动

**现象**：运行稳定的作业，周期性出现短暂延迟升高或 backpressure。

**根因**：

| 场景 | 抖动原因 |
| --- | --- |
| PartialCache | 大规模 compaction 后大量 LookupFile 失效，短时间内集中重建（下载 + 构建索引） |
| FullCache | compaction 后 changelog 量突增，RocksDB apply 时间变长，同步刷新时阻塞时间更长 |

**抖动时序图（PartialCache）**：


```
时间轴：
  t=0    稳定运行，LookupFile 热缓存命中率高
  t=10   Compaction 触发，大量旧 SST 失效
  t=11   大量 LookupFile 被 invalidate
  t=12   查询集中 miss，触发大量文件下载
  t=12~15  网络 IO 飙升，延迟升高（抖动窗口）
  t=15   新 LookupFile 建立完成，恢复正常

```


## 7 .3 缓解 Compaction 抖动的方案

**方案一：错开 Compaction 时间窗口**


```
-- 在业务低峰期触发 compaction（如凌晨）
-- 配合刷新黑名单，低峰期允许 compaction，高峰期抑制
'lookup.refresh.time-periods-blacklist' = '09:00->21:00'  -- 仅示例

```

> [!warning] 勘误：刷新黑名单不会控制 Compaction 何时执行
> 它只会阻止 Lookup 刷新；高峰期 Compaction 仍可能继续，只是 Lookup 暂时看不到新文件。真正错开 Compaction 要调整 Writer/独立 Compaction Job 的调度。用黑名单覆盖整个业务高峰还会让维表长期返回旧值，必须先确认业务容忍度。


**方案二：控制 Compaction 粒度**


```
-- 调大触发 compaction 的文件数阈值，减少 compaction 频率
'num-sorted-run.compaction-trigger' = '10'   -- 默认 5，调大后 compaction 更少但每次更大
'compaction.max-file-num'            = '50'  -- 单次 compaction 合并文件数上限

```


**方案三：开启异步 Lookup（最直接）**


```
-- 异步 IO 模式下，文件下载不阻塞主线程，抖动对吞吐影响最小
'lookup.async'             = 'true'
'lookup.async-thread-number' = '16'

```


**方案四：预热缓存（PartialCache）**

Compaction 后主动触发一次全量查询预热，让 LookupFile 在正式流量来临前重建完毕。适用于有明确流量低谷的场景。

## 7 .4 FullCache 的 Compaction 影响

FullCache 通过 changelog 增量刷新，compaction 本身不会直接导致缓存失效，但会影响刷新性能：


```
正常情况：
  每次刷新处理少量 changelog（几百行）→ RocksDB apply 快

Compaction 后：
  compaction 会产出大量 DELETE + INSERT 的 changelog（重写整个文件）
  → 单次刷新需处理大量 changelog → RocksDB apply 变慢 → 同步刷新阻塞时间变长

```


**解决**：配合`changelog-producer=lookup` 或`deletion-vectors.enabled=true`，compaction 不再产出全量 changelog，只产出真实变更，从根本上减少刷新压力。

> [!warning] 这条结论需要按 Changelog Producer 分开
> `full-compaction` 会在全量 Compaction 时生成 Changelog；`lookup` 在写入/Lookup Compaction 阶段补全变化；DV 改变旧版本行的失效表达。三者的触发时机不同，不能统一描述成“普通 Compaction 产出大量 DELETE+INSERT，打开任一选项后只剩真实变化”。排查时先查询表的实际 `changelog-producer` 和 Snapshot `commit_kind`。

---

## 八、多分区维表的动态分区行为

## 8 .1 动态分区参数说明

Paimon 1.x 已将动态分区参数从`lookup.dynamic-partition` 更名为`scan.partitions`，旧 key 作为 fallback 向后兼容，\*\*新配置应使用`scan.partitions`\*\*。

> [!note] 批注：当前官方文档确认 `scan.partitions` 支持 `max_pt()` 和 `max_two_pt()`
> 对 Lookup Source，最大分区会按 `lookup.dynamic-partition.refresh-interval` 周期刷新；对普通 Source，即使是 Streaming Job，也是在作业启动前确定一次。不要把 Lookup 的动态刷新行为推广到所有 `scan.partitions` 读取。

支持三种值：

| 值 | 说明 |
| --- | --- |
|`max_pt()` | 只加载分区值最大的那个分区 |
|`max_two_pt()` | 同时加载分区值最大的**两个**分区，适用于跨零点业务（凌晨新分区还未完整写入时需查前一天数据） |
|`dt=2026-10-08`  等具体值 | 指定固定分区，不动态探测 |

## 8 .2 max\_pt() 动态分区的工作机制

维表按时间分区时（如`dt`），通常只需要查最新分区的数据：


```
CREATE TABLE dim_table (
  user_id BIGINT,
  city    STRING,
  dt      STRING,
  PRIMARY KEY (user_id, dt) NOTENFORCED
) PARTITIONED BY (dt)
WITH (
'bucket'                                    = '16',
'scan.partitions'                           = 'max_pt()',   -- 新写法（旧: lookup.dynamic-partition）
'lookup.dynamic-partition.refresh-interval' = '1h'         -- 默认 1h
);

```


`max_pt()` 的工作流程：


```
每隔 lookup.dynamic-partition.refresh-interval（默认 1h）：
  ↓
扫描分区目录，找到最大分区值（如 dt=2026-07-05）
  ↓
若分区值发生变化（新分区出现）：
  ├── PartialCache：更新元数据，指向新分区文件
  └── FullCache：触发全量重新 bootstrap（加载新分区数据）

```


## 8 .3 max\_two\_pt() 跨零点场景

**适用场景**：凌晨新分区（如`dt=2026-07-06`）刚开始写入，数据尚不完整，同时需要保证查前一天（`dt=2026-07-05`）的完整数据。


```
CREATE TABLE dim_table (
  ...
) WITH (
  'scan.partitions'                           = 'max_two_pt()',  -- 同时加载最新两个分区
  'lookup.dynamic-partition.refresh-interval' = '10m'
);

```


**行为说明**：


```
t=00:05  新分区 dt=2026-07-06 出现，仅有少量数据
  ↓
max_two_pt() 同时持有：
  ├── dt=2026-07-05（完整）
  └── dt=2026-07-06（增量）
  ↓
查询先在 dt=2026-07-06 找，找不到再去 dt=2026-07-05 找
  → 用户不会因新分区数据不完整而查到空值

```

> [!warning] 勘误：`max_two_pt()` 只表示选中最大的两个分区，不等于“新分区优先、未命中再查旧分区”
> 官方配置说明没有承诺两个分区的 Lookup 优先级。若同一 `user_id` 在两天都存在，而 Join Key 不含 `dt`，实现可能返回多条、按内部顺序覆盖，或要求额外条件；不能据此保证平滑回退。跨零点方案必须用重复 Key、仅旧分区有值、仅新分区有值三组数据实测结果语义。


**PartialCache 路径**（平滑切换）：


```
t=23:59  当前分区 dt=2026-07-04，查询正常
t=00:01  新分区 dt=2026-07-05 出现
t=01:00  动态分区刷新触发（1h 间隔）
  ↓
  元数据切换到新分区
  旧分区 LookupFile 触发 DropFileCallback，被 invalidate
  ↓
t=01:00 之后：查询自动路由到新分区文件（按需下载）

```


**FullCache 路径**（有风险）：


```
分区切换 → 触发全量 bootstrap（加载新分区全量数据）
  ├── bootstrap 期间主流堆积
  └── 若分区数据量大，bootstrap 时间长，可能触发 checkpoint 超时

```


**建议**：分区维表优先使用 PartialCache（固定分桶），避免 FullCache 分区切换的重启风险。

## 8 .4 历史分区数据的缓存保留

| 路径 | 历史分区处理 |
| --- | --- |
| **PartialCache** | 旧分区的 LookupFile 在`lookup.cache-file-retention`（默认 1h）超时后自动清理，不会永久保留 |
| **FullCache** | 切换到新分区后，旧分区数据仍在 RocksDB 中，直到作业重启才清除（可能占用大量磁盘） |

> [!warning] 该 FullCache 结论需要源码或实验支持
> 动态分区刷新可能重建/替换 Full Cache，也可能增量维护，具体取决于版本；不能据此把“定期重启作业”当作清理方案。先观察分区切换前后 RocksDB 大小、查询旧 Key 的结果和刷新日志，再判断是否存在历史数据残留。

**FullCache 历史分区磁盘积压的缓解**：


```
-- 缩短 continuous.discovery-interval，让 FullCache 更快感知分区切换
'continuous.discovery-interval' = '10s'

-- 或定期重启作业（低峰期），清理旧分区的 RocksDB 数据

```


## 8 .5 分区维表的最佳实践


```
-- 推荐配置：分区维表 + PartialCache + 合理刷新间隔
CREATETABLE dim_user_partition (
  user_id BIGINT,
  city    STRING,
  dt      STRING,
  PRIMARY KEY (user_id) NOTENFORCED   -- 主键不含分区键，join 时更简洁
) PARTITIONED BY (dt)
WITH (
'bucket'                                    = '16',
'scan.partitions'                           = 'max_pt()',   -- 新写法，旧: lookup.dynamic-partition
'lookup.dynamic-partition.refresh-interval' = '10m',       -- 按需缩短，感知新分区更快
'lookup.cache-file-retention'               = '2h',        -- 略大于刷新间隔，避免热文件过早清理
'deletion-vectors.enabled'                  = 'true'
);

```


**注意**：若主键包含分区键（如`PRIMARY KEY (user_id, dt)`），join 条件必须同时指定两个字段，实际使用中通常不方便。建议主键只用业务键，分区键作为普通列。

> [!warning] 勘误：这段“最佳实践”DDL 有语法和建模错误
> `CREATETABLE` 应为 `CREATE TABLE`，`NOTENFORCED` 应为 `NOT ENFORCED`。固定分桶的分区主键表通常要求主键包含所有分区列，官方动态分区示例也是 `PRIMARY KEY (user_id, dt)`。Lookup 选择 `max_pt()` 后，Join SQL 可以只用 `user_id` 关联已选中的分区，并不等于表定义可以删掉 `dt` 主键列。若确实要让主键跨分区唯一，应另行评估动态桶的 Cross-Partition Upsert，不能在固定桶表中直接套用本文 DDL。

---

## 九、总结

下篇我们回到"为什么"，把上篇那些"照做就行"的结论背后的原理逐层拆开。把全文要点浓缩如下。

## 原理篇的核心认知

**1. FullCache 的三种实现，代价各不相同。**

| 子类 | 触发条件 | 关键特征 |
| --- | --- | --- |
| PrimaryKeyLookupTable | join 键 = 主键 | 一次点查，null 防穿透，刷新最轻 |
| SecondaryIndexLookupTable | join 键 ≠ 主键 | 双 CF 双索引，UPDATE 需撤旧建新，刷新最复杂 |
| NoPrimaryKeyLookupTable | 无主键 append | List 存储，不支持 DELETE，invalidate 代价高 |

三者共用 Caffeine + RocksDB 两层缓存，但查询次数、CF 数量、UPDATE 处理方式差异明显——这也是上篇强调"join 键尽量等于主键"的底层原因。

**2. PartialCache 的快与省，源于"元数据全量、文件按需"。**

- refresh() 只拉 DataFileMeta（纯内存，无文件 IO）→ 启动秒级
- get() 首次访问某 SST 才下载、编码为 LookupFile 并入 Caffeine → 内存/磁盘按需占用
- compaction 删旧文件时 DropFileCallback 立即 invalidate → 不会查到过期数据

这正是上篇"PartialCache 启动快、内存低、滞后短"的根因所在。

**3. Lookup Join 是非对齐的，滞后可量化。**

| 路径 | 典型滞后 |
| --- | --- |
| FullCache | 10s ~ 数分钟（受 RocksDB apply 影响） |
| PartialCache Local | 10s ~ 30s（只更新元数据） |
| PartialCache Remote | 秒级（Query Service 实时监听） |

这是设计取舍而非缺陷——对延迟敏感的字段应走实时流，而非维表。

**4. 异常与抖动都有迹可循。**

- TM 重启：FullCache 分钟级 bootstrap（有 checkpoint 超时风险），PartialCache 秒级重建
- Query Service 宕机：自动降级到 PartialCache Local
- Compaction：PartialCache 局部 LookupFile 失效、FullCache changelog 量突增 → 周期性反压的常见元凶，错峰 / 调参 / 开异步是三板斧

**5. Changelog 与 deletion-vectors 是 FullCache 的配套，PartialCache 不依赖。**

FullCache 靠增量 apply changelog 刷新，必须配`changelog-producer=lookup`；Paimon 0.8+ 推荐用`deletion-vectors.enabled=true` 替代，写入更好、语义同样完整。而 PartialCache 直接读最新 SST，对两者都没有要求。

> [!note] 批注：生产判断顺序
> 先确认 Join 的结果语义，再确认实际缓存路径；随后测量 Snapshot 提交到 Lookup 可见的延迟、重启恢复时间、Compaction 后 P99 和本地盘占用。只有这些证据对齐，才能判断问题来自表模型、刷新、缓存冷启动、RocksDB、Query Service 还是 Compaction。

## 复核资料

- [Apache Paimon：Lookup Joins](https://paimon.apache.org/docs/master/flink/sql-lookup/)
- [Apache Paimon：Configurations](https://paimon.apache.org/docs/master/maintenance/configurations/)
- [Apache Paimon：Changelog Producer](https://paimon.apache.org/docs/master/primary-key-table/changelog-producer/)
- [Apache Paimon：Data Distribution](https://paimon.apache.org/docs/master/primary-key-table/data-distribution/)
- [Apache Paimon：Deletion Vectors](https://paimon.apache.org/docs/master/primary-key-table/deletion-vectors/)
- 上篇：[[Flink 实时维表性能优化（上）：用对 Paimon Lookup 的完整实践（文章批注版）]]
- 相关笔记：[[Paimon MOR、COW、MOW 与 Deletion Vector]]
