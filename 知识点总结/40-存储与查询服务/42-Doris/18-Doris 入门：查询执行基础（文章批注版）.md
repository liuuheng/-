---
title: "Doris 入门篇06：查询执行基础"
author: "外星人的知识杂谈"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484334&idx=1&sn=8aae3381209890760975f1f7af5512c5&chksm=c1bf09b3f6c880a59f1fc90d0106686c999a19c205cfeaed40033043e6a94e451316c0a74dc8"
published_at: "2026-07-28T20:40:38+08:00"
annotated_at: "2026-11-18"
cssclasses:
  - wechat-article
  - doris-intro-annotated
tags:
  - doris
  - 文章批注
---

# Doris 入门篇06：查询执行基础

<div class="article-meta">外星人的知识杂谈 · 2026-07-28</div>

[原文链接](https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484334&idx=1&sn=8aae3381209890760975f1f7af5512c5&chksm=c1bf09b3f6c880a59f1fc90d0106686c999a19c205cfeaed40033043e6a94e451316c0a74dc8)

<!-- 正文按原文顺序保留；“批注”“核对说明”为新增。移除装饰封面及篇末导航。示例命令未在集群运行。 -->

> **定位：** 入门篇第 6 篇 | 理解一条 SQL 在 Doris 中的完整执行路径

---

## 一、背景动机

当你执行一条`SELECT` 语句时，Doris 内部经历了从文本到结果的复杂流转。理解这个过程，是后续学习查询优化、Profile 分析的基础。本篇只做全链路的概览，不深入各阶段细节。

---

## 二、SQL 执行全链路


```text
┌──────────────────────────────────────────────────────────────────┐
│ Client: SELECT count(*) FROM orders WHERE dt='2026-07-01'        │
└────────────────────────────────┬─────────────────────────────────┘
                                 │ MySQL Protocol
                                 ▼
┌──────────────────── FE (Frontend) ────────────────────────────────┐
│  ① Parse:     SQL文本 → AST (抽象语法树)                          │
│  ② Analyze:   语义校验（表/列是否存在、类型检查）                    │
│  ③ Optimize:  逻辑优化(RBO) + 代价优化(CBO) → 最优执行计划          │
│  ④ Plan:      逻辑计划 → 物理计划 → Fragment 拆分                  │
│  ⑤ Schedule:  将 Fragment 分配到 BE 节点                          │
└───────────┬──────────────────┬──────────────────┬────────────────┘
            │ RPC              │ RPC              │ RPC
            ▼                  ▼                  ▼
┌────────────────┐  ┌────────────────┐  ┌────────────────┐
│    BE Node 1   │  │    BE Node 2   │  │    BE Node 3   │
│  ⑥ Execute:    │  │  ⑥ Execute:    │  │  ⑥ Execute:    │
│  Scan+Filter   │  │  Scan+Filter   │  │  Scan+Filter   │
│  Local Agg     │  │  Local Agg     │  │  Local Agg     │
└───────┬────────┘  └───────┬────────┘  └───────┬────────┘
        │                   │                   │
        └───────────────────┼───────────────────┘
                            │ Exchange (Shuffle/Gather)
                            ▼
                  ┌────────────────────┐
                  │   BE (Coordinator) │
                  │  ⑦ Global Agg      │
                  │  ⑧ Result Sink     │
                  └─────────┬──────────┘
                            │
                            ▼
                  ┌────────────────────┐
                  │   Client: 结果返回   │
                  └────────────────────┘

```

> [!note] 批注：客户端连接 FE，BE 执行计算
>
> 图中底部的 BE 到 Client 是结果流向的简写。在常规 MySQL 协议连接下，客户端会话在 FE，BE 执行扫描和聚合，结果经 FE 返回。局部聚合让 BE 先对本地数据求部分计数，再汇总计数；一个百万行扫描可以只交换少量中间行。


---

## 三、各阶段详解

### 3.1 Parse（解析）

将 SQL 文本解析为抽象语法树（AST）。


```text
"SELECT count(*) FROM orders WHERE dt = '2026-07-01'"

→ AST:
  SelectStmt
  ├── selectList: [FunctionCallExpr(count, *)]
  ├── fromClause: [TableRef(orders)]
  └── whereClause: [BinaryPredicate(dt = '2026-07-01')]

```


Doris 2.0+ 使用 ANTLR4 作为 SQL Parser。

> [!warning] 核对说明：原文 AST 是示意
>
> 这里的节点名用于解释语法结构，不能当作 Nereids 的实际类名或调试输出。语法树能表达 `dt = '2026-07-01'` 的写法；表列存在性、类型转换以及能否裁剪分区，还要等后续分析和优化。

### 3.2 Analyze（语义分析）

- 检查表`orders` 是否存在
- 检查列`dt` 的类型是否与`'2026-07-01'` 兼容
- 解析别名、子查询展开
- 隐式类型转换（如 STRING → DATE）

### 3.3 Optimize（查询优化）

分为两个阶段：

**RBO（Rule-Based Optimization）— 基于规则：**

- 谓词下推：将 WHERE 条件推到最靠近数据源的位置
- 列裁剪：只读取查询需要的列
- 常量折叠：`1 + 1` →`2`
- 分区裁剪：根据 WHERE 条件确定只扫描哪些分区

**CBO（Cost-Based Optimization）— 基于代价：**

- 根据表的统计信息（行数、NDV、数据分布）估算各种执行方案的代价
- 选择最优的 Join 顺序和 Join 算法
- 决定是否使用 Runtime Filter

> [!note] 批注：代价低指估算成本低
>
> 优化器根据统计信息估算连接和分发的成本。行数或不同值数量（NDV）失准，可能使优化器误以为一侧足够小而广播。把计划中的估算行数和同次执行 Profile 的算子输出对齐，才能判断是估算失准，还是节点负载、倾斜等执行因素拖慢了查询。

### 3.4 Plan（执行计划生成）

将优化后的逻辑计划转换为可执行的物理计划，并拆分为多个 **Fragment。**


```text
Fragment 0 (在多个 BE 上并行)：
  OlapScanNode → 扫描本地 Tablet
  FilterNode   → 应用 WHERE 条件
  AggNode(LOCAL) → 本地预聚合

Fragment 1 (在协调 BE 上)：
  ExchangeNode → 收集各 BE 的中间结果
  AggNode(GLOBAL) → 全局聚合
  ResultSinkNode → 返回结果

```

> [!note] 批注：区分 Fragment、实例和 PipelineTask
>
> Fragment 是计划切分后的结构，Fragment Instance 是该结构的一次执行。同一个扫描 Fragment 可以在多个 BE 上运行多个实例；BE 还会把执行拆到 PipelineTask。因此，几个 Fragment 既不能推算参与了几台机器，也不能直接换算为几个线程。


### 3.5 Schedule（调度分发）

FE 的 Coordinator 将每个 Fragment 分配到具体的 BE 节点：

- **数据本地性：**

  优先将 Scan Fragment 分配到数据所在的 BE
- **负载均衡：**

  避免所有 Fragment 集中在少数节点
- **Colocate 感知：**

  同一 Colocate Group 的表可以本地 Join

### 3.6 Execute（执行）

BE 收到 Fragment 后，通过 **Pipeline 执行引擎** 执行：

- 每个算子（Scan/Filter/Agg/Join/Sort）是一个 Operator
- 数据以 Block（列式批次，默认 4096 行）为单位在算子间流转
- Pipeline 模型避免线程阻塞，支持高并发

> [!note] 批注：Block 行数不代表扫描量
>
> Block 是算子间传递的列式批次，实际行数会随批大小配置、过滤和数据尾部变化。定位慢扫描时，结合实际读取行数、字节和 Profile 耗时定位；只看批次条数或把每个 Block 固定按 4096 行计算，会误判数据量。

### 3.7 Exchange（数据交换）

当查询需要跨节点数据交换时（如 JOIN 的 Shuffle、全局聚合的 Gather），通过 Exchange 节点完成：

| 类型 | 说明 | 适用场景 |
| --- | --- | --- |
| Gather | 所有 BE 的数据汇聚到一个节点 | 全局 ORDER BY |
| Hash Shuffle | 按 Hash(key) 重分布 | Hash Join 两侧对齐 |
| Broadcast | 小表广播到所有节点 | 小表 JOIN 大表 |
| Bucket Shuffle | 一侧按另一侧 Bucket 分布 | 已知数据分布时优化 |

> [!note] 批注：网络开销要看交换两端实际传输
>
> Broadcast 会复制一侧输入，Hash Shuffle 会重分布输入，Gather 聚到一处。优化时关注进入 Exchange 的实际行数、字节以及各实例耗时分布；计划里出现 Exchange，只能证明存在交换边界，不证明网络已经成为瓶颈。

---

## 四、用 EXPLAIN 观察执行计划


```sql
EXPLAIN SELECT count(*) FROM orders WHERE dt = '2026-07-01' AND status = 'paid';

```


输出关键信息：


```text
PLAN FRAGMENT 0
  OUTPUT EXPRS: count(*)
  PARTITION: UNPARTITIONED

  RESULT SINK

  2:AGGREGATE (merge finalize)
    aggregate: count(*)

PLAN FRAGMENT 1
  PARTITION: HASH_PARTITIONED

  STREAM DATA SINK
    EXCHANGE ID: 2
    UNPARTITIONED

  1:AGGREGATE (update serialize)
    aggregate: count(*)

  0:OlapScanNode
    TABLE: orders
    PREAGGREGATION: ON
    partitions=1/30        ← 分区裁剪：30个分区只扫1个
    tablets=16/16
    tabletList=10001,10002,...
    cardinality=1000000
    avgRowSize=4.0

```


**关键看点：**

- `partitions=1/30`

  ：分区裁剪是否生效
- `PREAGGREGATION: ON`

  ：是否利用了存储层预聚合
- `cardinality`

  ：预估扫描行数
- Fragment 数量和 Exchange 类型

### EXPLAIN 的变体


```sql
EXPLAIN VERBOSE ...   -- 详细信息（含列类型、表达式树）
EXPLAIN GRAPH ...     -- 图形化展示（适合复杂查询）
EXPLAIN ANALYZED ...  -- 含实际统计信息的估算

```

> [!warning] 核对说明：计划与实测使用不同证据
>
> `partitions=1/30` 是计划显示的分区裁剪结果；`cardinality` 是估算值，`PREAGGREGATION: ON` 是扫描节点的预聚合标记，不能单凭它断定命中 Rollup 或物化视图。原文 `GRAPH` / `ANALYZED` 写法不要直接照抄；4.x 官方语法列出 `EXPLAIN [VERBOSE]`，运行时行数和耗时用实际执行的 Query Profile 核对。[EXPLAIN](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/data-query/EXPLAIN/) · [Query Profile](https://doris.apache.org/docs/4.x/query-acceleration/query-profile/)


---

## 五、Pipeline 执行模型（初探）

Doris 2.1+ 默认使用 Pipeline 执行引擎，替代传统 Volcano 模型：

|  | Volcano 模型 | Pipeline 模型 |
| --- | --- | --- |
| 执行方式 | 算子逐行 pull（next()） | 算子 push-based + 事件驱动 |
| 线程模型 | 每个 Fragment Instance 一个线程 | 固定线程池，Task 调度 |
| 阻塞处理 | 线程阻塞等待（如 Hash Build） | 让出线程，状态切换后被重调度 |
| 资源利用 | 并发高时线程暴增 | 线程数固定，CPU 利用率高 |

**简单理解：** Pipeline 模型就像餐厅的流水线——厨师不会站着等一道菜出锅，而是同时处理多道菜的不同步骤，提高了并行度和资源利用率。

> [!note] 批注：等待依赖时让出线程
>
> 例如 Hash Join 的 Probe Pipeline 要等 Build Pipeline 构建哈希表。在依赖就绪前，Probe Task 不占着工作线程轮询；线程可运行其他可执行的 Task。Profile 里应分开看算子实际执行与等待依赖的时间，等待长也不等于算子消耗了同等 CPU。[Pipeline 执行引擎](https://doris.apache.org/docs/4.x/query-acceleration/optimization-technology-principle/pipeline-execution-engine/)

---

## 六、查询性能的关键影响因素

| 因素 | 影响 | 优化方向 |
| --- | --- | --- |
| 分区裁剪 | 决定扫描的数据量级 | WHERE 条件覆盖分区列 |
| 索引命中 | 决定 Page 级数据跳过 | 合理建索引 |
| 预聚合 | 减少运行时聚合开销 | Aggregate Key + Rollup |
| 数据分布 | 影响并行度和倾斜 | 合理选择分桶键 |
| Exchange 量 | 网络传输开销 | Colocate Join / 减少 Shuffle |
| 并行度 | BE 资源利用 | Fragment Instance 数 |

---

## 七、实践建议


```sql
-- 查看实际执行 Profile（需先开启）
SET enable_profile = true;

-- 执行查询后查看 Profile
SHOW QUERY PROFILE "/";

-- 查看当前正在执行的查询
SHOW PROC '/current_queries';

-- 取消一个正在执行的慢查询
KILL QUERY query_id;

```


**初学者调试步骤：**

1. 先看`EXPLAIN`，确认分区裁剪和索引是否生效
2. 执行查询，看返回时间
3. 如果慢，开 Profile 看各算子耗时分布
4. 定位瓶颈：Scan 慢？Agg 慢？Exchange 多？

> [!warning] 核对说明：用 Query ID 对齐一次实际执行
>
> 可在同一会话打开 `enable_profile`，运行查询后用 `SHOW QUERY PROFILE` 找到对应 Query ID，再查看其详细 Profile。EXPLAIN 可看谓词和计划裁剪，但扫描时实际跳过多少数据要看 Profile。查慢因时优先比较各实例的最大值与分布，避免用平均值掩盖一个拖慢整条查询的热点。[Query Profile](https://doris.apache.org/docs/4.x/query-acceleration/query-profile/)

---

## 八、小结

| 要点 | 内容 |
| --- | --- |
| 全链路 | Parse → Analyze → Optimize → Plan → Schedule → Execute → Return |
| Fragment | 执行计划的分布式执行单元，一个 Fragment 在多个 BE 上并行 |
| Exchange | 跨节点数据交换（Gather/Shuffle/Broadcast） |
| Pipeline | 事件驱动的执行模型，固定线程池，高效利用 CPU |
| 核心工具 | EXPLAIN（看计划）、Profile（看实际执行） |
