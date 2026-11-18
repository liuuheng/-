---
title: "Doris 高阶篇05：查询优化器 Nereids 源码解析"
author: "外星人的知识杂谈"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484402&idx=1&sn=c2513a457c16ef920267d364846cee4f&chksm=c1bf09eff6c880f91946415abc4abf88a25633462ab2eeb2d10b9123a6b26991edfad3c16da9#wechat_redirect"
published_at: "2026-09-16T20:51:18+08:00"
annotated_at: "2026-11-18"
cssclasses:
  - wechat-article
  - doris-intro-annotated
tags:
  - doris
  - 文章批注
---

# Doris 高阶篇05：查询优化器 Nereids 源码解析

<div class="article-meta">外星人的知识杂谈 · 2026-09-16</div>

[原文链接](https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484402&idx=1&sn=c2513a457c16ef920267d364846cee4f&chksm=c1bf09eff6c880f91946415abc4abf88a25633462ab2eeb2d10b9123a6b26991edfad3c16da9#wechat_redirect)

<!-- 正文按原文顺序保留；批注和核对说明为新增。装饰封面和独立的篇末预告已去除。 -->

## 定位与阅读前提

本文面向希望深入理解 Doris 查询优化内核的开发者，从 Cascades 框架的设计哲学出发，逐步拆解 Nereids 优化器的 Memo 结构、规则系统、代价模型及优化阶段划分。

**阅读前提：**

- 了解关系代数基本概念（Selection/Projection/Join）
- 熟悉 SQL 查询执行的逻辑计划与物理计划区别
- 已阅读本系列前 4 篇（存储引擎、向量化执行、Pipeline 调度）

---

## 背景动机

Doris 在 2.0 版本之前使用基于规则的老优化器（legacy planner），其局限在于：

1. 无法进行基于代价的 Join Reorder
2. 缺乏统一的等价表达式搜索空间
3. 规则之间的应用顺序强依赖人工编排

Nereids 是 Doris 新一代基于 Cascades 论文（Columbia Optimizer）实现的查询优化器，目标是在有限时间内搜索到代价最优的物理执行计划。其核心思想是：**用 Memo 数据结构紧凑地表示等价计划搜索空间，通过 Transformation/Implementation 规则扩展空间，最终由 Cost Model 引导剪枝选出最优计划。**

---

## Memo 数据结构

Memo 是 Cascades 优化器的核心数据结构，以紧凑方式存储所有逻辑等价的计划表达式。

### Group

`Group` 代表一组逻辑等价的表达式集合。同一个 Group 中的所有`GroupExpression` 产出相同的逻辑输出列和行集合。


```text
源码路径: fe/fe-core/src/main/java/org/apache/doris/nereids/memo/Group.java

```


Group 的核心字段：

- `groupId`

  ：全局唯一标识
- `logicalExpressions`

  ：该 Group 中所有逻辑计划表达式
- `physicalExpressions`

  ：该 Group 中所有物理计划表达式
- `lowestCostPlans`

  ：Map<physicalproperties, groupexpression="" pair\>，记录满足不同物理属性要求下的最优计划

> [!warning] 核对说明｜物理属性是比较计划的前提
>
> 公众号该行的泛型文本已损坏。[Doris 4.1 的 `Group`](https://github.com/apache/doris/blob/branch-4.1/fe/fe-core/src/main/java/org/apache/doris/nereids/memo/Group.java)声明的是 `Map<PhysicalProperties, Pair<Cost, GroupExpression>>`。同一个 Group 的逻辑输出等价，但上游可能要求按连接键分布或按排序键有序。每种属性要求都可能对应不同的最低代价候选。

### GroupExpression

`GroupExpression` 是 Memo 中的基本节点，表示一个具体的算子加上其子 Group 引用。


```text
源码路径: fe/fe-core/src/main/java/org/apache/doris/nereids/memo/GroupExpression.java

```


核心字段：

- `plan`

  ：该表达式对应的逻辑/物理算子（Plan 节点）
- `children`

  ：子节点列表，类型为`List<Group>`（指向子 Group 而非具体表达式）
- `ownerGroup`

  ：所属的 Group
- `cost`

  ：当前最优 Cost
- `ruleMasks`

  ：BitSet，记录已应用过的规则，避免重复探索

> [!note] 批注｜子节点指向 Group 可复用搜索结果
>
> 父表达式只记录逻辑上需要哪个子 Group，选择哪个具体实现由子 Group 的物理属性要求和代价决定。因此多个父候选可以引用同一个子 Group；Memo 不需要为每一种父子组合重复存整棵树。

### Memo 本体


```text
源码路径: fe/fe-core/src/main/java/org/apache/doris/nereids/memo/Memo.java

```


`Memo` 类管理所有 Group 和 GroupExpression，提供`copyIn()` 方法将新的等价表达式插入正确的 Group，并处理 Group 合并（当发现两个 Group 实际等价时）。

### Memo 结构示意


```text
Memo
├── Group 0 (Root)
│   ├── GExpr: Join(G1, G2)        [logical]
│   ├── GExpr: HashJoin(G1, G2)    [physical]
│   └── GExpr: NLJoin(G1, G2)      [physical]
├── Group 1
│   ├── GExpr: Scan(tableA)        [logical]
│   └── GExpr: OlapScan(tableA)    [physical]
└── Group 2
    ├── GExpr: Filter(G3, pred)    [logical]
    └── GExpr: PhyFilter(G3, pred) [physical]

```


---

## 规则系统

Nereids 规则系统是优化器搜索空间扩展的驱动力。每条规则匹配一种 Pattern，将匹配的计划变换为新的等价计划。

### Rule 抽象基类


```text
源码路径: fe/fe-core/src/main/java/org/apache/doris/nereids/rules/Rule.java

```


核心字段：

- `ruleType`

  ：规则类型枚举（RuleType）
- `pattern`

  ：Pattern 匹配模板
- `rulePromise`

  ：规则优先级（RulePromise），决定执行顺序

### 规则分类

| 类型 | 目录 | 作用 |
| --- | --- | --- |
| Rewrite 规则 |`rules/rewrite/` | 等价改写（谓词下推、列裁剪等），不进入 Memo 搜索 |
| Exploration 规则 |`rules/exploration/` | Join Reorder、CBO 级变换 |
| Implementation 规则 |`rules/implementation/` | 逻辑算子 → 物理算子映射 |

**典型 Rewrite 规则：**

- `ColumnPruning`

  ：列裁剪
- `ConstantPropagation`

  ：常量传播
- `EliminateAggregate`

  ：聚合消除
- `ConvertInnerOrCrossJoin`

  ：Join 类型转换

**典型 Implementation 规则：**

- `LogicalJoinToHashJoin`

  ：LogicalJoin → PhysicalHashJoin
- `LogicalJoinToNestedLoopJoin`

  ：LogicalJoin → PhysicalNestedLoopJoin
- `LogicalOlapScanToPhysicalOlapScan`

  ：逻辑扫描 → 物理 OlapScan
- `AggregateStrategies`

  ：聚合算子多阶段拆分策略

> [!note] 批注｜改写的前提是语义等价
>
> 例如内连接中仅引用左表的过滤条件通常可以下推到左输入。左外连接右侧会补 `NULL`，把连接后的过滤条件随意移进右表或连接条件可能改变结果。判断规则是否适用，先读它匹配的算子、表达式和空值条件，再看是否有成本收益。

---

## 优化阶段

Nereids 的优化流程分为清晰的多个阶段，入口为`NereidsPlanner.plan()`：


```text
源码路径: fe/fe-core/src/main/java/org/apache/doris/nereids/NereidsPlanner.java

```


### 阶段流程


```text
SQL 文本
  │
  ▼
[Parse] → LogicalPlan (AST)
  │
  ▼
[Analyze] → 绑定表/列/函数
  │
  ▼
[Rewrite] → 等价改写（Rewriter 批量应用 rewrite 规则）
  │
  ▼
[toMemo] → 将 rewrite 后的计划初始化到 Memo
  │
  ▼
[DeriveStats] → 自底向上推导统计信息
  │
  ▼
[Optimize] → Cascades 搜索（explore + implement + cost）
  │       ├── DPHyp Join Enumeration（大表场景）
  │       └── OptimizeGroupJob（标准 Cascades）
  │
  ▼
[PostProcess] → 物理计划后处理（Fragment 分配、RuntimeFilter 生成等）
  │
  ▼
Physical Plan → 分布式执行

```

> [!note] 批注｜哪个阶段的输出适合定位哪类问题
>
> 绑定后的计划回答列和函数指向哪里，改写后的计划用于检查列裁剪或谓词下推，最后的物理与分布式计划才显示连接策略和 Exchange。运行中实际扫描行数和等待时间还要看 Query Profile。用同一条 SQL 的不同阶段作对照，才能分辨是规则没匹配、代价没选中，还是执行时负载异常。


### Rewriter


```text
源码路径: fe/fe-core/src/main/java/org/apache/doris/nereids/jobs/executor/Rewriter.java

```


Rewriter 以批次（Batch）的方式 topDown/bottomUp 应用 rewrite 规则，不涉及 Cost，目标是将逻辑计划化简到最优逻辑形态。

> [!note] 批注｜逻辑计划化简不等于找到最低执行代价
>
> Rewrite 会应用确定的规则来规范、简化计划。后续仍要比较 Join 算法、分布策略及算子实现。即使逻辑树明显变小，实际时延仍会受数据分布、并发和 I/O 影响。

### Optimizer（Cascades 搜索）


```text
源码路径: fe/fe-core/src/main/java/org/apache/doris/nereids/jobs/executor/Optimizer.java

```


Optimizer 的核心执行逻辑：

1. 将 rewrite 后的计划转入 Memo（`cascadesContext.toMemo()`）
2. 自底向上推导统计信息（`DeriveStatsJob`）
3. 判断是否使用 DPHyp（超过阈值 Join 数量时启用 DPHyp 枚举）
4. 提交`OptimizeGroupJob` 进入标准 Cascades 搜索

> [!warning] 核对说明｜DPHyp 按连接结构及会话配置选择
>
> [4.1 的 `Optimizer.isDpHyp`](https://github.com/apache/doris/blob/branch-4.1/fe/fe-core/src/main/java/org/apache/doris/nereids/jobs/executor/Optimizer.java)检查连续 Join 数及会话变量，还需满足未关闭 Join Reorder、未被 Leading 限制等条件。原图的“大表场景”容易被读成表的字节数触发；这里的主要维度是连接结构和设置。

### Cascades Job 体系


```text
源码路径: fe/fe-core/src/main/java/org/apache/doris/nereids/jobs/cascades/

```


| Job 类型 | 作用 |
| --- | --- |
|`OptimizeGroupJob` | 对一个 Group 发起优化，遍历其所有 GroupExpression |
|`OptimizeGroupExpressionJob` | 对一个 GroupExpression 应用 exploration/implementation 规则 |
|`ApplyRuleJob` | 执行具体的一条规则 |
|`CostAndEnforcerJob` | 计算 Cost 并插入 Enforcer（如 Exchange、Sort） |
|`DeriveStatsJob` | 推导 GroupExpression 的统计信息 |

> [!note] 批注｜Enforcer 的代价要和子计划一起看
>
> 如果一个便宜的子计划输出没有按 Join 键分布，父节点可能需要插入 Exchange 才能消费。另一个扫描代价略高的候选若已经满足所需分布，整段计划反而更便宜。不能只按单个算子的代价选最优实现。

---

## 代价模型

### 统计信息推导


```text
源码路径: fe/fe-core/src/main/java/org/apache/doris/nereids/stats/StatsCalculator.java

```


`StatsCalculator` 以 Visitor 模式遍历各类算子，为每个算子推导输出行数和列统计信息（NDV、null 比例、最小最大值等）。

关键估算类：

- `FilterEstimation`

  ：谓词过滤率估算
- `JoinEstimation`

  ：Join 基数估算
- `ExpressionEstimation`

  ：表达式选择性估算

> [!note] 批注｜统计误差会沿计划向上累积
>
> NDV 是列不同值数量。若优化器低估过滤后行数，就可能低估 Build 哈希表及 Broadcast 的代价。排查先从计划中第一个估算与实际明显偏离的节点入手，检查统计信息更新时间、相关列的分布和谓词口径，再看偏差如何传到 Join 上游。

### Cost 计算


```text
源码路径: fe/fe-core/src/main/java/org/apache/doris/nereids/cost/CostCalculator.java
源码路径: fe/fe-core/src/main/java/org/apache/doris/nereids/cost/CostModel.java

```


`CostCalculator.calculateCost()` 对每个 GroupExpression 计算 Cost，内部调用`CostModel` 以 Visitor 模式为每种物理算子赋代价。代价公式考虑：

- CPU 代价（行数 \* 表达式计算复杂度）
- 内存代价（HashTable 构建等）
- 网络代价（Shuffle/Broadcast）

`CostWeight` 定义了 CPU/Memory/Network 三维权重。

> [!note] 批注｜比较的是模型估算值
>
> Cost 用于候选之间的排序，不可当作毫秒或磁盘字节解读。实际热点、缓存状态、并发和溢写会影响运行表现。确认选优逻辑用 Memo 或物理计划，确认原计划执行代价用 Profile 的真实行数、内存和网络指标。

---

## 源码路径速查表

| 模块 | 路径 |
| --- | --- |
| Memo/Group/GroupExpression |`fe/.../nereids/memo/` |
| 规则基类 & RuleType |`fe/.../nereids/rules/Rule.java` ,`RuleType.java` |
| Rewrite 规则集 |`fe/.../nereids/rules/rewrite/` |
| Exploration 规则集 |`fe/.../nereids/rules/exploration/` |
| Implementation 规则集 |`fe/.../nereids/rules/implementation/` |
| Cascades Job |`fe/.../nereids/jobs/cascades/` |
| Optimizer 入口 |`fe/.../nereids/jobs/executor/Optimizer.java` |
| Rewriter 入口 |`fe/.../nereids/jobs/executor/Rewriter.java` |
| 统计信息推导 |`fe/.../nereids/stats/StatsCalculator.java` |
| 代价模型 |`fe/.../nereids/cost/CostModel.java` |
| NereidsPlanner 总入口 |`fe/.../nereids/NereidsPlanner.java` |
| Pattern 匹配 |`fe/.../nereids/pattern/` |
| 物理属性 & Enforcer |`fe/.../nereids/properties/` |

---

## 核心数据结构/类索引

| 类名 | 职责 |
| --- | --- |
|`Memo` | Cascades 搜索空间容器，管理所有 Group |
|`Group` | 逻辑等价表达式集合 |
|`GroupExpression` | Memo 中的基本节点（算子 + 子 Group 引用） |
|`Rule` | 规则抽象基类 |
|`RuleType` | 规则类型枚举 |
|`RulePromise` | 规则优先级 |
|`NereidsPlanner` | 优化器总入口 |
|`Optimizer` | Cascades 搜索执行器 |
|`Rewriter` | 逻辑改写批量执行器 |
|`CascadesContext` | 优化上下文（持有 Memo、JobScheduler、SessionVariable） |
|`OptimizeGroupJob` | 对 Group 发起 Cascades 优化的 Job |
|`CostAndEnforcerJob` | 计算代价并插入 Enforcer |
|`DeriveStatsJob` | 统计信息推导 Job |
|`StatsCalculator` | 按算子类型推导统计信息 |
|`CostCalculator` | Cost 计算入口 |
|`CostModel` | 各物理算子的代价公式 |
|`PhysicalProperties` | 物理属性需求（分布、排序） |
|`JoinOrderJob` | DPHyp Join 枚举 |

---

## 小结

Nereids 优化器采用经典 Cascades 框架：

1. Memo 紧凑表示搜索空间，Group 表达逻辑等价性
2. Rewrite 阶段先做确定性优化（谓词下推、列裁剪等）
3. Cascades 搜索阶段通过 Exploration 规则扩展等价计划，Implementation 规则生成物理实现
4. CostAndEnforcerJob 自底向上计算代价、插入必要的 Exchange/Sort Enforcer
5. 对大 Join 数量场景，DPHyp 算法替代暴力 Cascades 枚举

理解这套架构后，开发者可以：

- 新增自定义 Rewrite/Implementation 规则
- 调优代价模型参数
- 诊断为何优化器选择了某个物理计划（通过 EXPLAIN MEMO/EXPLAIN COST）

> [!warning] 核对说明｜检查命令的完整语法
>
> [Doris 4.x 诊断文档](https://doris.apache.org/docs/4.x/query-acceleration/performance-tuning-overview/analysis-tools/)列出 `EXPLAIN MEMO PLAN`（查看 CBO 搜索）、`EXPLAIN SHAPE PLAN`（查看计划骨架）和 `EXPLAIN VERBOSE`（查看详细物理计划）。原文的 `EXPLAIN MEMO/EXPLAIN COST` 不宜当作可直接运行的通用写法。

