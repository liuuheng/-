---
title: "Doris 进阶篇05：查询优化与SQL引擎"
author: "外星人的知识杂谈"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484368&idx=1&sn=856882a41eca5e4fabaa893e2cb1edea&chksm=c1bf09cdf6c880db411f2ee6bfd419c53206a22e51e73ea96403045089b0c229dc67f4f7ad47#wechat_redirect"
published_at: "2026-08-27T20:22:18+08:00"
annotated_at: "2026-11-18"
cssclasses:
  - wechat-article
  - doris-intro-annotated
tags:
  - doris
  - 文章批注
---

# Doris 进阶篇05：查询优化与SQL引擎

<div class="article-meta">外星人的知识杂谈 · 2026-08-27</div>

[原文链接](https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484368&idx=1&sn=856882a41eca5e4fabaa893e2cb1edea&chksm=c1bf09cdf6c880db411f2ee6bfd419c53206a22e51e73ea96403045089b0c229dc67f4f7ad47#wechat_redirect)

<!-- 保留原文正文、顺序与示例；批注和核对说明为新增。装饰封面与篇末导航已移除；代码未在集群运行。 -->

> **定位：** 进阶篇第 5 篇 | 掌握 Nereids 优化器原理及常见查询调优手段
>
> **阅读前提：** 入门篇第 06 篇（查询执行基础）、进阶篇第 04 篇（向量化执行引擎）

---

## 一、背景动机

即使向量化引擎让单行处理速度提升了数倍，如果执行计划本身不合理（选错 Join 顺序、谓词没有下推、未利用 Runtime Filter），性能依然会差几个数量级。

Doris 2.0 引入了新一代优化器 **Nereids**（基于 Cascades 框架的 CBO），替代了旧的 Rule-Based 优化器，能基于统计信息生成最优执行计划。本篇从优化器原理到实战调优全面展开。

---

## 二、Nereids 优化器架构

### 2.1 优化流程总览


```text
SQL 文本
   │
   ▼
┌──────────┐
│  Parser  │  → 语法解析，生成 AST
└────┬─────┘
     ▼
┌──────────┐
│ Analyzer │  → 语义分析，类型推断，权限校验
└────┬─────┘
     ▼
┌──────────────────┐
│ Logical Plan     │  → 逻辑计划树
└────┬─────────────┘
     ▼
┌──────────────────┐
│ Nereids CBO      │  → 基于代价的优化
│ ├── Exploration  │    探索等价计划（Join Reorder）
│ ├── Implementation│   选择物理算子（Hash/Sort Join）
│ └── Enforce      │    添加必要的数据分发（Shuffle/Broadcast）
└────┬─────────────┘
     ▼
┌──────────────────┐
│ Physical Plan    │  → 物理执行计划
└────┬─────────────┘
     ▼
┌──────────────────┐
│ Pipeline Build   │  → 生成 Pipeline 任务调度
└──────────────────┘

```


### 2.2 CBO 代价模型

Nereids 的代价估算基于三个维度：

| 代价维度 | 含义 | 影响因素 |
| --- | --- | --- |
| CPU Cost | 算子计算开销 | 行数 × 单行计算量 |
| IO Cost | 数据读取开销 | 扫描数据量、索引命中率 |
| Network Cost | 数据传输开销 | Shuffle 数据量、分区策略 |


```text
Total Cost = CPU_Weight × CPU_Cost + IO_Weight × IO_Cost + Net_Weight × Network_Cost

```


代价估算依赖统计信息的准确性。统计信息不准 → 代价估算错误 → 选择了错误的执行计划。

> [!warning] 核对说明｜代价最小指搜索空间内的估算结果
>
> 优化器只能在合法且实际探索到的计划中比较候选方案。原公式用于区分计算、IO 和传输成本，不是据以计算单次查询耗时的固定公式；即使统计没过期，列间相关性、热点或内存溢写也可能造成估算偏差。用计划估算行数与同次实际输出行数找偏差开始的位置。

---

## 三、统计信息收集

### 3.1 统计信息类型

| 类型 | 内容 | 用途 |
| --- | --- | --- |
| 表级统计 | 行数、数据量 | 估算扫描代价 |
| 列级统计 | NDV（基数）、NULL 比例、Min/Max | 估算过滤选择率 |
| 直方图 | 数据分布 | 精确估算范围查询选择率 |

### 3.2 收集统计信息


```sql
-- 收集整表统计信息（推荐定期执行）
ANALYZE TABLE db.table;

-- 收集指定列
ANALYZE TABLE db.table (col1, col2);

-- 收集直方图（高精度，耗时更长）
ANALYZE TABLE db.table (col1) WITH HISTOGRAM;

-- 收集指定分区
ANALYZE TABLE db.table PARTITION (p20260701);

-- 查看统计信息
SHOW COLUMN STATS db.table;
SHOW TABLE STATS db.table;

-- 自动统计信息收集（推荐开启）
SET GLOBAL enable_auto_analyze = true;

```

> [!warning] 核对说明｜按版本核对 ANALYZE 写法，以及作业是否完成
>
> Doris 4.x 的 `ANALYZE` 参考语法列出指定列、`WITH SYNC` 和百分比/行数采样；没有列出原文的 `PARTITION (...)` 与 `WITH HISTOGRAM` 写法。不要直接照抄。默认收集是异步的，收到作业 ID 后仍要确认收集结束，并核对关联列的新统计值。全局自动收集也不代表刚导入的数据已被统计覆盖。[ANALYZE](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/statistics/ANALYZE/)


### 3.3 统计信息过期的影响


```sql
-- 案例：统计信息显示 orders 表只有 100 行（实际已有 1 亿行）
-- 优化器选择了 Broadcast Join（以为是小表），实际执行 OOM

-- 解决：重新收集统计信息
ANALYZE TABLE orders;

```


**最佳实践：**

- 开启`enable_auto_analyze`，让 Doris 自动维护统计信息
- 大规模数据导入后手动触发 ANALYZE
- 对关键报表涉及的表设置更频繁的收集策略

---

## 四、Join 优化

### 4.1 Join Reorder

当 SQL 包含多表 JOIN 时，不同的 JOIN 顺序导致的执行代价差异可达百倍：


```sql
-- 3 表 JOIN，可能的顺序：
-- A JOIN B JOIN C
-- A JOIN C JOIN B  
-- B JOIN C JOIN A
-- ... 共 3! = 6 种（N 表则 N! 种）

SELECT * FROM orders o
JOIN users u ON o.user_id = u.id
JOIN products p ON o.product_id = p.id
WHERE o.dt = '2026-07-01';

```


Nereids CBO 基于统计信息选择最优顺序：

- 小表（过滤后行数少）作为 Build 侧
- 大表（过滤后行数多）作为 Probe 侧

> [!note] 批注｜排列数不等于实际候选 Join 树数
>
> 同一表排列还可能对应不同树形，外连接等语义约束又会排除部分重排。排查时看每一层输入和输出：最早发生行数放大或估算偏差的 Join，通常比末端总耗时更能解释后续代价。Build 侧看过滤后的实际行数和行宽，不能只看原表总行数。

### 4.2 Join 类型选择

| Join 类型 | 适用场景 | 内存消耗 | 网络消耗 |
| --- | --- | --- | --- |
| Broadcast | 右表小（< 数百万行） | 每个节点一份副本 | 右表广播到所有节点 |
| Shuffle（Partitioned） | 两表都大 | 按 Key 分片 | 两表都 Shuffle |
| Colocate | 同 Colocate Group | 无额外消耗 | 无 |
| Bucket Shuffle | 右表按左表分桶键分发 | 仅右表 Shuffle | 低 |


```sql
-- 强制指定 Join 类型（当优化器选错时）
SELECT /*+ SET_VAR(exec_mem_limit=8589934592) */ *
FROM orders o
JOIN [BROADCAST] users u ON o.user_id = u.id;

-- 或用 Hint
SELECT * FROM orders o
JOIN /*+ BROADCAST */ users u ON o.user_id = u.id;

```

> [!warning] 核对说明｜分发方式不等于 Join 算法
>
> Broadcast 和 Shuffle 描述 Hash Join 的数据如何到各 BE，并不改变 Join 的匹配语义。广播的成本取决于过滤和投影后的字节量、接收端数量和 Build 内存；“几百万行”不是安全线。4.x 文档规定右表前用 `JOIN [BROADCAST]` 或 `JOIN [SHUFFLE]`；原文注释式写法不要当成等价语法。[Join 分发 Hint](https://doris.apache.org/docs/4.x/query-acceleration/hints/distribute-hint/)


### 4.3 Colocate Join 的威力


```sql
-- 同 Colocate Group 的表，JOIN 时数据不需要 Shuffle
-- 前提：同 Group，同分桶键类型，同 Bucket 数

-- 查看 Colocate 状态
SHOW PROC '/colocation_group';

-- 执行计划中会显示
-- HASH JOIN (COLOCATE)  ← 说明走了本地 Join

```

> [!note] 批注｜同组且稳定还要匹配分桶等值条件
>
> `SHOW PROC '/colocation_group'` 可查看组的稳定性。还需 Join 条件覆盖对应分桶键，计划才可能选 Colocate。即使消除了这一步重分发，Join 仍需构建哈希表和执行 Probe，后续聚合也可能另有 Exchange。[Colocation Join](https://doris.apache.org/docs/4.x/query-acceleration/colocation-join/)


---

## 五、谓词下推

### 5.1 下推策略

谓词下推是将 WHERE 条件尽可能推到 Scan 阶段执行，减少数据传输量：


```sql
-- 原始 SQL
SELECT * FROM orders o JOIN users u ON o.user_id = u.id
WHERE o.dt = '2026-07-01' AND u.city = 'hangzhou';

-- 优化后执行逻辑：
-- orders 表 Scan 时直接应用 dt = '2026-07-01'（分区裁剪 + 谓词过滤）
-- users 表 Scan 时直接应用 city = 'hangzhou'
-- 两表大幅减少后再 JOIN

```


### 5.2 常见不能下推的场景

| 场景 | 原因 | 解决方案 |
| --- | --- | --- |
|`WHERE func(col) = value` | 函数计算后无法利用索引 | 改写为`col = reverse_func(value)` |
|`OR`  跨表条件 | 无法确定推给哪张表 | 改写为 UNION |
| 外连接的内表条件 | 语义不等价 | 确认业务逻辑是否可改为内连接 |
| 子查询中的条件 | 相关子查询难以下推 | 改写为 JOIN |

> [!warning] 核对说明｜改写前先保证多重集与 NULL 语义
>
> 把跨表 `OR` 改成 `UNION ALL`，可能让两个条件都成立的行重复输出；改成 `UNION` 又可能去掉原本合法的重复行。外连接中把右表过滤从 `WHERE` 移到 `ON` 也会改变空扩展行是否保留。先用边界数据核对结果，再用 `EXPLAIN` 检查谓词位置。对日期函数可考虑等价半开区间，不能假定任意函数都有单值逆函数。

### 5.3 验证谓词下推


```sql
EXPLAIN VERBOSE SELECT ...;

-- 查看 OlapScanNode 中的 PREDICATES 部分
-- 如果条件出现在 Scan 节点，说明成功下推

```


---

## 六、Runtime Filter

### 6.1 什么是 Runtime Filter

Runtime Filter 是 Doris 在执行期间动态生成的过滤条件。在 Hash Join 的 Build 阶段，根据 Build 侧数据构建 Filter，然后推送到 Probe 侧的 Scan 算子，提前过滤不可能匹配的数据。


```text
原始流程：
  Scan A (1亿行) ──JOIN──→ Scan B (1万行)
                              ↓
                    结果：1万行

使用 Runtime Filter：
  Scan B (1万行) → 构建 Bloom Filter (包含 B.id 的所有值)
       ↓ 推送 Filter
  Scan A (应用 Filter，跳过不在 B.id 中的行)
       ↓
  1亿行 → 过滤后仅剩 2万行 → JOIN
       ↓
  结果：1万行

效果：A 表扫描量从 1亿降到 2万

```

> [!note] 批注｜过滤后的输出行数与物理读取量分开看
>
> Join Runtime Filter 用 Build 侧的键削减 Probe 侧候选。若过滤能在存储层跳过块，物理读取量也可能下降；若数据已读到内存才过滤，下降的是 Scan 输出和后续 Join 工作量。连接键有重复时，结果行数也不由 Build 行数上限决定。


### 6.2 Runtime Filter 类型

| 类型 | 数据结构 | 适用场景 | 误判率 |
| --- | --- | --- | --- |
| IN | HashSet | Build 侧行数 < 1024 | 0（精确） |
| Bloom Filter | 布隆过滤器 | Build 侧行数适中 | 有（可调） |
| MinMax | Min/Max 范围 | Join Key 是有序数值 | 有（范围外精确） |

### 6.3 参数调优


```sql
-- 全局启用/禁用（默认启用）
SET runtime_filter_mode = "GLOBAL";  -- GLOBAL/LOCAL/OFF

-- 控制类型
SET runtime_filter_type = "IN_OR_BLOOM_FILTER,MIN_MAX";

-- Bloom Filter 大小
SET runtime_bloom_filter_min_size = 1048576;   -- 1MB
SET runtime_bloom_filter_max_size = 16777216;  -- 16MB

-- 等待超时（Probe 端等待 Filter 到达的最大时间）
SET runtime_filter_wait_time_ms = 1000;  -- 1秒

```

> [!warning] 核对说明｜4.x 文档用位掩码设置过滤器类型
>
> `runtime_filter_type` 的 `IN=1`、`BLOOM=2`、`MIN_MAX=4`、`IN_OR_BLOOM=8` 可求和；原文想表达的组合在此写法下是 `SET runtime_filter_type = 12`。调大等待时间会增加无收益时的等待开销，先根据实际 Profile 确认过滤器未就绪或错过了扫描。[Runtime Filter 调优](https://doris.apache.org/docs/4.x/query-acceleration/optimization-technology-principle/runtime-filter/)


### 6.4 Runtime Filter 生效判断


```sql
EXPLAIN SELECT ...;

-- 执行计划中查看：
-- HashJoinNode:
--   RF000[bloom] <- table_B.id
--
-- OlapScanNode (table_A):
--   runtime filters: RF000[bloom] -> table_A.user_id

```


如果 Scan 节点没有显示 runtime filters，可能原因：

- Filter 类型不匹配
- 跨 Fragment 传输超时
- Join Key 类型不匹配

> [!warning] 核对说明｜计划与运行时回答不同问题
>
> `EXPLAIN` 检查规划是否生成且下推了过滤器，不能证明过滤器已到达 Scan；网络超时也不能解释计划里根本没有过滤器。对实际执行看 `RuntimeFilterState` 与过滤行数。`READY` 但过滤很少时，应检查 Build 键覆盖率，而不是继续加等待时间。[Runtime Filter 生效判断](https://doris.apache.org/docs/4.x/query-acceleration/optimization-technology-principle/runtime-filter/)

---

## 七、表达式改写与优化

### 7.1 常量折叠


```sql
-- 优化前
SELECT * FROM t WHERE dt = DATE_ADD('2026-07-01', INTERVAL 1 DAY);
-- 优化后（编译期计算）
SELECT * FROM t WHERE dt = '2026-07-02';

```


### 7.2 隐式转换消除


```sql
-- 问题 SQL：user_id 是 BIGINT，但传入字符串
SELECT * FROM t WHERE user_id = '12345';
-- 如果触发隐式转换 CAST(user_id AS VARCHAR)，索引失效

-- 建议：保持类型一致
SELECT * FROM t WHERE user_id = 12345;

```


### 7.3 子查询改写


```sql
-- IN 子查询 → Semi Join
SELECT * FROM orders WHERE user_id IN (SELECT id FROM vip_users);
-- 等价于
SELECT o.* FROM orders o LEFT SEMI JOIN vip_users v ON o.user_id = v.id;

-- EXISTS 子查询 → Semi Join
SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id);
-- Nereids 自动改写为 Semi Join

```

> [!note] 批注｜Semi Join 保留左表行，不传播右表重复
>
> `IN` 和 `EXISTS` 示例只判断是否存在匹配。若把它换成普通内连接，右表同一个键出现多次会放大左表结果。这里也不能反推 `NOT IN` 和 `NOT EXISTS` 总等价；尤其右侧含 `NULL` 时，必须按原谓词的三值逻辑验证。


---

## 八、Query Hints

### 8.1 常用 Hints


```sql
-- 设置单查询变量
SELECT /*+ SET_VAR(exec_mem_limit=8589934592, parallel_fragment_exec_instance_num=8) */

  * FROM large_table;

-- 指定 Join 分发方式
SELECT * FROM t1 JOIN /*+ SHUFFLE */ t2 ON t1.id = t2.id;
SELECT * FROM t1 JOIN /*+ BROADCAST */ t2 ON t1.id = t2.id;

-- 指定 Join 顺序（禁用 Reorder）
SELECT /*+ LEADING(t1 t2 t3) */ *
FROM t1 JOIN t2 ON ... JOIN t3 ON ...;

-- 指定使用哪个索引
SELECT * FROM t1 /*+ USE_INDEX(idx_city) */ WHERE city = 'hangzhou';

```


### 8.2 Hint 使用场景

| 场景 | Hint | 用途 |
| --- | --- | --- |
| 优化器选错 Join 顺序 | LEADING | 强制指定顺序 |
| 选错分发方式 | SHUFFLE/BROADCAST | 手动指定 |
| 内存不足 | SET\_VAR(exec\_mem\_limit=…) | 提高限制 |
| 需要更高并行度 | SET\_VAR(parallel\_fragment\_exec\_instance\_num=…) | 单查询覆盖 |

> [!warning] 核对说明｜看 Hint 是否用上，再保留对照数据
>
> 优先用 `EXPLAIN SHAPE PLAN` 核对 `LEADING` 等 Hint 是否被采纳；Join 分发使用官方方括号写法。原文的 `USE_INDEX` 不能按通用 4.x 语法照搬，索引收益以实际 Scan/Profile 核对。调整单查询内存或并行度前先确定资源瓶颈，避免把代价转移给并发查询。[Leading Hint](https://doris.apache.org/docs/4.x/query-acceleration/hints/leading-hint/) · [Distribute Hint](https://doris.apache.org/docs/4.x/query-acceleration/hints/distribute-hint/)

---

## 九、常见慢查询模式与解决方案

### 9.1 诊断流程


```sql
-- Step 1: 查看执行计划
EXPLAIN VERBOSE SELECT ...;

-- Step 2: 执行并查看 Profile
SET enable_profile = true;
SELECT ...;
SHOW QUERY PROFILE "/";

-- Step 3: 定位瓶颈算子
-- 看哪个 Operator 的 Time 最长
-- 看 RowsRead vs RowsReturned 的比例

```


### 9.2 常见模式

| 模式 | 表现 | 原因 | 解决 |
| --- | --- | --- | --- |
| 全表扫描 | ScanRows 极大 | 无分区裁剪/无索引命中 | 加分区条件/建索引 |
| 数据倾斜 | 某个 Instance 执行时间远大于其他 | 某个分桶键值集中 | 换分桶键/加 Bucket |
| Join 爆炸 | Join 后行数远大于输入 | 一对多关系未预期 | 检查 Join 条件/加去重 |
| Shuffle 过重 | Exchange 时间长 | 大表 Shuffle | Colocate/Bucket Shuffle |
| 内存不足 Spill | 执行时间暴增 | 中间结果太大 | 增大内存/优化 SQL |
| Runtime Filter 未生效 | Scan 行数未减少 | Filter 超时/类型不匹配 | 增大等待时间/检查类型 |

### 9.3 典型优化案例


```sql
-- 案例：UV 统计查询耗时 30s
-- 原始 SQL
SELECT dt, COUNT(DISTINCT user_id) FROM events 
WHERE dt BETWEEN '2026-07-01' AND '2026-07-07'
GROUP BY dt;

-- 优化方案 1：使用 BITMAP 预聚合（参见进阶篇第 2 篇）
-- 优化方案 2：如果只能在 Duplicate 表上查，增加并行度
SET parallel_pipeline_task_num = 16;

-- 优化方案 3：确保统计信息准确
ANALYZE TABLE events (dt, user_id);

```

> [!note] 批注｜三种手段解决不同瓶颈
>
> Bitmap 预聚合改变扫描粒度，更新统计影响优化器的选择，提高并行度则改变运行时资源分配。先确认按日 UV 的结果口径，再逐项试验并对照扫描行数、Shuffle 量、内存和耗时，避免无法判断哪一项生效。


---

## 十、小结

| 要点 | 内容 |
| --- | --- |
| Nereids CBO | 基于 Cascades 框架，依赖统计信息选择最优计划 |
| 统计信息 | ANALYZE TABLE 收集，开启 auto\_analyze 保持新鲜 |
| Join 优化 | Reorder + 类型选择（Broadcast/Shuffle/Colocate） |
| Runtime Filter | 执行期动态生成 Bloom/MinMax/IN Filter，大幅减少 Scan |
| 调优工具 | EXPLAIN + Profile + Hints |
