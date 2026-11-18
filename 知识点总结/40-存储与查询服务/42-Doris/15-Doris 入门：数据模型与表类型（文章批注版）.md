---
title: "Doris 入门篇03：数据模型与表类型"
author: "外星人的知识杂谈"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484313&idx=1&sn=f26b0379593d85d5c08a8611530527ac&chksm=c1bf0984f6c880926be9d79faa6ebad02671f3dda03a213a4629b688556301b5fd384a8142dc#wechat_redirect"
published_at: "2026-07-25T19:16:00+08:00"
annotated_at: "2026-11-18"
cssclasses:
  - wechat-article
  - doris-intro-annotated
tags:
  - doris
  - 文章批注
---

# Doris 入门篇03：数据模型与表类型

<div class="article-meta">外星人的知识杂谈 · 2026-07-25</div>

[原文链接](https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484313&idx=1&sn=f26b0379593d85d5c08a8611530527ac&chksm=c1bf0984f6c880926be9d79faa6ebad02671f3dda03a213a4629b688556301b5fd384a8142dc#wechat_redirect)

<!-- 保留原文正文、示意代码与顺序；根据捕获网页恢复11个三级标题。“批注”“核对说明”为补充。移除装饰封面及下一篇导航；代码未在集群执行。 -->

> **定位：** 入门篇第 3 篇 | 理解 Doris 三种数据模型的语义及 MoR/CoW 实现
>
> **阅读前提：** 已了解 Tablet/Rowset/Segment 存储层次（第 02 篇）

---

## 一、背景动机

建表时选择数据模型，本质是在回答一个问题：**相同 Key 的多行数据写入后，Doris 应该如何处理？**

- 全部保留？（明细日志场景）
- 只留最新一行？（状态更新场景）
- 按规则自动聚合？（指标累加场景）

选错模型，后期几乎无法补救（只能重建表导数据），所以这是建表时最重要的决策。

<!-- codex-annotation-start: model-grain -->
> [!note] 批注｜先定义一行代表什么，再决定同键记录怎样处理
> 事件明细、订单当前状态和每日指标，分别对应不同的数据粒度。相同订单可以产生多次事件；保留事件历史与保留一条当前状态，是两种不同要求，不能只凭“都有 order_id”就选择相同模型。
>
> 原文“无法补救”需要限定：表模型不能直接当作普通属性随意切换，但有原始明细或可重放来源时，可以新建正确模型并重新导入。真正不能仅从聚合表还原的，是已合并丢失的事件级细节；重建表不会自动找回这些信息。小结中的“不可逆性”也应按这个范围理解。[Doris 数据模型与变更边界](https://doris.apache.org/docs/dev/key-features/data-model/)
<!-- codex-annotation-end: model-grain -->

---

## 二、三种数据模型

### 2.1 Duplicate Key（明细模型）


```sql
CREATE TABLE user_log (
    dt        DATE,
    user_id   BIGINT,
    action    VARCHAR(64),
    page      VARCHAR(128),
    duration  INT
)
DUPLICATE KEY(dt, user_id)   -- Key 列仅用于排序，不去重
PARTITION BY RANGE(dt) (...)
DISTRIBUTED BY HASH(user_id) BUCKETS 16;

```

<!-- codex-annotation-start: sql-fragments -->
> [!warning] 核对说明｜原文建表代码是示意，尚不能直接执行
> 本篇六段建表代码都用 `PARTITION BY RANGE(...) (...)` 省略了实际分区定义，不是可以直接复制执行的完整 SQL。运行前必须根据实际数据范围补齐合法分区，或选择目标版本支持的自动分区方式；不能把括号中的 `...` 原样提交。
>
> 下面仍保留全部原始代码及参数，未擅自补入分区日期，也没有在集群上执行。本篇两段 Unique Key 示例还存在分区列约束问题，已在相邻位置另作说明。[Doris 3.x 手动分区语法](https://doris.apache.org/docs/3.x/table-design/data-partitioning/manual-partitioning/)
<!-- codex-annotation-end: sql-fragments -->


**语义：** 写一行存一行，完全不做任何合并或去重。

- Key 列的作用仅仅是**排序**（决定数据在 Segment 内的物理顺序）
- 适合：原始日志、埋点明细、事件流——任何需要保留每一条原始记录的场景
- 优点：写入最快（无额外开销）
- 缺点：查询聚合完全在运行时计算，无法利用预聚合加速

<!-- codex-annotation-start: duplicate-compaction -->
> [!warning] 核对说明｜保留重复记录不等于不合并物理文件
> Duplicate Key 不按这些 Key 列把多条业务记录折叠成一条；相同内容再次作为新记录成功写入，仍会保留。Key 主要组织排序，不能替代事件 ID 去重，也不会仅因表名叫“日志”就保证上游重试不会重复计数。
>
> 它仍会做 Compaction，把多个 Rowset / 小文件整理成较少的有序文件，同时保留应有的业务记录。排序、索引、副本和附加物化视图也有成本，因此“完全不做任何合并”“写入最快、无额外开销”都表述过满。基表自身不预聚合与能否另建聚合物化视图，要分开判断。[Duplicate Key 模型](https://doris.apache.org/docs/3.x/table-design/data-model/duplicate/)、[Compaction 机制](https://doris.apache.org/docs/3.x/admin-manual/trouble-shooting/compaction/)
<!-- codex-annotation-end: duplicate-compaction -->

### 2.2 Aggregate Key（聚合模型）


```sql
CREATE TABLE site_metrics (
    dt        DATE,
    site_id   INT,
    city      VARCHAR(64),
    pv        BIGINT       SUM,          -- 自动累加
    uv        BITMAP       BITMAP_UNION,  -- 自动 bitmap 合并
    max_cost  DOUBLE       MAX            -- 自动取最大值
)
AGGREGATE KEY(dt, site_id, city)
PARTITION BY RANGE(dt) (...)
DISTRIBUTED BY HASH(site_id) BUCKETS 8;

```


**语义：** 相同 Key 的行，在写入/Compaction 时自动按声明的聚合函数合并。

支持的聚合函数：

| 函数 | 语义 | 典型用途 |
| --- | --- | --- |
| SUM | 累加 | PV、金额 |
| MAX / MIN | 取极值 | 最大耗时、最早时间 |
| REPLACE | 替换为最新值 | 状态字段 |
| REPLACE\_IF\_NOT\_NULL | 非空才替换 | 部分更新 |
| BITMAP\_UNION | Bitmap 合并 | 精确 UV |
| HLL\_UNION | HLL 合并 | 近似 UV |

<!-- codex-annotation-start: aggregate-input -->
> [!note] 批注｜聚合函数决定怎样解释重复输入
> `SUM` 适合累加增量。同一批业务事件如果以两个成功的新批次重复导入，PV 等值会再次累加；Bitmap 并集对同一个 ID 则仍只保留一个集合成员。于是同表可能出现 UV 没变、PV 翻倍，不能用“UV 正常”证明输入没有重复。
>
> `REPLACE` 的替换顺序也不自动取业务时间最大的一行；导入批次顺序与同批次替换规则需要单独核对。`REPLACE_IF_NOT_NULL` 忽略新输入中的 NULL，所以还要确认业务是否需要把已有值显式更新为 NULL。Stream Load 的 label 可在其保留规则内防止批次重试，仍不能替代业务事件去重。[聚合模型更新语义](https://doris.apache.org/docs/3.x/data-operate/update/update-of-aggregate-model/)、[Stream Load label 机制](https://doris.apache.org/docs/3.x/data-operate/import/import-way/stream-load-manual/)
<!-- codex-annotation-end: aggregate-input -->

**关键理解：** 聚合发生在**写入链路**（MemTable flush 时合并）和**Compaction 链路**（后台合并时再次聚合），所以查询时扫描行数远小于写入行数。

<!-- codex-annotation-start: aggregate-read -->
> [!warning] 核对说明｜Aggregate 还需要在查询阶段合并
> 聚合发生在导入、Compaction 和查询三个阶段。相同聚合键的数据可能仍分布在多个未合并的 Rowset 中，查询需要继续合并，才能得到该键的最终逻辑结果。不能把一次 flush 理解成“全表这个键的聚合已经完成”。
>
> 例如第一批同键 PV 为 10，第二批为 5，后台尚未把它们合成一个物理结果时，查询仍应得到 15。实际减少多少行取决于相同聚合键的重复程度、文件状态和查询范围；如果几乎每条输入的完整 Key 都不同，就不能保证扫描行数“远小于”输入行数。[Doris Aggregate Key 的三个聚合阶段](https://doris.apache.org/docs/3.x/table-design/data-model/aggregate/)
<!-- codex-annotation-end: aggregate-read -->

### 2.3 Unique Key（唯一键模型）


```sql
CREATE TABLE order_status (
    order_id   BIGINT,
    user_id    BIGINT,
    status     VARCHAR(32),
    amount     DECIMAL(10,2),
    update_time DATETIME
)
UNIQUE KEY(order_id)
PARTITION BY RANGE(update_time) (...)
DISTRIBUTED BY HASH(order_id) BUCKETS 16;

```

<!-- codex-annotation-start: unique-partition-key -->
> [!warning] 核对说明｜update_time 未进入 Unique Key，示例存在建表约束问题
> 按本次核对的 Doris 3.x 普通 Range 分区规则，Unique 表的分区列必须包含在 Key 中。原例按 `update_time` 分区，却只声明 `UNIQUE KEY(order_id)`，即使补齐分区范围也不符合该约束。
>
> 直接把 `update_time` 加进 Key 并不能无条件修好业务语义：完整键将变为 `(order_id, update_time)`，同一订单每次修改时间变化都会成为不同 Key，无法按订单号覆盖当前状态；Key 列在字段定义中的前缀顺序也需调整。应先决定保留全表订单当前状态，还是保留按时间区分的历史版本，再确定键和分区设计。[分区列约束](https://doris.apache.org/docs/3.x/table-design/data-partitioning/manual-partitioning/)、[建表 Key 前缀要求](https://doris.apache.org/docs/3.x/sql-manual/sql-statements/table-and-view/table/CREATE-TABLE/)
<!-- codex-annotation-end: unique-partition-key -->


**语义：** 相同主键（Key 列）只保留**最新一行**（整行覆盖）。

<!-- codex-annotation-start: unique-latest-partial -->
> [!note] 批注｜“最新”由什么决定，少给字段又会怎样
> Unique 表通过 UPSERT 处理同键数据：键不存在则插入，存在则更新，不是发现重复键后拒绝写入。这里的“只保留”指查询的逻辑结果，磁盘上可能暂时仍有旧版本，后面会通过读取规则或 Compaction 处理。
>
> 普通的 `update_time` 列不会自动参与新旧判断。业务变更可能乱序到达时，需要显式配置 Sequence 列并保证序列值能表达所需顺序；序列相同的冲突也要另行定义，不能仅凭时间列存在就认定旧事件不会覆盖新状态。[Unique 更新顺序与 Sequence](https://doris.apache.org/docs/3.x/data-operate/update/unique-update-concurrent-control/)
>
> 整行 UPSERT 与部分列更新也不同。默认整行语义下，未提供的 Value 列可能被补成默认值或 NULL，并不自动保留旧值。只想改订单状态时，需要按目标版本和导入方式启用部分列更新；MoW 模式本身不会自动替用户作这个选择。[部分列更新配置](https://doris.apache.org/docs/3.x/data-operate/update/partial-column-update/)
<!-- codex-annotation-end: unique-latest-partial -->

本质上 Unique Key 是 Aggregate Key 的特例：所有 Value 列的聚合函数都是`REPLACE`。

<!-- codex-annotation-start: unique-replace -->
> [!warning] 核对说明｜这一等价应限定在历史 MoR 实现
> 官方历史说明把“相当于所有 Value 使用 REPLACE”用于解释 Unique 的 MoR 实现。它有助于理解同键替换，但不能推广为 MoW 的实现等价：MoW 要定位旧行、维护 Delete Bitmap 并写入新行。部分列更新、Sequence 和删除语义仍需分别检查，不能把 Aggregate 的 REPLACE 表当作 Unique 表直接替换。[官方历史数据模型说明](https://apache.googlesource.com/doris/+/07ea35020142a2ec273c565cf265fd57c346d302/docs/zh-CN/docs/data-table/data-model.md)
<!-- codex-annotation-end: unique-replace -->

---

## 三、三模型对比

| 维度 | Duplicate Key | Aggregate Key | Unique Key |
| --- | --- | --- | --- |
| 数据保留 | 全部保留 | 按 Key 合并 | 按 Key 去重（保留最新） |
| 写入开销 | 最低 | 中（需计算聚合） | 中-高（需维护去重逻辑） |
| 查询聚合 | 运行时全量计算 | 预聚合，读少量行 | 运行时或预合并 |
| 数据更新 | 不支持（追加写） | 通过聚合语义实现 | 原生支持 |
| Rollup/MV | 不支持降维 Rollup | 支持 | 有限支持 |
| 典型场景 | 日志、埋点 | UV 统计、指标累加 | 订单状态、用户画像 |

<!-- codex-annotation-start: materialized-view -->
> [!warning] 核对说明｜Duplicate 可以通过附加物化视图预聚合
> 表中的“Duplicate 不支持降维 Rollup”“无法利用预聚合”不能笼统成立。Duplicate 基表保留明细，但可以额外建立聚合物化视图；Doris 3.x 同步物化视图文档就给出了在明细表上按部分列分组并计算 COUNT / SUM 的例子。
>
> 要分别检查基表模型、同步或异步物化视图支持的定义、刷新时效和查询是否实际命中改写。异步视图的结果还有刷新时间边界，不能仅因创建成功就认定所有查询都使用了预聚合。表中的写入开销与查询性能也应通过相同负载验证。[同步物化视图](https://doris.apache.org/docs/3.x/sql-manual/sql-statements/table-and-view/sync-materialized-view/CREATE-MATERIALIZED-VIEW/)、[异步物化视图](https://doris.apache.org/docs/3.x/query-acceleration/materialized-view/async-materialized-view/functions-and-demands/)
<!-- codex-annotation-end: materialized-view -->

### 选型决策树


```
需要保留原始明细吗？
  ├─ 是 → Duplicate Key
  └─ 否 → 需要自定义聚合逻辑吗？
              ├─ 是（SUM/BITMAP/HLL等） → Aggregate Key
              └─ 否（只需主键去重/更新） → Unique Key

```

<!-- codex-annotation-start: model-selection -->
> [!note] 批注｜决策树适合选择一张表，不能替代整条链路设计
> 一条业务链路可以同时需要明细、当前状态和汇总表：事件明细用于追溯或重算，Unique 状态表服务当前状态查询，Aggregate 表保存已确定粒度的指标。选择其中一种模型，不意味着其余数据需求消失。
>
> 判断 Key 是否合适，可以逐条检查两次同键输入：第二条应多保留一条事件、覆盖旧状态，还是把某个指标增量加进去？如果这三个答案对应不同下游需求，就应考虑分别维护，而不是让一张表同时承担相互冲突的语义。
<!-- codex-annotation-end: model-selection -->


---

## 四、MoR 与 CoW：Unique Key 的两种实现

Unique Key 表需要实现“相同主键只保留最新行”的语义，有两种工程实现路径：

### 4.1 Merge-on-Read（MoR，读时合并）

**写入时：**

- 直接追加写入新数据，不检查是否有旧行存在
- 写入速度快，与 Duplicate Key 相当

**读取时：**

- 扫描所有版本的 Rowset
- 按主键做 merge，只返回最新版本的行
- 需要额外的 CPU 开销进行去重


```
写入流程：
Row(order_id=1, v1) → Rowset[2,2]
Row(order_id=1, v2) → Rowset[3,3]  ← 不覆盖旧数据

查询流程：
扫描 Rowset[2,2] + Rowset[3,3] → merge by order_id → 返回 v2

```


**优点：** 写入快，吞吐高
**缺点：** 读放大（Rowset 越多，merge 开销越大）

<!-- codex-annotation-start: mor-read -->
> [!note] 批注｜MoR 合并的是当前快照需要的候选版本
> “扫描所有版本”应理解为读取范围及当前快照所需的候选 Rowset，再按键解析逻辑结果；不是每次都扫描全表或所有已过期历史文件。分区、Key 条件和索引仍可能缩小范围，具体要看实际计划。
>
> 同键替换还限制了某些过滤的提前执行。例如旧行状态为“已支付”，新行状态为“已取消”，如果先按“已支付”过滤掉新行，再合并，就可能错误地把旧状态当成最终结果。MoR 的成本和优化限制来自这种必须先判断哪个版本有效的依赖关系。[Unique 模型的 MoR 读取机制](https://doris.apache.org/docs/3.x/table-design/data-model/unique/)
<!-- codex-annotation-end: mor-read -->

### 4.2 Merge-on-Write（MoW / CoW，写时合并）

Doris 2.0+ 默认启用 MoW 模式（通过`enable_unique_key_merge_on_write = true`）。

<!-- codex-annotation-start: mow-default -->
> [!warning] 核对说明｜MoW 默认版本是 2.1，MoW 与 CoW 也不是同义词
> MoW 在 Doris 1.2 引入，从 2.1 起成为新建 Unique 表的默认实现；原文此处及后续表格、建表注释、小结中的“2.0+ 默认”应统一按这个边界理解。升级不会自动把已有 MoR 表转换成 MoW，应查看实际表的 `SHOW CREATE TABLE` 属性。[Doris 2.1 发布说明](https://doris.apache.org/blog/release-note-2.1.0/)
>
> 本节标题中的“MoW / CoW”也需区分。MoW 指 Doris 把同键版本解析工作提前到写入阶段；CoW 通常指更新时复制、重写受影响的数据文件。Doris 的 Delete Bitmap 方案通常追加新行并标记旧行，不等同于每次更新立即重写旧数据所在的整个文件。[Doris 更新机制](https://doris.apache.org/docs/3.x/data-operate/update/update-overview/)、[Iceberg 文件更新机制](https://iceberg.apache.org/docs/latest/spark-writes/)
<!-- codex-annotation-end: mow-default -->

**写入时：**

- 写入新行的同时，标记旧行为“已删除”（通过 Delete Bitmap）
- 需要查找旧行位置，有一定额外开销

**读取时：**

- 直接根据 Delete Bitmap 跳过已删除行
- 无需 merge 多版本，读取简单高效


```
写入流程：
Row(order_id=1, v1) → Rowset[2,2], Bitmap: {}
Row(order_id=1, v2) → Rowset[3,3], Bitmap: {Rowset[2,2] 中 order_id=1 的行}

查询流程：
扫描时跳过 Bitmap 标记的行 → 直接返回 v2（无需 merge）

```

<!-- codex-annotation-start: bitmap-snapshot -->
> [!note] 批注｜Delete Bitmap 标记旧行，不会立刻改写旧文件
> 第二次写入仍会生成包含新行的数据文件，同时记录哪些旧行不再属于新快照的有效结果。旧 Segment 的业务内容不会因此原地变成新值；后台 Compaction 再按有效性规则整理文件并清理旧数据。
>
> 图中的 Bitmap 是帮助理解行位置标记的简图，不能当成所有查询共享的、忽略版本的即时开关。系统还要遵守事务提交、发布和查询快照边界；已经开始的读取不能因后来一次写入就任意丢掉它所需的旧版本。[Doris 主键更新与版本机制](https://doris.apache.org/docs/3.x/data-operate/update/update-overview/)
<!-- codex-annotation-end: bitmap-snapshot -->


**优点：** 读取快，无读放大
**缺点：** 写入时需要维护 Delete Bitmap，有锁竞争（高并发写入场景）

### 4.3 MoR vs MoW 对比

| 维度 | MoR（读时合并） | MoW（写时合并） |
| --- | --- | --- |
| 写入性能 | 高（纯追加） | 中（需更新 Delete Bitmap） |
| 读取性能 | 低（需 merge） | 高（直接跳过删除行） |
| 适合场景 | 写多读少 | 读多写少（绝大多数场景） |
| Doris 默认 | 2.0 前默认 | 2.0+ 默认 |
| 并发写入 | 无锁 | 有 Bitmap 锁竞争 |

<!-- codex-annotation-start: mow-performance -->
> [!warning] 核对说明｜省去读时主键合并，不等于没有读放大或锁
> MoW 省去了 MoR 在查询时按主键合并多个行版本的工作，仍可能访问多个 Rowset / Segment、检查 Bitmap 和读取候选页。部分列更新还可能在写入时读取旧行以补齐字段，因此“读取快、无读放大”不能理解成整个链路没有额外读取。
>
> MoR 没有同样的 MoW Bitmap 维护步骤，也不能推出整个实现“无锁”。MoW 的并发控制方式、竞争位置和成本受版本、部署模式、导入及 Compaction 并发影响。“读多写少”可以帮助初步判断，但最终应测量查询延迟、写入吞吐和资源占用。[Doris 更新与并发机制](https://doris.apache.org/docs/3.x/data-operate/update/update-overview/)
<!-- codex-annotation-end: mow-performance -->

### 4.4 类比数据湖的 CoW/MoR

如果你了解 Iceberg/Hudi/Delta 的 CoW/MoR 概念：

|  | Doris Unique MoW | Iceberg CoW | Hudi MoR |
| --- | --- | --- | --- |
| 核心机制 | Delete Bitmap 标记 | 重写整个文件 | Log + Base merge |
| 写放大 | 低（只更新 bitmap） | 高（全文件重写） | 低 |
| 读效率 | 高 | 高 | 中（需 merge log） |

Doris MoW 的 Delete Bitmap 方案可以看作是 CoW 和 MoR 的折中：不需要像 Iceberg CoW 那样重写整个文件，又能保证读取时无需 merge。

<!-- codex-annotation-start: cow-analogy -->
> [!warning] 核对说明｜“只更新 bitmap”漏掉了新行和后台合并成本
> 表中 Doris MoW 的“低（只更新 bitmap）”不完整：写入需要保存新行及相关索引，还要维护旧行有效性；后台 Compaction 会带来额外读取和重写。它通常避免的是本次更新立即复制整个旧文件，不是只改变几个比特就完成所有工作。
>
> 不同系统的 CoW / MoR 还受文件大小、更新比例、缓存和维护策略影响。这个类比有助于区分“追加新行加标记”与“重写受影响文件”，不能仅凭名称或表中三个“高 / 低”比较整体写放大与查询性能。[Doris 更新成本](https://doris.apache.org/docs/3.x/data-operate/update/update-overview/)、[Iceberg 写入与文件重写](https://iceberg.apache.org/docs/latest/spark-writes/)
<!-- codex-annotation-end: cow-analogy -->

---

## 五、建表示例与场景映射

### 场景 1：用户行为日志（Duplicate）


```sql
CREATE TABLE user_behavior_log (
    event_time  DATETIME,
    user_id     BIGINT,
    event_type  VARCHAR(32),
    page_id     VARCHAR(128),
    duration_ms INT,
    properties  JSON
)
DUPLICATE KEY(event_time, user_id)
PARTITION BY RANGE(event_time) (...)
DISTRIBUTED BY HASH(user_id) BUCKETS 32;

```


### 场景 2：每日活跃用户统计（Aggregate + BITMAP）


```sql
CREATE TABLE daily_active_users (
    dt          DATE,
    channel     VARCHAR(64),
    platform    VARCHAR(32),
    uv          BITMAP  BITMAP_UNION,
    pv          BIGINT  SUM
)
AGGREGATE KEY(dt, channel, platform)
PARTITION BY RANGE(dt) (...)
DISTRIBUTED BY HASH(channel) BUCKETS 8;

```

<!-- codex-annotation-start: bitmap-uv -->
> [!note] 批注｜Bitmap 保存用户集合，UV 要在查询范围内合并计数
> 仅把 `uv` 声明为 `BITMAP BITMAP_UNION` 还没有提供用户集合。导入时要把用户 ID 转成合法的 Bitmap，ID 还需与真实用户一一对应；字符串哈希存在碰撞，`BITMAP_HASH` 的结果不能无条件称作精确 UV。精确映射与目标函数的支持范围必须同时满足。[Bitmap 精确去重](https://doris.apache.org/docs/3.x/query-acceleration/distinct-counts/bitmap-precise-deduplication/)、[BITMAP_HASH 定义](https://doris.apache.org/docs/3.x/sql-manual/sql-functions/scalar-functions/bitmap-functions/bitmap-hash/)
>
> 查询多天或多个渠道的去重用户时，应在所需范围内对 `uv` 做并集，再计算集合大小，例如使用 `BITMAP_UNION_COUNT(uv)`。各渠道 UV 直接相加会重复统计跨渠道用户；对聚合表执行 `COUNT(*)` 统计的也不是用户集合成员数。`pv SUM` 需要的是可累加事件增量，不能把每日累计快照反复当新增量写入。
<!-- codex-annotation-end: bitmap-uv -->


### 场景 3：订单状态实时更新（Unique + MoW）


```sql
CREATE TABLE orders (
    order_id     BIGINT,
    user_id      BIGINT,
    status       VARCHAR(16),
    amount       DECIMAL(12,2),
    create_time  DATETIME,
    update_time  DATETIME
)
UNIQUE KEY(order_id)
PARTITION BY RANGE(create_time) (...)
DISTRIBUTED BY HASH(order_id) BUCKETS 16
PROPERTIES (
    "enable_unique_key_merge_on_write" = "true"  -- 2.0+ 默认即为 true
);

```

<!-- codex-annotation-start: orders-key -->
> [!warning] 核对说明｜create_time 分区同样需要检查完整唯一键
> 这个示例也把分区列 `create_time` 放在 `UNIQUE KEY(order_id)` 之外，按前述 3.x 约束不能直接建成。若改成复合键，必须同时调整 Key 列顺序，并确认每次更新都携带相同、稳定的创建时间；否则会形成另一个 Key，而不是覆盖原订单。
>
> “某订单写入了新分区”不会自动删除它在旧分区中的记录。改变完整 Key 或分区归属，需要明确处理旧键删除与新键插入；普通 UPDATE 也不能任意修改 Key 列。本例显式设为 `true` 可以表达 MoW 意图，但旁边“2.0+ 默认”的注释仍应按 2.1 边界理解。[Unique 分区约束](https://doris.apache.org/docs/3.x/table-design/data-model/unique/)、[UPDATE 的列限制](https://doris.apache.org/docs/3.x/sql-manual/sql-statements/data-modification/DML/UPDATE/)
<!-- codex-annotation-end: orders-key -->


---

## 六、常见误区

| 误区 | 正确理解 |
| --- | --- |
| “Duplicate Key 的 Key 用来去重” | 错。Key 仅决定排序，不做任何去重 |
| “Unique Key 写入前会查重” | MoW 模式确实会查旧行位置；MoR 模式不查，读时合并 |
| “Aggregate 表不能查明细” | 对。写入后明细已被合并丢失，无法还原 |
| “选 Unique 就够了，不需要 Aggregate” | 错。UV 用 BITMAP\_UNION 预聚合效果比 Unique + count distinct 好 100 倍 |

<!-- codex-annotation-start: misconception-boundaries -->
> [!warning] 核对说明｜聚合粒度仍可查询，“100 倍”没有测试依据
> Aggregate 表可以查询聚合键粒度的数据及聚合结果；不能还原的是已经合并掉的逐事件明细。例如按日期、站点、城市聚合后，可以查这些维度的 PV，但无法从 PV 总数恢复每一次访问的原始页面和时间。若 Key 本身更细，保留的信息范围也会不同。
>
> 原文未提供“100 倍”的数据集、UV 口径、SQL、版本、硬件或执行计划，不能把它作为固定性能结论。Aggregate + Bitmap 与 Unique 表统计解决的输入语义、结果粒度和维护成本也要先对齐，再在同等条件下比较查询和更新成本。[Aggregate Key 语义](https://doris.apache.org/docs/3.x/table-design/data-model/aggregate/)、[表设计与模型选择](https://doris.apache.org/docs/3.x/table-design/best-practice/)
<!-- codex-annotation-end: misconception-boundaries -->

---

## 七、小结

| 要点 | 内容 |
| --- | --- |
| 三种模型 | Duplicate（全保留）、Aggregate（写时聚合）、Unique（主键去重） |
| 选型核心 | 是否需要明细？是否需要自定义聚合？是否需要更新？ |
| MoR vs MoW | 读时合并 vs 写时标记（Delete Bitmap），2.0+ 默认 MoW |
| 不可逆性 | 模型选错只能重建表，务必在建表前想清楚 |
