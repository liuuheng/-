---
title: "Doris 进阶篇01：建表与库表优化"
author: "外星人的知识杂谈"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484344&idx=1&sn=125a493e0365a5eb95f6a067e3414042&chksm=c1bf09a5f6c880b3c22be1438bced06d2b0cabe22b854e0de167bacbc5249474269ea0066be2#rd"
published_at: "2026-07-31T20:18:00+08:00"
annotated_at: "2026-11-18"
cssclasses:
  - wechat-article
  - doris-intro-annotated
tags:
  - doris
  - 文章批注
---

# Doris 进阶篇01：建表与库表优化

<div class="article-meta">外星人的知识杂谈 · 2026-07-31</div>

[原文链接](https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484344&idx=1&sn=125a493e0365a5eb95f6a067e3414042&chksm=c1bf09a5f6c880b3c22be1438bced06d2b0cabe22b854e0de167bacbc5249474269ea0066be2#rd)

<!-- 正文按原文顺序保留；导读取自同页微信摘要。批注与核对说明为新增，示例命令未在集群运行。 -->

> **定位（原文摘要）：** 进阶篇第 1 篇 | 掌握生产环境建表的核心决策与优化手段。
>
> **阅读前提（原文摘要）：** 入门篇全部（尤其第 02/03 篇的存储模型与数据模型）

---

## 一、背景动机

建表是 Doris 使用中影响最深远的决策。一张表建错，后续所有调优都是治标不治本。生产环境中 80% 的性能问题可以追溯到建表阶段的设计失误：分区粒度不对、分桶数拍脑袋、模型选错、Key 列顺序不合理。

> [!warning] 核对说明｜先用计划和实测定位问题
>
> 原文的「80%」没有给出样本或统计口径，不能当成生产环境的比例。先用 `EXPLAIN` 看常见查询保留了哪些分区和 Tablet，再用 Query Profile 看实际扫描和 Exchange 开销。表结构是否需要改，取决于这些开销是否来自当前布局；如果慢在并发排队或下游算子，改分区未必有效。[查询 Profile](https://doris.apache.org/docs/4.x/query-acceleration/query-profile/)

---

## 二、分区策略

### 2.1 分区的本质

分区决定了数据的**生命周期管理粒度**和**查询裁剪粒度**。分区选错：

- 粒度太粗（月分区存日级数据）→ 查询裁剪效果差，每次扫描整月数据
- 粒度太细（小时分区）→ 元数据爆炸（Partition 数 × Bucket 数 × 副本数 = Tablet 总数）

> [!note] 批注｜逻辑 Tablet 与副本实例分开算
>
> 以存算一体部署的单个基表索引为例，逻辑 Tablet 数等于各分区桶数之和；各分区的桶数相同时才可写成「分区数 × 桶数」。再乘以对应的副本数，得到物理副本实例数。30 个日分区、每分区 16 桶、3 副本，得到 480 个基表逻辑 Tablet、1,440 个副本实例；还有 Rollup 时需另计它的 Tablet 和副本。[分区与分桶](https://doris.apache.org/docs/4.x/key-features/partitioning-and-bucketing/)

### 2.2 Range 分区（最常用）


```sql
-- 按天分区（最常见）
PARTITION BY RANGE(dt) (
    PARTITION p20260701 VALUES LESS THAN ("2026-07-02"),
    PARTITION p20260702 VALUES LESS THAN ("2026-07-03")
)

-- 动态分区（自动创建/删除）
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "16"
);

```

> [!warning] 核对说明｜分区名不约束日期，动态规则也不是立即执行
>
> `VALUES LESS THAN` 只写上界。示例的首个分区 `p20260701` 没显式限定从 7 月 1 日开始，因此也可能接收更早的日期；若只想包含 7 月 1 日，可用 `VALUES [("2026-07-01"), ("2026-07-02"))` 明确两端。动态分区由调度器按时间单位创建或删除，不是每次写入时触发。调整 `start` 前要核对保留期和补数范围；`dynamic_partition.buckets` 会覆盖动态新建分区的桶数。[手动分区](https://doris.apache.org/docs/4.x/table-design/data-partitioning/manual-partitioning/) · [动态分区](https://doris.apache.org/docs/4.x/table-design/data-partitioning/dynamic-partitioning/)


### 2.3 List 分区


```sql
-- 按业务线分区
PARTITION BY LIST(business) (
    PARTITION p_music VALUES IN ("music"),
    PARTITION p_video VALUES IN ("video"),
    PARTITION p_live  VALUES IN ("live")
)

```


适合：数据量差异大的枚举维度（不同业务线数据量差距 10 倍以上）。

> [!note] 批注｜按业务线隔离取决于查询和管理需求
>
> List 分区的直接收益是业务线谓词可以裁剪其他分区，也能按业务线管理数据。仅有数据量差异不会让大分区内部的数据更均匀；大业务线如果仍写入同一个热 Tablet，要检查它的分桶键和桶数。不同业务量相近但经常需要独立查询或管理时，同样可以用 List 分区。

### 2.4 Auto Partition（2.1+）


```sql
-- 按天自动创建分区，无需提前定义
PARTITION BY RANGE(dt) ()
PROPERTIES (
    "dynamic_partition.enable" = "false",
    "auto_partition" = "true",
    "auto_partition_prefix" = "p"
);

```


Auto Partition 在数据写入时自动创建不存在的分区，免去手动维护或动态分区配置。

> [!warning] 核对说明｜Doris 用 AUTO PARTITION BY 声明自动分区
>
> 按 Doris 4.x 的 DDL，把示例的分区声明改为 `AUTO PARTITION BY RANGE (date_trunc(dt, 'day')) ()`；原示例的 `auto_partition` 和 `auto_partition_prefix` 不是这套 DDL 的写法。自动分区按写入数据中的 `dt` 创建缺失分区，动态分区按系统时间推进窗口。仅开启自动分区不表示旧分区会回收；保留策略需另按部署版本配置。可用 `SHOW PARTITIONS FROM 表名` 核对写入前后的分区范围。[自动分区](https://doris.apache.org/docs/4.x/table-design/data-partitioning/auto-partitioning/)

### 2.5 分区粒度选择

| 场景 | 建议粒度 | 原因 |
| --- | --- | --- |
| 日增量 < 1GB | 月分区 | 避免分区过多 |
| 日增量 1GB~100GB | 天分区 | 平衡裁剪效果与元数据量 |
| 日增量 > 100GB | 天分区 + 更多 Bucket | 单分区数据量大，靠 Bucket 并行 |
| 实时写入 + 短时效查询 | 小时分区（谨慎） | 仅用于高时效场景，注意 Tablet 总数 |

> [!note] 批注｜用常用查询时间范围和保留周期验证
>
> 设想查询常带一天的日期条件，且需要按日下线历史数据：日分区既能按日裁剪，也能按日删分区。换成月分区后，查询不一定真读完整个月，仍可能靠排序和索引跳过块，但不能在分区层直接排除同月其他日。表里的日增量档位只用于估算，最终看典型查询保留的分区、累计 Tablet 数和实际扫描量。

---

## 三、分桶策略

### 3.1 分桶数计算

**目标：** 单个 Tablet 数据量在 100MB ~ 1GB 之间。


```text
Bucket 数 = 单分区数据量 / 目标 Tablet 大小

示例：

- 日增量 10GB，天分区
- 目标 Tablet 大小 500MB
- Bucket 数 = 10GB / 500MB = 20

```

> [!warning] 核对说明｜用压缩后单分区大小估算，并检查大桶
>
> 公式中的数据量应和目标 Tablet 使用同一口径，优先用实际导入后的压缩数据量，不用源文件大小乘副本数代替。公式只给平均值：即使平均满足目标，热键所在桶也可能远大于平均。`100MB~1GB` 是原文的建议，不是产品限制；用 `SHOW TABLETS FROM 表名` 检查大小分布，再以写入、Compaction 和查询并行为依据调整。[官方 POC 分桶建议](https://doris.apache.org/docs/4.x/getting-started/before-you-start-the-poc/)


### 3.2 Hash 分桶 vs Random 分桶


```sql
-- Hash 分桶：查询可定位到具体 Tablet，减少扫描
DISTRIBUTED BY HASH(user_id) BUCKETS 16

-- Random 分桶：写入均匀，无热点
DISTRIBUTED BY RANDOM BUCKETS 16

```


|  | Hash 分桶 | Random 分桶 |
| --- | --- | --- |
| 写入均匀性 | 依赖 Key 分布 | 绝对均匀 |
| 查询优化 | WHERE user\_id=X 只扫 1 个 Tablet | 必须扫所有 Tablet |
| Colocate Join | 支持 | 不支持 |
| 适合场景 | 有明确的高频过滤键 | 日志/无固定查询模式 |

> [!warning] 核对说明｜一个 Tablet 是按分区计算
>
> 单列 `HASH(user_id)` 且查询包含可识别的单个等值条件时，通常是每个保留分区缩到一个桶，跨 3 个分区仍可扫描 3 个逻辑 Tablet。多列 Hash 需要规划器能确定分桶列组合；不能把只筛一个分桶列当成必然裁剪。Random 没有按键裁剪能力，在 4.x 中适用于 Duplicate Key 表；它减少按业务键倾斜的风险，但不同批次大小和单 Tablet 导入模式都可能造成短时不均。[数据分桶](https://doris.apache.org/docs/4.x/table-design/data-partitioning/data-bucketing/)

### 3.3 Bucket 数过多/过少的代价

| 问题 | 表现 | 解决 |
| --- | --- | --- |
| Bucket 过多 | 小文件多、Compaction 压力大、FE 元数据内存高 | 减少 Bucket 数 |
| Bucket 过少 | 单 Tablet 过大、查询并行度不足 | 增加 Bucket 数 |

> [!warning] 核对说明｜修改默认桶数不会重分已有分区
>
> `ALTER TABLE ... MODIFY DISTRIBUTION` 改的是新分区的默认桶数，已有分区保留原来的 Tablet 布局；若动态分区单独设置了 `dynamic_partition.buckets`，也要检查该覆盖值。治理历史大桶时应单独评估重写数据的成本，并用 `SHOW PARTITIONS` 对比已有和新建分区的 `Buckets`。[ALTER TABLE DISTRIBUTION](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/table-and-view/table/ALTER-TABLE-DISTRIBUTION/)

---

## 四、Key 列设计

### 4.1 Key 列顺序原则

前缀索引取 Key 列前 36 字节，查询时按从左到右前缀匹配。因此：

1. **高频等值过滤列放最前面**

   （如 dt, user\_id）
2. **基数从低到高排列**

   （先 dt 再 user\_id 再 event\_type）
3. **不要把大 VARCHAR 放在前面**

   （会占满 36 字节配额）

### 4.2 示例


```sql
-- 好：dt 在前，user_id 次之
DUPLICATE KEY(dt, user_id, event_type)

-- 差：url 占满 36 字节，后续列无法进入前缀索引
DUPLICATE KEY(url, dt, user_id)

```

> [!warning] 核对说明｜按常见谓词排排序键，不必固定由低基数到高基数
>
> 对 `DUPLICATE KEY(dt, user_id, event_type)`，`dt` 等值查询，以及同时按 `dt`、`user_id` 过滤的查询，可利用左侧前缀；只按 `user_id` 查询则不能依赖这个前缀。若 `dt` 已用于日分区且同一分区内取值固定，把它放在最左侧是否划算要看分区内的查询形态。`36` 字节只是上限；遇到 `VARCHAR` 即在该列截止，即使它没用完 36 字节，后续列也不会纳入前缀。[前缀索引](https://doris.apache.org/docs/4.x/table-design/index/prefix-index/)


---

## 五、Rollup 与物化视图

### 5.1 同步物化视图（Rollup）

在 Aggregate Key 表上创建降维 Rollup，查询时自动路由：


```sql
-- 底表 9 个 Key 列
CREATE TABLE metrics (...) AGGREGATE KEY(dt, hour, channel, platform, page, action, ...);

-- Rollup：只保留 3 个维度，查询自动命中
ALTER TABLE metrics ADD ROLLUP rollup_daily (dt, channel, pv SUM, uv BITMAP_UNION);

```


**核心价值：** 将 290 万行的扫描降到 27 行（真实案例，参见 Bitmap UV 优化）。

> [!warning] 核对说明｜ADD ROLLUP 列表填已有列名
>
> 按 Doris 4.x 文档，`ADD ROLLUP` 的列列表引用基表或来源索引中的已有列。若基表的 `pv` 和 `uv` 已分别声明 `SUM` 和 `BITMAP_UNION`，示意可写为 `ALTER TABLE metrics ADD ROLLUP rollup_daily (dt, channel, pv, uv);`；括号里不再写聚合类型。按示意列名分组维度是 `dt`、`channel` 两个，无法直接回答以 `page` 等被省略的列分组的查询。建索引任务完成后，用 `EXPLAIN` 核对实际扫描对象；原文给出的行数降幅只适用于所述案例。[ALTER TABLE ROLLUP](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/table-and-view/table/ALTER-TABLE-ROLLUP/) · [同步物化视图](https://doris.apache.org/docs/4.x/query-acceleration/materialized-view/sync-materialized-view/)

### 5.2 异步物化视图（2.1+）


```sql
CREATE MATERIALIZED VIEW mv_daily_uv AS
SELECT dt, channel, BITMAP_UNION(uv) AS uv, SUM(pv) AS pv
FROM metrics
GROUP BY dt, channel;

-- 刷新策略
REFRESH MATERIALIZED VIEW mv_daily_uv AUTO;

```


支持跨表 JOIN、定时/触发刷新、透明查询改写。

> [!warning] 核对说明｜刷新范围与触发方式分别配置
>
> `REFRESH MATERIALIZED VIEW mv_daily_uv AUTO` 是手工发起一次刷新，`AUTO` 让 Doris 判断需刷哪些分区，不能单独开启自动调度。定时或随基表提交触发要在建视图时声明对应触发策略。即使视图存在，也要检查刷新是否成功、视图分区是否够新、目标查询是否被改写；不要按已建视图就会命中来估算收益。[异步物化视图](https://doris.apache.org/docs/4.x/query-acceleration/materialized-view/async-materialized-view/functions-and-demands/) · [手工刷新](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/table-and-view/async-materialized-view/REFRESH-MATERIALIZED-VIEW/)

---

## 六、Colocate Group


```sql
-- 建表时指定 Colocate Group
CREATE TABLE orders (...) DISTRIBUTED BY HASH(user_id) BUCKETS 16
PROPERTIES ("colocate_with" = "user_group");

CREATE TABLE users (...) DISTRIBUTED BY HASH(user_id) BUCKETS 16
PROPERTIES ("colocate_with" = "user_group");

```


**效果：** 同 Group 的表，相同 Hash 值的数据一定在同一个 BE 上，JOIN 时无需 Shuffle（Local Join）。

**约束：** 同 Group 表必须相同 Bucket 数和分桶键类型。

> [!warning] 核对说明｜同组的表还要满足 Join 条件与布局稳定
>
> 同组表的对应桶副本按组规则放在同一组 BE 上；不是每个 Hash 值只对应一台机器。要走 Colocate Join，两侧应属于同组，分桶列类型和顺序、桶数及副本数符合组约束，Join 等值条件覆盖对应分桶列，组还要处于稳定状态。可用 `SHOW PROC '/colocation_group'` 看 `IsStable`，再用 `EXPLAIN` 查当前 Join 是否真的采用 Colocate，不能只看表属性。[Colocation Join](https://doris.apache.org/docs/4.x/query-acceleration/colocation-join/)

---

## 七、副本与存储策略


```sql
-- 线上表 3 副本（高可用）
"replication_allocation" = "tag.location.default: 3"

-- 历史分区降副本 + 冷存储
ALTER TABLE t MODIFY PARTITION (p202601, p202602)
SET (
    "replication_allocation" = "tag.location.default: 1",
    "storage_medium" = "HDD"
);

-- 冷热分层：30 天后自动迁移到 HDD
"storage_cooldown_time" = "2026-08-01 00:00:00"

```

> [!warning] 核对说明｜副本数、磁盘层级与保留期分开设置
>
> 降成 1 副本减少冗余；数据更旧不代表仍处于可接受的恢复范围。先确认存算一体集群在相应标签下有足够的 BE 和目标介质，再检查修改后的分区与副本实际状态。`storage_cooldown_time` 示例是绝对时间点，不会为每个分区自动计算「30 天后」。本地 SSD 到 HDD 的迁移也不同于对象存储归档，不能把这组参数直接套到存算分离部署。[本地磁盘分层](https://doris.apache.org/docs/4.x/table-design/tiered-storage/tiered-ssd-hdd/)


---

## 八、生产建表 Checklist

| # | 检查项 | 不检查的后果 |
| --- | --- | --- |
| 1 | 数据模型选对了吗？ | 后期无法修改，只能重建 |
| 2 | 分区粒度合理吗？ | 查询慢或元数据爆炸 |
| 3 | 分桶数算过了吗？ | Tablet 过大或过小 |
| 4 | Key 列顺序对吗？ | 前缀索引失效 |
| 5 | 副本数设合理了吗？ | 资源浪费或数据丢失 |
| 6 | 需要 Colocate 吗？ | JOIN 性能差 |
| 7 | 需要 Rollup 吗？ | 聚合查询慢 |

> [!note] 批注｜用代表性 SQL 验收候选表结构
>
> 对同一批样本运行同一组业务查询，先核对写入和查询结果，再比较计划中的分区、Tablet、物化视图和 Join 分布方式。最后观察实际扫描量、写入吞吐和 Compaction 压力；一个表结构如果只缩短单条查询，却让线上导入跟不上，需要继续调整。

---

## 九、小结

| 要点 | 内容 |
| --- | --- |
| 分区 | 决定生命周期和裁剪粒度，天分区最通用 |
| 分桶 | 目标单 Tablet 100MB~1GB，Hash 或 Random |
| Key 列 | 高频过滤列在前，影响前缀索引效果 |
| Rollup | Aggregate Key 的杀手锏，预聚合降维 |
| Colocate | 消除 JOIN Shuffle 的利器 |
