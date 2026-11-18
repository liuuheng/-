---
title: "Doris 进阶篇08：网易云音乐落地最佳实践"
author: "外星人的知识杂谈"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484371&idx=1&sn=a1d22d29263ddbcba524e9af4da1ee82&chksm=c1bf09cef6c880d8abbe120f6b226835d03609c10e2e0dc02a2f4b2129f9ad6a4591b16a73ea#wechat_redirect"
published_at: "2026-09-03T11:51:13+08:00"
annotated_at: "2026-11-18"
cssclasses:
  - wechat-article
  - doris-intro-annotated
tags:
  - doris
  - 文章批注
---

# Doris 进阶篇08：网易云音乐落地最佳实践

<div class="article-meta">外星人的知识杂谈 · 2026-09-03</div>

[原文链接](https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484371&idx=1&sn=a1d22d29263ddbcba524e9af4da1ee82&chksm=c1bf09cef6c880d8abbe120f6b226835d03609c10e2e0dc02a2f4b2129f9ad6a4591b16a73ea#wechat_redirect)

<!-- 保留原文正文、顺序与示例；批注和核对说明为新增。装饰封面与篇末导航已移除；代码未在集群运行。 -->

> **定位：** 进阶篇第 8 篇 | 从 5 大迁移场景提炼 Doris 生产落地方法论
>
> **阅读前提：** 进阶篇前 7 篇全部（尤其第 01/03/05/06 篇）

---

## 一、背景动机

网易云音乐从 2022 年开始引入 Doris，至今已完成 5 大核心场景的落地。每个场景都是从一个“旧引擎”迁移到 Doris，覆盖了 OLAP 领域最典型的几类工作负载：

| 场景 | 旧引擎 | 核心负载 | 落地时间 |
| --- | --- | --- | --- |
| 广告 | Druid | 实时聚合 + 多维分析 | 2023 |
| 日志 | ClickHouse | 高吞吐写入 + 明细查询 | 2023 |
| 会员 | Kylin | 预聚合报表 | 2023 |
| Kudu 替换 | Kudu | 实时更新 + 批量读取 | 2024 |
| Ad-hoc | Impala | 联邦查询 + 即席分析 | 2024 |

本篇不重复各场景的详细技术细节（详见内部文档），而是提炼通用的迁移方法论、关键技术决策和经验教训。

> [!note] 批注｜按工作负载比较迁移方案
>
> 五个案例检验的是不同能力：日志要保留明细，广告和会员报表依赖可复用的聚合结果，Kudu 替换要保持同键更新语义，即席查询还要访问未迁移的数据。文中的 DDL 是简化示例，迁移时间和收益按原文记录；内部材料未公开，不能用这些数字直接承诺其他集群的收益。

---

## 二、场景一：广告 Druid → Doris

### 2.1 迁移动机

| Druid 痛点 | Doris 解决方案 |
| --- | --- |
| 缺乏完善的监控告警机制 | 接入 Prometheus + Grafana 全面监控 |
| 维度扩展困难（Schema 变更代价大） | 动态列/维表关联，灵活扩展 |
| 跨 7 天查询慢或超时 | 分区裁剪 + 物化视图，长时间范围查询秒级返回 |
| 无法对接有数 BI（HTTP 协议兼容差） | 标准 MySQL 协议，BI 工具直连 |

### 2.2 核心技术方案

**数据模型：** Aggregate Key 表 + 预聚合


```sql
-- 广告 Stat 表设计（简化示例）
CREATE TABLE ad_stat (
    dt              DATE,
    hour            TINYINT,
    ad_id           BIGINT,
    creative_id     BIGINT,
    platform        VARCHAR(16),
    channel         VARCHAR(32),
    -- 指标（聚合）
    impression      BIGINT SUM,
    click           BIGINT SUM,
    cost            DECIMAL(18,4) SUM,
    uv              BITMAP BITMAP_UNION
) AGGREGATE KEY(dt, hour, ad_id, creative_id, platform, channel)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(ad_id) BUCKETS 16
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.end" = "3"
);

```


**数据链路：** Flink 实时消费 Kafka → Stream Load 写入 Doris

**关键优化：**

1. 使用 BITMAP 做 UV 预聚合，查询从分钟级降到毫秒级
2. 配合 Rollup 做多维度降维，覆盖 80% 高频查询
3. 使用 HyperLoop CDC 能力实时同步维表

> [!note] 批注｜UV 为什么要合并集合
>
> 同一用户可能在多个小时或渠道重复出现，日 UV 不能用小时 UV 直接相加。`BITMAP_UNION` 保留可继续合并的用户集合，按日合并后再取基数；同一个用户的位图 ID 必须跨批次一致。曝光、点击和金额则用 `SUM`，需要单独验证导入重试是否导致重复累加。

### 2.3 落地成果

| 指标 | Druid | Doris | 提升 |
| --- | --- | --- | --- |
| P99 查询延迟 | 10s+ | 500ms | 20x |
| 维度扩展周期 | 周级 | 天级 | 显著降低 |
| 查询时间范围 | 7 天内 | 不限 | 突破限制 |
| 监控告警 | 无 | 全面 | 从 0 到 1 |
| 上线后累计召回问题 | - | 5 次 | 直接业务价值 |

> [!note] 批注｜区分分区裁剪和报表预聚合
>
> 日期谓词能裁掉范围之外的分区，但范围越宽，一般仍要读更多分区。要定位收益，可同时看命中的分区数和 `EXPLAIN` 中实际扫描的表或 Rollup，并用相同查询范围、并发和缓存条件对比 P99。原表的“不限”只说明案例不再受原先的 7 天限制。

---

## 三、场景二：日志 ClickHouse → Doris

### 3.1 迁移动机

| ClickHouse 痛点 | Doris 解决方案 |
| --- | --- |
| 运维成本高（坏盘/扩容需人工介入） | 自恢复 + 自动均衡 |
| 使用复杂（本地表/分布式表/多引擎） | 统一表模型 |
| 并发查询能力差 | 可控的并发 + 资源隔离 |
| 写入单点宕机需人工恢复 | 多副本自动 Failover |
| 费用高（云服务收费） | 自建集群，节省 70 万/年 |

### 3.2 核心技术方案

**数据模型：** Duplicate Key 明细表（保留全量日志明细）


```sql
-- 日志表设计（简化示例）
CREATE TABLE app_logs (
    dt           DATE,
    log_time     DATETIME,
    service      VARCHAR(64),
    level        VARCHAR(16),
    trace_id     VARCHAR(64),
    user_id      BIGINT,
    message      TEXT,
    properties   VARIANT
) DUPLICATE KEY(dt, log_time, service)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY RANDOM BUCKETS 32
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "compaction_policy" = "time_series"  -- 时序 Compaction 策略
);

-- 倒排索引（全文检索）
CREATE INDEX idx_msg ON app_logs (message) USING INVERTED 
PROPERTIES ("parser" = "unicode");

-- Bloom Filter（trace_id 精确查找）
ALTER TABLE app_logs SET ("bloom_filter_columns" = "trace_id");

```


**关键技术点：**

1. **Auto Partition：**

   数据写入时自动创建分区，免去手动维护
2. **倒排索引：**

   替代 ClickHouse 的全文检索能力，支持 MATCH 语法
3. **VARIANT 类型：**

   日志属性动态变化，VARIANT 自动适应 Schema 演进
4. **Time Series Compaction：**

   针对时序数据优化的 Compaction 策略，减少写入放大

> [!warning] 核对说明｜示例 DDL 用的是 Dynamic Partition
>
> 原例用 `PARTITION BY RANGE(dt) ()` 配合 `dynamic_partition.*` 属性，属于[动态分区](https://doris.apache.org/docs/4.x/table-design/data-partitioning/dynamic-partitioning/)，由调度器按时间窗口维护。下方文字描述的[自动分区](https://doris.apache.org/docs/4.x/table-design/data-partitioning/auto-partitioning/)用 `AUTO PARTITION`，按写入数据的分区值创建分区。日志回补旧分区前，先确认旧日期分区如何创建、何时过期；两种机制的行为不同。

### 3.3 落地成果

| 指标 | ClickHouse | Doris | 说明 |
| --- | --- | --- | --- |
| 年成本 | 高 | 节省 70 万 | 下线 31 台机器 |
| 运维成本 | 高（人工介入） | 低（自恢复） | 坏盘/扩容自动化 |
| P99 查询延迟 | 22s | 17s | 降低 20%+ |
| 并发能力 | 差 | 好 | 可灵活调控 |
| 写入稳定性 | 需人工恢复 | 自动重试 | FE/BE 不可用时自动 Failover |

> [!note] 批注｜重试成功与数据不重不漏要分开验证
>
> 节点切换和自动重试可以恢复写入，但客户端收到超时时，上一批数据可能已经提交。Duplicate Key 表会保留重复记录。压测时应覆盖“服务端已提交、客户端没收到响应”的场景，并按实际导入协议检查批次标识、重放结果和源端位点，才能判断故障恢复后的数据是否正确。

---

## 四、场景三：会员 Kylin → Doris

### 4.1 迁移动机

| Kylin 痛点 | Doris 解决方案 |
| --- | --- |
| Cube 构建耗时 6 小时+ | 直接查询聚合表，10 分钟内就绪 |
| 架构复杂（ZK+HBase+HDFS+Kylin） | 仅 FE + BE 两种角色 |
| 维护成本高（35+ 台机器） | 仅需 5 台机器 |
| 查询最大耗时 4min+ | 最大耗时 20s |

### 4.2 核心技术方案

**Kylin Cube → Doris Aggregate Key + Rollup：**


```text
Kylin 方案：
  ADS 层数据 → MapReduce 构建 Cube → Kylin → 查询
  耗时：6 小时+，消耗 1500+ vcore

Doris 方案：
  ADS 层数据 → Spark/Flink 直接写入 Doris Aggregate 表 → 查询
  耗时：10 分钟+

```

```sql
-- 会员分析表（简化示例）
CREATE TABLE member_analysis (
    dt              DATE,
    member_type     VARCHAR(32),
    channel         VARCHAR(32),
    platform        VARCHAR(16),
    -- 指标
    pay_amount      DECIMAL(18,2) SUM,
    pay_count       BIGINT SUM,
    member_uv       BITMAP BITMAP_UNION
) AGGREGATE KEY(dt, member_type, channel, platform)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(member_type) BUCKETS 8;

-- 添加 Rollup（自动路由查询）
ALTER TABLE member_analysis ADD ROLLUP 
    rollup_daily(dt, member_type, pay_amount, pay_count, member_uv);

```


**优化过程（5 轮压测迭代）：**

1. 明细模型（性能不达标）
2. 聚合模型（有改善但仍不够）
3. Spark 端预聚合 + 聚合模型（显著改善）
4. 物化视图（进一步优化）
5. 三阶段聚合方案（最终方案，满足要求）

> [!note] 批注｜把 Cube 构建换成可逐层合并的指标
>
> 示例 Rollup 保留 `dt` 和 `member_type`，可以服务不依赖 `channel`、`platform` 的查询；按渠道筛选的 SQL 需要读取还保有该列的结构。Spark 预聚合、Aggregate Key 表和 Rollup 能否组合，取决于中间指标能否再次聚合。`SUM` 值可以求和，UV 要合并位图；若计算平均值，要分别传递分子和分母。

### 4.3 落地成果

| 指标 | Kylin | Doris | 提升 |
| --- | --- | --- | --- |
| 数据就绪时间 | 6 小时+ | 10 分钟+ | 35x |
| 最大查询耗时 | 4min+ | 20s | 12x |
| 存储空间 | 220TB+ | 13TB+ | 15x |
| 机器数量 | 35+ 台 | 5 台 | 成本降低 80%+ |

---

## 五、场景四：Kudu → Doris

### 5.1 迁移动机

| Kudu 痛点 | Doris 解决方案 |
| --- | --- |
| 生态逐渐萎缩 | 活跃社区 + 快速迭代 |
| 实时更新场景性能有限 | Unique Key MoW 模式高效更新 |
| 与 Spark 集成存在连接池/Split 问题 | 标准 JDBC/Multi-Catalog 接入 |
| 运维工具匮乏 | 完善的监控/运维体系 |

### 5.2 核心技术方案

**数据模型：** Unique Key（Merge-on-Write 模式）


```sql
-- Kudu 的 Upsert 语义 → Doris Unique Key MoW
CREATE TABLE realtime_metrics (
    dt           DATE,
    metric_id    BIGINT,
    dimension_1  VARCHAR(64),
    dimension_2  VARCHAR(64),
    value        DOUBLE,
    update_time  DATETIME
) UNIQUE KEY(dt, metric_id, dimension_1, dimension_2)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(metric_id) BUCKETS 16
PROPERTIES (
    "enable_unique_key_merge_on_write" = "true",  -- MoW 模式
    "store_row_column" = "true"                   -- 行存优化点查
);

```


**关键技术点：**

1. **Merge-on-Write：**

   写入时即合并（vs Merge-on-Read 读时合并），读取性能更好
2. **部分列更新：**`partial_columns = true`

   ，只更新指定列，减少写入量
3. **Group Commit：**

   合并小批次写入，减少 Rowset 碎片


```http
-- 部分列更新（仅更新 value 和 update_time）
-- Stream Load Header:
-- partial_columns: true
-- columns: metric_id, dimension_1, dimension_2, dt, value, update_time

```

> [!warning] 核对说明｜部分列更新与 Group Commit 分别验证
>
> 对已存在的行，Stream Load 以 `partial_columns: true` 写入时必须提供完整的 Unique Key。原例中的 `dt` 是 Key 的一部分，日期改变会形成另一个 Key。即使上游只发送待改列，MoW 写入仍可能需要读取旧列补齐行；不要把网络数据量的下降直接当作磁盘写入量的下降。
>
> 按 [Doris 4.x Group Commit 文档](https://doris.apache.org/docs/4.x/key-features/group-commit/)，使用 `partial_columns: true` 的请求会回退到常规导入路径，不能把 §5.2 的两项优化当成同一批次上同时生效。若源事件可能乱序，还需用业务版本字段验证旧更新不会覆盖新状态。


### 5.3 落地成果

| 指标 | 改进 |
| --- | --- |
| 数据更新延迟 | 满足实时要求 |
| Spark 读取兼容性 | 通过 Multi-Catalog 解决 |
| 运维复杂度 | 统一到 Doris 运维体系 |

---

## 六、场景五：Impala → Doris（Ad-hoc 查询）

### 6.1 迁移动机

| Impala 痛点 | Doris 解决方案 |
| --- | --- |
| 查询性能中等 | 向量化 + Pipeline 引擎更快 |
| 不支持 Hive 之外的数据源 | Multi-Catalog 联邦查询 |
| 元数据依赖 Hive Metastore | FE 自管理 + 多源 Catalog |
| 并发有限 | 可扩展的并发能力 |

### 6.2 核心技术方案

**Multi-Catalog 联邦查询：**


```sql
-- 创建 Hive Catalog（对接已有数仓）
CREATE CATALOG hive_catalog PROPERTIES (
    "type" = "hms",
    "hive.metastore.uris" = "thrift://hive-metastore:9083"
);

-- 创建 HDFS Catalog
CREATE CATALOG hdfs_catalog PROPERTIES (
    "type" = "hms",
    "hive.metastore.uris" = "thrift://hive-metastore:9083",
    "hadoop.username" = "hdfs"
);

-- 跨 Catalog 查询
SELECT a.*, b.user_name
FROM internal.db.doris_table a
JOIN hive_catalog.warehouse.dim_user b 
ON a.user_id = b.user_id
WHERE a.dt = '2026-07-01';

```


**优势：**

- 无需将所有数据迁入 Doris
- 热数据在 Doris（高性能），冷数据留在 Hive/HDFS（低成本）
- 统一查询入口，用户体验一致

> [!warning] 核对说明｜两个示例都连接 Hive Metastore
>
> 原例的 `hdfs_catalog` 只是起了不同名字，`"type" = "hms"` 与元数据地址仍指向同一个 Hive Metastore。按 [Hive Catalog 文档](https://doris.apache.org/zh-CN/docs/4.x/lakehouse/catalogs/hive-catalog/)，先由 Metastore 找到表、分区和文件位置，再按文件系统配置读取 HDFS。排查跨 Catalog 查询时，应分别核查元数据可见性、数据文件访问权限，以及连接前两侧实际扫描的数据量。

---

## 七、迁移方法论总结

### 7.1 五步迁移法


```text
Step 1: 评估（1~2 周）
  ├── 分析现有系统痛点
  ├── 评估数据规模和查询模式
  ├── 确定 Doris 表模型（Duplicate/Aggregate/Unique）
  └── 资源评估（节点数、磁盘、内存）

Step 2: 设计（1~2 周）
  ├── 表结构设计（分区/分桶/Key 列）
  ├── 索引方案设计
  ├── 数据链路设计（实时/离线/CDC）
  └── 监控告警设计

Step 3: 压测优化（2~4 周）
  ├── 数据导入压测
  ├── 查询性能压测（典型 SQL）
  ├── 多轮优化迭代（通常 3~5 轮）
  └── 确定最终方案

Step 4: 双跑验证（2~4 周）
  ├── 新旧系统并行运行
  ├── 数据一致性校验
  ├── 性能对比验证
  └── 故障演练

Step 5: 切换上线（1~2 周）
  ├── 灰度切流
  ├── 全量切换
  ├── 旧系统保留观察期
  └── 旧系统下线

```

> [!note] 批注｜双跑门槛用数据覆盖来定义
>
> 先让新旧引擎对齐到同一可见位点或分区快照，再比较同一查询的结果。至少覆盖迟到、回补、重复投递、删除和乱序更新中与本场景相关的情况；对报表还要统一时区、空值和去重口径。灰度前约定一致性、数据可见延迟和查询 P99 的通过条件，以及切回旧引擎所需的位点和保留期。


### 7.2 表模型选型决策树


```text
你的核心负载是什么？
│
├── 预聚合报表（指标统计）
│   └── Aggregate Key + Rollup
│       适合：广告监控、会员分析、业务报表
│
├── 日志/事件明细存储
│   └── Duplicate Key + 倒排索引
│       适合：日志检索、行为分析、审计追踪
│
├── 实时数据更新（Upsert）
│   └── Unique Key (MoW)
│       适合：实时指标、维表更新、Kudu 替换
│
└── 联邦查询 / Ad-hoc
    └── Multi-Catalog + 内部热表
        适合：数仓查询、多源分析

```


### 7.3 常见陷阱与规避

| 陷阱 | 表现 | 规避方法 |
| --- | --- | --- |
| 分桶数拍脑袋 | Tablet 过大或过小 | 按公式计算：分区数据量 / 500MB |
| Key 列顺序错误 | 前缀索引失效，查询全表扫描 | 高频过滤列在前，大 VARCHAR 靠后 |
| 忽视 Compaction 调优 | Version 堆积，写入报错 -235 | 提前规划 Compaction 线程和内存 |
| BITMAP 列过多 | Compaction Score 持续偏高 | 控制 BITMAP 列数量，增大 Compaction 资源 |
| 未收集统计信息 | 优化器选错执行计划 | 开启 auto\_analyze |
| 未做双跑验证 | 上线后发现数据不一致 | 至少双跑 2 周 |
| 报表无时间限制 | 全表扫描拖垮集群 | 卡点检查，确保查询带时间条件 |

> [!warning] 核对说明｜500 MB 不是固定的分桶阈值
>
> [Doris 4.x 导入最佳实践](https://doris.apache.org/docs/4.x/data-operate/import/load-best-practices/)给出的常见目标是每个 Tablet 约 1–10 GB。先用高峰分区的压缩后数据量估算初始桶数，再用 `SHOW TABLETS` 检查实际大小、冷热桶差异和分片分布。桶数过多也会增加元数据和 Compaction 开销，原表的 `/ 500MB` 只可当案例起点。

---

## 八、性能对比总结

| 场景 | 旧引擎 | Doris | 核心指标提升 |
| --- | --- | --- | --- |
| 广告 | Druid | Doris | 查询 20x，维度扩展从周到天 |
| 日志 | ClickHouse | Doris | 成本 -70 万/年，P99 -20% |
| 会员 | Kylin | Doris | 就绪 35x，存储 15x，成本 -80% |
| Kudu | Kudu | Doris | 统一运维，实时更新稳定 |
| Ad-hoc | Impala | Doris | 联邦查询，性能更优 |

> [!warning] 核对说明｜表内数字存在近似口径
>
> 会员案例的 `220TB+`、`13TB+` 和 `15x` 均按原表保留，原文没有给出可重算的精确值和副本口径。评估自身迁移时，先统一压缩、保留周期和副本的计量口径，再把物理存储、导入计算以及运维成本分别对照。查询收益则用同一 SQL 集合和并发水平比较。

---

## 九、经验教训

### 9.1 架构选型层面

1. **统一引擎的价值远大于单点性能：**

   维护 5 套引擎的总成本 > 一套 Doris 即使某些场景稍逊
2. **Doris 不是银弹：**

   超大规模全文检索仍需考虑 ES，超高频点查需评估 Redis/HBase
3. **先做生态建设再推业务：**

   监控、告警、SOP、文档准备好后，业务推广事半功倍

### 9.2 技术实施层面

1. **压测迭代是关键：**

   会员场景经过 5 轮压测才找到最优方案，切勿急于上线
2. **Compaction 是生命线：**

   高频写入场景必须提前规划 Compaction 资源
3. **统计信息不能忽视：**

   多次慢查询问题根因是统计信息过期
4. **报表端必须卡点：**

   不带时间条件的全表扫描是集群杀手

### 9.3 组织协作层面

1. **与业务方深度协作：**

   了解真实使用场景比猜测重要
2. **与社区共建：**

   遇到底层 Bug 积极反馈社区，获得专业支持
3. **沉淀知识库：**

   每次落地形成标准文档，后续场景复用

---

## 十、小结

| 要点 | 内容 |
| --- | --- |
| 5 大场景 | 广告(Druid)、日志(CK)、会员(Kylin)、Kudu、Ad-hoc(Impala) |
| 核心价值 | 统一引擎降低总成本，显著提升性能和运维效率 |
| 方法论 | 评估→设计→压测→双跑→切换，5 步标准化流程 |
| 表模型 | Aggregate(聚合)、Duplicate(明细)、Unique(更新)，按负载选型 |
| 核心教训 | 压测迭代不可省、Compaction 提前规划、统计信息必须维护 |

---

> 本篇为进阶篇最后一篇。更深入的源码剖析、分布式一致性、云原生架构等内容，请参见高阶篇。
