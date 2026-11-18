---
title: "Doris 入门篇05：数据导入方式"
author: "外星人的知识杂谈"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484333&idx=1&sn=26019e2d388cadfffd099531f93710e6&chksm=c1bf09b0f6c880a62fb32e2f0df00afe24116f2d1c779e29fc7befe75debf006bce5cba2065f"
published_at: "2026-07-27T20:25:00+08:00"
annotated_at: "2026-11-18"
cssclasses:
  - wechat-article
  - doris-intro-annotated
tags:
  - doris
  - 文章批注
---

# Doris 入门篇05：数据导入方式

<div class="article-meta">外星人的知识杂谈 · 2026-07-27</div>

[原文链接](https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484333&idx=1&sn=26019e2d388cadfffd099531f93710e6&chksm=c1bf09b0f6c880a62fb32e2f0df00afe24116f2d1c779e29fc7befe75debf006bce5cba2065f)

<!-- 正文按原文顺序保留；“批注”“核对说明”为新增。移除装饰封面及篇末导航。示例命令未在集群运行。 -->

> **定位：** 入门篇第 5 篇 | 掌握 Doris 各种导入方式的适用场景与基本用法
>
> **阅读前提：** 已了解 Rowset/Segment 存储结构（第 02 篇）

---

## 一、背景动机

数据导入是 OLAP 系统的入口。不同业务场景对导入的实时性、吞吐量、原子性有不同要求。Doris 提供了多种导入方式，核心区别在于：**谁发起、数据从哪来、是否实时。**

---

## 二、导入方式全景


```text
                    ┌────────────────────────────────┐
                    │         Doris 导入方式           │
                    └────────────────────────────────┘
                         │              │           │
           ┌─────────────┼──────────────┼───────────┼─────────────┐
           ▼             ▼              ▼           ▼             ▼
     Stream Load    Broker Load   Routine Load   INSERT INTO   Connector
     (HTTP 推送)    (拉取文件)    (消费 Kafka)   (SQL 语句)    (Spark/Flink)
      同步/实时      异步/批量      持续消费        同步          框架集成

```


| 方式 | 数据来源 | 实时性 | 单次数据量 | 底层机制 |
| --- | --- | --- | --- | --- |
| Stream Load | HTTP 推送 | 实时（同步） | < 10GB | 直接写 BE |
| Broker Load | HDFS/S3 文件 | 批量（异步） | TB 级 | BE 拉取远程文件 |
| Routine Load | Kafka 消费 | 准实时（持续） | 无上限 | 封装 Stream Load |
| INSERT INTO | SQL 语句 | 实时（同步） | 小量 | 走 FE 查询引擎 |
| Spark/Flink Connector | 框架并行写入 | 实时/批量 | 无上限 | 封装 Stream Load |

> [!note] 批注：先识别谁切批、谁重试
>
> 导入链路即使都落到 BE，重试责任也不同。Stream Load 由调用方切批并追踪响应；Broker Load 由 Doris 调度文件读取；Routine Load 由 FE 调度消费任务；Flink Connector 可以将提交绑定到 checkpoint。选方案时确认批次标识、恢复位置和可见性由谁追踪。原表的 `< 10GB` 是建议量级，实际请求限制看目标版本和 `streaming_load_max_mb`。[Stream Load](https://doris.apache.org/docs/4.x/data-operate/import/import-way/stream-load-manual/)

---

## 三、各导入方式详解

### 3.1 Stream Load（最常用）

通过 HTTP 协议将数据直接推送到 Doris BE，同步返回导入结果。


```bash
curl --location-trusted -u user:password \
    -H "label:load_20260701_001" \
    -H "column_separator:," \
    -H "columns: user_id, name, age, city" \
    -T data.csv \
    -XPUT http://fe_host:8030/api/db/table/_stream_load

```


**关键参数：**

| 参数 | 说明 |
| --- | --- |
|`label` | 导入标签，全局唯一，用于幂等去重 |
|`column_separator` | 列分隔符（CSV），建议用`\x01` 避免冲突 |
|`format` | 数据格式：csv / json / parquet / orc |
|`max_filter_ratio` | 允许的错误行比例（0~1） |
|`strict_mode` | 严格模式，类型不匹配的行是否报错 |
|`timeout` | 超时时间（默认 600s） |
|`two_phase_commit` | 两阶段提交（先 precommit 再 commit） |

**核心特点：**

- 一次 Stream Load = 一个事务 = 一个 Rowset
- 原子性：全部成功或全部失败
- 幂等性：相同 label 重复提交会返回已有结果

> [!warning] 核对说明：Label、事务和 Rowset 的粒度不同
>
> Label 用于在同一数据库和有效保留期内识别重复批次；同一逻辑批次重试沿用原 Label，并核对已有任务状态，不要直接生成新 Label。请求走 FE 地址时可以被重定向到 BE。一个事务可向多个 Tablet 写入；Rowset 属于 Tablet，不能把全表导入事务数直接等同于全表 Rowset 数。[Stream Load](https://doris.apache.org/docs/4.x/data-operate/import/import-way/stream-load-manual/)

### 3.2 Broker Load（大批量离线）

从 HDFS/S3/本地文件系统拉取数据，适合 TB 级历史数据导入。


```sql
LOAD LABEL db.label_20260701 (
    DATA INFILE("hdfs://cluster/path/data_*.parquet")
    INTO TABLE target_table
    FORMAT AS "parquet"
    (col1, col2, col3)
)
WITH HDFS (
    "fs.defaultFS" = "hdfs://cluster",
    "hadoop.username" = "doris"
)
PROPERTIES (
    "timeout" = "3600"
);

```


**特点：**

- 异步执行，通过`SHOW LOAD` 查看进度
- 支持通配符匹配多个文件
- 自动拆分为多个子任务并行导入

> [!warning] 核对说明：远端文件与客户端本地文件
>
> `LOAD LABEL` 返回代表已受理，需用 `SHOW LOAD` 确认最终状态。路径对负责读取的 Doris 节点应可达，且有权限；客户端磁盘上的 `data.csv` 并不会因提交 SQL 自动上传，通常用 Stream Load 发送。S3 和 HDFS 的写法都沿用 Broker Load 名称，当前版本通常无需额外 Broker 进程。[Broker Load](https://doris.apache.org/docs/4.x/data-operate/import/import-way/broker-load-manual/)

### 3.3 Routine Load（Kafka 持续消费）

订阅 Kafka Topic，持续消费数据写入 Doris。


```sql
CREATE ROUTINE LOAD db.routine_job ON target_table
COLUMNS SEPARATOR ",",
COLUMNS(user_id, event_type, event_time)
PROPERTIES (
    "desired_concurrent_number" = "3",
    "max_batch_interval" = "20",
    "max_batch_rows" = "200000",
    "format" = "json",
    "jsonpaths" = "[\"$.user_id\", \"$.event_type\", \"$.event_time\"]"
)
FROM KAFKA (
    "kafka_broker_list" = "broker1:9092,broker2:9092",
    "kafka_topic" = "user_events",
    "property.group.id" = "doris_consumer_group"
);

```


**关键参数：**

| 参数 | 说明 | 建议 |
| --- | --- | --- |
|`desired_concurrent_number` | 消费并发数 | ≤ Kafka 分区数 |
|`max_batch_interval` | 攒批间隔（秒） | 10~30s |
|`max_batch_rows` | 单批最大行数 | 10 万~50 万 |
|`max_batch_size` | 单批最大字节 | 100MB~1GB |

**底层机制：** Routine Load 本质是 FE 定时调度的 Stream Load 任务。每个消费周期：从 Kafka 拉数据 → 攒批 → 执行 Stream Load → 提交 offset。

> [!warning] 核对说明：消费进度随事务提交
>
> FE 把作业拆成独立的 Routine Load Task，BE 消费 Kafka 并写入，任务提交时 FE 更新作业进度。它不等价于外部客户端定时请求 HTTP Stream Load。`SHOW ROUTINE LOAD` 的 `Progress` 和 `Lag` 按 Kafka 分区解读；某个分区持续落后时，先查该分区写入速度、导入任务报错和并发分配。[Routine Load](https://doris.apache.org/docs/4.x/data-operate/import/import-way/routine-load-manual/) · [SHOW ROUTINE LOAD](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/data-modification/load-and-export/SHOW-ROUTINE-LOAD/)

### 3.4 INSERT INTO


```sql
-- 从另一张表导入
INSERT INTO target_table
SELECT col1, col2, col3 FROM source_table WHERE dt = '2026-07-01';

-- 直接插入值（仅适合少量数据）
INSERT INTO target_table VALUES (1, 'test', 100);

```


**注意：** INSERT INTO VALUES 每条都是一个独立事务，会产生大量小 Rowset。大批量数据不要用这种方式。

> [!note] 批注：单语句可以包含多行
>
> 重复发送许多单行 `INSERT` 容易产生小事务和小 Rowset。一个 `INSERT ... VALUES (...), (...)` 可以批量插入；Group Commit 还会改变多条语句与事务的对应关系。先确认客户端是否已攒批，再用导入延迟和 Compaction 压力判断是否需要合并请求。

### 3.5 Spark/Flink Doris Connector

通过 Spark/Flink 框架并行写入，底层封装 Stream Load。

**Flink 示例（SQL 方式）：**


```sql
CREATE TABLE doris_sink (
    user_id BIGINT,
    event_type STRING,
    event_time TIMESTAMP
) WITH (
    'connector' = 'doris',
    'fenodes' = 'fe_host:8030',
    'table.identifier' = 'db.table',
    'username' = 'root',
    'password' = '',
    'sink.properties.format' = 'json',
    'sink.properties.strip_outer_array' = 'true'
);

INSERT INTO doris_sink SELECT * FROM kafka_source;

```


**Spark 示例：**


```python
df.write.format("doris")
  .option("doris.fenodes", "fe_host:8030")
  .option("doris.table.identifier", "db.table")
  .option("user", "root")
  .option("password", "")
  .save()

```


**核心特点：**

- 每个 Spark/Flink 并行算子独立发起 Stream Load
- 支持 Exactly-Once（Flink 两阶段提交）
- 写入吞吐随并行度线性扩展

> [!warning] 核对说明：Flink Exactly-Once 取决于写入模式
>
> Flink 使用随 checkpoint 提交的流式写入时，要启用 checkpoint 和 `sink.enable-2pc`，并为作业配置稳定且不与其他作业冲突的 `sink.label-prefix`；还要验证故障后能从同一 checkpoint 或 savepoint 恢复。`sink.enable.batch-mode=true` 不依赖 checkpoint 提交，不能套用流式两阶段提交的保证。Spark 的 `doris.sink.enable-2pc` 是独立选项，也不能仅凭开启它断言整条作业端到端 Exactly-Once。并发吞吐受 Kafka 分区、BE、Tablet 分布和 Compaction 等限制。[Flink Connector](https://doris.apache.org/docs/4.x/connection-integration/data-integration/flink-doris-connector/) · [Spark Connector](https://doris.apache.org/docs/4.x/connection-integration/data-integration/spark-doris-connector/)

---

## 四、Group Commit（攒批写入）

Doris 2.1+ 引入 Group Commit 机制，解决高频小批量写入产生大量 Rowset 的问题。


```sql
-- 建表时开启
PROPERTIES (
    "group_commit_interval_ms" = "10000",  -- 10秒攒批
    "group_commit_data_bytes" = "134217728" -- 128MB 触发
);

```


**原理：** 多个并发的 Stream Load 请求被 BE 合并为一个 Rowset 写入，减少 Rowset 数量，降低 Compaction 压力。

**适用场景：** 高并发实时写入（IoT、日志采集等每秒数千请求的场景）。

> [!warning] 核对说明：攒批阈值与启用模式分开配置
>
> 原文的两个表属性控制按时间或数据量触发，不能单靠它们判断请求使用了哪种模式。按目标版本设置 Group Commit，并核对实际响应：`sync_mode` 等待合并事务提交；`async_mode` 可在写入 WAL 后返回，查询要等之后提交才可见。需要导入后立即查询的链路，要区分两种返回时点。[Group Commit](https://doris.apache.org/docs/4.x/data-operate/import/load-best-practices/group-commit-manual/)

---

## 五、导入方式选型


```text
数据来源是什么？
├── Kafka 持续流 → Routine Load（或 Flink Connector）
├── HDFS/S3 文件 → Broker Load
├── 本地文件/HTTP → Stream Load
├── 另一张 Doris 表 → INSERT INTO SELECT
└── Spark/Flink 框架 → Connector

选择 Routine Load vs Flink Connector？
├── 简单 ETL（过滤/列映射） → Routine Load（运维简单）
└── 复杂逻辑（JOIN/聚合/多流合并） → Flink Connector

```


---

## 六、导入原子性与幂等性

| 特性 | 说明 |
| --- | --- |
| **原子性** | 一次导入要么全部成功，要么全部回滚，不会出现部分数据写入 |
| **幂等性** | 同一个 Label 重复提交返回已有结果（不会重复写入数据） |
| **版本可见性** | 数据写入后需要 Publish Version 才对查询可见（通常毫秒级） |
| **两阶段提交** | Stream Load 支持`two_phase_commit`，先 Precommit 再 Commit，适合跨系统事务 |

> [!warning] 核对说明：事务原子性与源记录完整性分开核对
>
> 导入的原子性约束事务最终提交的记录。若配置允许过滤错误行，导入成功不代表每条输入都入库，要核对 `NumberLoadedRows`、`NumberFilteredRows` 和错误明细。Stream Load 若返回 `Publish Timeout`，官方含义是已完成导入但可见性可能延后；不要把它按失败直接重发。两阶段提交还需要调用方按协议完成 Commit 或 Abort。[Stream Load 返回结果](https://doris.apache.org/docs/4.x/data-operate/import/import-way/stream-load-manual/)

---

## 七、实践建议

1. **单次 Stream Load 尽量大：**

   目标每次写入产生的 Segment > 100MB，避免产生大量小 Rowset
2. **CSV 分隔符用不可见字符：**

   如`\x01`，避免数据中包含分隔符导致解析错误
3. **设置合理的 timeout：**

   大数据量导入时调大超时（默认 600s 可能不够）
4. **监控 Routine Load 消费延迟：**`SHOW ROUTINE LOAD`

    查看 lag
5. **高频写入场景开 Group Commit：**

   减少 Rowset 数量，降低 Compaction 压力

> [!note] 批注：用延迟和小 Rowset 数量反推批大小
>
> 原文的 `100MB` 是调优参考，不是 Segment 的统一下限。测试时保持相同输入与分桶，记录每批负载、可见延迟、单个 Tablet 的版本增长和 Compaction 压力，再调整切批策略。批过大也可能把内存、超时或重试代价推高。

---

## 八、小结

| 要点 | 内容 |
| --- | --- |
| 最常用 | Stream Load（HTTP 同步推送，简单直接） |
| 实时流 | Routine Load / Flink Connector |
| 离线批量 | Broker Load（TB 级文件导入） |
| 核心约束 | 每次导入 = 一个 Rowset；单次数据量越大越好 |
| 幂等保证 | Label 机制确保不重复写入 |
