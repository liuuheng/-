---
title: "Doris 进阶篇03：存储优化与Compaction调优"
author: "外星人的知识杂谈"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484366&idx=1&sn=a46dae628434ff3bd3a12e77cae2ae1f&chksm=c1bf09d3f6c880c51c2f899024cac719149515038393cfcd160271da31bbcbd2dbe1f91b0f78#wechat_redirect"
published_at: "2026-08-24T22:10:29+08:00"
annotated_at: "2026-11-18"
cssclasses:
  - wechat-article
  - doris-intro-annotated
tags:
  - doris
  - 文章批注
---

# Doris 进阶篇03：存储优化与Compaction调优

<div class="article-meta">外星人的知识杂谈 · 2026-08-24</div>

[原文链接](https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484366&idx=1&sn=a46dae628434ff3bd3a12e77cae2ae1f&chksm=c1bf09d3f6c880c51c2f899024cac719149515038393cfcd160271da31bbcbd2dbe1f91b0f78#wechat_redirect)

<!-- 保留原文正文、顺序与示例；批注和核对说明为新增。装饰封面与篇末导航已移除；代码未在集群运行。 -->

> **定位：** 进阶篇第 3 篇 | 理解 Compaction 机制并掌握生产环境调优手段
>
> **阅读前提：** 入门篇第 02 篇（存储模型基础）、进阶篇第 01 篇（建表与库表优化）

---

## 一、背景动机

Doris 采用 LSM-Tree 类存储架构：数据写入时先落入 Rowset（类似 SSTable），后续通过 Compaction 合并成更大的有序文件。Compaction 做得好，查询时只需扫描少量大文件；做得差，Tablet 内碎片化严重（Version 堆积），查询延迟飙升、内存压力增大。

在生产环境中，Compaction 是影响 Doris 集群健康度的核心因素之一。本篇将从原理到参数逐层展开，最终给出面向不同负载场景的调优方案。

---

## 二、Compaction 核心机制

### 2.1 两种 Compaction 策略

Doris 的 Compaction 分为两种：

| 类型 | 全称 | 触发时机 | 合并范围 | 目标 |
| --- | --- | --- | --- | --- |
| Cumulative | 增量合并 | 高频触发 | 最近写入的小 Rowset | 减少小文件数量 |
| Base | 全量合并 | 低频触发 | 所有 Cumulative 合并后的结果 + Base Rowset | 最终生成单一有序大文件 |


```text
写入流程：
  MemTable → flush → Rowset(version=N)

Compaction 流程：
  Rowset[v1] + Rowset[v2] + ... + Rowset[vN]  (Cumulative)
        ↓
  Rowset[v1-vN]  (合并后)
        ↓
  Base Rowset[v0-vN]  (Base Compaction)

```

> [!warning] 核对说明｜合并的产物是 Rowset，仍可能有多个 Segment
>
> Compaction 按 Tablet 选择连续的版本范围，把多个输入 Rowset 合为新 Rowset；并不承诺输出一个物理文件。一次导入也可能跨多个 Tablet，每个 Tablet 的 Rowset 可由多次 MemTable flush 产生的 Segment 组成。Cumulative 和 Base 是合并任务类别；`size_based` 与 `time_series` 则是决定如何挑选输入的表级策略，两组概念不要混用。[Compaction 原理](https://doris.apache.org/docs/4.x/admin-manual/trouble-shooting/compaction-principles/)


### 2.2 Cumulative Point

Cumulative Point 是 Base Compaction 和 Cumulative Compaction 的分界线：

- **Cumulative Point 左侧**

  （version <= cumulative\_point）：属于 Base Compaction 的管辖范围
- **Cumulative Point 右侧**

  （version > cumulative\_point）：属于 Cumulative Compaction 的管辖范围

当 Cumulative Compaction 将右侧 Rowset 合并到足够大（或满足条件）时，Cumulative Point 会右移，Base Compaction 接管更大范围的合并。

> [!note] 批注｜它是版本区间的边界，不是某个任务的进度
>
> 检查 Tablet 时把 `cumulative point` 与实际 Rowset 版本范围一起读，例如 `[3-11]` 已合成一个 Rowset，但仍覆盖 9 个逻辑版本。边界前移说明一段结果不再由增量合并处理，不能据此断定 Base Compaction 已经完成。具体候选范围由策略决定。[查看 Compaction 状态](https://doris.apache.org/docs/4.x/admin-manual/open-api/be-http/compaction-status/)

### 2.3 Compaction Score

每个 Tablet 有一个 Compaction Score，用于表征其“紧迫程度”：


```text
Cumulative Score = 未合并的 Rowset 数量（Cumulative Point 右侧）
Base Score = Base Compaction 涉及的 Rowset 数量（Cumulative Point 左侧）
Tablet Score = max(Cumulative Score, Base Score)

```


Score 越高，优先级越高。可通过 BE 的 Web UI（`http://be_host:webserver_port/tablets_page`）查看各 Tablet 的 Compaction Score。

> [!warning] 核对说明｜Score 表示合并压力，不等于版本数
>
> 原公式用于理解两个方向的积压，不能按固定等式计算 4.x 的 Score。实现会按 Rowset 布局估算参与归并的路径数；有重叠 Segment 时，一个 Rowset 可以贡献多个分值。定位时对比 Cumulative、Base 的分值及趋势，再看物理 Rowset/Segment 数、版本数和任务失败记录。[Compaction Score](https://doris.apache.org/docs/4.x/admin-manual/trouble-shooting/compaction-principles/)

### 2.4 Compaction 触发条件

Cumulative Compaction 触发条件（满足任一）：

1. Cumulative Point 右侧 Rowset 数量 >=`min_cumulative_compaction_num_singleton_deltas`（默认 5）
2. 距上次 Cumulative Compaction 超过`cumulative_compaction_skip_window_seconds`

Base Compaction 触发条件：

1. Cumulative Point 左侧有多个 Rowset
2. 满足大小比例要求：所有候选 Rowset 总大小 >= Base Rowset 大小 ×`base_compaction_min_data_ratio`

> [!warning] 核对说明｜先确定表的策略和参数版本
>
> 原文的两组阈值并非通用触发规则。4.x 的 `size_based` 会结合候选 Rowset、分值和大小判断；`time_series` 另有文件数、数据量与时间阈值。先通过 `SHOW CREATE TABLE` 确认 `compaction_policy`，再查对应版本的 BE 配置与任务日志。符合触发条件仍要等磁盘和线程池有调度容量。[Compaction 优化](https://doris.apache.org/docs/4.x/admin-manual/trouble-shooting/compaction/)

---

## 三、关键参数详解

### 3.1 并发与资源控制

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
|`max_compaction_threads` | -1（自动） | Compaction 线程池大小，-1 时取 CPU 核数的 1/4 |
|`compaction_memory_limit_per_worker` | 2GB | 单个 Compaction 任务的内存上限 |
|`total_permits_for_compaction_score` | 10000 | 全局 Compaction 并发许可总量 |
|`compaction_task_num_per_disk` | 4 | 每块磁盘并发 Compaction 任务数上限 |
|`compaction_task_num_per_fast_disk` | 8 | SSD 磁盘并发 Compaction 任务数 |

> [!warning] 核对说明｜4.x 分开控制 Base 与 Cumulative 线程池
>
> 4.x 文档列出 `max_base_compaction_threads`、`max_cumu_compaction_threads`，另有限制每盘任务的 `compaction_task_num_per_disk` 和 `compaction_task_num_per_fast_disk`。原表中的 `max_compaction_threads` 和 `compaction_memory_limit_per_worker` 不宜按 4.x 参数直接使用；以在用 BE 的配置项为准。调大线程数之前先检查现有线程是否饱和、磁盘是否还有余量。[BE 配置项](https://doris.apache.org/docs/4.x/admin-manual/config/be-config/)

### 3.2 Cumulative Compaction 参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
|`min_cumulative_compaction_num_singleton_deltas` | 5 | 触发 Cumulative 的最小 Rowset 数 |
|`max_cumulative_compaction_num_singleton_deltas` | 1000 | 单次 Cumulative 最多合并的 Rowset 数 |
|`cumulative_compaction_skip_window_seconds` | 30 | 超时触发间隔 |
|`cumulative_size_based_promotion_size_mbytes` | 1024 | 单个 Rowset 达到此大小则提升到 Base 层 |
|`cumulative_size_based_promotion_ratio` | 0.05 | Rowset 占 Base 比例超此值则提升 |

> [!warning] 核对说明｜用 4.x 参数名核对配置
>
> 4.x 文档使用 `cumulative_compaction_min_deltas`、`cumulative_compaction_max_deltas` 描述候选 Rowset 的分值范围；这也不是把原文的数量阈值简单换个名字。达到提升条件的配置要结合当前合并策略查看，不能跨版本复用原表默认值。[常用 Compaction 参数](https://doris.apache.org/docs/4.x/admin-manual/trouble-shooting/compaction-principles/)

### 3.3 Base Compaction 参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
|`base_compaction_min_rowset_num` | 5 | 触发 Base 的最小 Rowset 数 |
|`base_compaction_min_data_ratio` | 0.3 | 候选总大小 / Base 大小 >= 此值才触发 |
|`base_compaction_interval_seconds_since_last_operation` | 86400 | 距上次 Base Compaction 超此秒数强制触发 |

---

## 四、Compaction 常见问题诊断

### 4.1 Version 堆积（-235 错误）

当 Tablet 的 Version 数超过阈值（默认`max_tablet_version_num = 500`），写入会报错：


```text
ERROR: too many versions. tablet_id=xxx, version_count=501, limit=500

```


**诊断流程：**


```sql
-- 查看各 BE 上 Compaction Score 最高的 Tablet
SHOW PROC '/statistic';

-- 查看具体 Tablet 的 Version 详情
SHOW TABLET tablet_id;

```


**排查方向：**

| 原因 | 表现 | 解决 |
| --- | --- | --- |
| 写入频率过高 | 每秒多次 Stream Load | 改用 Group Commit 或增大批次 |
| Compaction 线程不足 | Score 高但合并慢 | 增大`max_compaction_threads` |
| 内存不足 | Compaction 失败 | 增大`compaction_memory_limit_per_worker` |
| 单 Tablet 数据量巨大 | Base Compaction 耗时过长 | 增加 Bucket 数分散数据 |
| DELETE 操作积压 | 大量 Delete Predicate 版本 | 等待 Base Compaction 消化 |

> [!warning] 核对说明｜实际版本数和允许上限是不同值
>
> 4.x 文档列出的 `max_tablet_version_num` 默认为 2000，生产集群也可能覆盖，不能把原文的 500 当统一阈值。用报错的 `tablet_id` 找到具体副本，再查对应 BE 的版本、Rowset 和合并失败状态。若只有少数 Tablet 堆积，先排查写入倾斜；若普遍增长，再比较事务频率与合并速率。增加新分区的桶数不会重切旧分区。[BE 配置项](https://doris.apache.org/docs/4.x/admin-manual/config/be-config/) · [Tablet 合并状态](https://doris.apache.org/docs/4.x/admin-manual/open-api/be-http/compaction-status/)

### 4.2 Compaction 导致 OOM

大 Tablet 做 Base Compaction 时可能消耗大量内存（需要将多个 Rowset 的数据读入内存排序合并）。


```text
# BE 日志中的典型表现
W0715 10:23:45.123456 compaction_task.cpp:xxx] 
  Compaction failed: Memory exceed limit. tablet=12345

```


**解决方案：**

1. 降低`compaction_memory_limit_per_worker`（限制单任务内存）
2. 降低`compaction_task_num_per_disk`（减少并发）
3. 增加 Tablet 数（通过增加 Bucket 数，分散单 Tablet 数据量）

> [!note] 批注｜区分单任务峰值与并发内存叠加
>
> 多路归并按批读入数据，并不把所有输入 Rowset 一次载入内存。检查任务日志里的合并类型、输入范围和内存来源：若多任务同时运行才接近上限，可降低并发；单个宽表的大任务就过高时，才分别评估垂直合并、读批大小及行号映射开销。不要用原文的单任务参数名直接调 4.x。[垂直 Compaction 与内存](https://doris.apache.org/docs/4.x/admin-manual/trouble-shooting/compaction/)

### 4.3 磁盘 IO 被 Compaction 打满


```bash
# 监控指标
# doris_be_compaction_bytes_per_second（BE metric）
# 结合 iostat 观察磁盘利用率
iostat -x 1

```


解决方案：设置 Compaction 限速：


```ini
# be.conf
compaction_max_write_mbytes_per_sec = 100  # 限制 Compaction 写入速度

```

> [!warning] 核对说明｜先确认争抢再限并发
>
> `iostat` 能显示设备排队和延迟，要与同一时间窗的查询延迟、合并任务数及吞吐对齐。4.x BE 配置文档未列出原示例的 `compaction_max_write_mbytes_per_sec`；优先核对已支持的每盘并发和线程池参数。降并发后同时观察查询 P95、写入和版本数，避免只把压力推迟。[BE 配置](https://doris.apache.org/docs/4.x/admin-manual/config/be-config/)


---

## 五、冷热数据分层

### 5.1 存储介质分层（SSD → HDD）

Doris 支持通过`storage_medium` 和`storage_cooldown_time` 实现热数据落 SSD、冷数据自动迁移到 HDD：


```sql
-- 建表时指定初始存储介质和冷却时间
CREATE TABLE hot_table (...)
PROPERTIES (
    "storage_medium" = "SSD",
    "storage_cooldown_time" = "2026-08-01 00:00:00"
);

-- 动态分区 + 冷热分层
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.hot_partition_num" = "7",      -- 最近 7 天在 SSD
    "dynamic_partition.storage_medium" = "HDD"        -- 更早的在 HDD
);

```


**工作原理：**

- 数据首次写入时落在 SSD 上
- 超过`storage_cooldown_time` 后，后台线程自动迁移到 HDD
- 配合动态分区的`hot_partition_num`，可实现滚动冷却

> [!note] 批注｜绝对冷却时间和动态热分区是两种配置
>
> 前一个例子的时间对相应分区是一个绝对时间点，后一个是动态创建分区时按热窗口选介质。存算一体部署须有满足副本标签的 SSD/HDD；用 `SHOW PARTITIONS` 和实际副本分布核对生效范围，别把 `hot_partition_num=7` 直接解读为任意表的“所有最近七天数据都在 SSD”。[SSD/HDD 分层](https://doris.apache.org/docs/4.x/table-design/tiered-storage/tiered-ssd-hdd/)

### 5.2 远程存储分层（HDFS / S3）

Doris 2.0+ 支持将冷数据下沉到 HDFS 或对象存储（S3/OSS/COS），本地仅保留热数据：


```sql
-- 创建远程存储资源
CREATE RESOURCE "s3_cold_storage"
PROPERTIES (
    "type" = "s3",
    "s3.endpoint" = "http://s3.amazonaws.com",
    "s3.region" = "us-east-1",
    "s3.access_key" = "xxx",
    "s3.secret_key" = "xxx",
    "s3.bucket" = "doris-cold-data"
);

-- 创建存储策略
CREATE STORAGE POLICY "cold_after_30d"
PROPERTIES (
    "storage_resource" = "s3_cold_storage",
    "cooldown_ttl" = "30d"
);

-- 建表或分区上应用策略
CREATE TABLE tiered_table (...)
PROPERTIES (
    "storage_policy" = "cold_after_30d"
);

```

> [!note] 批注｜冷数据迁移与存算分离不是同一个部署机制
>
> 原文的 `storage_policy` 描述存算一体下把冷数据迁往远端的策略；存算分离一般直接以远端存储作为数据层，BE 本地盘主要承担缓存。配置前确认版本和部署形态，不能把 `SSD → HDD → S3` 看成同一分区必经的三步迁移链。示例中的密钥只是占位符。


### 5.3 File Cache（本地缓存加速远程读取）

冷数据在远程存储上，查询时需要网络 IO。File Cache 在本地 SSD 上缓存热点冷数据的文件块：


```ini
# be.conf
file_cache_path = [{"path": "/ssd1/doris_cache", "total_size": 107374182400}]
# 配置 100GB 本地缓存空间

```


**缓存策略：** LRU（最近最少使用），热点冷数据自动缓存到本地。

> [!note] 批注｜缓存只影响延迟，不改变数据保留
>
> `total_size` 是缓存容量上限，示例 `107374182400` 字节约等于 100 GiB。缓存未命中要读远端；缓存满触发淘汰不等于源数据被删除。4.x 存算分离文档描述的是多队列 LRU，还可有 TTL 队列，不能用单一全局 LRU 解释所有对象的淘汰。[File Cache](https://doris.apache.org/docs/4.x/compute-storage-decoupled/file-cache/)

### 5.4 分层架构全景


```text
┌─────────────────────────────────────────────────────────┐
│                    查询层（FE/BE）                        │
├─────────────────────────────────────────────────────────┤
│  热数据（最近 N 天）     │  温数据          │  冷数据      │
│  SSD                    │  HDD             │  HDFS/S3    │
│  高 IOPS、低延迟         │  大容量、成本低    │  极低成本    │
├─────────────────────────────────────────────────────────┤
│           File Cache（SSD，LRU 缓存冷数据热点块）          │
└─────────────────────────────────────────────────────────┘

```


---

## 六、不同场景的调优方案

### 6.1 高频实时写入场景（如广告监控、日志）

特征：每秒数千次小批量写入，Cumulative Score 易飙高。


```ini
# be.conf 推荐配置
max_compaction_threads = 16                              # 充足线程
compaction_task_num_per_fast_disk = 8                    # SSD 并发
min_cumulative_compaction_num_singleton_deltas = 3       # 更激进触发
max_cumulative_compaction_num_singleton_deltas = 2000    # 单次合并更多

```


**配合手段：**

- 使用 Group Commit 合并小批次写入
- 增大客户端批次大小（减少 Rowset 产生频率）

### 6.2 大批量离线导入场景（如 Spark/Broker Load）

特征：单次导入数据量大，但频率低，Base Compaction 压力大。


```ini
# be.conf 推荐配置
compaction_memory_limit_per_worker = 4294967296          # 4GB，充足内存
base_compaction_min_data_ratio = 0.2                     # 更积极触发 Base
base_compaction_interval_seconds_since_last_operation = 3600  # 1小时强制

```


### 6.3 高并发查询场景（如报表平台）

特征：查询QPS高，需要限制 Compaction 对查询的影响。


```ini
# be.conf 推荐配置
max_compaction_threads = 4                               # 限制 Compaction 线程
compaction_task_num_per_disk = 2                         # 减少磁盘争抢
compaction_max_write_mbytes_per_sec = 50                 # 限速

```

> [!warning] 核对说明｜场景参数是原文样例，不是可直接套用的配置
>
> 高频小批先看事务/Rowset 生成速度，用批次合并降低增量；大批导入先看单 Tablet 数据量和 Base 输入范围。查询受影响时要用同时间窗验证是否真的由 Compaction 的 CPU 或 IO 占用造成；按活跃策略逐个调整已核实的参数，并保留对照基线。


---

## 七、监控与运维

### 7.1 关键监控指标

| 指标 | 含义 | 告警阈值建议 |
| --- | --- | --- |
|`doris_be_tablet_compaction_score` | Tablet Compaction Score | > 100 Warning, > 300 Critical |
|`doris_be_compaction_bytes_total` | Compaction 累计处理字节 | 观测趋势 |
|`doris_be_compaction_deltas_total` | Compaction 累计合并 Rowset 数 | 观测趋势 |
|`max_tablet_version_num` （SHOW PROC） | 当前最大 Version 数 | > 300 需关注 |

> [!warning] 核对说明｜核对指标维度、计数方式和用途
>
> 4.x 公开指标含 `doris_be_tablet_base_max_compaction_score`、`doris_be_tablet_cumulative_max_compaction_score` 与 `doris_be_tablet_version_num_distribution`。BE 最大值不是每 Tablet 的时间序列；配置项 `max_tablet_version_num` 也不代表现场最大值。对累计字节看窗口增量或速率，告警结合持续时间、版本分布和写入拒绝率，别照搬原阈值。[监控指标](https://doris.apache.org/docs/4.x/admin-manual/maint-monitor/metrics/)

### 7.2 手动触发 Compaction

紧急场景下可通过 HTTP API 手动触发：


```bash
# 对指定 Tablet 触发 Cumulative Compaction
curl -X POST "http://be_host:8040/api/compaction/run?tablet_id=12345&compact_type=cumulative"

# 对指定 Tablet 触发 Base Compaction
curl -X POST "http://be_host:8040/api/compaction/run?tablet_id=12345&compact_type=base"

# 查看 Compaction 状态
curl "http://be_host:8040/api/compaction/show?tablet_id=12345"

```

> [!warning] 核对说明｜触发成功和任务执行完成分开确认
>
> 命令应指向持有目标 Tablet 副本的 BE，使用该 BE 实际的 `webserver_port` 并满足其认证配置。`POST .../run` 用于发起任务；用 `GET .../run_status?tablet_id=...` 检查运行状态，再用 `GET .../show?tablet_id=...` 核对输入版本和最近成功状态。原文命令是示例，没有执行。[触发接口](https://doris.apache.org/docs/4.x/admin-manual/open-api/be-http/compaction-run/) · [查看状态](https://doris.apache.org/docs/4.x/admin-manual/open-api/be-http/compaction-status/)


### 7.3 日常巡检 SQL


```sql
-- 查看集群整体 Tablet 状态统计
SHOW PROC '/statistic';

-- 查看指定表的 Tablet 分布和 Version 情况
SHOW TABLET FROM db.table;

-- 查看正在进行的 Compaction 任务
SHOW PROC '/compactions';

```


---

## 八、实践建议速查

| 场景 | 核心动作 |
| --- | --- |
| Version 堆积报错 | 增大 Compaction 线程 + 减少写入频次 + 检查内存 |
| Compaction 慢 | 增大内存限制 + 增加线程 + 检查磁盘 IO |
| 查询受 Compaction 影响 | 限速 + 减少并发 + 错峰调度 |
| 磁盘空间不足 | 启用冷热分层 + 降副本 + 清理历史分区 |
| 写入吞吐不足 | Group Commit + 增大批次 + Auto Partition |

---

## 九、小结

| 要点 | 内容 |
| --- | --- |
| 双层 Compaction | Cumulative 处理增量碎片，Base 做全量合并 |
| Score 机制 | 数值越高优先级越高，监控核心指标 |
| 资源平衡 | Compaction 线程/内存/IO 与查询负载竞争 |
| 冷热分层 | SSD→HDD→HDFS/S3，File Cache 加速冷数据读取 |
| 场景化调优 | 高频写入/大批量导入/高并发查询，配置策略不同 |
