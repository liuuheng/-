---
title: "Paimon 精讲（一）：架构、写入、读取与核心元数据（文章批注版）"
aliases:
  - "Paimon 精讲（一）：10分钟建立完整架构认知，老鸟也能查漏补缺"
author: "胖泽的技术笔记"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s/VcO9_aEqneaoJp1Mu5NJtQ"
published_at: "2026-07-22"
reviewed_at: "2026-10-14"
tags:
  - Flink
  - Paimon
  - Paimon-精讲
---

# Paimon 精讲（一）：架构、写入、读取与核心元数据（文章批注版）

> [!info] 来源、整理方式与版本边界
> 原文：[《Paimon 精讲（一）：10分钟建立完整架构认知，老鸟也能查漏补缺》](https://mp.weixin.qq.com/s/VcO9_aEqneaoJp1Mu5NJtQ)，胖泽的技术笔记，2026-07-22。正文按原文章节顺序保留技术内容、代码、表格、字段和案例；新增内容统一标为“批注”“勘误”或“版本边界”。原文没有声明准确的 Paimon/Flink 版本，内部类、默认值和源码路径不视为稳定 API。

> [!warning] 批注：阅读前先校准版本边界
> - Snapshot 记录的是一次已提交的表状态及其 Manifest 引用，不直接保存业务行；`baseManifestList` 与 `deltaManifestList` 共同解析当前可见文件。
> - 原文的 Bucket 分类没有覆盖当前主键表的 Postpone Bucket（`bucket=-2`）。当前主键表应区分 Fixed、Dynamic、Postpone；Append 表的 `bucket=-1` 则是 Unaware Bucket，二者不能只因取值相同就视为一种机制。
> - `changelog-producer=lookup` 不是 CDC 表的默认答案。下游接受 Upsert 时可使用 `none`；上游能提供完整旧值时才适合 `input`；只有缺少旧值且下游需要完整撤回流时才评估 `lookup`。
> - 原文参数默认值、内部类和文件字段来自特定源码版本。部署时以实际 Paimon Connector 的 `$options`、`$schemas` 和依赖源码为准。
> 因为没有刻意控制字数，想着尽量讲清楚讲透彻，最后写完发现字数有点多，所以分两部分介绍：第一部分面向非技术人员，用类比方式介绍 Paimon 的定位与适用场景；第二部分面向技术人员，从组件职责、读写路径、文件规范、设计权衡等维度深入解析架构，并在架构基础上延伸介绍核心特性。

---

## 第一部分：面向非技术人员

### 一句话定位

> **Paimon 是一个"既能实时记账、又能随时翻历史账本"的智能数据仓库底座。**

### 用"超级图书馆"来类比架构

| 技术概念 | 非技术比喻 | 作用 |
| --- | --- | --- |
| **Catalog** | 图书馆总目录 | 知道哪张"表"在哪里 |
| **LSM-Tree** | 先记草稿本、定期整理上架 | 高效处理实时写入和更新 |
| **Snapshot（快照）** | 图书馆每天的"馆藏快照照片" | 支持查历史、不影响当前操作 |
| **Manifest 文件** | 每批书的入库清单 | 追踪数据文件的变化记录 |
| **Changelog** | 每日新书变动公告 | 让下游实时感知数据变化 |

### 三大核心能力

**1. 实时 + 历史，两手抓**

> 传统数据库像"快餐店"——只服务当下；传统数据仓库像"档案馆"——只存历史；
> **Paimon 像"既有现做窗口、又有完整档案的超市"——实时和历史都能查。**

**2. 改数据不怕乱（ACID 事务）**

> 多个部门同时往表里写数据，就像多个收银员同时记账，Paimon 保证账本永远不会记混、不会丢数据。

**3. 一份数据，多种工具都能用**

> 数据存一次，Flink（实时计算）、Spark（批量计算）、BI 报表工具都可以直接读，不需要到处复制数据。

### 最适合的业务场景

| 场景 | 通俗描述 | 典型例子 |
| --- | --- | --- |
| **数据库实时同步** | 业务数据库有变化，秒级同步到数仓 | 订单状态实时更新到报表 |
| **实时数据湖** | 日志、埋点数据流入，边存边查 | 用户行为实时分析 |
| **流批一体分析** | 同一份数据，既能做实时看板，又能跑隔夜报表 | 财务日报 + 实时收款监控 |
| **增量数据处理** | 只处理"新变化"的数据，不重复扫描全量 | 大规模 ETL 降本提速 |
| **时间旅行查询** | 查"昨天下午3点的数据是什么状态" | 数据回溯、审计对账 |

### Paimon 的定位图


```
       数据实时性要求
           ↑
    高  ●  Kafka（快但查不了历史）
        │
        │       ● Paimon
        │    （实时 + 可查询 + 低成本）
        │
    低  ●  传统数仓（慢但查得了历史）
           ────────────────────→
        低        存储成本       高

Paimon 填补了"又快又便宜又能查历史"的空白地带。

```


---

## 第二部分：面向技术人员

> **架构介绍 ≠ 特性介绍**：架构介绍回答"系统由什么组成、怎么运转"；特性介绍回答"系统能做什么"。本部分先讲架构，再在架构基础上延伸特性。

---

### 一、核心术语与组件

在深入架构之前，先明确 Paimon 的核心术语：

| 组件 | 说明 |
| --- | --- |
| **Schema** | 表的字段定义、主键定义、分区键定义及配置选项，存于`schema/` 目录，支持版本演化 |
| **Snapshot** | 某一特定时间点已提交数据的统一访问入口，是 MVCC 的核心载体 |
| **Manifest List** | Avro 格式文件，聚合多个 Manifest 文件的元信息（分区统计、文件数等） |
| **Manifest** | Avro 格式文件，记录数据文件或 Changelog 文件的增删变更（ADD / DELETE） |
| **Data File** | 实际存储数据的列存文件（ORC/Parquet），存于分区 + Bucket 目录下 |
| **Changelog File** | 由`changelog-producer` 生成的行级变更记录（before/after），结构与 Data File 相同 |
| **Global Index** | 面向 Bucket 或 Partition 的全局索引，包含 Hash Index 和 Deletion Vector |
| **Data File Index** | 面向单个数据文件的列级索引（BloomFilter / Bitmap / BSI），存于`index/` 目录或内嵌 Manifest |

---

### 二、文件存储布局（官方 Spec）

这是理解 Paimon 架构的基础，所有组件都体现在物理文件层次上：


```
warehouse/
└── default.db/
    └── my_table/
        ├── schema/                          # Schema 元数据（DDL 变更历史）
        │   ├── schema-0                     # 建表时的初始 Schema
        │   └── schema-1                     # ALTER TABLE 后的新版本
        │
        ├── snapshot/                        # 快照目录
        │   ├── EARLIEST                     # 提示文件（指向最早快照，可能不精确）
        │   ├── LATEST                       # 提示文件（指向最新快照，可能不精确）
        │   ├── snapshot-1                   # JSON 格式快照文件
        │   └── snapshot-2
        │
        ├── manifest/                        # 文件索引层
        │   ├── manifest-list-<uuid>         # ManifestList：聚合多个 Manifest
        │   ├── manifest-<uuid>              # Data Manifest：追踪数据文件变更
        │   └── index-manifest-<uuid>        # Index Manifest：追踪索引文件变更
        │
        ├── index/                           # Global Index 文件
        │   └── index-<uuid>                 # Hash Index 或 Deletion Vector 文件
        │
        └── dt=2024-07/                      # 分区目录（按分区键）
            └── bucket-0/                    # Bucket 目录（并发写入单元）
                ├── data-<uuid>-0.orc        # 数据文件（命名：data-{uuid}-{id}.{format}）
                ├── data-<uuid>-1.orc
                └── changelog-<uuid>.orc     # Changelog 文件（结构同数据文件）

```


**各目录一句话说明**：

-`schema/` → 表结构的版本历史，每次 DDL 变更新增一个文件
-`snapshot/` → 每次 commit 新增一个 snapshot-N 文件，ID 必须连续
-`manifest/` → 所有文件变更的索引层，Snapshot 通过它找到数据文件
-`index/` → 全局索引，支持动态分桶和删除向量
-`dt=xxx/bucket-N/` → 实际数据落地位置，按分区 + Bucket 双层隔离

---

### 三、核心模块划分与职责


```
┌─────────────────────────────────────────────────────────────────────┐
│                         计算引擎（外部）                              |
│            Flink / Spark / Hive / Trino / StarRocks                 │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
          ┌───────────────────▼────────────────────┐
          │             Catalog Layer              │  ← 元数据入口
          │   表注册 / 发现 / Schema 版本管理         │
          └───────────────────┬────────────────────┘
                              │
          ┌───────────────────▼────────────────────┐
          │              Table API                  │  ← 读写统一接口
          │     TableWrite / TableRead / TableScan  │
          └────────┬──────────────────┬────────────┘
                   │                  │
       ┌───────────▼──────┐  ┌────────▼──────────────────────┐
       │   Write Path     │  │         Read Path              │
       │                  │  │                                │
       │  MemTable        │  │  Snapshot Manager              │
       │  → FlushBuffer   │  │  → ManifestList                │
       │  → LSM-Tree      │  │  → Manifest                    │
       │  → Compaction    │  │  → Global Index（分桶路由）      │
       └───────────┬──────┘  │  → Data File Index（列裁剪）    │
                   │         │  → DataFile Reader             │
                   │         └────────┬───────────────────────┘
                   │                  │
          ┌────────▼──────────────────▼────────────┐
          │           Manifest System               │  ← 文件索引层
          │  ManifestList → Manifest → DataFile     │
          │              ↘ Index Manifest           │
          └────────────────────┬───────────────────┘
                               │
          ┌────────────────────▼────────────────────┐
          │             FileIO Layer                 │  ← 存储抽象层
          │         HDFS / S3 / OSS / GCS            │
          └─────────────────────────────────────────┘

```


| 模块 | 职责 |
| --- | --- |
| **Catalog Layer** | 管理数据库、表的元数据；维护 Schema 版本历史 |
| **Table API** | 对外暴露统一的读写接口，屏蔽底层引擎差异 |
| **MemTable** | 写入内存缓冲区，按主键排序 + 去重，攒批后触发 flush |
| **LSM-Tree Engine** | 管理多层 SST 文件，负责 flush 和 Compaction |
| **Snapshot Manager** | 管理快照生命周期，实现 MVCC，提供时间旅行入口 |
| **Manifest System** | 文件索引层，记录每个 Snapshot 对应哪些数据文件和索引文件 |
| **Global Index** | 维护主键→Bucket 的映射（Hash Index）或行级删除标记（Deletion Vector） |
| **Data File Index** | 文件级列索引（BloomFilter/Bitmap/BSI），加速文件内查询裁剪 |
| **FileIO Layer** | 存储抽象，屏蔽不同对象存储的 API 差异 |

---

### 四、写入路径：一条数据怎么落盘

> [!note] 补充批注：文件落盘不等于提交可见
> `Writer` 先生成数据文件和待提交元数据；只有 `Committer` 成功发布新的 Snapshot 后，读端才会沿该 Snapshot 看到这批文件。排查“对象存储已有文件但查询不到”时，应先检查 Snapshot 是否提交成功，而不是只查看数据目录。


```
Producer（Flink Task / Spark Writer）
      │
      ▼
  MemTable（内存）
  └─ 按 primary key 排序 + 去重（内存内合并）
  └─ 写入 _VALUE_KIND / _SEQUENCE_NUMBER 系统列
      │
      │ 达到 write-buffer-size 阈值 或 checkpoint 触发
      ▼
  flush → Level-0 Data File（ORC/Parquet）
  └─ 文件内有序，文件间无序，允许 key 重叠
  └─ 文件名格式：data-{uuid}-{id}.{format}
      │
      │ 写入侧同步 或 异步 Compaction 触发
      ▼
  Compaction
  └─ Level-0 → Level-1 → ... → Level-N
  └─ 跨层归并排序，消除重复 key，应用 MergeFunction
  └─ 生成新 Data File，标记旧文件为 DELETE
      │
      ▼
  生成 Manifest（Avro）
  └─ 记录新增文件（ADD）和删除文件（DELETE）
  └─ 携带每个文件的行数、主键范围、列统计等元数据
      │
      ▼
  生成 ManifestList（Avro）
  └─ 聚合本次变更的所有 Manifest 文件引用
      │
      ▼
  commit → 原子写入新 Snapshot 文件（JSON）
  └─ ID 严格递增且连续，写入成功后才对外可见
  └─ 新 Snapshot 引用 baseManifestList + deltaManifestList
  └─ 基于 rename 语义保证原子性

```


**关键设计**：写入和 Compaction 是**解耦**的。写入路径只负责 flush 到 Level-0，Compaction 异步进行，因此写入延迟低且稳定。

---

### 五、读取路径：一次查询怎么执行

> [!note] 补充批注：读路径以 Snapshot 为入口
> 一次读取通常沿 `Snapshot → Manifest List → Manifest → Data File` 定位数据，并在途中执行分区、统计信息和索引裁剪。直接枚举数据目录不能还原表的有效状态，因为目录中可能同时存在未提交、已删除或尚未清理的文件。


```
Query（SQL / DataStream API）
      │
      ▼
  Snapshot Manager
  └─ 读取 LATEST 提示文件，定位最新（或指定）Snapshot
  └─ 若提示文件不精确，扫描全部 snapshot-N 确定边界
      │
      ▼
  ManifestList → Manifest
  └─ 读取 baseManifestList + deltaManifestList
  └─ 利用 ManifestList 中的 _PARTITION_STATS 跳过不相关 Manifest
  └─ 合并 ADD / DELETE 记录，得到有效数据文件列表
      │
      ▼
  Partition Pruning + Bucket 路由
  └─ 分区裁剪：过滤掉不匹配查询条件的分区目录
  └─ Bucket 路由：
       Fixed Bucket → hash(primary_key) % bucket_num 直接定位
       Dynamic Bucket → 查询 Global Hash Index 确定 Bucket
      │
      ▼
  Data File Index 裁剪（如已建索引）
  └─ BloomFilter：等值过滤，跳过肯定不含目标值的文件
  └─ BSI：范围查询加速
  └─ Bitmap：低基数列等值查询
      │
      ▼
  DataFile Reader
  └─ 读取 ORC/Parquet，利用列存做列裁剪和谓词下推
  └─ 若存在 Deletion Vector，按位图跳过已删除行
      │
      ▼
  Merge-on-Read（仅 Primary Key Table）
  └─ 多层 SST 文件中相同 key 的记录按 _SEQUENCE_NUMBER 归并
  └─ 应用 MergeFunction（Deduplicate / Aggregation / Partial-Update）
  └─ 返回最终合并结果

```


**关键设计**：Manifest 系统是读性能的核心——通过`_PARTITION_STATS` 分区统计实现 Manifest 级剪枝，通过 File Index 实现文件级裁剪，无需像传统 Hive 一样全量列举存储目录。

---

### 六、Snapshot 数据结构详解

Snapshot 是 Paimon 所有操作的统一入口，是一个 JSON 文件，字段如下：

| 字段 | 说明 |
| --- | --- |
|`version` | 快照格式版本，当前为 **3** |
|`id` | 快照 ID，与文件名一致，从 1 开始严格连续 |
|`schemaId` | 本次提交时对应的 Schema 版本 |
|`baseManifestList` | 指向前序所有历史变更的 ManifestList（存量） |
|`deltaManifestList` | 指向本次快照新增变更的 ManifestList（增量） |
|`changelogManifestList` | 本次产生的 Changelog 文件索引，无则为`null` |
|`indexManifest` | 本次涉及的 Global Index 文件索引，无则为`null` |
|`commitUser` | 通常由 UUID 生成，标识流任务的写入者，用于故障恢复 |
|`commitIdentifier` | 流式写入事务 ID，一个事务可产生多次不同 commitKind 的提交 |
|`commitKind` | 变更类型：`APPEND` /`COMPACT` /`OVERWRITE` /`ANALYZE` |
|`timeMillis` | 提交时间戳（毫秒） |
|`logOffsets` | 提交时对应的 Log 系统偏移量（如 Kafka offset） |
|`totalRecordCount` | 本快照所有变更的总记录数 |
|`deltaRecordCount` | 本次新增变更的记录数 |
|`changelogRecordCount` | 本次产生的 Changelog 记录数 |
|`watermark` | Flink Watermark 值；无水印时为`Long.MIN_VALUE` |
|`statistics` | 表统计信息文件名（用于 CBO 优化） |

**Snapshot 层次关系**：


```
Snapshot
 ├── baseManifestList   → 历史全量数据文件索引
 ├── deltaManifestList  → 本次增量数据文件索引
 ├── changelogManifestList → CDC 变更日志索引（可选）
 └── indexManifest      → Global Index 文件索引（可选）

```


---

### 七、Manifest 数据结构详解

Manifest 以 Avro 格式存储，分为两类：

### 7 .1 ManifestList（清单列表）

聚合多个 Manifest 文件的元信息：

| 字段 | 含义 |
| --- | --- |
|`_FILE_NAME` | Manifest 文件名 |
|`_FILE_SIZE` | Manifest 文件大小 |
|`_NUM_ADDED_FILES` | 该 Manifest 中新增文件数 |
|`_NUM_DELETED_FILES` | 该 Manifest 中删除文件数 |
|`_PARTITION_STATS` | 分区字段的 min/max 统计，**用于查询时跳过不相关 Manifest** |
|`_SCHEMA_ID` | 写入该 Manifest 时的 Schema 版本 ID |

### 7 .2 Data Manifest（数据清单）

追踪数据文件与 Changelog 文件的 ADD / DELETE 变更：

| 字段 | 含义 |
| --- | --- |
|`_KIND` |`ADD`  或`DELETE` |
|`_PARTITION` | 分区规格（BinaryRow 编码） |
|`_BUCKET` | 文件所属 Bucket |
|`_TOTAL_BUCKETS` | 写入时的总 Bucket 数（用于 Bucket 变更后的校验） |
|`_FILE._FILE_NAME` | 数据文件名 |
|`_FILE._FILE_SIZE` | 文件大小 |
|`_FILE._ROW_COUNT` | 总行数（含增删） |
|`_FILE._MIN_KEY`  /`_MAX_KEY` | 文件主键范围，用于查询裁剪 |
|`_FILE._KEY_STATS` | 主键列 min/max/null 统计 |
|`_FILE._VALUE_STATS` | 值列统计信息 |
|`_FILE._LEVEL` | LSM Tree 层级（Level-0 ~ Level-N） |
|`_FILE._SCHEMA_ID` | 文件写入时的 Schema 版本 |
|`_FILE._SEQUENCE_NUMBER` | 序列号范围，用于 Merge-on-Read 时判断新旧 |
|`_FILE._DELETE_ROW_COUNT` | 删除行数（总行数 = 新增行数 + 删除行数） |
|`_FILE._EMBEDDED_FILE_INDEX` | 若索引较小，直接内嵌到 Manifest，避免额外 I/O |
|`_FILE._FILE_SOURCE` | 文件来源：`APPEND` 或`COMPACT` |

### 7 .3 Index Manifest（索引清单）

追踪 Global Index 文件（Hash Index / Deletion Vector）的变更：

| 字段 | 含义 |
| --- | --- |
|`_KIND` |`ADD`  或`DELETE` |
|`_INDEX_TYPE` |`"HASH"`  或`"DELETION_VECTORS"` |
|`_FILE_NAME` | 索引文件名 |
|`_DELETIONS_VECTORS_RANGES` | Deletion Vector 的偏移量和长度信息 |

---

### 八、数据文件（Data File）内部结构

### 8 .1 文件列结构对比

**Primary Key Table**（以`T(a PK, b, c)` 为例，文件含 6 列）：

| 列名 | 类型 | 说明 |
| --- | --- | --- |
|`_KEY_a` | 与主键同类型 | 主键列副本（带前缀避免列名冲突，启用 thin-mode 后可省略） |
|`_VALUE_KIND` | TINYINT | 行类型：`0=INSERT`，`1=DELETE` |
|`_SEQUENCE_NUMBER` | BIGINT | 用于 Merge-on-Read 时判断记录先后顺序 |
|`a` | 原始类型 | 实际主键值 |
|`b` | 原始类型 | 业务列 |
|`c` | 原始类型 | 业务列 |

> 启用`data-file.thin-mode` 后，`_KEY_a` 可省略，文件含 5 列。

**Append-Only Table**（以`T(a, b, c)` 为例，文件含 3 列）：

仅存储业务列，无任何系统列，与普通 Parquet/ORC 文件无异。

### 8 .2 Changelog 文件

结构与 Data File **完全相同**，仅对 Primary Key Table 生效。类似数据库 Binlog，记录行级别的 before/after 变更，供下游流式消费。

---

### 九、索引系统详解

Paimon 有两套独立的索引体系：

### 9 .1 Global Index（全局索引，存于`index/` 目录）
| 类型 | 用途 | 数据粒度 | 实现 |
| --- | --- | --- | --- |
| **Hash Index** | 动态分桶时，维护主键哈希值 → Bucket 的映射，写入时路由 | 主键级别 | 连续 4 字节 Big-Endian int 序列 |
| **Deletion Vector** | 标记每个数据文件中已删除的行位置，避免 Merge-on-Read | 行位置级别 | RoaringBitmap（32-bit 默认，64-bit 可兼容 Iceberg） |

两种索引的核心区别：

- **Hash Index** 解决"写哪个 Bucket"问题（路由）
- **Deletion Vector** 解决"哪些行已失效"问题（标记删除），可替代部分 Merge-on-Read 场景

### 9 .2 Data File Index（文件级索引，存于`index/` 或内嵌 Manifest）
每个数据文件对应一个索引文件，结构为 **HEAD（列名/偏移目录）+ BODY（索引字节流）**：

| 索引类型 | 配置参数 | 适用场景 | 支持数据类型 |
| --- | --- | --- | --- |
| **BloomFilter** |`file-index.bloom-filter.columns` | 等值过滤（概率型，无假阴性） | 通用 |
| **Bitmap** |`file-index.bitmap.columns` | 低基数列等值查询 | Int/BigInt/Date/VarChar 等 |
| **BSI（Bit-Slice Index）** |`file-index.bsi.columns` | **数值范围查询** 加速 | Int/BigInt/Date/Decimal 等 |

> 小索引文件可直接内嵌到 Manifest 的`_EMBEDDED_FILE_INDEX` 字段，避免额外 I/O。

---

### 十、Snapshot 与 Manifest 的协作关系（MVCC 实现）


```
snapshot-1                          snapshot-2（新 commit）
    │                                    │
    ├─ baseManifestList ─────────────────┤ ← 复用，代价为 O(1)
    │       └─ manifest-A                ├─ baseManifestList
    │           └─ data-file-1  ADD      │       └─ manifest-A（复用）
    │           └─ data-file-2  ADD      │
    │                                    └─ deltaManifestList（本次新增）
    └─ deltaManifestList（空）                    └─ manifest-B
                                                     └─ data-file-3  ADD
                                                     └─ data-file-4  ADD

```


**设计决策**：

- Manifest 采用**增量叠加**而非全量复写，每次 commit 只写 delta 部分，代价 O(delta)
- 读取时合并 base + delta 得到完整有效文件集合
- 旧 Snapshot 持有旧 ManifestList 引用，天然实现**快照隔离**，无需分布式锁
- Snapshot ID 必须严格连续，断号即意味着提交失败

---

### 十一、Compaction 的位置与作用

Compaction 是 LSM-Tree 架构的核心代价，也是读写性能的平衡点：


```
写入压力大时（Level-0 文件积累）：
  读性能下降 ← 需跨多个 Level 扫描文件做 Merge-on-Read

Compaction 执行后：
  Level-0 文件归并到 Level-N（文件内有序、文件间不重叠）
  同一 key 只保留最新版本（或聚合结果）
  读性能恢复 ← 文件数减少，扫描范围缩小

Compaction 的代价：
  写放大（同一份数据被多次重写）
  CPU / IO 资源消耗

```


Paimon 提供两种 Compaction 触发模式：

- **同步 Compaction**：写入 checkpoint 时触发，资源消耗集中，简单可控
- **异步 Compaction（Lookup 模式）**：独立 Compaction 任务并行执行，写入链路延迟更低

---

### 十二、整体设计的核心工程权衡

| 设计选择 | 带来的收益 | 付出的代价 |
| --- | --- | --- |
| LSM-Tree 存储引擎 | 写入低延迟、高吞吐 | 写放大、读需 Merge-on-Read |
| Snapshot + Manifest 增量索引 | MVCC、时间旅行、原子提交、低 commit 代价 | 元数据文件增多，需定期 Snapshot 过期清理 |
| ManifestList 分区统计 | 查询时跳过无关 Manifest，减少元数据扫描 | 写入时需维护统计信息 |
| 列存格式（ORC/Parquet） | 高压缩比、列裁剪、谓词下推 | 行级随机写入代价高（需 LSM 缓冲） |
| 两套索引体系（Global + File） | 全局路由 + 文件级裁剪，读性能分层优化 | 索引维护成本，写入放大 |
| Bucket 分片写入 | 并发写入隔离、局部有序 | Fixed Bucket 数固定，扩展需重建；Dynamic Bucket 需维护 Hash Index |
| FileIO 抽象层 | 多云存储兼容 | 对象存储无原子 rename，需额外处理并发安全 |

> **一句话总结 Paimon 的架构本质**： 它是把 **RocksDB 的 LSM-Tree** 与 **Iceberg 的 Snapshot/Manifest 文件索引**结合在一起，运行在对象存储之上，并通过 **Changelog File + Global Index** 为 Flink 流式场景深度优化的 Table Format。

---

## 第三部分：核心特性（架构的自然延伸）

有了以上架构基础，核心特性就是架构能力的直接体现。

---

### 一、两种核心表类型

| 特性 | **Primary Key Table** | **Append-Only Table** |
| --- | --- | --- |
| 写入语义 | Upsert / Delete（有`_VALUE_KIND` 系统列） | 仅 Insert（无系统列） |
| 存储引擎 | LSM-Tree，多层 SST | 直接写 ORC/Parquet |
| 合并方式 | Deduplicate / Aggregation / Partial-Update | 无需合并 |
| Bucket 路由 | hash(pk) % N 或 Dynamic Hash Index | 可不分 Bucket（bucket=-1） |
| 读放大 | 有（Level 间需 Merge-on-Read） | 无 |
| 写放大 | Compaction 触发 | 最小 |
| 适用场景 | CDC 同步、状态更新、宽表拼接 | 日志、埋点、大规模批量写入 |

---

### 二、流批一体读写

- **写入侧**：同一套 LSM-Tree 引擎，Flink 流式写入和 Spark 批量写入共用同一套文件格式
- **读取侧**：Snapshot 机制天然支持两种模式：

- **批量读**：绑定某个 Snapshot，读取静态全量数据
- **流式读**：监听 Snapshot 链的新增，消费增量变更（类似 Kafka 消费 offset）

---

### 三、Changelog 生产机制


> [!note] 批注：Changelog、Merge Engine、Table Mode 是三条独立配置轴
> Merge Engine 决定同一主键的多条记录如何合并；Changelog Producer 决定流式消费者能看到哪些变更；MOR、COW、MOW 决定版本在写入、Compaction 和读取阶段如何消解。只看到“CDC”或“主键表”不足以推导出必须使用 `lookup`。
Changelog 是 Paimon 区别于其他 Lake Format 的核心能力，本质是从架构中的 Snapshot Diff 或写入路径中产生 before/after 记录：

| 模式 | 原理 | 延迟 | 适用场景 |
| --- | --- | --- | --- |
|`none` （默认） | Level-0 Compaction 时对比产生，不完整 | 高 | 不需要精确 Changelog |
|`input` | 写入时直接透传上游 changelog-mode 数据 | 最低 | Flink 上游已产生 changelog |
|`lookup` （推荐） | 写入前 lookup 旧值，自动生成完整`+I/-U/+U/-D` 四元组 | 低 | CDC 同步场景 |
|`full-compaction` | Full Compaction 后对比前后两个 Snapshot 的 Manifest diff | 分钟级 | 对实时性要求不高的消费场景 |

---

### 四、时间旅行查询

直接来自 Snapshot 机制，指定 Snapshot ID 或时间戳，读取对应版本的 ManifestList：


```
-- 按 Snapshot ID 查询
SELECT * FROM orders /*+ OPTIONS('scan.snapshot-id'='3') */;

-- 按时间戳查询
SELECT * FROM orders /*+ OPTIONS('scan.timestamp-millis'='1720000000000') */;

-- 增量读取（两个 Snapshot 之间的变更）
SELECT * FROM orders /*+ OPTIONS('incremental-between'='2,5') */;

```


---

### 五、MergeFunction（合并策略）

本质是 LSM-Tree Compaction 时的可插拔合并逻辑，在`_SEQUENCE_NUMBER` 排序后对同一主键的多条记录做自定义处理：


```
-- Deduplicate（默认）：按 sequence 保留最新记录
CREATE TABLE orders (...) WITH ('merge-engine' = 'deduplicate');

-- Partial Update：只更新写入的字段，其余保持原值
-- 适合多流分别写入宽表不同字段
CREATE TABLE wide_table (...) WITH (
  'merge-engine' = 'partial-update',
  'fields.col_a.sequence-group' = 'col_b,col_c'
);

-- Aggregation：按聚合函数合并，适合预聚合场景
CREATE TABLE metrics (
  id BIGINT, pv BIGINT, uv BIGINT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'merge-engine' = 'aggregation',
  'fields.pv.aggregate-function' = 'sum',
  'fields.uv.aggregate-function' = 'max'
);

-- First Row：只保留第一条到达的记录
CREATE TABLE dedup_table (...) WITH ('merge-engine' = 'first-row');

```


---

### 六、Schema Evolution

- Schema 变更写入`schema/schema-N` 新文件，旧 Schema 文件保留
- 每个 Snapshot 携带`schemaId`，读取时按对应 Schema 解析历史文件
- Manifest 中每个数据文件条目也携带`_SCHEMA_ID`，读取时自动做字段兼容映射
- 新旧 Schema 数据文件可共存，支持列增加、列改名、列类型拓宽等操作

---

### 七、Bucket 模式对比


> [!warning] 勘误：当前主键表还有 Postpone Bucket
> 当前官方分类为 Fixed（正整数）、Dynamic（`-1`）和 Postpone（`-2`）。Postpone 会先暂存数据，再由批处理或 Compaction 分配真实 Bucket；它适合分区规模差异大、无法提前统一定桶的场景。Append 表的 Unaware Bucket 也使用 `-1`，但它不维护主键到 Bucket 的动态索引。
| 维度 | Fixed Bucket（bucket=N） | Dynamic Bucket（bucket=-1） |
| --- | --- | --- |
| **路由方式** |`hash(bucket-key) % N` | 查询 Global Hash Index 确定 Bucket |
| **数据局部性** | 好，同一主键始终在同一 Bucket | 一般，依赖 Hash Index 一致性 |
| **扩展性** | 差，扩容需重新建表 | 好，自动扩展 |
| **跨 Bucket 去重** | 不支持 | 支持（Hash Index 全局唯一） |
| **适用场景** | 数据量稳定、主键分布均匀 | 数据量增长不可预测、需全局唯一主键 |
| **Append 表（bucket=-1）** | 不分 Bucket，写入 bucket-0，无并发限制 | — |

---

### 八、与同类产品技术对比

| 特性 | **Paimon** | **Hudi** | **Iceberg** |
| --- | --- | --- | --- |
| 核心存储引擎 | LSM-Tree | Copy-on-Write / MOR | Copy-on-Write |
| 原生流式读写 | ⭐⭐⭐（为 Flink 设计） | ⭐⭐（有限支持） | ⭐⭐（有限支持） |
| 实时 Upsert | ⭐⭐⭐ | ⭐⭐⭐ | ⭐（需额外 Merge） |
| Changelog 输出 | ⭐⭐⭐（原生支持四元组） | ⭐ | ❌ |
| 读性能（Compaction 后） | ⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐ |
| Deletion Vector | ✅（支持） | ✅ | ✅（v2+） |
| 生态成熟度 | 成长中（Apache 顶级项目） | 成熟 | 成熟 |
| 最适合场景 | Flink 主导的实时数仓 | 批处理 + 增量更新 | 大规模批量分析 |

---

### 九、场景选型决策树


```
需要 CDC / Upsert 语义？
  ├─ 是 → Primary Key Table
  │       ├─ 下游需实时消费变更？        → changelog-producer = lookup
  │       ├─ 需多流分别写宽表不同字段？   → merge-engine = partial-update
  │       ├─ 需预聚合？                  → merge-engine = aggregation
  │       ├─ 数据量稳定、分布均匀？       → Fixed Bucket
  │       └─ 数据量不可预测？            → Dynamic Bucket（bucket=-1）
  │
  └─ 否 → Append-Only Table
          ├─ 高吞吐日志写入、不需分 Bucket → bucket=-1 + ORC 格式
          ├─ 需按某字段路由并发读写？       → bucket=N + bucket-key
          └─ 需加速点查 / 范围查询？       → 配置 file-index（BloomFilter/BSI）

```


---

### 十、核心参数调优速查


> [!warning] 批注：速查表不能替代版本核对
> `file-index.bsi.columns` 属于旧版命名，当前文档使用 `file-index.range-bitmap.columns`。`deletion-vectors.enabled=true` 表示启用 MOW 读取路径，还要核对 Merge Engine、Changelog Producer、Compaction 可见性和部署版本兼容性，不能只打开一个选项。
| 参数 | 默认值 | 调优方向 |
| --- | --- | --- |
|`num-sorted-run.compaction-trigger` | 5 | 增大 → 减少写放大，增大读放大 |
|`compaction.max.file-num` | 50 | 控制单次 Compaction 合并文件数上限 |
|`lookup.cache-file-retention` | 1h | Lookup Compaction 本地缓存保留时长 |
|`snapshot.num-retained.min` | 10 | 最少保留快照数，影响时间旅行可查范围 |
|`snapshot.time-retained` | 1h | 快照保留时长，过期后 Snapshot 可被清理 |
|`changelog-producer` | none | CDC 场景推荐`lookup` |
|`write-buffer-size` | 256mb | 内存充足时调大，降低 flush 频率 |
|`data-file.thin-mode` | false | 开启后主键列不再额外存副本，节省存储 |
|`file-index.bloom-filter.columns` | — | 指定需要 BloomFilter 索引的列（等值查询加速） |
|`file-index.bsi.columns` | — | 指定需要 BSI 索引的列（范围查询加速） |
|`deletion-vectors.enabled` | false | 开启 Deletion Vector，避免 Merge-on-Read 代价 |

## 复核资料

- [Paimon Basic Concepts](https://paimon.apache.org/docs/master/concepts/basic-concepts/)
- [Snapshot Specification](https://paimon.apache.org/docs/master/concepts/spec/snapshot/)
- [Primary-Key Data Distribution](https://paimon.apache.org/docs/master/primary-key-table/data-distribution/)
- [Changelog Producer](https://paimon.apache.org/docs/master/primary-key-table/changelog-producer/)

## 系列导航

- 下一篇：[[Paimon 精讲（二）：Flink SQL 写入与 Changelog 生成机制（文章批注版）]]
