---
title: "Doris 入门篇 02：存储模型基础"
author: "外星人的知识杂谈"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s/12DixKHf-yi6yvxzJ-N9-w"
published_at: "2026-07-24T19:15:00+08:00"
annotated_at: "2026-11-18"
cssclasses:
  - wechat-article
  - doris-intro-annotated
tags:
  - doris
  - 文章批注
---

# Doris 入门篇 02：存储模型基础

<div class="article-meta">外星人的知识杂谈 · 2026-07-24</div>

[原文链接](https://mp.weixin.qq.com/s/12DixKHf-yi6yvxzJ-N9-w)

<!-- 保留原文正文与顺序；按捕获网页恢复14个三级标题。“批注”“核对说明”为新增，原文示例命令未执行。移除装饰封面及文末下一篇导航。 -->

> **定位：** 入门篇第 2 篇 | 理解数据在 Doris 中的物理组织方式
>
> **阅读前提：** 已了解 Doris FE/BE 架构（第 01 篇）

---

## 一、背景动机

OLAP 引擎的性能差异，80% 来自存储层设计。理解数据从逻辑表到物理文件的完整映射关系，是后续学习查询优化、Compaction 调优的基础。

<!-- codex-annotation-start: scope-and-evidence -->
> [!warning] 核对说明｜适用架构与“80%”的证据范围
> 原文没有给出“80%”对应的测试、负载或计算方法，不能把这个比例当作测得的性能归因。存储布局会影响读取列数、裁剪范围和文件合并成本，查询计划、数据分布和计算资源也会影响最终耗时。
>
> 本篇的 BE 本地多副本、SSD / HDD 迁移等内容主要按存算一体架构解释。存算分离使用共享存储和本地缓存，不能把这里的本地副本数、磁盘迁移流程直接套过去。
<!-- codex-annotation-end: scope-and-evidence -->

---

## 二、逻辑数据组织：从表到 Tablet

### 2.1 层次结构


```
Database (库)
  └── Table (表)
        └── Partition (分区)：按时间/范围划分数据的逻辑单元
              └── Tablet (桶/分片)：数据分布的最小逻辑单元
                    └── Replica (副本)：同一 Tablet 的多份拷贝，分布在不同 BE

```

<!-- codex-annotation-start: logical-levels -->
> [!note] 批注｜分区、Tablet 与副本分别回答什么问题
> Partition 决定一行属于哪一段逻辑数据，也便于按时间删除整段数据；分桶规则决定该分区内一行落到哪个 Tablet；Replica 则是在不同 BE 上保存同一个 Tablet 的数据拷贝。查找一条记录和决定保存几份数据，是两个不同问题。
>
> Tablet 不是某个单独的物理文件。一个 Tablet 的某份副本中还会有多个 Rowset 和 Segment；下文讨论的是这份副本内部的数据组织。分区也不限于图中的时间 / 范围方式，Doris 还支持 List 分区。
<!-- codex-annotation-end: logical-levels -->


### 2.2 Partition（分区）

分区是数据管理的基本单元，决定数据的生命周期管理和查询裁剪粒度：


```sql
-- Range 分区（最常用，按时间）
PARTITION BY RANGE(dt) (
    PARTITION p20260701 VALUES LESS THAN ("2026-07-02"),
    PARTITION p20260702 VALUES LESS THAN ("2026-07-03")
)

-- 动态分区：自动按天创建/删除
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.start" = "-30",   -- 保留30天
"dynamic_partition.end" = "3"        -- 预创建3天

```

<!-- codex-annotation-start: partition-boundaries -->
> [!warning] 核对说明｜分区名不决定范围，动态窗口包含当天
> `LESS THAN` 指定的是上界。原例没有为首分区指定下界，因此 `p20260701` 实际覆盖 `[MIN_VALUE, 2026-07-02)`；`p20260702` 覆盖 `[2026-07-02, 2026-07-03)`。首分区不会仅因名字带“20260701”就只保存 7 月 1 日。需要严格的一天范围时，应明确给出下界和上界。[Doris 手动分区](https://doris.apache.org/docs/3.x/table-design/data-partitioning/manual-partitioning/)
>
> 动态分区以当天为偏移 `0`。`start=-30` 在历史分区齐全时保留过去 30 天加当天；`end=3` 包含未来 3 天，连同当天共 4 个当前 / 未来分区。`create_history_partition` 默认不启用，不能仅设置 `start` 就认定新表已经补齐历史分区；实际边界还要结合分区时区和调度时点检查。[Doris 动态分区](https://doris.apache.org/docs/3.x/table-design/data-partitioning/dynamic-partitioning/)
>
> 上面的 Range 定义和动态属性是建表片段，不是两段可以独立执行的完整 SQL；动态属性还需放在建表或修改表的属性配置中。
<!-- codex-annotation-end: partition-boundaries -->


**分区的作用：**

- 查询时 Partition Pruning，只扫描涉及的分区
- 过期数据整分区删除，比 DELETE 高效几个数量级
- 冷热分层按分区粒度管理

<!-- codex-annotation-start: partition-pruning -->
> [!note] 批注｜裁剪、删除分区和释放文件是不同操作
> Partition Pruning 依靠查询条件与分区边界排除不可能命中的分区。表按 `dt` 分区，但查询没有能够约束 `dt` 的条件时，并不会因为建了分区就自动少扫描。可在 `EXPLAIN` 中检查实际命中的分区，而不是只看 DDL。
>
> 删除整个分区可以直接移除一段逻辑数据，省去逐行筛选；物理文件何时清理还取决于系统回收流程。原文“高效几个数量级”没有对应测试，不能当固定倍数。若只删除分区中少量符合条件的记录，整分区删除也会改变业务语义。[Doris 分区与分桶机制](https://doris.apache.org/docs/3.x/table-design/data-partitioning/basic-concepts/)
<!-- codex-annotation-end: partition-pruning -->

### 2.3 Tablet（分桶）

每个 Partition 进一步按 Hash 或 Random 分桶：


```sql
-- Hash 分桶
DISTRIBUTED BY HASH(user_id) BUCKETS 16

-- Random 分桶（无热点写入场景）
DISTRIBUTED BY RANDOM BUCKETS 16

```


**Tablet 是 Doris 中数据分布、副本、Compaction 的最小单位。**

设计原则：

- 单个 Tablet 数据量建议 100MB ~ 1GB
- Tablet 数量 = Partition 数 × Bucket 数 × 副本数
- 过多 Tablet 会加重元数据管理负担（FE 内存压力）

<!-- codex-annotation-start: tablet-counts -->
> [!warning] 核对说明｜逻辑分片数与副本实例数要分开计算
> 对只计一个物化索引、各分区桶数相同的表，逻辑 Tablet 数是“分区数 × 桶数”，再乘副本数才是副本实例数。例如 20 个分区、每分区 16 桶，得到 320 个逻辑 Tablet；三副本对应 960 份副本实例。不同分区桶数不同时逐分区求和，同步物化索引的 Tablet 还需另计。
>
> “100MB～1GB”不是跨版本的统一目标。本次核对的 3.x 自动分桶文档建议约 1～10GB，并仍需结合表模型、扫描并行度、导入和维护成本调整。桶太多会形成较多小分片，桶太少又可能限制并行度。[Doris 自动分桶](https://doris.apache.org/docs/3.x/table-design/data-partitioning/auto-bucket/)
>
> Hash 分桶是否均匀取决于键值分布；Random 分桶不按用户键做稳定映射，也不能按 Hash 分桶键裁剪，且在本次核对的版本范围主要用于 Duplicate Key 表。不能把“Random”理解成对所有表和所有写入热点都有效。
<!-- codex-annotation-end: tablet-counts -->

### 2.4 Replica（副本）


```sql
-- 默认 3 副本
"replication_allocation" = "tag.location.default: 3"

-- 历史冷数据降为 1 副本（节省存储）
ALTER TABLE t MODIFY PARTITION p202601
SET ("replication_allocation" = "tag.location.default: 1");

```


副本分布策略：

- 同一 Tablet 的多副本分布在**不同 BE 节点**
- 写入时基于 Quorum（多数写成功即返回）
- 读取时选择负载最低的副本

<!-- codex-annotation-start: replica-semantics -->
> [!warning] 核对说明｜副本成功数、数据可见性与读调度
> 默认三副本通常需要至少两份成功；两副本的多数派仍是两份，一副本则是一份。实际门槛还可能由 `min_load_replica_num` 等配置调整。“副本写成功”“事务已提交”和“发布后查询可见”要分别检查，不能据此推出写完两份就立即可查。[Doris 建表副本属性](https://doris.apache.org/docs/3.x/sql-manual/sql-statements/table-and-view/table/CREATE-TABLE/)
>
> 读取必须先选择可用且拥有所需版本的副本，再由调度逻辑分配扫描。原文的“负载最低”不是无条件保证，例如本地副本选择配置也会影响选择。把历史数据降为一副本会减少存储占用，同时失去其他副本提供的故障恢复能力；是否采用需要结合数据能否重建和故障容忍要求判断。[Doris FE 配置](https://doris.apache.org/zh-CN/docs/3.x/admin-manual/config/fe-config/)
<!-- codex-annotation-end: replica-semantics -->

---

## 三、物理数据组织：从 Tablet 到文件

### 3.1 层次结构


```
Tablet
  └── Rowset：一次数据变更产生的不可变文件集合
        └── Segment：单个物理文件（默认上限 256MB）
              ├── Column Data：列式数据块（以 Page 为单位）
              ├── Column Index：各列索引（ZoneMap/Bloom/Ordinal）
              ├── Short Key Index：前缀索引
              └── Footer：文件元信息

```

<!-- codex-annotation-start: segment-size -->
> [!warning] 核对说明｜Segment 大小与文件层级的适用范围
> Rowset 是一组文件及其元信息，Segment 是其中的列式数据文件；一份 Rowset 可以包含多个 Segment。图中的“256MB”不能理解为所有版本、所有生成路径都遵守的绝对文件上限。
>
> 256MiB 出现在部分历史常量和配置说明中，但导入与不同 Compaction 路径的切分规则、运行配置可能不同；本次核对的 `branch-3.0` 分支与 3.x 文档也存在数值差异。排查文件大小时，应查实际版本及对应写入 / Compaction 配置，而不是看到大于 256MB 的文件就判断异常。[3.x BE 配置](https://doris.apache.org/docs/3.x/admin-manual/config/be-config/)、[官方 3.0 分支配置源码](https://raw.githubusercontent.com/apache/doris/branch-3.0/be/src/common/config.cpp)
<!-- codex-annotation-end: segment-size -->


### 3.2 Rowset

Rowset 是一次**原子性数据变更**的结果，具有以下特性：

- **不可变：**

  一旦生成不会被修改，只能被 Compaction 合并生成新 Rowset
- **版本号：**`[start_version, end_version]`

  ，支持 MVCC 快照读
- **来源：**

  每次 Stream Load / Compaction / Schema Change 都会产生新 Rowset


```
时间线：
Load1 → Rowset [2,2]
Load2 → Rowset [3,3]
Load3 → Rowset [4,4]
Compaction → Rowset [2,4]  （合并上面三个）

```

<!-- codex-annotation-start: rowset-versions -->
> [!note] 批注｜一次导入、一次 flush 和一个 Rowset 的关系
> 普通 Stream Load 中，一个 Tablet 的写入通道可以多次 flush，生成多个 Segment，最后组成这个 Tablet 本次导入的 Rowset；一次导入涉及多个 Tablet 时，也会产生多个 Rowset。flush 不是事务提交边界，“一次原子性数据变更”也不意味着全表只生成一个文件集合。
>
> 上面的 `[2,2]`、`[3,3]`、`[4,4]` 描述同一个 Tablet 的版本。合并结果 `[2,4]` 覆盖原来的版本范围，没有再插入一批相同的业务记录。正在读取的查询可能仍使用旧 Rowset，系统在安全时机才清理旧文件；MVCC 快照读不代表用户可以无限期查询任意历史版本。[Doris Stream Load 与版本发布原理](https://doris.apache.org/blog/principle-of-Doris-Stream-Load/)
<!-- codex-annotation-end: rowset-versions -->


**问题：** 如果写入频率高、Compaction 跟不上，Rowset 数量会不断增长，导致：

- 读取时需要 merge 多个版本，读放大
- 当 Rowset 数超过阈值（默认 10000），写入被拒绝

<!-- codex-annotation-start: version-limit -->
> [!warning] 核对说明｜版本数量不等于最大版本号
> `max_tablet_version_num` 的“默认 10000”未被本次核对支持：3.x BE 配置页及 `branch-3.0` 源码给出的是 2000，部分旧 FAQ 又保留了其他数值。因此应通过 `SHOW BACKEND CONFIG;` 核对目标 BE 的实际配置，不能直接把文章数字用作告警阈值。
>
> 这里要检查的是 Tablet 累积的版本 / Rowset 压力，不是最大的版本号。`[2,4]` 的区间长度是三，不代表合并后仍有三个 Rowset。出现版本积压时，需要同时检查导入批次、合并进度和资源情况；单纯调高拒写阈值并不会消除小文件或读放大。[官方 BE 配置](https://doris.apache.org/docs/3.x/admin-manual/config/be-config/)
<!-- codex-annotation-end: version-limit -->

### 3.3 Segment 文件

每个 Rowset 包含一个或多个 Segment 文件，Segment 是 Doris 最小的物理存储文件：


```
┌─────────────────────────────────────────────┐
│              Segment 文件结构                 │
├─────────────────────────────────────────────┤
│  Short Key Index (前缀索引)                  │
├─────────────────────────────────────────────┤
│  Column 1: Page 0 | Page 1 | Page 2 | ...  │  ← 列式存储
│  Column 2: Page 0 | Page 1 | Page 2 | ...  │
│  Column 3: Page 0 | Page 1 | Page 2 | ...  │
│  ...                                         │
├─────────────────────────────────────────────┤
│  Column Index (ZoneMap / Bloom / Bitmap)     │
├─────────────────────────────────────────────┤
│  Ordinal Index (各列 Page 偏移量)            │
├─────────────────────────────────────────────┤
│  Footer (文件元信息、schema、统计)            │
└─────────────────────────────────────────────┘

```

<!-- codex-annotation-start: segment-indexes -->
> [!note] 批注｜文件结构图描述职责，不代表固定字节顺序
> 图中的数据区、索引和 Footer 用于说明读文件时需要哪些信息，不能据此手工推算真实文件偏移。Footer 保存列和索引位置等元信息，读取器再按需要定位对应内容；具体布局受存储格式版本影响。
>
> Ordinal Index 帮助根据行号定位列页；Short Key Index 利用排序键前缀缩小范围；ZoneMap 用 Min / Max 等统计排除不可能命中的数据块。BloomFilter 等索引还依赖建表配置，图中列出的索引不代表每个 Segment 全部具备。ZoneMap 或 BloomFilter 判断“可能存在”后，仍需要读取候选数据并检查谓词。[Doris 索引机制](https://doris.apache.org/docs/dev/table-design/index/index-overview/)
<!-- codex-annotation-end: segment-indexes -->


### 3.4 Page（数据页）

Page 是列数据的最小 I/O 单位（默认 64KB）：

- 每个 Page 存储一列的若干行数据
- 每个 Page 有独立的压缩（LZ4/ZSTD）
- 每个 Page 有自己的 ZoneMap（Min/Max）用于数据跳过
- 读取时以 Page 为粒度解压、过滤

<!-- codex-annotation-start: page-io -->
> [!warning] 核对说明｜64KiB 页不等于每次磁盘读取固定 64KiB
> 本次核对的 3.0 分支中，`storage_page_size` 默认值为 65536，即 64KiB，并可受表属性影响。它是数据页组织的尺度；编码、压缩和实际数据会改变文件中页的字节数，底层还可能预读或合并读取。因此不能把 Page 大小直接等同于磁盘或对象存储请求大小。[页大小默认定义](https://raw.githubusercontent.com/apache/doris/branch-3.0/gensrc/proto/olap_file.proto)、[Segment Writer 对页大小的使用](https://raw.githubusercontent.com/apache/doris/branch-3.0/be/src/olap/rowset/segment_v2/segment_writer.cpp)
>
> 每列的 Page 包含该列一段行位置上的值。查询只涉及少数列时，可以避开无关列；统计信息又可以跳过不可能匹配的页。Min / Max 与条件有重叠只说明该页“可能命中”，不代表页内每行都符合条件。
<!-- codex-annotation-end: page-io -->

---

## 四、列式存储 vs 行式存储

### 4.1 为什么 OLAP 选择列存

| 维度 | 行存（MySQL） | 列存（Doris） |
| --- | --- | --- |
| 存储方式 | 一行数据连续存储 | 同一列数据连续存储 |
| 适合场景 | 点查、事务（读写少量行） | 聚合分析（扫描大量行的少数列） |
| I/O 效率 | SELECT \* 高效 | SELECT col1, col2 高效（只读需要的列） |
| 压缩率 | 低（不同类型混存） | 高（同类型数据压缩效果好） |
| CPU 友好 | 随机访问 | 顺序扫描 + SIMD 向量化 |

<!-- codex-annotation-start: row-vs-column -->
> [!note] 批注｜比较时同时看读多少行、返回多少列
> 列存的直接收益是少读无关列。对一张很宽的表做大范围聚合，但只需要两列时，这个差别可能很大；按主键只取一行并返回全部列时，列存则需要定位并读取多个列的数据。不能仅凭 `SELECT *` 或 `SELECT col1, col2` 的写法判断效率，还要看命中行数、索引和缓存。
>
> 表中的随机 / 顺序访问和压缩率也是常见负载下的概括，行存可以顺序扫描和压缩，列存也可能发生分散读取。SIMD 是对一批值并行运算的 CPU 执行方式，列式批次方便采用它，但仅有磁盘上的列式布局不保证所有算子都使用 SIMD。
<!-- codex-annotation-end: row-vs-column -->

### 4.2 列存的压缩优势

同一列数据类型相同、值域相近，压缩算法效果显著：

| 编码方式 | 适用场景 | 压缩效果 |
| --- | --- | --- |
| Dictionary Encoding | 低基数列（状态、类型） | 极高（10:1+） |
| Run-Length Encoding (RLE) | 连续重复值（排序后的分区列） | 极高 |
| Bit Packing | 整数列（值域较小） | 高 |
| Delta Encoding | 递增序列（时间戳、自增 ID） | 高 |
| LZ4 / ZSTD | 通用压缩 | 中-高 |

实际生产中，Doris 表的压缩率通常在 **3:1 ~ 10:1。**

<!-- codex-annotation-start: encoding-compression -->
> [!note] 批注｜编码和压缩是两层处理，压缩比需要实际测量
> Dictionary、RLE 等编码先利用重复值、连续值或整数值域改变数据表示；LZ4 / ZSTD 再对字节做通用压缩。例如状态列只有少量不同字符串时，可以先用较短的字典编号表示。实际编码由类型、存储格式和实现决定，这张表不是所有列都会依次启用的算法清单。
>
> 列类型相同并不保证值域相近，也不保证高压缩比。原文的“10:1+”“3:1～10:1”缺少数据集和口径，不能直接作为容量规划依据。测量时应说明未压缩数据怎样计量、是否包含索引，以及是否计入多副本和 Compaction 临时空间；同时比较解压 CPU 与查询耗时。[Doris 列编码与压缩](https://doris.apache.org/docs/3.x/table-design/column-compression/)
<!-- codex-annotation-end: encoding-compression -->

---

## 五、LSM-like 写入模型

Doris 的写入模型借鉴了 LSM-Tree 的思想：


```
写入路径：
Data → MemTable (内存) → flush → Segment (磁盘) → Compaction → 合并大文件

对比 LSM-Tree：
Data → MemTable → flush → L0 SSTable → Compaction → L1/L2...

```


### 核心特点

1. **写入先到内存：**

   MemTable 积累到阈值（默认 100MB）后 flush 成 Segment
2. **文件不可变：**

   写入产生新 Rowset，不修改已有文件
3. **后台合并：**

   Compaction 线程定期合并小文件，减少读放大
4. **写入高吞吐：**

   顺序写磁盘，无随机 I/O

<!-- codex-annotation-start: lsm-memtable -->
> [!warning] 核对说明｜LSM-like 描述追加和合并思路
> 这里借鉴的是“内存缓冲、写新文件、后台合并”的思路，不能由此认定 Doris 具有图中 SSTable 的完整 L0 / L1 / L2 层级。表模型决定同键记录如何保留、覆盖或聚合；文件追加本身不决定这些业务语义。
>
> 原文“MemTable 默认 100MB”也有版本依赖。本次核对的 `branch-3.0` 中 `write_buffer_size` 是 209715200，即 200MiB，聚合表还存在专用阈值。内存阈值不等于压缩后 Segment 的大小，结束写入或内存压力等情况也可能触发 flush。“顺序写”说明减少原地修改，不代表整个读写链路没有随机 I/O 或额外索引、合并开销。[官方 3.0 分支配置](https://raw.githubusercontent.com/apache/doris/branch-3.0/be/src/common/config.cpp)
<!-- codex-annotation-end: lsm-memtable -->

### 读放大与写放大的 Tradeoff

|  | 写放大 | 读放大 |
| --- | --- | --- |
| 不做 Compaction | 低（只写一次） | 高（读时需 merge 大量 Rowset） |
| 频繁 Compaction | 高（数据被反复重写） | 低（读时只扫少量大文件） |
| Doris 策略 | Cumulative 合并小文件 + Base 合并历史大文件，平衡二者 |  |

<!-- codex-annotation-start: compaction-cost -->
> [!note] 批注｜Compaction 以额外读写换取后续少读
> 读放大是为了得到查询结果而额外读取的数据；写放大是为了写入或维护这些数据而额外重写的数据。Compaction 读取若干旧 Rowset、生成新的 Rowset，再替换元数据中的版本覆盖关系，因此会消耗 I/O、CPU、内存和临时空间，收益是后续查询面对更少的读取路径。
>
> “合并成大文件”没有改变表模型的约束：Duplicate Key 要保留明细，Aggregate Key 按规则聚合，Unique Key 遵守更新与删除规则。Cumulative / Base 是原文介绍的常见机制，具体选择和触发还受 `compaction_policy` 等配置影响，不能把它理解成每次导入后立即固定运行两轮。[Doris Compaction 策略](https://doris.apache.org/docs/3.x/admin-manual/trouble-shooting/compaction/)
<!-- codex-annotation-end: compaction-cost -->

---

## 六、数据生命周期

一条数据从写入到被查询到的完整流程：


```
1. Client 发起 Stream Load → FE 路由到目标 BE

2. BE 接收数据 → 写入 MemTable（内存排序缓冲区）

3. MemTable 写满 → flush 为 Segment 文件 → 封装成 Rowset [v, v]

4. FE Publish Version → Tablet 版本号推进 → 数据对查询可见

5. 后台 Cumulative Compaction → 合并相邻小 Rowset

6. 后台 Base Compaction → 将 Cumulative 结果与 Base Rowset 合并

7. 冷却时间到达 → 数据从 SSD 迁移到 HDD / HDFS（冷热分层）

8. 分区过期 → 整分区数据删除

```

<!-- codex-annotation-start: publish-and-cooldown -->
> [!warning] 核对说明｜写入可见性、远程冷却和分区过期的边界
> 第 1～4 步省略了事务准备、提交与发布的细节。FE 把 Stream Load 请求路由到 Coordinator BE，后者负责向目标 Tablet 写入；MemTable 可以多次 flush 后再组成 Rowset。文件已生成或事务处于 `COMMITTED`，都不能单独证明查询已经可见；还需完成版本发布。FE 协调发布，数据文件并不是由 FE 写入。[Stream Load 事务与发布过程](https://doris.apache.org/blog/principle-of-Doris-Stream-Load/)
>
> 第 7 步混合了两套存储分层机制。`storage_medium=SSD` 配合 `storage_cooldown_time` 管理本地 SSD 向 HDD 的迁移，需要存在可用且正确标记的目标 HDD 路径；它不会因时间到达就自动写入 HDFS。[SSD / HDD 层级存储](https://doris.apache.org/zh-CN/docs/3.x/table-design/tiered-storage/tiered-ssd-hdd/)
>
> 远程冷却需要另外配置 S3 / HDFS Resource、Storage Policy 和 `cooldown_ttl` 等属性，再关联表或分区，并检查目标版本及表模型是否支持。第 8 步的“过期”也需要动态分区等实际删除策略；普通静态分区不会只因日期变旧就自动消失。[Doris 3.x 远程存储](https://doris.apache.org/zh-CN/docs/3.x/table-design/tiered-storage/remote-storage/)
<!-- codex-annotation-end: publish-and-cooldown -->


---

## 七、实践建议

### 观察 Tablet 状态


```sql
-- 查看表的 Tablet 分布
SHOW TABLETS FROM db.table;

-- 查看 Tablet 诊断信息
ADMIN SHOW TABLET DIAGNOSIS FROM db.table;

-- 查看 Rowset 版本信息
SHOW TABLET 12345 STORAGE FORMAT VERBOSE;

```

<!-- codex-annotation-start: diagnostic-syntax -->
> [!warning] 核对说明｜两条命令需要更正，存储格式也不是 Rowset 版本
> 原文代码按原样保留，但本次核对的 3.x 文档不支持 `ADMIN SHOW TABLET DIAGNOSIS FROM db.table;`。先用 `SHOW TABLETS FROM db.table;` 找到实际 Tablet ID，再使用 `SHOW TABLET DIAGNOSIS 12345;` 诊断该 ID。[官方诊断语法](https://doris.apache.org/zh-CN/docs/3.x/sql-manual/sql-statements/table-and-view/data-and-status-management/SHOW-TABLET-DIAGNOSIS/)
>
> 存储格式命令是 `SHOW TABLET STORAGE FORMAT VERBOSE;`，不在中间加 Tablet ID；它用于查看 V1 / V2 等存储格式，不能用来查询 Rowset 的版本区间。检查指定 Tablet 可先执行 `SHOW TABLET 12345;` 并按返回的 `DetailCmd` 继续定位，Rowset 明细还需进入对应 BE 的诊断信息。`db.table` 和 `12345` 都是原文示例，实际执行前必须替换。本次未连接集群运行这些命令。[官方存储格式语法](https://doris.apache.org/docs/3.x/sql-manual/sql-statements/table-and-view/data-and-status-management/SHOW-TABLET-STORAGE-FORMAT/)
<!-- codex-annotation-end: diagnostic-syntax -->


### 关键参数感知

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
|`replication_num` | 3 | 副本数 |
|`bucket_size` | 自动 | 单 Bucket 目标数据量 |
|`max_tablet_version_num` | 10000 | Rowset 版本数上限，超过拒绝写入 |
|`storage_medium` | HDD/SSD | 存储介质 |
|`storage_cooldown_time` | 无 | 冷却时间（到期迁移到 HDD） |

<!-- codex-annotation-start: configuration-scope -->
> [!warning] 核对说明｜表属性、BE 配置和估算值不能混用
> 这张表混合了不同层次的配置。副本和存储介质通常通过表 / 分区属性管理；`max_tablet_version_num` 是 BE 配置，不能作为同类表属性随意写入 DDL。原文没有给出各项的版本和作用域，使用时要分别核对。
>
> 本次未在 Doris 3.x 自动分桶依据中确认 `bucket_size` 是该用途的有效参数。官方做法是 `BUCKETS AUTO` 配合 `estimate_partition_size`；后者表示预估的单分区数据量，不是单桶的硬大小上限，也不保证数据增长后自动重分现有分区。不要把原表中的 `bucket_size` 直接抄进建表属性。[Doris 自动分桶配置](https://doris.apache.org/docs/3.x/table-design/data-partitioning/auto-bucket/)
<!-- codex-annotation-end: configuration-scope -->

---

## 八、小结

| 要点 | 内容 |
| --- | --- |
| 逻辑层次 | Database → Table → Partition → Tablet → Replica |
| 物理层次 | Tablet → Rowset → Segment → Page |
| 存储方式 | 列式存储，同列数据连续，压缩率高 |
| 写入模型 | LSM-like：MemTable → flush → Compaction |
| 核心 Tradeoff | 读放大 vs 写放大，通过 Compaction 策略平衡 |
