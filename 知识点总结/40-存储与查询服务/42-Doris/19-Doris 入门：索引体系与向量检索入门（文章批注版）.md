---
title: "Doris 入门篇07：索引体系与向量检索入门"
author: "外星人的知识杂谈"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484338&idx=1&sn=511862a4d62a48f81c9534320ea53b88&chksm=c1bf09aff6c880b98fdc29343d35180b5d2d36003f1d8757da83903447ad9108ebab4d676df7"
published_at: "2026-07-29T20:23:00+08:00"
annotated_at: "2026-11-18"
cssclasses:
  - wechat-article
  - doris-intro-annotated
tags:
  - doris
  - 文章批注
---

# Doris 入门篇07：索引体系与向量检索入门

<div class="article-meta">外星人的知识杂谈 · 2026-07-29</div>

[原文链接](https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484338&idx=1&sn=511862a4d62a48f81c9534320ea53b88&chksm=c1bf09aff6c880b98fdc29343d35180b5d2d36003f1d8757da83903447ad9108ebab4d676df7)

<!-- 正文按原文顺序保留；“批注”“核对说明”为新增。移除装饰封面及篇末导航。示例命令未在集群运行。 -->

> 定位：

---

## 一、背景动机

OLAP 查询的性能瓶颈往往在数据扫描阶段。索引的本质是：**在不扫描全量数据的前提下，快速定位或跳过不相关的数据。**

Doris 提供了从粗粒度到细粒度的多层索引体系，能在不同场景下大幅减少扫描量。此外，随着 AI/RAG 场景兴起，Doris 也引入了向量索引能力，将 OLAP 分析与向量检索统一在一个引擎中。

---

## 二、索引层次总览


```text
过滤粒度：粗 ──────────────────────────────────────────────── 细

分区裁剪         前缀索引          ZoneMap        Bloom Filter    Bitmap/倒排索引
(Partition)     (Short Key)      (Min/Max)      (Page 级)       (行级精确)
  │                │                │               │               │
  ▼                ▼                ▼               ▼               ▼
跳过整个分区    跳过行组(1024行)   跳过整个Page    跳过整个Page    精确定位行

```


| 索引类型 | 过滤粒度 | 数据结构 | 适用查询 | 建表方式 |
| --- | --- | --- | --- | --- |
| 分区裁剪 | 分区级 | 元数据判断 | WHERE 包含分区列 | PARTITION BY |
| 前缀索引 | 行组级(1024行) | 稀疏 B-Tree | Key 列前缀等值/范围 | 自动（Key 列前 36 字节） |
| ZoneMap | Page 级 | Min/Max | 数值范围过滤 | 自动（所有列） |
| Bloom Filter | Page 级 | 布隆过滤器 | 高基数等值查询 | 需声明 |
| Bitmap 索引 | 行级 | Roaring Bitmap | 低基数等值查询 | 需声明 |
| 倒排索引 | Token 级 | Lucene-based | 全文检索 / 等值 / 范围 | 需声明 |
| NGram BF | 子串级 | N-Gram + Bloom | LIKE ‘%keyword%’ | 需声明 |

> [!note] 批注：粒度表用于理解，不是固定执行顺序
>
> 分区裁剪决定哪些分区进入扫描范围；不同存储索引可以在扫描器内跳过范围或缩小候选行集。谓词类型、列类型和数据分布决定能用哪个索引。把原表当作“各结构能过滤到哪里”的对照，实际过滤效果用 Query Profile 核对。

---

## 三、各索引详解

### 3.1 前缀索引（Short Key Index）

Doris **自动**为每张表建立前缀索引，无需手动声明。

**原理：** 取 Key 列的前 36 字节作为索引键，每 1024 行建一个索引条目。查询时先用索引定位到目标行组，再在行组内扫描。


```sql
CREATE TABLE t (
    user_id   BIGINT,     -- 8 字节 ← 前缀索引包含
    event_type VARCHAR(32), -- 最多 32 字节 ← 部分包含（累计 36 字节截断）
    dt        DATE,
    value     INT
)
DUPLICATE KEY(user_id, event_type, dt);

```


**前缀索引能加速的查询：**


```sql
-- 能命中（从左到右前缀匹配）
WHERE user_id = 12345
WHERE user_id = 12345 AND event_type = 'click'

-- 不能命中（跳过了 user_id）
WHERE event_type = 'click'
WHERE dt = '2026-07-01'

```


**优化技巧：** 将高频查询的过滤列放在 Key 列最前面。

> [!warning] 核对说明：36 字节是上限，遇到 VARCHAR 会截断
>
> 前缀索引基于排序键的最左侧列；4.x 文档还规定遇到 `VARCHAR` 时在该列截断，后续排序列不再进入这个短键前缀。示例更适合解释 `user_id` 单独过滤和跳过 `user_id` 的区别；不能按 `VARCHAR(32)` 简单填满剩余 28 字节来推断每列覆盖情况。[前缀索引](https://doris.apache.org/docs/4.x/table-design/index/prefix-index/)

### 3.2 ZoneMap（Min/Max 索引）

**自动**为每个列的每个 Page 记录最小值和最大值。

**原理：** 查询时如果 WHERE 条件与某 Page 的 [min, max] 范围无交集，直接跳过该 Page。


```sql
-- 假设某 Page 的 amount 列：min=100, max=500
WHERE amount > 600   -- 该 Page 被跳过（不扫描）
WHERE amount = 300   -- 该 Page 需要扫描（在范围内）

```


**对排序敏感：** 数据按 Key 列排序后，Key 列的 ZoneMap 区间更窄，过滤效果更好。非 Key 列数据分散，ZoneMap 效果有限。

> [!note] 批注：范围相交仅表示无法跳过
>
> `amount = 300` 落在 `[100,500]` 并不说明 Page 中有 300。排序或聚集让相邻数据的值域变窄，才更容易让谓词与某个 Page 的范围不相交；对全局分散在各 Page 的列，等值过滤即使很少命中，也可能跳不过多少 Page。

### 3.3 Bloom Filter 索引

需要在建表时显式声明。


```sql
CREATE TABLE t (
    ...
) PROPERTIES (
    "bloom_filter_columns" = "user_id, device_id"
);

-- 或后续添加
ALTER TABLE t SET ("bloom_filter_columns" = "user_id, device_id");

```


**原理：** 为指定列的每个 Page 构建布隆过滤器。查询等值条件时，先查 Bloom Filter——如果返回“不存在”，跳过该 Page。

**适用场景：** 高基数列（如 user\_id、device\_id）的等值过滤。

**注意：**

- 只对等值查询有效（`=`、`IN`），对范围查询无效
- 有假阳性（可能误判为存在），但无假阴性
- 会增加存储开销（每列每 Page 约 4KB）

> [!note] 批注：看值分布，别只看全表基数
>
> 如果待查值在多数 Page 中都存在，Bloom Filter 几乎不能跳过数据，即便这个列整体基数很高。反过来，“可能存在”还要继续读取验证。估算收益时对照真实谓词做测试，通过 `RowsBloomFilterFiltered` 看实际跳过的行数；原文每 Page 大小仅作估算参考。[BloomFilter 索引](https://doris.apache.org/docs/4.x/table-design/index/bloomfilter/)

### 3.4 Bitmap 索引


```sql
-- 创建 Bitmap 索引
CREATE INDEX idx_status ON orders (status) USING BITMAP;

-- 删除
DROP INDEX idx_status ON orders;

```


**原理：** 为列的每个取值建一个 Bitmap（位图），第 N 位为 1 表示第 N 行含有该值。查询时做位运算快速定位。

**适用场景：** 低基数列（取值几十~几百种，如状态、性别、平台）。

**不适用：** 高基数列（每个值只出现几次，Bitmap 占用反而大于原始数据）。

> [!warning] 核对说明：不要照抄 USING BITMAP 到 4.x
>
> 原文在解释按值记录行位置的索引概念。Doris 4.x `CREATE INDEX` 文档列出的索引类型是 `INVERTED`、`NGRAM_BF` 和 `ANN`，并未列出 `BITMAP`。这也和第 04 篇的 `BITMAP` 数据类型不同：后者用于保存集合，不是 `status` 列二级索引的 DDL。[CREATE INDEX](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/table-and-view/index/CREATE-INDEX/)

### 3.5 倒排索引（Inverted Index）

Doris 2.0+ 引入，基于 CLucene 实现。


```sql
-- 建表时声明
CREATE TABLE logs (
    log_time  DATETIME,
    level     VARCHAR(16),
    message   TEXT,
    INDEX idx_msg (message) USING INVERTED PROPERTIES("parser" = "unicode")
)
DUPLICATE KEY(log_time)
DISTRIBUTED BY RANDOM BUCKETS 16;

-- 全文检索查询
SELECT * FROM logs WHERE message MATCH_ANY 'error timeout';

-- 等值/范围也能走倒排索引
SELECT * FROM logs WHERE level = 'ERROR';

```


**适用场景：**

- 日志全文检索（替代 ES）
- VARCHAR 列的等值/范围查询加速
- VARIANT 列的子字段索引

> [!warning] 核对说明：原文只为 message 声明索引
>
> `idx_msg` 只覆盖 `message`，因此 `WHERE level = 'ERROR'` 不会使用它。要用倒排索引做 `level` 等值过滤，需要为相应列建索引并选择适合精确值的配置。对已有历史数据，还要按版本和部署模式核对索引是否已构建完成；看到索引定义不等于历史 Segment 已有索引数据。[倒排索引](https://doris.apache.org/docs/4.x/table-design/index/inverted-index/overview/)

### 3.6 NGram Bloom Filter


```sql
-- 建表时声明
INDEX idx_url (url) USING NGRAM_BF PROPERTIES("gram_size" = "3", "bf_size" = "1024")

```


**原理：** 将字符串拆成 N 个字符的子串（N-Gram），对每个 Page 的所有 N-Gram 建布隆过滤器。

**适用场景：**`LIKE '%keyword%'` 模糊匹配加速（普通索引对中间匹配无效）。

> [!note] 批注：搜索词长度必须覆盖一个 gram
>
> `gram_size=3` 时，`LIKE '%ab%'` 没有足够长的连续字面串供该索引过滤；`LIKE '%abc%'` 才可能利用索引。索引只能剔除不可能含有指定片段的块，候选数据仍要用原 `LIKE` 谓词验证。[NGram BloomFilter](https://doris.apache.org/docs/4.x/table-design/index/ngram-bloomfilter-index/)

---

## 四、索引命中判断

用`EXPLAIN` 验证索引是否被使用：


```sql
EXPLAIN SELECT * FROM orders WHERE status = 'paid';

```


关注输出中的：

- `SHORT-CIRCUIT-CONDITIONS`

  ：前缀索引命中
- `PREDICATES`

  ：下推到 Scan 的过滤条件
- `tabletList`

  ：扫描的 Tablet 数量（分区裁剪效果）

> [!warning] 核对说明：下推、裁剪与索引实际过滤要分开看
>
> `PREDICATES` 说明谓词下推到 Scan；`tabletList` 说明计划扫描哪些 Tablet。两者都不能证明某个存储索引过滤了行。`SHORT-CIRCUIT-CONDITIONS` 也不是通用的前缀索引命中标识。可以先看计划中的 `partitions`、`tablets` 和谓词，再从同次执行的 Profile 查扫描过滤指标；例如 Bloom Filter 用 `RowsBloomFilterFiltered`。[Query Profile](https://doris.apache.org/docs/4.x/query-acceleration/query-profile/) · [BloomFilter 验证](https://doris.apache.org/docs/4.x/table-design/index/bloomfilter/)

---

## 五、向量索引与 AI 场景整合

### 5.1 什么是向量检索

在 AI/RAG 场景中，文本/图片/音频被 Embedding 模型转换为高维向量（如 768 维 float 数组）。**向量检索**就是在数据库中找到与查询向量最相似（距离最近）的 K 个向量。


```text
查询："推荐类似《晴天》的歌曲"
  → Embedding Model → query_vector [0.12, -0.34, 0.56, ...]
  → 在数据库中找 Top-K 最近邻
  → 返回相似歌曲列表

```


### 5.2 Doris 的向量能力

Doris 2.1+ 支持向量数据存储和 ANN（Approximate Nearest Neighbor）近似最近邻检索：


```sql
-- 建表（存储向量）
CREATE TABLE song_embeddings (
    song_id     BIGINT,
    song_name   VARCHAR(256),
    embedding   ARRAY<FLOAT>    -- 向量存储为 FLOAT 数组
)
DUPLICATE KEY(song_id)
DISTRIBUTED BY HASH(song_id) BUCKETS 8;

-- 创建向量索引
CREATE INDEX idx_emb ON song_embeddings (embedding)
USING INVERTED
PROPERTIES (
    "index_type" = "HNSW",         -- 索引算法
    "metric_type" = "L2",          -- 距离度量：L2/COSINE/IP
    "dim" = "768",                 -- 向量维度
    "M" = "16",                    -- HNSW 参数
    "efConstruction" = "200"       -- 构建时精度
);

```


**支持的索引算法：**

| 算法 | 特点 | 适用场景 |
| --- | --- | --- |
| HNSW | 查询快，构建慢，内存占用大 | 在线查询，对延迟敏感 |
| IVF | 构建快，查询中等 | 大数据量，允许稍高延迟 |

**距离度量：**

| 度量 | 含义 | 适用 |
| --- | --- | --- |
| L2 | 欧氏距离 | 通用 |
| COSINE | 余弦相似度 | 文本 Embedding |
| IP | 内积 | 归一化后等价于 COSINE |

> [!warning] 核对说明：4.0 原生 ANN 使用 USING ANN
>
> Doris 4.0 新增 `ARRAY<FLOAT>` 上的原生 ANN 索引，不能用原文的 `USING INVERTED` 建向量索引。官方示例在非空向量列上用 `USING ANN`，属性用 `"index_type" = "hnsw"`、`"metric_type" = "l2_distance"` 和 `"dim" = "768"`。4.x 文档的原生距离度量是 L2 和内积；若用内积模拟余弦相似度，入库向量与查询向量都要做 L2 归一化。[Vector Index](https://doris.apache.org/docs/4.x/key-features/vector-index/)

### 5.3 向量查询语法


```sql
-- Top-K 向量检索
SELECT song_id, song_name,
       L2_DISTANCE(embedding, [0.12, -0.34, 0.56, ...]) AS distance
FROM song_embeddings
ORDER BY distance
LIMIT 10;

```

> [!warning] 核对说明：精确距离和 ANN 排序写法不同
>
> 原文 `L2_DISTANCE` 是精确距离函数，不能凭 `ORDER BY ... LIMIT` 推断命中 ANN。4.x 示例使用 `ORDER BY l2_distance_approximate(embedding, <完整查询向量>) LIMIT 10` 的形态；实际数组要与索引维度一致，`...` 不能作为值。用 `EXPLAIN` 看 `AnnTopN`，再用相同过滤条件下的精确检索计算 Recall@K，判断召回率是否接受。[Vector Index](https://doris.apache.org/docs/4.x/key-features/vector-index/)


### 5.4 RAG 场景整合方式

将 Doris 作为 RAG（Retrieval-Augmented Generation）的向量数据库：


```text
┌─────────────────────────────────────────────────────────┐
│                    RAG 架构                               │
│                                                          │
│  用户问题 → Embedding → Doris 向量检索(Top-K)            │
│                              │                           │
│                              ▼                           │
│              检索到的相关文档 + 用户问题                    │
│                              │                           │
│                              ▼                           │
│                    LLM 生成最终回答                        │
└─────────────────────────────────────────────────────────┘

```


**Doris 作为向量库的优势**（相比独立向量数据库 Milvus/Pinecone）：

- 向量检索 + 结构化过滤在同一引擎中完成（如“只在最近 7 天的文档中检索”）
- 不需要额外维护一套独立的向量数据库
- 利用 Doris 已有的权限、监控、运维体系

**局限：**

- 向量索引能力相比专业向量数据库（Milvus）较新，功能覆盖不如专业方案全面
- 超大规模向量数据（十亿级）场景性能需评估

> [!note] 批注：检索过滤与文档授权在 Doris 查询侧检查
>
> “最近 7 天”和“当前用户有权访问”都应成为实际检索范围的约束。在先取全库 Top-K、再剔除不合格结果的方案里，即使合格集合足够大，也可能只剩很少的结果。对照同一合格集合中的精确 Top-K，分别核对授权过滤、召回率以及延迟。

### 5.5 与 MCP/Agent 结合

结合 Doris MCP（Model Context Protocol）Server，可以实现：


```text
用户自然语言提问
  → Agent 判断意图
  → Text2SQL（结构化查询） + 向量检索（语义查询）
  → 混合结果返回

```


这使得一个 Doris 集群同时服务：

- 传统 BI 报表查询（SQL）
- AI 语义检索（向量）
- Agent 工具调用（MCP）

---

## 六、索引选型决策


```text
查询条件是什么类型？
├── 等值查询 (=, IN)
│   ├── Key 列前缀 → 前缀索引（自动）
│   ├── 高基数列 (user_id) → Bloom Filter
│   └── 低基数列 (status) → Bitmap 索引
├── 范围查询 (>, <, BETWEEN)
│   ├── 数值列 → ZoneMap（自动）+ 排序优化
│   └── 字符串列 → 倒排索引
├── 模糊匹配 (LIKE '%keyword%')
│   └── NGram Bloom Filter
├── 全文检索 (MATCH)
│   └── 倒排索引
└── 向量相似度 (Top-K ANN)
    └── HNSW / IVF 向量索引

```


---

## 七、实践建议

1. **先确认分区裁剪是否生效：**

   分区裁剪比任何索引都重要，一个分区裁剪不生效可能扫描 100 倍数据
2. **Key 列顺序决定前缀索引效果：**

   高频过滤条件放前面
3. **不要过度建索引：**

   每个索引都有写入和存储开销
4. **倒排索引是瑞士军刀：**

   等值、范围、全文检索都能加速，日志场景优先考虑
5. **向量索引是新增能力：**

   适合已有 Doris 集群想增加 AI 检索的场景，不需要引入额外系统

---

## 八、小结

| 要点 | 内容 |
| --- | --- |
| 自动索引 | 前缀索引 + ZoneMap，无需声明，所有表自带 |
| 手动索引 | Bloom Filter / Bitmap / 倒排 / NGram / 向量，按需创建 |
| 过滤层次 | 分区 → 行组 → Page → 行 → Token，逐层细化 |
| 向量检索 | ARRAY+ HNSW/IVF 索引，支持 ANN Top-K |
| AI 整合 | RAG 语义检索 + 结构化过滤统一在 Doris 中 |

---

入门篇完结。你已经建立了 Doris 的完整概念地图：架构 → 存储 → 模型 → 类型 → 导入 → 查询 → 索引。
