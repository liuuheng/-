---
title: "Doris 进阶篇07：向量检索与AI场景实战"
author: "外星人的知识杂谈"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484370&idx=1&sn=e0d992e098dd88ffb1c968dcf35179a2&chksm=c1bf09cff6c880d9d0066799eb9be8614aadabeac14963af30513cfe22be560f3979d02deef8#wechat_redirect"
published_at: "2026-08-31T22:04:24+08:00"
annotated_at: "2026-11-18"
cssclasses:
  - wechat-article
  - doris-intro-annotated
tags:
  - doris
  - 文章批注
---

# Doris 进阶篇07：向量检索与AI场景实战

<div class="article-meta">外星人的知识杂谈 · 2026-08-31</div>

[原文链接](https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484370&idx=1&sn=e0d992e098dd88ffb1c968dcf35179a2&chksm=c1bf09cff6c880d9d0066799eb9be8614aadabeac14963af30513cfe22be560f3979d02deef8#wechat_redirect)

<!-- 保留原文正文、顺序与示例；批注和核对说明为新增。装饰封面与篇末导航已移除；代码未在集群运行。 -->

> **定位：** 进阶篇第 7 篇 | 掌握 Doris 向量索引调优及 AI/RAG 场景落地方法
>
> **阅读前提：** 入门篇第 07 篇（索引体系与向量检索入门）、进阶篇第 06 篇（索引进阶）

---

## 一、背景动机

随着大模型（LLM）和 RAG（检索增强生成）的普及，向量检索成为数据基础设施的核心能力之一。传统方案需要独立部署 Milvus/Pinecone 等向量数据库，但这引入了额外的运维复杂度和数据同步问题。

Doris 2.1+ 原生支持向量索引（HNSW），可以在一个引擎中同时完成：

- **结构化查询：**

  WHERE status = ‘active’ AND dt > ‘2026-01-01’
- **向量检索：**

  找到语义最相似的 Top-K 文档
- **混合查询：**

  先结构化过滤，再向量排序

这使得 Doris 成为 “OLAP + 向量检索” 统一架构的理想选型。

---

## 二、HNSW 索引原理

### 2.1 从暴力搜索到 ANN

| 方法 | 时间复杂度 | 精度 | 适用规模 |
| --- | --- | --- | --- |
| 暴力搜索（Flat） | O(N×D) | 100% | < 10 万 |
| IVF（倒排文件） | O(N/nlist × D) | 95%+ | 百万级 |
| HNSW（分层导航图） | O(log N × D) | 97%+ | 千万~亿级 |

HNSW（Hierarchical Navigable Small World）是当前主流的 ANN 索引算法，Doris 选择它的原因：

- 查询性能优异（O(logN) 复杂度）
- 召回率高（通常 > 95%）
- 支持增量插入（适合实时写入场景）
- 内存可控（通过参数调节）

> [!note] 批注｜近似检索用什么衡量
>
> 精确检索为每个候选向量计算一次距离，单次查询的计算量随候选行数 `N` 和向量维度 `D` 增长。ANN 只访问一部分候选，减少计算量，但可能漏掉真实近邻。原文的复杂度和召回率可用于理解趋势；实际要受向量分布、过滤条件和索引参数影响。

### 2.2 HNSW 核心参数


```text
HNSW 结构示意：

Layer 2:  O ─── O ─── O              (稀疏，快速定位区域)
          │     │     │
Layer 1:  O─O─O─O─O─O─O─O─O          (中等密度)
          │ │ │ │ │ │ │ │ │
Layer 0:  OOOOOOOOOOOOOOOOOOO          (最密，精确搜索)

```


三个核心参数：

| 参数 | 含义 | 影响 | 推荐范围 |
| --- | --- | --- | --- |
|`M` | 每个节点的邻居连接数 | M 越大，图越密，召回越高，但内存和构建时间增加 | 16~64 |
|`efConstruction` | 构建时搜索的候选列表大小 | 越大，索引质量越高，构建越慢 | 100~500 |
|`efSearch` | 查询时搜索的候选列表大小 | 越大，召回越高，查询越慢 | 64~512 |

> [!note] 批注｜区分建索引参数和查询参数
>
> `M` 控制图中连接的规模，`efConstruction` 控制建图时找邻居的搜索范围；这两个参数会改变索引。`efSearch` 控制查询时探索的候选范围，可在已有图上调整。相同的 `efSearch` 在不同质量的图上可以得到不同的召回率。

### 2.3 参数间的关系


```text
                    构建质量
                       ↑
                       │
            efConstruction ↑ 
                       │
    内存消耗 ←── M ──→ 召回率
                       │
                       │
            efSearch ──→ 查询延迟 vs 召回率

```


**黄金规则：**

- `efConstruction >= 2 × M`

  （否则图的质量不够）
- `efSearch >= K`

  （K 是 Top-K 的 K，否则结果不足）
- 先调`efConstruction`（一次性，影响索引质量），再调`efSearch`（查询时，动态调整）

---

## 三、Doris 向量索引实战

### 3.1 建表与索引创建


```sql
-- 创建包含向量列的表
CREATE TABLE doc_embeddings (
    doc_id       BIGINT,
    doc_title    VARCHAR(256),
    content      TEXT,
    category     VARCHAR(32),
    create_time  DATETIME,
    embedding    ARRAY<FLOAT>    -- 向量列（如 768 维 BERT 向量）
) DUPLICATE KEY(doc_id)
DISTRIBUTED BY HASH(doc_id) BUCKETS 8;

-- 创建 HNSW 向量索引
CREATE INDEX idx_embedding ON doc_embeddings (embedding) 
USING INVERTED 
PROPERTIES (
    "index_type" = "HNSW",
    "metric_type" = "L2",           -- 距离度量：L2（欧氏距离）或 COSINE
    "M" = "32",                      -- 邻居数
    "efConstruction" = "200"         -- 构建精度
);

```

> [!warning] 核对说明｜Doris 4.x 的 ANN 建表写法
>
> 原文保留了示意 DDL。按 [Doris 4.x 向量索引文档](https://doris.apache.org/docs/4.x/key-features/vector-index/)，原生 ANN 向量索引从 4.0 开始提供，写法是 `INDEX idx_embedding (embedding) USING ANN PROPERTIES ("index_type"="hnsw", "metric_type"="l2_distance", "dim"="768")`，列类型是 `ARRAY<FLOAT> NOT NULL`。另一种度量是 `inner_product`；HNSW 建图属性是 `max_degree` 和 `ef_construction`。原例的 `USING INVERTED` 属于倒排索引语法，不能创建这个 ANN 索引。


### 3.2 距离度量选择

| 度量类型 | 公式 | 适用场景 | Doris 语法 |
| --- | --- | --- | --- |
| L2（欧氏距离） | sqrt(sum((a-b)^2)) | 绝对距离敏感 |`metric_type = "L2"` |
| COSINE（余弦相似度） | dot(a,b)/( | a | × |
| IP（内积） | dot(a,b) | 已归一化的向量 |`metric_type = "IP"` |

**选型建议：**

- 文本语义检索（BERT/OpenAI embeddings）→ COSINE
- 图像相似度 → L2
- 已做 L2 归一化的向量 → IP（等价于 COSINE 但更快）

> [!warning] 核对说明｜原网页表格截断，补全余弦公式
>
> 公众号原表的余弦行从公式中间断开，末两格也错列；上表保留页面中可见的内容。余弦相似度是 `dot(a,b) / (norm(a) × norm(b))`，这里的 `norm` 是 L2 范数，两个向量都必须非零。结果越大越相似。建库和查询时若都做了单位长度归一化，余弦相似度等于内积，可用 `inner_product` 索引与降序排序表达；不能仅改索引属性而省略归一化。

### 3.3 向量查询


```sql
-- 基本 ANN 查询：找最相似的 10 个文档
SELECT doc_id, doc_title, 
       L2_DISTANCE(embedding, [0.1, 0.2, ..., 0.5]) AS distance
FROM doc_embeddings
ORDER BY L2_DISTANCE(embedding, [0.1, 0.2, ..., 0.5])
LIMIT 10;

-- 设置 efSearch 提高召回率
SET topn_filter_ratio = 0.5;  -- 候选集放大系数

```

> [!warning] 核对说明｜区分精确距离计算和 ANN
>
> Doris 4.x 识别的是 `ORDER BY l2_distance_approximate(embedding, 查询向量) ASC LIMIT K` 或 `ORDER BY inner_product_approximate(embedding, 查询向量) DESC LIMIT K`，函数和排序方向要与索引度量匹配。没有 `_approximate` 的同类函数可用于精确计算和召回率对照。HNSW 查询参数是 `hnsw_ef_search`；原例里的 `topn_filter_ratio` 不能用来调 HNSW 候选范围。


### 3.4 efSearch 调优

| efSearch | 召回率（Top-10） | 查询延迟 | 适用场景 |
| --- | --- | --- | --- |
| 32 | ~85% | 2ms | 实时推荐（延迟优先） |
| 64 | ~92% | 4ms | 常规检索 |
| 128 | ~97% | 8ms | 高精度检索 |
| 256 | ~99% | 15ms | 对召回极敏感场景 |
| 512 | ~99.5% | 30ms | 几乎等于暴力搜索 |

> [!note] 批注｜召回率和延迟要用同一批查询测
>
> 用相同查询向量、过滤条件和度量先求精确的前 K 个 ID。若有至少 K 个候选，`Recall@K` 可按 ANN 结果和精确结果的交集大小除以 K 计算；候选不足 K 个时要单独标记或按实际基准结果数计算。扫不同 `hnsw_ef_search`，同时记录 P95 延迟及负载条件。表中数值不能拿来当本集群的阈值。

---

## 四、混合检索（向量 + 结构化过滤）

### 4.1 为什么需要混合检索

纯向量检索的问题：

- 返回的结果可能不满足业务约束（如已删除的文档、不属于当前用户的数据）
- 无法利用分区裁剪优化（全表向量搜索）

混合检索 = 先用结构化条件缩小范围，再在子集中做向量检索。

### 4.2 Doris 混合检索模式


```sql
-- 模式 1：Pre-filtering（先过滤，再向量搜索）
-- 适合过滤条件选择率高的场景（过滤后剩余数据少）
SELECT doc_id, doc_title, 
       COSINE_DISTANCE(embedding, [0.1, 0.2, ...]) AS sim
FROM doc_embeddings
WHERE category = 'tech'                      -- 结构化过滤
  AND create_time > '2026-01-01'             -- 时间范围
ORDER BY COSINE_DISTANCE(embedding, [0.1, 0.2, ...])
LIMIT 10;

-- 模式 2：利用分区裁剪加速
-- 按时间分区的表，时间过滤自动裁剪分区，大幅减少向量搜索范围
SELECT * FROM doc_embeddings
WHERE dt = '2026-07-01'                      -- 分区裁剪
ORDER BY L2_DISTANCE(embedding, [...])
LIMIT 10;

```

> [!warning] 核对说明｜先确定候选集合，再验证执行计划
>
> “在某个日期范围内找最相似的 K 条”和“先全局找 K 条，再筛出日期合格的记录”结果可能不同。Doris 4.x 的 ANN 路径支持预过滤；具体谓词是否能在索引路径中处理，需要用目标版本的 `EXPLAIN` 验证。原例的 `dt` 不在 §3.1 建表语句中，运行前需补齐列或改用表中实际的日期列。


### 4.3 混合检索最佳实践

| 策略 | 适用场景 | 性能特征 |
| --- | --- | --- |
| 分区裁剪 + ANN | 有明确时间范围的检索 | 最快（只搜索少量分区） |
| 倒排索引过滤 + ANN | 有标签/类别条件 | 快（索引过滤后再 ANN） |
| 纯 ANN | 无约束条件 | 依赖索引质量 |

---

## 五、RAG 架构与 Doris

### 5.1 RAG 架构总览


```text
┌────────────────────────────────────────────────────────────────┐
│                      RAG 系统架构                               │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  用户问题 → [Embedding 模型] → Query Vector                    │
│                                          │                     │
│                                          ▼                     │
│  ┌─────────────────────────────────────────────────┐          │
│  │              Doris（统一存储层）                   │          │
│  │                                                  │          │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────────┐  │          │
│  │  │ 向量索引  │  │ 倒排索引  │  │ 结构化数据    │  │          │
│  │  │ (HNSW)   │  │ (CLucene) │  │ (分区/分桶)  │  │          │
│  │  └────┬─────┘  └────┬─────┘  └──────┬───────┘  │          │
│  │       │              │               │          │          │
│  │       └──────────────┼───────────────┘          │          │
│  │                      │                          │          │
│  │              混合检索结果（Top-K 文档）            │          │
│  └──────────────────────┼──────────────────────────┘          │
│                         │                                      │
│                         ▼                                      │
│  [Reranker 重排] → Top-N 最终文档 → [LLM 生成回答]             │
│                                                                │
└────────────────────────────────────────────────────────────────┘

```


### 5.2 Doris 作为 RAG 向量库的优势

| 维度 | 独立向量库（Milvus） | Doris 统一方案 |
| --- | --- | --- |
| 部署运维 | 额外集群 | 复用现有 Doris 集群 |
| 数据同步 | 需要 ETL 同步 | 数据已在 Doris 中 |
| 混合查询 | 需要跨库查询 | 单 SQL 完成 |
| 元数据过滤 | 支持有限 | 完整 SQL 能力 |
| 大规模分析 | 不擅长 | 原生 OLAP 能力 |
| 向量搜索性能 | 更优（专用优化） | 满足大多数场景 |

### 5.3 RAG 数据入库示例


```python
# Python 示例：文档切片 + Embedding + 写入 Doris
import requests
from openai import OpenAI

client = OpenAI()

def embed_and_store(documents, doris_url):
    """文档向量化并存入 Doris"""
    for doc in documents:
        # 1. 文档切片
        chunks = split_document(doc, chunk_size=512, overlap=50)

        for i, chunk in enumerate(chunks):
            # 2. 生成 Embedding
            response = client.embeddings.create(
                model="text-embedding-3-small",
                input=chunk.text
            )
            embedding = response.data[0].embedding  # 1536 维

            # 3. 写入 Doris（通过 Stream Load）
            row = {
                "doc_id": doc.id,
                "chunk_id": i,
                "content": chunk.text,
                "embedding": embedding,
                "category": doc.category,
                "create_time": doc.create_time
            }
            stream_load_to_doris(doris_url, "doc_embeddings", row)

```

> [!warning] 核对说明｜入库向量与查询向量的约定
>
> §3.1 是 768 维建表示意，这一节使用的维度是 1536，且写入字段出现了 `chunk_id`。实际建表要与入库字段、索引 `dim` 对齐；入库和查询还要使用同一套向量模型及预处理。仅维度相同，不能说明不同模型生成的向量可以互相检索。


### 5.4 RAG 检索示例


```python
def rag_retrieve(question, doris_conn, top_k=5):
    """RAG 检索：混合搜索"""
    # 1. 问题向量化
    q_embedding = get_embedding(question)
    embedding_str = str(q_embedding)

    # 2. 混合检索 SQL
    sql = f"""
    SELECT doc_id, chunk_id, content,
           COSINE_DISTANCE(embedding, {embedding_str}) AS distance
    FROM doc_embeddings
    WHERE create_time > '2025-01-01'    -- 结构化过滤
    ORDER BY distance
    LIMIT {top_k}
    """

    # 3. 执行查询
    results = doris_conn.execute(sql)

    # 4. 组装上下文
    context = "\n---\n".join([r.content for r in results])
    return context

```

> [!note] 批注｜权限过滤也定义检索范围
>
> 如果不同用户可见的文档不同，先把租户或文档可见性写成候选过滤条件，再对候选做 Top K。查询参数应使用驱动支持的绑定方式或经过校验的结构化输入，尤其不要直接拼接用户提供的过滤表达式。SQL 示例里的占位符和示例函数需要按真实接口实现。


---

## 六、MCP/Agent 与 Doris 集成

### 6.1 Text2SQL + 向量检索的 Agent 架构


```text
┌─────────────────────────────────────────────────────┐
│                  Agent 系统                          │
├─────────────────────────────────────────────────────┤
│                                                     │
│  用户自然语言问题                                     │
│       │                                             │
│       ▼                                             │
│  ┌──────────┐     ┌───────────────────────┐        │
│  │ Router   │────→│ 结构化查询分支         │        │
│  │ (LLM)    │     │ Text2SQL → Doris 执行  │        │
│  │          │     └───────────────────────┘        │
│  │          │                                      │
│  │          │     ┌───────────────────────┐        │
│  │          │────→│ 语义检索分支            │        │
│  │          │     │ Embedding → ANN 检索   │        │
│  │          │     └───────────────────────┘        │
│  │          │                                      │
│  │          │     ┌───────────────────────┐        │
│  │          │────→│ 混合分支               │        │
│  └──────────┘     │ 结构化 + 语义联合       │        │
│                   └───────────────────────┘        │
│                                                     │
└─────────────────────────────────────────────────────┘

```


### 6.2 Doris MCP Server

MCP（Model Context Protocol）允许 LLM Agent 直接与 Doris 交互：


```json
{
  "doris_mcp_server": {
    "url": "http://127.0.0.1:8000/sse",
    "headers": {},
    "timeout": 60,
    "sse_read_timeout": 300
  }
}

```


Agent 通过 MCP 可以执行的操作：

- 查询表结构（`SHOW CREATE TABLE`）
- 执行 SQL 查询
- 获取统计信息
- 执行向量检索

> [!note] 批注｜给 Agent 的数据库权限如何收敛
>
> 以内置工具最终使用的数据库账号为准，给它只读权限并限制可见库表或视图；同时限制查询时间和资源，这样自然语言生成的 SQL 出错也有明确边界。原文的 SSE 地址属于接入示意，实际传输配置取决于 MCP Server 和客户端版本。

### 6.3 实际部署模式

| 模式 | 架构 | 适用场景 |
| --- | --- | --- |
| All-in-Doris | 结构化 + 向量都在 Doris | 中等规模（< 1 亿向量） |
| Doris + 专用向量库 | 结构化在 Doris，向量在 Milvus | 超大规模向量 |
| Doris + ES | 结构化在 Doris，全文在 ES | 复杂全文检索 |

---

## 七、性能调优实践

### 7.1 索引构建优化

| 场景 | 问题 | 解决方案 |
| --- | --- | --- |
| 大批量数据导入后索引构建慢 | efConstruction 过高 | 初始导入时降低 efConstruction，后续增量保持高值 |
| 索引占用内存过大 | M 值过高 | 降低 M（32→16），牺牲少量召回换内存 |
| Compaction 时向量索引合并慢 | 向量维度高 + 数据量大 | 增大 Compaction 内存限制 |

> [!note] 批注｜建图调优要看导入和段合并的成本
>
> 查询侧扩大候选范围不会重建索引；修改建图参数则应计入重建成本，并检查历史数据是否已由新参数构建。除导入吞吐外，压测还要观察 Compaction 期间的 CPU、内存和读写延迟，避免只根据空闲集群的查询时延选参。

### 7.2 查询优化


```sql
-- 技巧 1：利用分区裁剪减少搜索范围
-- 时间分区表，先按时间过滤
SELECT * FROM docs WHERE dt = '2026-07-01'
ORDER BY L2_DISTANCE(embedding, [...]) LIMIT 10;

-- 技巧 2：适当增大 LIMIT 后在应用层重排
-- 取 Top-50 候选，应用层用 Cross-Encoder Rerank 到 Top-10
SELECT * FROM docs 
ORDER BY COSINE_DISTANCE(embedding, [...]) LIMIT 50;

-- 技巧 3：组合倒排索引预过滤
SELECT * FROM docs 
WHERE category = 'tech' AND content MATCH_ANY 'database'
ORDER BY COSINE_DISTANCE(embedding, [...]) LIMIT 10;

```


### 7.3 容量规划


```text
单个向量的内存占用（HNSW 索引部分）：
  = 向量维度 × 4 字节 (float32) + M × 2 × 4 字节 (邻居指针)

示例：768 维，M=32
  = 768 × 4 + 32 × 2 × 4 = 3072 + 256 = 3328 字节/向量

1 亿向量的索引内存需求：
  = 1亿 × 3328 ≈ 310 GB

实际生产建议：

  - 100 万向量：~3.1 GB（单 BE 轻松承载）
  - 1000 万向量：~31 GB（需要足够内存的 BE）
  - 1 亿向量：~310 GB（建议分布在多个 Tablet/BE 上）

```

> [!note] 批注｜容量估算先区分文件体积与内存
>
> 按原表的 `3,328 B/条` 估算，1 亿条约为 `332.8 GB`（约 `310 GiB`），这是单份示意值。集群规划还要分开考虑副本与分布、索引缓存或常驻内存、构建峰值以及 Compaction 期间新旧段共存的开销。


---

## 八、实践建议速查

| 场景 | 建议配置 |
| --- | --- |
| 知识库 RAG（百万级文档） | M=32, efConstruction=200, COSINE, 分区按时间 |
| 商品推荐（千万级） | M=16, efConstruction=128, IP, 分区按类目 |
| 图片相似检索 | M=48, efConstruction=300, L2 |
| 实时写入 + 检索 | M=16, efConstruction=100（降低写入延迟） |
| 高召回要求 | efSearch=256+, M=48+ |
| 低延迟要求 | efSearch=32~64, M=16 |

---

## 九、小结

| 要点 | 内容 |
| --- | --- |
| HNSW 参数 | M（图密度）、efConstruction（构建质量）、efSearch（查询精度） |
| 混合检索 | 结构化过滤 + 向量排序，Doris 单 SQL 完成 |
| RAG 架构 | Doris 统一存储向量+元数据，减少架构复杂度 |
| MCP/Agent | 通过 MCP 协议让 LLM Agent 直接查询 Doris |
| 容量规划 | 内存 = 向量数 × (维度×4 + M×8)，按需分布式扩展 |
