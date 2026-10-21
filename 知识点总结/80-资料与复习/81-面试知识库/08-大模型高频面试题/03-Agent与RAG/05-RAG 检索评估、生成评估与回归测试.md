---
tags:
  - 大模型
  - 面试
source: "尚硅谷大模型技术之高频面试题-V2.1.6.docx"
source_blocks: "1501-1522"
reviewed: 2026-10-21
---

# RAG 检索评估、生成评估与回归测试

把没有检索到、检索到了但没用好、答案引用错误分别计量。

前篇：[[知识点总结/80-资料与复习/81-面试知识库/08-大模型高频面试题/03-Agent与RAG/04-混合召回、RRF、重排与HyDE|混合召回、RRF、重排与HyDE]] ｜ 后篇：[[知识点总结/80-资料与复习/81-面试知识库/08-大模型高频面试题/04-多模态/01-ViT、CLIP与跨模态检索|ViT、CLIP与跨模态检索]]

## 深入理解（新增）

建立带问题、有效时间、相关证据 ID 和可接受答案的评估集。Recall@K 衡量相关证据覆盖，MRR 衡量首个相关结果排名；每个问题先独立计分，再汇总，避免长文档或热门问题支配结果。

回答正确但无证据支撑与有证据却答错是不同错误。应同时检查回答正确性、证据忠实性、引用定位、拒答表现和时延。无答案问题不能塞进分母为零的 Recall，应单独评估正确拒答和误拒答。

LLM 裁判会受提示词、模型和答案风格影响，需要固定版本、盲评顺序，并抽样人工复核。线上点击率和用户满意度也可能受位置、流量变化影响，不能直接替代同条件离线回归。



## 核心代码（新增）

以下是可在 CPU 运行的机制或业务契约示例，完整源码与测试见 [[知识点总结/80-资料与复习/81-面试知识库/08-大模型高频面试题/06-代码实验/01-核心代码实验与验证范围|核心代码实验与验证范围]]。示例不包含完整模型训练或外部平台部署。

```python
def retrieval_metrics(retrieved, relevant, k=5):
    if not relevant:
        raise ValueError("no-answer cases must be evaluated separately")
    top = list(dict.fromkeys(retrieved))[:k]
    hit = set(top) & set(relevant)
    reciprocal_rank = next((1 / rank for rank, doc in enumerate(top, 1) if doc in relevant), 0.0)
    return {"recall_at_k": len(hit) / len(set(relevant)), "mrr": reciprocal_rank}

m = retrieval_metrics(["c2", "c1", "c1"], {"c1", "c3"}, k=3)
assert m == {"recall_at_k": 0.5, "mrr": 0.5}
```

## 追问与验证（新增）

构造一个相关证据排第六的查询，比较 K=5 和 K=10 的 Recall，并说明这不能证明生成答案一定正确。

## 官方资料与论文

- [Retrieval-Augmented Generation](https://arxiv.org/abs/2005.11401)

核验日期：2026-10-21。链接中的 `stable`、`main` 或平台文档会继续变化；新增纯 Python/NumPy 实验与原文旧版框架代码已分开标注。

## 原文保留

> [!warning] 原文批注
> RAG 可以减少无依据生成，但不能保证消除幻觉；自动裁判分数需要固定口径并经人工抽查。

<!-- source-block:1501 -->
### 评估

<!-- source-block:1502 -->
#### 如何评价 RAG项目效果的好坏?

<!-- source-block:1503 -->
从检索效果、生成效果、端到端效果三个核心维度综合衡量，结合量化指标、定性评估和场景适配性，形成完整的评估体系。

<!-- source-block:1504 -->
方式：人工评估（业务部门、运营人员）+指标评估（benchmark跑量化指标，使用开源框架Ragas）：

<!-- source-block:1505 -->
#### RAG有哪些关键评估指标和能力”

<!-- source-block:1506 -->
记住指标名，文本说明用于理解。

<!-- source-block:1507 -->
**关键指标：**

<!-- source-block:1508 -->
**检索阶段：**

<!-- source-block:1509 -->
上下文召回率（Context Recall）: 检索到的相关上下文占所有相关上下文的比例

<!-- source-block:1510 -->
上下文相关性（Context Relevance）: 检索到的上下文与用户Query的相关程度

<!-- source-block:1511 -->
上下文精确度（Context Precision）: 检索内容中相关信息的比例

<!-- source-block:1512 -->
**生成阶段：**

<!-- source-block:1513 -->
答案真实性（Groundedness）：生成内容是否有上下文支撑，是否存在臆造

<!-- source-block:1514 -->
答案相关性（Answer Relevance）：生成内容与问题的相关性

<!-- source-block:1515 -->
忠诚度（Faithfulness）：生成内容与上下文的一致性，是否存在曲解。

<!-- source-block:1516 -->
**关键能力：**

<!-- source-block:1517 -->
抗噪声能力（Noise Robustness）：评估模型处理与问题相关但缺乏实质性信息的噪声文档的能力。

<!-- source-block:1518 -->
负向拒绝（Negative Rejection）：评估模型在检索到的文档不包含回答问题所需知识时拒绝回答的能力。

<!-- source-block:1519 -->
信息整合（Information Integration）：评估模型从多个文档中综合信息以回答复杂问题的能力。

<!-- source-block:1520 -->
反事实鲁棒性（Counterfactual Robustness）：评估模型识别并忽略文档中已知错误的能力。

<!-- source-block:1521 -->
#### 什么是评估框架RAGAS

<!-- source-block:1522 -->
Ragas 是一个开源的、用于评估 RAG 系统质量的端到端评估框架，使用基于 LLM 和传统指标精确评估 RAG 系统中的检索质量、生成质量以及二者之间的耦合关系。
