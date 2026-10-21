---
tags:
  - 大模型
  - 面试
source: "尚硅谷大模型技术之高频面试题-V2.1.6.docx"
source_blocks: "4144-4287"
reviewed: 2026-10-21
---

# ITS知识库的面试问答与项目复盘

用评估集、消融实验和坏例说明知识库项目的贡献。

前篇：[[知识点总结/80-资料与复习/81-面试知识库/08-大模型高频面试题/05-项目实战/27-ITS知识库的故障定位与优化实验|ITS知识库的故障定位与优化实验]] ｜ 后篇：[[知识点总结/80-资料与复习/81-面试知识库/08-大模型高频面试题/05-项目实战/29-掌柜智库的文档、多模态与索引|掌柜智库的文档、多模态与索引]]

## 深入理解（新增）

构建评估集时覆盖专有名词、跨段答案、旧版本、权限受限和无答案问题。只抽高频简单问题会高估泛化；训练或提示词调优用过的问题不能继续当独立验收集。

消融可以逐步比较词项、向量、融合、重排及 query rewrite，但每步保持候选和生成预算可比。一个模块的收益可能来自多用了 token 或多检索了候选，而非模块本身更好。

指标复盘同时报告分母、样本数、数据时间和抽查规则。团队日志能支持系统效果，个人代码和实验记录能支持个人贡献，两类证据都应准备。



## 核心代码（新增）

以下是可在 CPU 运行的机制或业务契约示例，完整源码与测试见 [[知识点总结/80-资料与复习/81-面试知识库/08-大模型高频面试题/06-代码实验/01-核心代码实验与验证范围|核心代码实验与验证范围]]。示例不包含完整模型训练或外部平台部署。

```python
from collections import defaultdict

def retrieval_metrics(retrieved, relevant, k=5):
    if not relevant:
        raise ValueError("no-answer cases must be evaluated separately")
    top = list(dict.fromkeys(retrieved))[:k]
    hit = set(top) & set(relevant)
    reciprocal_rank = next((1 / rank for rank, doc in enumerate(top, 1) if doc in relevant), 0.0)
    return {"recall_at_k": len(hit) / len(set(relevant)), "mrr": reciprocal_rank}

def rrf_fuse(rankings, constant=60):
    if constant <= 0:
        raise ValueError("constant must be positive")
    scores = defaultdict(float)
    for ranking in rankings:
        seen = set()
        for rank, doc_id in enumerate(ranking, start=1):
            if doc_id in seen:
                continue
            seen.add(doc_id)
            scores[doc_id] += 1 / (constant + rank)
    return sorted(scores.items(), key=lambda item: (-item[1], item[0]))

m = retrieval_metrics(["c2", "c1", "c1"], {"c1", "c3"}, k=3)
assert m == {"recall_at_k": 0.5, "mrr": 0.5}

ranked = rrf_fuse([["sku-A", "sku-B"], ["sku-B", "sku-C"]])
assert ranked[0][0] == "sku-B"
```

## 追问与验证（新增）

在无答案与有答案集合分别报告结果，再说明若合成单一“准确率”，它会受集合比例怎样影响。

## 官方资料与论文

- [Retrieval-Augmented Generation](https://arxiv.org/abs/2005.11401)

核验日期：2026-10-21。链接中的 `stable`、`main` 或平台文档会继续变化；新增纯 Python/NumPy 实验与原文旧版框架代码已分开标注。

## 原文保留

> [!warning] 原文批注
> 教材项目指标需要完整分母与对照条件；不同评估集上的百分比不能直接比较。
>
> 原文的团队人数、业务规模、效果百分比、硬件与成本均保留为教材案例陈述；本次未独立复现这些项目指标。

<!-- source-block:4144 -->
### 面试高频问题回答

<!-- source-block:4145 -->
#### 项目介绍类

<!-- source-block:4146 -->
##### 简单介绍以下这个项目

<!-- source-block:4147 -->
这是⼀个基于RAG技术的企业级智能知识库问答系统。系统从企业知识库采集数据，通过⽂档切分、向量化存储到Chroma数据库。用户提问时，采⽤多路召回策略（向量检索+关键词检索）找到 相关⽂档，然后通过LLM⽣成准确的答案。前端使⽤Vue3+Element Plus，后端使⽤ FastAPI + LangChain。

<!-- source-block:4148 -->
##### 为什么选择这个技术栈？

<!-- source-block:4149 -->
- FastAPI：⾼性能异步框架，适合IO密集型的LLM调用场景

<!-- source-block:4150 -->
- LangChain：成熟的LLM应⽤框架，提供⽂档处理、向量存储、链式调⽤等功能

<!-- source-block:4151 -->
- Chroma：轻量级本地向量数据库，开发调试方便，无需部署外部服务

<!-- source-block:4152 -->
- Vue3：响应式组合式API，更好的TypeScript⽀持和性能

<!-- source-block:4153 -->
#### 技术深度类

<!-- source-block:4154 -->
##### 解释以下RAG的工作原理？

<!-- source-block:4155 -->
RAG（Retrieval-Augmented Generation）检索增强⽣成，核心思想是：

<!-- source-block:4156 -->
- Retrieval（检索）：将用户问题转换为向量，在向量数据库中检索相似⽂档

<!-- source-block:4157 -->
- Augmented（增强）：将检索到的文档作为上下⽂，与用户问题⼀起构建Prompt

<!-- source-block:4158 -->
- Generation（⽣成）：LLM基于增强后的Prompt生成答案优势：解决LLM知识过时、幻觉等问题，让回答基于实际⽂档内容。

<!-- source-block:4159 -->
##### 为什么要采用多路召回而不是单纯的向量检索？

<!-- source-block:4160 -->
单纯向量检索存在以下问题：

<!-- source-block:4161 -->
- 中⽂嵌⼊模型对某些专业术语理解不够准确

<!-- source-block:4162 -->
- 语义相似但关键词不同的文档可能排名靠后

<!-- source-block:4163 -->
- 对于精确匹配场景（如型号、错误代码）召回率不高

<!-- source-block:4164 -->
多路召回策略： 第⼀路向量检索：捕获语义相似性 第二路关键词检索：捕获精确匹配 融合后去重、重排，提⾼整体召回率和准确率

<!-- source-block:4165 -->
##### 文档切分时为什么要设置overlap？

<!-- source-block:4166 -->
overlap（重叠区间）的作用：

<!-- source-block:4167 -->
- 防止边界处语义断裂：如果一个概念刚好在切分边界，没有overlap会导致语义信息不完整。

<!-- source-block:4168 -->
- 提高检索命中率：边界词汇在相邻chunk中都出现，增加被检索到的概率

<!-- source-block:4169 -->
- 典型设置：chunk\_size=1500，overlap=200，重叠率约13%

<!-- source-block:4170 -->
##### Chroma返回的时什么分数？如何理解？

<!-- source-block:4171 -->
Chroma 的similarity\_search\_with\_score返回的是L2距离（欧几里得距离），不是余弦相似度。

<!-- source-block:4172 -->
- L2距离越小，相似度越高

<!-- source-block:4173 -->
- 如需转换为0-1的相似度分数：![原文图示或公式](../assets/原文图-251.png)

<!-- source-block:4174 -->
- 在重排阶段需要注意这个区别，与自己计算的余弦相似度分数进行归一化

<!-- source-block:4175 -->
#### 问题解决类

<!-- source-block:4176 -->
##### 项目中遇到的最大的挑战是什么？

<!-- source-block:4177 -->
最大挑战是提升检索准确率。

<!-- source-block:4178 -->
**问题表现：**

<!-- source-block:4179 -->
早期只⽤向量检索，对于包含专业术语的问题召回率不高。

<!-- source-block:4180 -->
**解决过程：**

<!-- source-block:4181 -->
**分析Case发现关键词匹配能捕获向量检索遗漏的⽂档**

<!-- source-block:4182 -->
**设计两阶段关键词检索：粗排（Jaccard）→ 精排（向量余弦）**

<!-- source-block:4183 -->
**融合两路结果，去重后统⼀重排**

<!-- source-block:4184 -->
**召回率从65%提升到85%**

<!-- source-block:4185 -->
##### 如何保证LLM回答的准确性？

<!-- source-block:4186 -->
多层保障机制：

<!-- source-block:4187 -->
- 检索质量：多路召回+重排，确保检索到最相关的⽂档

<!-- source-block:4188 -->
- Prompt设计：明确要求"基于事实回答，不编造信息"

<!-- source-block:4189 -->
- temperature=0：使⽤确定性输出，避免随机性导致的错误

<!-- source-block:4190 -->
- 引⽤来源：要求LLM引⽤参考资料编号，便于溯源验证

<!-- source-block:4191 -->
#### 扩展思考类

<!-- source-block:4192 -->
##### 如果要部署到生产环境，需要做哪些改进？

<!-- source-block:4193 -->
回答要点：

<!-- source-block:4194 -->
- 向量数据库：Chroma换成Milvus/Pinecone，支持分布式和高并发

<!-- source-block:4195 -->
- 缓存层：添加Redis缓存热点问答，减少重复计算

<!-- source-block:4196 -->
- 异步队列：大文件上传适用Celery异步处理

<!-- source-block:4197 -->
- 监控告警：接入Prometheus + Grafana监控API性能

<!-- source-block:4198 -->
- 负载均衡：多实例部署，使用Nginx负载均衡

<!-- source-block:4199 -->
##### 如何评估RAG系统的效果？

<!-- source-block:4200 -->
主要评估指标：

<!-- source-block:4201 -->
- 召回率（Recall）：检索到相关文档的比例

<!-- source-block:4202 -->
- 准确率（Precision）：检索结果中相关文档的比例

<!-- source-block:4203 -->
- MRR（Mean Reciprocal Rank）：第一个相关结果的排名倒数

<!-- source-block:4204 -->
- Answer Quality:人工评估答案的准确性、完整性、可读性

<!-- source-block:4205 -->
- 响应时间：端到端延迟，包括检索+生成

<!-- source-block:4206 -->
### 面试口述版项目总结

<!-- source-block:4207 -->
#### 30s版

<!-- source-block:4208 -->
我开发了⼀个基于RAG技术的企业级智能知识库问答系统。系统采⽤多路召回策略（向量检索+关键词检索）提升检索准确率，使⽤两阶段排序（粗排+精排）优化结果质量。后端使⽤ FastAPI + LangChain + Chroma，前端使⽤ Vue 3 + Element Plus。我主要负责检索服务和文档处理模块的 设计与实现。

<!-- source-block:4209 -->
#### 2分钟版

<!-- source-block:4210 -->
**项⽬背景**

<!-- source-block:4211 -->
这是⼀个企业级智能知识库问答系统，⽬标是让⽤⼾通过自然语言提问，快速从海量技术⽂档中获取精准答案。传统关键词搜索无法理解语义，我们通过RAG技术解决这个问题。

<!-- source-block:4212 -->
**技术架构**

<!-- source-block:4213 -->
系统分为四个核⼼模块：

<!-- source-block:4214 -->
- 数据采集：爬⾍从企业知识库采集⽂档，转换为Markdown格式

<!-- source-block:4215 -->
- 文档摄⼊：智能切分⽂档，向量化后存储到Chroma数据库

<!-- source-block:4216 -->
- 检索服务：多路召回 + 两阶段排序，提升检索质量

<!-- source-block:4217 -->
- 问答服务：构建Prompt调⽤LLM⽣成结构化答案

<!-- source-block:4218 -->
后端使⽤ FastAPI 框架，集成LangChain处理⽂档和向量存储。⼤模型使⽤千问3-32B，通过 SiliconFlow API调⽤。前端使⽤ Vue 3，⽀持⽂档上传和实时问答。

<!-- source-block:4219 -->
**我的职责**

<!-- source-block:4220 -->
我主要负责检索服务的设计与实现，包括多路召回策略、两阶段排序算法、去重与重排逻辑。同时设计了标题注⼊机制解决⽂档切分后的语义丢失问题。

<!-- source-block:4221 -->
**技术亮点**

<!-- source-block:4222 -->
项⽬有⼏个技术亮点：

<!-- source-block:4223 -->
- 多路召回策略，将召回率从65%提升到85%+；

<!-- source-block:4224 -->
- 标题注⼊机制，让每个chunk保留来源信息；

<!-- source-block:4225 -->
- Prompt⼯程优化，实现基于事实回答和去特定化处理。

<!-- source-block:4226 -->
#### 5分钟版

<!-- source-block:4227 -->
**项目背景与目标**

<!-- source-block:4228 -->
这是⼀个企业级的智能知识库问答系统，服务于企业的技术支持场景。传统的关键词搜索无法理解用户的真实意图，⽐如用户问"电脑开不了机"，可能搜不到"计算机启动失败"的⽂档。我们的⽬标是通过RAG技术，让系统理解语义，精准匹配问题和答案。

<!-- source-block:4229 -->
**整体架构设计**

<!-- source-block:4230 -->
系统采⽤前后端分离架构，分为三层：

<!-- source-block:4231 -->
**展示层：Vue 3 + Element Plus，实现⽂档上传和智能问答界⾯**

<!-- source-block:4232 -->
**服务层：FastAPI 应⽤，提供上传和问答两个核⼼接口**

<!-- source-block:4233 -->
**数据层：Chroma向量数据库存储⽂档向量，⽀持相似度检索**

<!-- source-block:4234 -->
核⼼是RAG流程：用户提问 → 检索相关⽂档 → 构建Prompt → LLM⽣成答案。

<!-- source-block:4235 -->
**核心技术实现**

<!-- source-block:4236 -->
- 多路召回：单纯向量检索对中⽂专业术语理解不够准确，我设计了双路召回策略。第⼀路向量检索捕获语义相似性，第二路关键词检索捕获精确匹配。两路结果合并后去重、重排，召回率从65%提 升到85%+。

<!-- source-block:4237 -->
- 两阶段排序：关键词检索路使⽤两阶段排序。粗排阶段⽤Jieba分词 + Jaccard相似度快速筛选 Top-50候选；精排阶段将候选向量化，计算余弦相似度得到最终排名。这样既保证效率又保证准确性。

<!-- source-block:4238 -->
- 标题注⼊：⽂档切分后各个chunk会丢失上下⽂信息。我设计了标题注⼊机制，在每个chunk开头添加"⽂档来源:{标题}"前缀，让LLM能正确引⽤来源。

<!-- source-block:4239 -->
- Prompt⼯程：设计了四项回答要求⸺基于事实回答不编造、去特定化处理移除品牌型号、使用有序列表保持结构清晰、引⽤来源便于验证。

<!-- source-block:4240 -->
**遇到的挑战与解决方案**

<!-- source-block:4241 -->
最⼤的挑战是提升检索准确率。早期只用向量检索，对于包含专业术语的问题召回率不⾼。分析 Case后发现，关键词匹配能捕获向量检索遗漏的⽂档。于是设计了多路召回 + 两阶段排序的方案，效果显著提升。

<!-- source-block:4242 -->
另⼀个挑战是文档切分导致语义丢失。用户问XX问题怎么解决"，检索到的chunk可能只包含解决方案，没有问题描述，LLM⽆法判断是否匹配。通过标题注⼊机制解决，每个chunk都携带来源信息。

<!-- source-block:4243 -->
**项目成果与收获**

<!-- source-block:4244 -->
项目实现了完整的RAG问答功能，⽀持⽂档上传、智能检索、结构化回答。技术上，深⼊理解了 RAG架构、向量检索原理、⽂档处理策略。⼯程上，锻炼了从需求分析到架构设计再到编码实现的 全流程能⼒。

<!-- source-block:4245 -->
如果有机会优化，我会考虑引⼊Reranker模型进⼀步提升精排效果，以及添加用户反馈机制持续优化检索策略。

<!-- source-block:4246 -->
#### STAR法则回答模板

<!-- source-block:4247 -->
**问题：请介绍一个你解决过的技术难题**

<!-- source-block:4248 -->
**S（Situation）-情境：**

<!-- source-block:4249 -->
在开发知识库问答系统时，早期只使用向量检索，对于包含专业术语的问题召回率只有65%左右。

<!-- source-block:4250 -->
**T（Task）-任务：**

<!-- source-block:4251 -->
我需要提升检索的召回率和准确率，确保用户能找到真正相关的文档

<!-- source-block:4252 -->
**A（Action）-行动：**

<!-- source-block:4253 -->
我设计了多路召回 + 两阶段排序的方案：

<!-- source-block:4254 -->
- 第一路向量检索：用embedding模型捕获语义相似性

<!-- source-block:4255 -->
- 第二路用关键词检索：使用Jieba分词 + Jaccard相似度捕获精确匹配

<!-- source-block:4256 -->
- 粗排快速过滤Top-50，精排用向量余弦相似度精准排序

<!-- source-block:4257 -->
- 两路结果合并后去重、统一重排，返回Top-2给LLM

<!-- source-block:4258 -->
同时设计了标题注入机制，让每个chunk保留来源信息。

<!-- source-block:4259 -->
**R（Reslut）-结果：**

<!-- source-block:4260 -->
- 方案上线后，召回率从65%提升到85%+

<!-- source-block:4261 -->
- 用户反馈答案相关性明显提高，LLM也能正确引用来源了

<!-- source-block:4262 -->
#### 常见追问

<!-- source-block:4263 -->
| 追问 | 应答要点 |
| --- | --- |
| 文档切分的overlap有什么用？ | 防止边界处语义断裂，提高边界词汇被检索到的概率 |
| 如何保证LLM回答准确？ | 多路召回保证检索质量+Prompt要求基于事实+temperature=0确定性输出 |
| 项目有什么可以优化的地方？ | 引入Reranker模型、支持更多文档格式、添加用户反馈机制 |
| 为什么用Chroma不用Milvus？ | Chroma轻量级、本地部署方便，适合开发调试；生产环境下可换成Milvus |
| 为什么用LangChain？ | 成熟的LLM应用框架，提供文档加载、切分、向量存储等完整工具链 |
| Chroma返回的时什么分数？ | L2距离（欧氏距离），越小越相似余弦相似度需要自己计算 |

<!-- source-block:4264 -->
### 技术亮点速记

<!-- source-block:4265 -->
#### 多回路召回策略

<!-- source-block:4266 -->
- 向量检索(语义匹配)+关键词检索（精确匹配）

<!-- source-block:4267 -->
- 两路结果合并、去重、统一排序

<!-- source-block:4268 -->
- 召回率从65%提升到85%+

<!-- source-block:4269 -->
#### 两阶段排序

<!-- source-block:4270 -->
- 粗排：Jieba分词 + Jaccard相似度，快速过滤

<!-- source-block:4271 -->
- 精排：向量余弦相似度，精准排序

<!-- source-block:4272 -->
- 兼顾效率与准确性

<!-- source-block:4273 -->
#### 智能文档切分

<!-- source-block:4274 -->
- 动态策略：&lt;3000字不切分，≧3000字递归字符切分

<!-- source-block:4275 -->
- Chunk\_size=1500，overlap=200

<!-- source-block:4276 -->
- 分隔符优先级：##标题 &gt; \*\*加粗\*\* &gt; 段落 &gt; 行

<!-- source-block:4277 -->
#### 标题注入机制

<!-- source-block:4278 -->
- 每个chunk添加“文档来源：{标题}”前缀

<!-- source-block:4279 -->
- 解决切分后语义丢失、来源追溯问题

<!-- source-block:4280 -->
#### Prompt工程优化

<!-- source-block:4281 -->
- 基于事实回答、不编遭信息

<!-- source-block:4282 -->
- 去特定化处理，移除具体品牌型号

<!-- source-block:4283 -->
- 结构化输出，使用有序列表

<!-- source-block:4284 -->
- 引用来源，便于溯源验证

<!-- source-block:4285 -->
#### 异步文件处理

<!-- source-block:4286 -->
- aiofiles流式读写，1MB缓冲

<!-- source-block:4287 -->
- 避免大文件OOM问题
