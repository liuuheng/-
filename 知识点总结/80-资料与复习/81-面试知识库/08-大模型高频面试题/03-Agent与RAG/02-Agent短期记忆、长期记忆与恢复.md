---
tags:
  - 大模型
  - 面试
source: "尚硅谷大模型技术之高频面试题-V2.1.6.docx"
source_blocks: "1208-1265"
reviewed: 2026-10-21
---

# Agent短期记忆、长期记忆与恢复

记忆是可检索的数据，恢复是可重放的执行状态，二者不能混为一谈。

前篇：[[知识点总结/80-资料与复习/81-面试知识库/08-大模型高频面试题/03-Agent与RAG/01-Function Calling 与Agent执行闭环|Function Calling 与Agent执行闭环]] ｜ 后篇：[[知识点总结/80-资料与复习/81-面试知识库/08-大模型高频面试题/02-模型原理与训练/11-PPO、DPO与GRPO的目标和实现|PPO、DPO与GRPO的目标和实现]]

## 深入理解（新增）

短期消息窗口服务当前推理，长期记忆保存跨会话事实或偏好。把全部历史不断追加会增加成本，并把过期事实重新带入上下文。记忆应保留来源、时间、主体与更正关系，检索前执行租户和用户过滤。

Checkpoint 记录图执行状态，thread_id 关联同一运行链路；它不保证外部支付或消息发送“只执行一次”。节点恢复时可能重跑，因此外部写操作必须依赖独立幂等机制，或者拆成可确认的执行步骤。

摘要压缩历史会丢失细节。应保留可回到原始证据的标识，并明确哪些字段是用户确认事实、哪些只是模型推测。对过期记忆使用版本替换或失效标记，避免多个互相矛盾的事实同时被召回。



## 核心代码（新增）

以下是可在 CPU 运行的机制或业务契约示例，完整源码与测试见 [[知识点总结/80-资料与复习/81-面试知识库/08-大模型高频面试题/06-代码实验/01-核心代码实验与验证范围|核心代码实验与验证范围]]。示例不包含完整模型训练或外部平台部署。

```python
import json

def validate_tool_call(payload):
    data = json.loads(payload)
    if not isinstance(data, dict) or set(data) != {"tool", "args"}:
        raise ValueError("invalid envelope")
    if data["tool"] != "get_order" or not isinstance(data["args"], dict):
        raise ValueError("tool not allowed")
    order_id = data["args"].get("order_id")
    if set(data["args"]) != {"order_id"} or not isinstance(order_id, str) or not order_id.isdecimal():
        raise ValueError("invalid order_id")
    return data

call = validate_tool_call(
    '{"tool":"get_order","args":{"order_id":"123"}}')
assert call["args"]["order_id"] == "123"
# 接下来由执行服务核验调用者身份与订单归属；此函数未实现授权。
```

## 追问与验证（新增）

模拟节点在外部写成功后、checkpoint 前崩溃，说明恢复时如何用幂等键防止重复执行。

## 官方资料与论文

- [LangGraph 持久化](https://docs.langchain.com/oss/python/langgraph/persistence)
- [LangGraph 人工介入与恢复](https://docs.langchain.com/oss/python/langgraph/interrupts)

核验日期：2026-10-21。链接中的 `stable`、`main` 或平台文档会继续变化；新增纯 Python/NumPy 实验与原文旧版框架代码已分开标注。

## 原文保留

> [!warning] 原文批注
> 持久化 checkpointer 不等于长期语义记忆，也不自动提供外部系统的 exactly-once 副作用。
>
> 不要直接删除停用词或只保留向量来替代全部原始会话：否定、时间与主体可能被破坏，嵌入通常也不能还原原文。原文中的轮数、80/20 比例是启发式参数，需要按任务验证。

<!-- source-block:1208 -->
### Agent记忆管理

<!-- source-block:1209 -->
#### 记忆的本质是什么？

<!-- source-block:1210 -->
记忆的本质不是数据存储，而是上下文维护；核心问题是如何在有限的token窗口内保持对话的连贯性，所以有记忆的分级策略：长短期记忆的平衡。

<!-- source-block:1211 -->
#### LangChain中有哪些记忆类型？

<!-- source-block:1212 -->
**ConversationBufferMemory**

<!-- source-block:1213 -->
- 优点：简单直接，保留完整历史

<!-- source-block:1214 -->
- 缺点：token消耗快，成本高

<!-- source-block:1215 -->
- 适用：短对话、调试场景

<!-- source-block:1216 -->
**ConversationBufferWindowMemory（滑动窗口）**

<!-- source-block:1217 -->
考点：

<!-- source-block:1218 -->
**如何确定合适的窗口大小？**

<!-- source-block:1219 -->
- 分析对话结构：任务型对话k小(3-5)，开放式对话k大(8-10)

<!-- source-block:1220 -->
- 平衡token成本：k值 = (总token限额 - 当前prompt) / 每轮平均token

<!-- source-block:1221 -->
- 实际测试调整：从k=3开始，根据任务完成率逐步优化

<!-- source-block:1222 -->
**处理长对话的取舍策略？**

<!-- source-block:1223 -->
关键保留，次要丢弃：优先保留用户指令、系统响应、实体信息，过滤寒暄和确认语句。

<!-- source-block:1224 -->
**ConversationSummaryMemory（摘要记忆）**

<!-- source-block:1225 -->
考点：

<!-- source-block:1226 -->
**摘要生成的质量如何控制？**

<!-- source-block:1227 -->
- 结构优化：按\[事实\|意图\|决策\]分段摘要，避免信息混合

<!-- source-block:1228 -->
- 关键保留：强制保留数字、日期、命名实体

<!-- source-block:1229 -->
- 定期完整化：每5轮保存一次完整对话快照

<!-- source-block:1230 -->
**信息丢失问题如何解决？**

<!-- source-block:1231 -->
可以采用分级存储策略：

<!-- source-block:1232 -->
- 核心信息：直接存储（如用户需求）

<!-- source-block:1233 -->
- 次要信息：摘要存储

<!-- source-block:1234 -->
- 背景信息：向量化存储（需时可检索）

<!-- source-block:1235 -->
**Token效率和信息完整性权衡？**

<!-- source-block:1236 -->
- 80/20原则：用20%的token存储80%的关键信息。

<!-- source-block:1237 -->
- 动态调整：重要对话（如需求确认）保存全文，日常对话保存摘要。

<!-- source-block:1238 -->
**ConversationKnowledgeGraphMemory（知识图谱）**

<!-- source-block:1239 -->
考点：

<!-- source-block:1240 -->
**如何构建和维护对话中的实体关系？**

<!-- source-block:1241 -->
三层构建：

<!-- source-block:1242 -->
- 抽取层：NER提取实体，关系分类器提取关系

<!-- source-block:1243 -->
- 融合层：新实体与图谱合并（相同实体合并，冲突实体版本管理）

<!-- source-block:1244 -->
- 修剪层：定期清理低频实体和过时关系

<!-- source-block:1245 -->
**在复杂对话优势如何体现(同样是GraphRAG的优势)？**

<!-- source-block:1246 -->
- 多跳推理：能回答"上次提到的A公司的产品价格是多少？"

<!-- source-block:1247 -->
- 矛盾检测：能发现"之前说喜欢咖啡，现在说从不喝咖啡"的矛盾

<!-- source-block:1248 -->
- 个性化：构建用户偏好图谱，实现精准推荐

<!-- source-block:1249 -->
#### 超长对话处理

<!-- source-block:1250 -->
三级缓存架构：

<!-- source-block:1251 -->
**L0（热数据）：最近3轮完整对话（BufferWindowMemory）**

<!-- source-block:1252 -->
**L1（温数据）：前10轮关键摘要（SummaryMemory）**

<!-- source-block:1253 -->
**L2（冷数据）：全会话向量索引（VectorStoreRetrieverMemory）**

<!-- source-block:1254 -->
动态降级机制：对话超过20轮，自动启动摘要压缩。

<!-- source-block:1255 -->
#### Token优化策略

<!-- source-block:1256 -->
三阶段压缩：

<!-- source-block:1257 -->
**实时压缩：去除重复、停用词**

<!-- source-block:1258 -->
**摘要压缩：每5轮生成一次增量摘要**

<!-- source-block:1259 -->
**语义压缩：向量化表示，仅存储嵌入**

<!-- source-block:1260 -->
选择性召回：仅提取与当前query相关的历史片段

<!-- source-block:1261 -->
#### 上下文一致性

<!-- source-block:1262 -->
实体图谱+状态机：

<!-- source-block:1263 -->
**维护核心实体状态表（如{用户: {偏好: xx, 上次询问: xx}}）**

<!-- source-block:1264 -->
**每次回复前检查与历史状态的一致性**

<!-- source-block:1265 -->
**冲突时优先采用最新信息，但标记矛盾点供用户确认**
