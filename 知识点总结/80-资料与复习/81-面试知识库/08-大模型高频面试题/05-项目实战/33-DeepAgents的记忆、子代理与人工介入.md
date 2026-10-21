---
tags:
  - 大模型
  - 面试
source: "尚硅谷大模型技术之高频面试题-V2.1.6.docx"
source_blocks: "5039-5087"
reviewed: 2026-10-21
---

# DeepAgents的记忆、子代理与人工介入

长期记忆、子代理和人工介入都需要可审计的边界。

前篇：[[知识点总结/80-资料与复习/81-面试知识库/08-大模型高频面试题/05-项目实战/32-DeepAgents深度搜索的工具与编排|DeepAgents深度搜索的工具与编排]] ｜ 后篇：[[知识点总结/80-资料与复习/81-面试知识库/08-大模型高频面试题/05-项目实战/34-Coze商户管家的结构化工作流|Coze商户管家的结构化工作流]]

## 深入理解（新增）

子代理上下文隔离可以减少主上下文膨胀，但并不天然隔离权限。代理能调用什么工具、读取什么数据，必须由服务端配置和身份上下文决定。

长期记忆中的事实需要来源、时间与更正机制。搜索获得的内容不应自动写成用户偏好或永久事实；记忆召回应遵守用户和租户边界，并避免把过期结论当作当前状态。

人工介入应绑定待执行动作及参数快照，恢复时若参数变化需要重新确认。interrupt 之后节点可重放，网络写操作要避免在确认前执行，也不能依赖进程内变量代表审批状态。



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

批准动作 A 后篡改参数为 B，测试是否拒绝复用旧批准；再模拟进程重启，检查审批与任务状态能否恢复。

## 官方资料与论文

- [Deep Agents 官方概览](https://docs.langchain.com/oss/python/deepagents/overview)
- [LangGraph 人工介入与恢复](https://docs.langchain.com/oss/python/langgraph/interrupts)
- [LangGraph 持久化](https://docs.langchain.com/oss/python/langgraph/persistence)

核验日期：2026-10-21。链接中的 `stable`、`main` 或平台文档会继续变化；新增纯 Python/NumPy 实验与原文旧版框架代码已分开标注。

## 原文保留

> [!warning] 原文批注
> 子代理不是权限隔离机制；人工介入也不自动提供跨进程持久化和外部副作用幂等。
>
> 原文的团队人数、业务规模、效果百分比、硬件与成本均保留为教材案例陈述；本次未独立复现这些项目指标。

<!-- source-block:5039 -->
### 项目面试介绍

<!-- source-block:5040 -->
面试官您好，我最近做的一个项目是 基于 DeepAgents 的深度搜索系统 ，核心目标是把传统“问一次、答一次”的检索方式，升级成“ 搜索-阅读-反思-再搜索 ”的闭环研究流程。

<!-- source-block:5041 -->
这个项目本质上是一个 1主 + N专 的多智能体协作系统：主智能体负责理解用户需求、拆解任务和最终汇总，子智能体分别处理公网搜索、企业数据库查询和私有知识库检索，各自并行工作，再回传给主智能体统一生成结果。

<!-- source-block:5042 -->
在 应用场景 上，它适合复杂信息分析类任务。比如：

<!-- source-block:5043 -->
第一，政策解读场景，用户问“最近平台政策有没有变化”，系统会同时做外网检索和知识库核验，输出结构化解释；

<!-- source-block:5044 -->
第二，经营分析场景，用户问“某项业务指标为什么异常”，数据库子智能体会先识别表结构再执行 SQL，给出可追溯的数据证据；

<!-- source-block:5045 -->
第三，研究交付场景，系统能把多源信息自动整理成 Markdown，并一键转成 PDF，直接用于汇报。

<!-- source-block:5046 -->
在 技术栈 上，我主要用了 LangChain + LangGraph + DeepAgents 作为智能体框架；

<!-- source-block:5047 -->
服务层用 FastAPI + WebSocket 实现实时流式输出；

<!-- source-block:5048 -->
工具层接入了 Tavily 做外网搜索、 MySQL 做结构化数据查询、 RAGFlow 做企业私有知识检索；

<!-- source-block:5049 -->
另外用 Pydantic 做状态和参数校验， asyncio + ContextVars 处理并发下的会话隔离，保证多用户同时使用时不串数据。

<!-- source-block:5050 -->
这个项目我认为有几个亮点 ：

<!-- source-block:5051 -->
第一，架构亮点是多智能体分工明确，能处理复杂、多步骤任务，不是单点问答；

<!-- source-block:5052 -->
第二，质量亮点是有防幻觉设计，比如数据库查询先探表再SQL，搜索任务限制角度和轮次，减少无效调用；

<!-- source-block:5053 -->
第三，工程亮点是全链路可观测和实时回传，用户能看到系统“怎么思考、怎么调用工具”；

<!-- source-block:5054 -->
第四，交付亮点是端到端闭环，从问题输入到报告输出可以自动完成。

<!-- source-block:5055 -->
如果一句话总结，这个项目让我把“LLM Demo”推进到了“ 可落地的智能研究与分析系统 ”，重点不只是模型调用，而是 多智能体协作、工具编排、工程可用性和业务交付能力 。

<!-- source-block:5057 -->
### DeepAgent相关面试题

<!-- source-block:5058 -->
#### DeepAgents、LangChain、LangGraph 分别是什么关系？为什么不直接只用 LangChain？

<!-- source-block:5059 -->
可以理解为三层： LangChain 负责模型与工具抽象， LangGraph 负责有状态流程编排与持久化运行时， DeepAgents 在外层提供“可直接落地的深度智能体能力”，内置规划、子代理、文件系统、长期记忆。只用 LangChain 适合简单单Agent，复杂多步骤任务用 DeepAgents 更省工程成本。

<!-- source-block:5060 -->
#### DeepAgents 的核心能力有哪些？你最看重哪一个？

<!-- source-block:5061 -->
文档里四个核心能力：任务规划与分解（ write\_todos 体系）、上下文管理（ ls/read\_file/write\_file/edit\_file ）、子代理委派（ task ）、长期记忆（基于 Store 跨会话）。我最看重“规划+上下文管理”，因为它直接解决复杂任务可执行性和上下文溢出两个落地痛点。

<!-- source-block:5062 -->
#### write\_todos/read\_todos/update\_todos/delete\_todos 在项目里怎么用？

<!-- source-block:5063 -->
把大任务先拆成可执行步骤，用 write\_todos 建计划；执行中用 read\_todos 读取当前状态；遇到信息变化动态 update\_todos ；无效步骤 delete\_todos 。这样任务是“可观测、可调整、可收敛”的，不是一次性拍脑袋推理。

<!-- source-block:5064 -->
#### DeepAgents 怎么避免上下文窗口爆掉？

<!-- source-block:5065 -->
它不是把所有中间结果塞进模型上下文，而是把大结果外置到文件系统或存储中；模型只在需要时精准读取片段。比如大篇幅搜索结果先 write\_file ，后续按需 read\_file ，这样上下文只保留当前决策所需信息，降低 token 和遗忘风险。

<!-- source-block:5066 -->
#### 子代理（SubAgent）什么时候该用，什么时候不该用？

<!-- source-block:5067 -->
该用在多步骤、专业分工、工具差异大、主代理需要保持“高层协调”时；不该用于一步可完成、强依赖连续上下文、或成本高于收益的任务。文档强调子代理的价值是“上下文隔离+专业化”，不是为了炫技拆分。

<!-- source-block:5068 -->
#### 主代理如何决定调用哪个子代理？

<!-- source-block:5069 -->
主要依据子代理配置里的 description 与任务语义匹配，命中后会自动产生 task 工具调用并传 subagent\_type/description 。所以 description 必须写成“可触发、可执行、边界清晰”的职责描述，不能写空泛口号。

<!-- source-block:5070 -->
#### DeepAgents 的流式输出你怎么解析？ chunk 一般看什么？

<!-- source-block:5071 -->
重点看 model 和 tools 两类节点： model 看是否有 tool\_calls （说明在决策下一步）， tools 看工具返回数据；当 model 无 tool\_calls 且有 content，通常是最终回复。这样能完整还原“思考-调用-观察-回答”链路，便于调试与前端实时展示。

<!-- source-block:5073 -->
#### 多智能体最大的风险是什么？你怎么治理？

<!-- source-block:5074 -->
文档给了两大风险： Token 成本失控 和 调试黑箱 。治理策略是：

<!-- source-block:5075 -->
- 先设“是否启用多Agent”的准入线（简单任务禁用多Agent）；

<!-- source-block:5076 -->
- 控制交互轮数与上下文长度；

<!-- source-block:5077 -->
- 强制全链路 tracing 和日志，不可观测不上线。

<!-- source-block:5078 -->
这是多Agent从 Demo 到生产的底线。

<!-- source-block:5079 -->
#### DeepAgents支持子代理嵌套吗，比如CEO→CTO→Coder三层？

<!-- source-block:5080 -->
当前文档示例明确指出深层嵌套不稳定/不支持，实践上建议扁平化调度（如 CEO 直接调度 CTO、Coder）。也就是“可用优先于理论层级完美”，先保证可控和稳定。

<!-- source-block:5081 -->
#### DeepAgents 怎么做HITL（人机审批）？关键点是什么？

<!-- source-block:5082 -->
- 在 interrupt\_on 标记高风险工具（approve/edit/reject）；

<!-- source-block:5083 -->
- 配置 checkpointer 保存中断状态；

<!-- source-block:5084 -->
- 恢复执行时使用同一个 thread\_id ，并按 action\_requests 顺序提交 decisions 。

<!-- source-block:5085 -->
关键是“同线程恢复 + 顺序一致”，否则审批恢复链路会断。
