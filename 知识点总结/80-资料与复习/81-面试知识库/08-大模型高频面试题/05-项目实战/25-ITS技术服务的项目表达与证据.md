---
tags:
  - 大模型
  - 面试
source: "尚硅谷大模型技术之高频面试题-V2.1.6.docx"
source_blocks: "3818-3980"
reviewed: 2026-10-21
---

# ITS技术服务的项目表达与证据

项目复盘要能从一次真实请求解释模块、失败与验证结果。

前篇：[[知识点总结/80-资料与复习/81-面试知识库/08-大模型高频面试题/05-项目实战/24-ITS技术服务的工具调用、SSE与性能|ITS技术服务的工具调用、SSE与性能]] ｜ 后篇：[[知识点总结/80-资料与复习/81-面试知识库/08-大模型高频面试题/05-项目实战/26-ITS知识库的数据摄入与混合检索|ITS知识库的数据摄入与混合检索]]

## 深入理解（新增）

以一条具体技术问题为线索，说明入口如何路由、检索哪些资料、何时调用工具、如何汇总及何时转人工。只罗列框架名无法证明理解数据和控制流。

“响应变快”需要拆成队列、检索、模型首 token、完整生成与外部工具时间。流式输出可能降低感知等待，却不降低完整计算时间；指标口径应在面试中明确。

原文数字可作为需要复核的案例材料，个人叙述应仅包含实际承担和有证据的修改。准备一个失败样本及定位过程，往往比堆叠无法复现的高百分比更有解释力。



## 核心代码（新增）

以下是可在 CPU 运行的机制或业务契约示例，完整源码与测试见 [[知识点总结/80-资料与复习/81-面试知识库/08-大模型高频面试题/06-代码实验/01-核心代码实验与验证范围|核心代码实验与验证范围]]。示例不包含完整模型训练或外部平台部署。

```python
import json

def sse_event(event, data, event_id):
    if any(c in event for c in "\r\n") or any(c in str(event_id) for c in "\r\n"):
        raise ValueError("event/id must be single-line")
    body = json.dumps(data, ensure_ascii=False)
    return f"id: {event_id}\nevent: {event}\ndata: {body}\n\n"

event = sse_event("progress", {"state": "running"}, "task-1:1")
assert event.endswith("\n\n")
# 最终成功必须另发 done/终态，不能把 progress 当成成功。
```

## 追问与验证（新增）

针对一次超时请求按阶段还原时间线，区分平均时延、首 token 时延与端到端 p95。

## 官方资料与论文

- [LangGraph 工作流](https://docs.langchain.com/oss/python/langgraph/overview)
- [asyncio 任务与超时](https://docs.python.org/3/library/asyncio-task.html)

核验日期：2026-10-21。链接中的 `stable`、`main` 或平台文档会继续变化；新增纯 Python/NumPy 实验与原文旧版框架代码已分开标注。

## 原文保留

> [!warning] 原文批注
> 原文成本、吞吐和提升幅度不是本次复现结果；没有原始记录时应作为待验证案例口径。
>
> 原文的团队人数、业务规模、效果百分比、硬件与成本均保留为教材案例陈述；本次未独立复现这些项目指标。

<!-- source-block:3818 -->
### 面试高频问题与回答

<!-- source-block:3819 -->
#### 项目介绍类

<!-- source-block:3820 -->
##### 问题1：简单介绍一下这个项目

<!-- source-block:3821 -->
回答要点：这是⼀个基于多智能体架构的智能客服系统，主要服务于企业的线下服务⽹络。系统采⽤主调度 +专家的两级智能体架构，⽀持技术问题咨询、实时资讯查询、服务站定位与导航三⼤核⼼功能。技术上，后端使⽤FastAPI + OpenAI Agents SDK，通过MCP协议 集成百度地图和搜索引擎，前端使⽤ Vue 3，实现了流式响应的对话体验。 我在项⽬中主要负责多智能体框架设计和后端核⼼功能开发。

<!-- source-block:3822 -->
##### 为什么选择多智能体架构而不是单一大模型？

<!-- source-block:3823 -->
选择多智能体主要基于以下几点考虑：

<!-- source-block:3824 -->
职责分离：不同领域的问题由专门的智能体处理，便于独立优化提示词和工具

<!-- source-block:3825 -->
可扩展性：新增业务领域只需添加新的专家智能体，不影响现有功能

<!-- source-block:3826 -->
成本优化：主调度只做意图识别和路由，可以使用较小的模型；专家智能体根据任务的复杂度选择合适的模型

<!-- source-block:3827 -->
#### 技术深度类

<!-- source-block:3828 -->
##### SSE流式响应是如何实现的？和WebSocket有什么区别？

<!-- source-block:3829 -->
**实现方式：**

<!-- source-block:3830 -->
- 后端使用FastAPI的StreamingResponse，配合Python的AsyncGenerator生成SSE数据流。

<!-- source-block:3831 -->
- 前端使用fetch+ReadableStream 逐块读取，解析data：前缀的JSON数据

<!-- source-block:3832 -->
**与WebSocket的区别**

<!-- source-block:3833 -->
| 特性 | SSE | WebSocket |
| --- | --- | --- |
| 方向 | 单向（服务端→客户端） | 双向 |
| 协议 | HTTP | 独立协议 |
| 重连 | 自动重连 | 需手动实现 |
| 适用场景 | 服务端推送 | 实时双向通信 |

<!-- source-block:3834 -->
对于AI对话这种只需要服务端推送的场景，SSE更简单且足够使用。

<!-- source-block:3835 -->
##### MCP（模型上下文工程）协议是什么？你是如何集成的？

<!-- source-block:3836 -->
MCP（Model Context Protocol）是由Anthropic提出的标准化工具集成协议，用于让AI模型调用外部工具。

<!-- source-block:3837 -->
**集成方式：**

<!-- source-block:3838 -->
使用MCPServerSse客户端连接MCP服务（如百度地图）

<!-- source-block:3839 -->
MCP服务会自动暴露可用工具列表（如geocode、ip\_location）

<!-- source-block:3840 -->
在智能体定义时通过mcp\_servers参数注册

<!-- source-block:3841 -->
智能体运行时可以像调用本地函数一样调用MCP工具

<!-- source-block:3842 -->
**优势：**

<!-- source-block:3843 -->
标准化协议、工具自动发现、跨平台兼容。

<!-- source-block:3844 -->
##### 数据库连接池是如何工作的？为什么要用连接池？

<!-- source-block:3845 -->
**为什么要用连接池？**

<!-- source-block:3846 -->
- 数据库连接创建开销大（TCP三次握手、认证等）

<!-- source-block:3847 -->
- 高并发时频繁创建/销毁连接会导致性能下降

<!-- source-block:3848 -->
- 数据库有最大连接数限制

<!-- source-block:3849 -->
**工作原理：**

<!-- source-block:3850 -->
- 应用启动时预创建一定数量的连接（mincached）

<!-- source-block:3851 -->
- 请求到来时从池中获取空闲连接

<!-- source-block:3852 -->
- 请求结束后将连接归还到池中（而非真正关闭）

<!-- source-block:3853 -->
- 连接池维护最大连接数(maxconnections)，超出时排队等待

<!-- source-block:3854 -->
我们的配置：使用DBUtils的PooledDB，最大5个连接，初始两个空闲连接。

<!-- source-block:3855 -->
#### 问题解决类

<!-- source-block:3856 -->
##### 遇到过什么技术难点？如何解决的？

<!-- source-block:3857 -->
**回答示例一：用户定位问题**

<!-- source-block:3858 -->
**问题：**

<!-- source-block:3859 -->
用户不一定会提供具体地址，导致无法准确推荐附近服务站。

<!-- source-block:3860 -->
**解决方案：**

<!-- source-block:3861 -->
设计了三级定位降级策略

<!-- source-block:3862 -->
- 优先从用户输入中提取地址进行地理编码

<!-- source-block:3863 -->
- 如果没有地址，使用IP定位获取大致位置

<!-- source-block:3864 -->
- 如果IP定位也失败，使用默认位置（北京）作为兜底

<!-- source-block:3865 -->
这样保证了功能的可用性，同时尽可能提高定位精度。

<!-- source-block:3866 -->
**回答示例二：流式消息状态管理**

<!-- source-block:3867 -->
**问题：**

<!-- source-block:3868 -->
SSE返回的消息分为思考、过程、答案三种类型，需要分别渲染，状态管理容易混乱。

<!-- source-block:3869 -->
**解决方案：**

<!-- source-block:3870 -->
- 定义统一的消息枚举类型

<!-- source-block:3871 -->
- 维护\[当前正在接受的消息\]的引用变量

<!-- source-block:3872 -->
- 根据SSE事件类型判断是追加内容还是创建新消息

<!-- source-block:3873 -->
- 消息发送完成后重置状态

<!-- source-block:3874 -->
##### 如何保证系统的高可用？

<!-- source-block:3875 -->
外部服务降级：MCP服务不可用时有兜底方案（如默认位置）

<!-- source-block:3876 -->
连接池管理：数据库使用连接池，避免连接耗尽

<!-- source-block:3877 -->
错误重试：智能体调用失败时支持自动重试

<!-- source-block:3878 -->
日志监控：多级日志(info/error/debug)便于问题定位

<!-- source-block:3879 -->
上下文裁剪：历史对话子保留最近N轮，避免token超限

<!-- source-block:3880 -->
#### 设计思考类

<!-- source-block:3881 -->
##### 如果让你重新设计这个系统，你会做出哪些改进？

<!-- source-block:3882 -->
回答要点：

<!-- source-block:3883 -->
引入向量数据库：使用Milvus/Faiss存储知识库文档，提升语义检索精度

<!-- source-block:3884 -->
对话摘要：长对话使用LLM生成摘要，而非简单裁剪

<!-- source-block:3885 -->
分布式部署：使用Kubernetes实现水平扩展

<!-- source-block:3886 -->
可观测性：集成OpenTelemetry实现分布式追踪

<!-- source-block:3887 -->
A/B测试框架：对比不同提示词和模型的效果

<!-- source-block:3888 -->
##### 多智能体之间是如何通信的？

<!-- source-block:3889 -->
**本项目采用的是函数调用的通信方式：**

<!-- source-block:3890 -->
子智能体被包装为@function\_tool装饰的函数

<!-- source-block:3891 -->
主调度智能体通过工具调用的方式调用子智能体

<!-- source-block:3892 -->
子智能体执行完成后，结果作为工具调用的返回值传回

<!-- source-block:3893 -->
**这种方式的优点是：**

<!-- source-block:3894 -->
实现简单，不需要消息队列

<!-- source-block:3895 -->
同步调用，易于追踪调试

<!-- source-block:3896 -->
与OpenAI Function Calling原生兼容

<!-- source-block:3897 -->
如果是更加复杂的场景（如多智能体并行，异步协作），可能需要引入消息队列（如Redis/RabbitMQ）

<!-- source-block:3898 -->
#### 项目延申类

<!-- source-block:3899 -->
##### 这个项目支持多少并发？如何进一步提升？

<!-- source-block:3900 -->
**当前瓶颈分析：**

<!-- source-block:3901 -->
数据库连接池5个连接

<!-- source-block:3902 -->
LLM API调用延迟较高（通常1-10秒）

<!-- source-block:3903 -->
单实例部署

<!-- source-block:3904 -->
**提升方案：**

<!-- source-block:3905 -->
水平扩展：部署多个FastAPI实例，使用Nginx负载均衡

<!-- source-block:3906 -->
异步优化：确保所有I/O操作都是异步的

<!-- source-block:3907 -->
流控限速：保护LLM API，防止超限

<!-- source-block:3908 -->
连接池扩容：根据并发数量调整数据库连接池大小

<!-- source-block:3909 -->
### 面试口述版项目总结

<!-- source-block:3910 -->
#### 30秒版

<!-- source-block:3911 -->
我参与开发了⼀个基于多智能体架构的智能客服系统。系统采⽤「主调度+专家」的两级架构，⽀持技术咨询、实时资讯和服务站导航三⼤功能。后端使⽤ FastAPI + OpenAI Agents SDK，通过 MCP 协议集成百度地图，前端使⽤ Vue 3 实现流式对话体验。我主要负责多智能体框架设计和后端核⼼开发。

<!-- source-block:3912 -->
#### 2分钟版

<!-- source-block:3913 -->
**项目背景**

<!-- source-block:3914 -->
这是⼀个企业级智能客服系统，⽬标是为企业的线下服务⽹络提供智能化⽀持。用户可以通过对话 ⽅式咨询技术问题、查询实时资讯，以及定位附近的服务站并获取导航。

<!-- source-block:3915 -->
**技术架构**

<!-- source-block:3916 -->
系统采⽤多智能体架构，分为三层：

<!-- source-block:3917 -->
主调度智能体：负责意图识别和任务路由

<!-- source-block:3918 -->
技术专家智能体：处理技术问题和资讯查询

<!-- source-block:3919 -->
业务专家智能体：处理服务站查询和导航

<!-- source-block:3920 -->
后端使⽤ FastAPI 框架，集成 OpenAI Agents SDK 实现智能体编排。通过 MCP 协议对接百度地图 和搜索引擎。前端使⽤ Vue 3，实现了 SSE 流式响应，⽤⼾能实时看到 AI 的思考过程。

<!-- source-block:3921 -->
**我的职责**

<!-- source-block:3922 -->
我主要负责多智能体框架的设计与实现，包括智能体间的⼯具调⽤机制、SSE 流式响应的前后端实 现，以及⽤⼾定位的三级降级策略设计。

<!-- source-block:3923 -->
**技术亮点**

<!-- source-block:3924 -->
项⽬有⼏个技术亮点：⼀是多智能体的⼯具化设计，⼦智能体被封装为可调⽤的⼯具；⼆是三级定 位策略保证了服务的可⽤性；三是完整的会话管理⽀持多轮对话。

<!-- source-block:3925 -->
#### 5分钟版

<!-- source-block:3926 -->
**项⽬背景与⽬标**

<!-- source-block:3927 -->
这是⼀个企业级的智能客服系统，服务于企业的线下服务站⽹络。传统客服系统响应慢、⼈⼯成本 ⾼，我们的⽬标是通过 AI 技术实现 7x24 ⼩时的智能服务，覆盖三个核⼼场景：技术问题咨询、实 时信息查询、以及服务站定位导航。

<!-- source-block:3928 -->
**整体架构设计**

<!-- source-block:3929 -->
系统采⽤前后端分离架构，分为四层：

<!-- source-block:3930 -->
**展示层：Vue 3 + Element Plus，实现聊天界⾯和会话管理**

<!-- source-block:3931 -->
**网关层：FastAPI 应⽤，提供 RESTful API 和 SSE 流式接⼝**

<!-- source-block:3932 -->
**业务层：多智能体服务，包含主调度和两个专家智能体**

<!-- source-block:3933 -->
**基础设施层：AI 模型、MCP 服务、MySQL 数据库、⽇志系统**

<!-- source-block:3934 -->
核⼼是多智能体协作机制：主调度智能体接收⽤⼾请求后，根据意图选择合适的⼦智能体处理。⼦智能体通过 @function\_tool 装饰器包装为⼯具，这样主调度可以像调⽤函数⼀样调⽤它们。

<!-- source-block:3935 -->
**核⼼技术实现**

<!-- source-block:3936 -->
流式响应：使⽤ FastAPI 的 StreamingResponse 配合 Python AsyncGenerator，将 AI 的思考过 程、⼯具调⽤、最终答案分类型推送给前端。前端通过 ReadableStream 逐块解析，实时更新 UI。

<!-- source-block:3937 -->
MCP 集成：MCP 是 Model Context Protocol，⼀种标准化的⼯具集成协议。我们⽤它对接百度地图服务，智能体可以调⽤地理编码、IP 定位、⽣成导航链接等能⼒。

<!-- source-block:3938 -->
定位策略：⽤⼾查询服务站时可能不提供具体地址，我设计了三级降级⽅案：优先解析⽤户输⼊中的地址，其次使⽤ IP 定位，最后兜底到默认位置。这样既保证了准确性，⼜确保功能可⽤。

<!-- source-block:3939 -->
**遇到的挑战与解决⽅案**

<!-- source-block:3940 -->
最⼤的挑战是智能体意图误判。⽐如⽤户说「帮我看看」，可能是技术问题也可能是服务站查询。我们通过提⽰词⼯程优化，明确定义了每种意图的关键词特征，并设计了「只响应明确需求，不推 测隐含意图」的原则。

<!-- source-block:3941 -->
另⼀个挑战是前端状态管理。SSE 返回的消息有三种类型，需要分别渲染。我设计了消息类型枚举 和「当前消息引⽤」机制，确保追加内容时不会创建重复消息。

<!-- source-block:3942 -->
**项⽬成果与收获**

<!-- source-block:3943 -->
项⽬最终实现了完整的智能客服功能，⽀持多轮对话和会话持久化。技术上，我深⼊理解了多智能 体架构设计、SSE 流式通信、MCP 协议集成。⼯程上，锻炼了从需求分析到架构设计再到编码实现 的全流程能⼒。

<!-- source-block:3944 -->
如果有机会重构，我会考虑引⼊向量数据库提升知识检索精度，以及添加 A/B 测试框架对⽐不同提⽰词的效果。

<!-- source-block:3945 -->
#### STAR法则回答模板

<!-- source-block:3946 -->
**问题：请介绍一个你解决过的技术难题**

<!-- source-block:3947 -->
**S（Situation）-情境：**

<!-- source-block:3948 -->
在开发智能客服系统时，用户查询附近服务站经常定位不准

<!-- source-block:3949 -->
**T（Task）-任务：**

<!-- source-block:3950 -->
我们需要设计一个可靠的定位方案，在各种情况下都能给出合理的位置

<!-- source-block:3951 -->
**A（Action）-行动：**

<!-- source-block:3952 -->
我设计了三级降级策略：

<!-- source-block:3953 -->
- 优先解析用户输入中的地址（如‘我在中关村’）

<!-- source-block:3954 -->
- 如果没有地址，使用IP定位获取城市级位置

<!-- source-block:3955 -->
- 如果IP定位失败，使用默认位置（北京）作为兜底

<!-- source-block:3956 -->
同时我在返回结果中标注了数据来源，让用户知道结果的可信度

<!-- source-block:3957 -->
**R（Result）-结果：**

<!-- source-block:3958 -->
方案上线后，服务站查询成功率从75%提升到99%，用户反馈定位准确性也明显提高。

<!-- source-block:3959 -->
#### 常见追问

<!-- source-block:3960 -->
| 追问 | 应答要点 |
| --- | --- |
| 为什么FastAPI不用Flask？ | 原神异步支持、自动OpenAPI文档、性能更好 |
| 为什么SSE不用WebSocket？ | AI对话时单向推送，SSE更简单，自动重连 |
| 多智能体会不会增加延迟？ | 会，但职责分离带来的可维护性收益更大；可通过模型选型优化。 |
| 如何保证AI回答质量？ | 提示词工程+知识库兜底+禁止编造原则 |
| 项目上线了吗？有数据吗？ | 内部测试阶段/日均XX请求/平均响应X秒（根据实际情况） |

<!-- source-block:3961 -->
### 技术亮点速记

<!-- source-block:3962 -->
#### 多智能体架构

<!-- source-block:3963 -->
- 主调度+专家的两级设计

<!-- source-block:3964 -->
- 子智能体包装为@function\_tool

<!-- source-block:3965 -->
- 职责分离、可扩展、故障隔离

<!-- source-block:3966 -->
#### SSE流式响应

<!-- source-block:3967 -->
- FastAPI StreamingResponse + AsyncGenerator

<!-- source-block:3968 -->
- 前端 ReadableStream + TextDecoder

<!-- source-block:3969 -->
- 三类事件: THINKING / PROCESS / ANSWER

<!-- source-block:3970 -->
#### MCP协议集成

<!-- source-block:3971 -->
- Anthropic 提出的标准化工具协议

<!-- source-block:3972 -->
- MCPServerSse 连接百度地图、搜索引擎

<!-- source-block:3973 -->
- 工具自动发现、跨平台兼容

<!-- source-block:3974 -->
#### 三级定位策略

<!-- source-block:3975 -->
- 地址解析 → IP 定位 → 默认位置

<!-- source-block:3976 -->
- 保证功能可用性的降级方案

<!-- source-block:3977 -->
#### 数据库连接池

<!-- source-block:3978 -->
- DBUtils PooledDB

<!-- source-block:3979 -->
- 避免频繁创建/销毁连接

<!-- source-block:3980 -->
- 最大 5 连接，初始 2 连接
