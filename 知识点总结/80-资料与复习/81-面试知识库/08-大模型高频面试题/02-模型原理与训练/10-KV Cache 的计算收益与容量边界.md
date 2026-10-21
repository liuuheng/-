---
tags:
  - 大模型
  - 面试
source: "尚硅谷大模型技术之高频面试题-V2.1.6.docx"
source_blocks: "1121-1130"
reviewed: 2026-10-21
---

# KV Cache 的计算收益与容量边界

缓存用空间换取已处理 token 的 K/V 重用，主要服务自回归解码。

前篇：[[知识点总结/80-资料与复习/81-面试知识库/08-大模型高频面试题/02-模型原理与训练/09-PEFT、LoRA与微调数据集|PEFT、LoRA与微调数据集]] ｜ 后篇：[[知识点总结/80-资料与复习/81-面试知识库/08-大模型高频面试题/03-Agent与RAG/01-Function Calling 与Agent执行闭环|Function Calling 与Agent执行闭环]]

## 深入理解（新增）

Prefill 一次处理输入上下文，Decode 每次增加新 token。缓存避免为历史 token 重复计算 K/V，但新 query 仍需读取历史键值参与注意力。长上下文下，解码可能受缓存带宽与容量限制。

理想稠密缓存字节数为 2*batch*layers*tokens*kv_heads*head_dim*bytes。前面的 2 对应 K 与 V。示例配置 batch=1、32 层、4096 token、8 个 KV 头、128 维、每元素 2 字节，结果为 512 MiB。

实际服务还需要考虑分页块、碎片、并发请求长度分布、前缀共享与量化元数据。不能用单请求平均长度替代高峰容量规划；应同时观察首 token 时延、每 token 时延与缓存占用。

### 可编辑公式

$$\mathrm{bytes}_{KV}=2BLT H_{KV}d_h s$$

$B$ 为并发 batch，$L$ 为层数，$T$ 为缓存 token 数，$H_{KV}$ 为 KV 头数，$d_h$ 为头维度，$s$ 为每元素字节数。

## 核心代码（新增）

以下是可在 CPU 运行的机制或业务契约示例，完整源码与测试见 [[知识点总结/80-资料与复习/81-面试知识库/08-大模型高频面试题/06-代码实验/01-核心代码实验与验证范围|核心代码实验与验证范围]]。示例不包含完整模型训练或外部平台部署。

```python
def kv_cache_bytes(batch, layers, tokens, kv_heads, head_dim, bytes_per_element=2):
    return 2 * batch * layers * tokens * kv_heads * head_dim * bytes_per_element

size = kv_cache_bytes(1, 32, 4096, 8, 128)
assert size == 512 * 1024**2
print(size / 1024**2, "MiB")
```

## 追问与验证（新增）

将 kv_heads 从 32 改成 8，核验理论缓存缩减；再说明为何端到端时延不一定缩短为四分之一。

## 官方资料与论文

- [Transformers 推理内存优化](https://huggingface.co/docs/transformers/llm_tutorial_optimization)

核验日期：2026-10-21。链接中的 `stable`、`main` 或平台文档会继续变化；新增纯 Python/NumPy 实验与原文旧版框架代码已分开标注。

## 原文保留

> [!warning] 原文批注
> KV Cache 避免重复的历史 K/V 计算，却不把每步对历史序列的注意力读取降为常数。

<!-- source-block:1121 -->
### KV Cache

<!-- source-block:1122 -->
#### 介绍一下KV Cache

<!-- source-block:1123 -->
KV-Cache（Key-Value Cache）是Transformer模型推理阶段的优化机制。

<!-- source-block:1124 -->
核心思想

<!-- source-block:1125 -->
推理时，Transformer预测每个token都需要用到所有历史token的K和V，KV-Cache 将这些K和V缓存起来，生成时直接复用，不必重新计算。

<!-- source-block:1126 -->
#### KV Cache优缺点

<!-- source-block:1127 -->
**优点**

<!-- source-block:1128 -->
不必重复计算注意力，推理更快。

<!-- source-block:1129 -->
**缺点**

<!-- source-block:1130 -->
缓存占用随序列长度的增长而增大，会带来额外的显存开销。
