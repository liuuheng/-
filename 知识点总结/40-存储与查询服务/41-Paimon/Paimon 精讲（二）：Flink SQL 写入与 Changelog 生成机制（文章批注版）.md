---
title: "Paimon 精讲（二）：Flink SQL 写入与 Changelog 生成机制（文章批注版）"
aliases:
  - "Paimon 精讲（二）：FlinkSQL 写 Paimon，Changelog 在背后默默做了哪些事？"
author: "胖泽的技术笔记"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s/6ajHhiEvyw0IcirKOgciYA"
published_at: "2026-07-24"
reviewed_at: "2026-10-14"
tags:
  - Flink
  - Paimon
  - Paimon-精讲
---

# Paimon 精讲（二）：Flink SQL 写入与 Changelog 生成机制（文章批注版）

> [!info] 来源、整理方式与版本边界
> 原文：[《Paimon 精讲（二）：FlinkSQL 写 Paimon，Changelog 在背后默默做了哪些事？》](https://mp.weixin.qq.com/s/6ajHhiEvyw0IcirKOgciYA)，胖泽的技术笔记，2026-07-24。正文按原文章节顺序保留技术内容、代码、表格、字段和案例；新增内容统一标为“批注”“勘误”或“版本边界”。原文没有声明准确的 Paimon/Flink 版本，内部类、默认值和源码路径不视为稳定 API。

> [!warning] 批注：先区分“没有完整 Changelog”与“流读没有变化”
> `changelog-producer=none` 不生成独立的完整撤回流，但增量读取仍可获得 Upsert 变化；需要旧值的下游可能由 Flink Normalize 保存状态补齐。原文“只维护最新状态、默认不生成 changelog”的表述容易被理解成流读完全没有数据。
> `input` 不会替上游补造缺失的 `UPDATE_BEFORE`，也不会把 Partial Update 自动还原成完整逻辑行；只有输入本身已经是完整 Changelog 时才正确。
> Exactly-Once 依赖 Flink Checkpoint、Paimon Committer 和原子发布 Snapshot 的完整链路，不是 Changelog 文件单独提供的保证。
## 引言

用 Flink SQL 写 Paimon 时，你是否遇到过这样的困惑：插入一条数据，下游却收到两条消息？开启 changelog-producer 后性能骤降？

这些现象的背后，正是 Paimon 的 Changelog 机制在起作用。它是实现流式读取、增量消费和 Exactly-Once 语义的核心。

本文将拆解 Flink SQL 写入 Paimon 时的 Changelog 生成逻辑，对比不同表模式下的行为差异，并给出 changelog-producer 的配置建议，助你避开常见坑点。

## 1 . Changelog 的生成时机

Paimon 中 changelog 的生成时机取决于表类型和`changelog-producer` 配置。

### 1 .1 主键表（Primary Key Table）


> [!note] 批注：四种 Producer 的判断顺序
> 先判断下游是否只需要按主键覆盖；能接受 Upsert 时优先 `none`。下游需要完整 `-U/+U/-D` 时，再检查上游是否已有旧值：有则评估 `input`，没有则评估 `lookup`；能接受周期级延迟且希望通过两次全量 Compaction 求差异时才选 `full-compaction`。
| 模式 | 生成时机 |
| --- | --- |
|`none` （默认） | 不生成 changelog，只维护最新状态 |
|`input` | 写入时直接将上游 Flink 的 CDC 流（+I/-U/+U/-D）作为 changelog 存储 |
|`lookup` | Compaction 时通过 lookup 查找旧值，生成 before/after changelog |
|`full-compaction` | Full Compaction 时对比前后快照差异生成 changelog |

### 1 .2 非主键表（Append-only Table）

非主键表即 append-only table，**不支持`changelog-producer` 配置**，默认不生成 changelog。

---

## 2 . 两种表类型的区别

| 特性 | 主键表 | 非主键表（append-only） |
| --- | --- | --- |
| 主键定义 | 必须有 | 没有 |
| 支持操作 | INSERT/UPDATE/DELETE | 仅 INSERT |
| 数据合并 | 会合并（LSM） | 不合并，保留所有行 |
| Changelog | 可配置生成 | 不支持 |
| 典型场景 | CDC 同步、维度表 | 日志表、事件流 |

---

## 3 . lookup/full-compaction 适合的场景

`lookup`/`full-compaction`**不是**适合 append-only table（非主键表），而是适合 **append-only 数据流写入主键表** 的场景。

### 3 .1 核心原因

上游是 append-only 数据流（只有 INSERT，无 CDC 语义）写入主键表时：

-`input` 模式要求上游提供`-U/+U/-D` 等 CDC 操作，**不适合**
-`lookup`/`full-compaction` 在 Compaction 时自动推断变更，**适合**

### 3 .2 lookup 模式工作流程

> [!warning] 补充批注：不要把 lookup compaction 一概理解为后台异步任务
> 它是否阻塞提交，取决于写入与 Compaction 的部署方式及 `write-only` 等配置。判断延迟来源时，要分别观察 Writer、Compaction 和 Committer，而不能仅凭“使用 lookup”推断 Changelog 一定异步产生。


```
上游发送：
  +I[1, Alice, 100]
  +I[1, Alice, 150]   ← 同一主键，但只标记为 INSERT

Compaction 时：
  1. 读取新记录：[1, Alice, 150]
  2. Lookup 旧值：[1, Alice, 100]
  3. 生成 changelog：
     -U[1, Alice, 100]   ← before
     +U[1, Alice, 150]   ← after

```


---

## 4 . Changelog 暴露给下游的延迟


> [!warning] 勘误：延迟不只由 Checkpoint 和发现间隔决定
> `lookup` 的可见性还受 Lookup Compaction 是否追上、写入是否等待 Compaction、Commit 是否成功影响；`full-compaction` 则必须等到一次 Full Compaction 提交。持续积压时，缩短 `continuous.discovery-interval` 只能更快发现已经提交的 Snapshot，不能补足 Compaction 吞吐。
端到端延迟由**两个独立机制**共同决定：

### 4 .1`input` 模式
```
总延迟 ≈ checkpoint interval（上游提交） + continuous.discovery-interval（下游扫描）

```


- changelog 在 checkpoint 时随数据一起提交
- 下游 Source 通过`continuous.discovery-interval`（默认 10s）独立扫描新 snapshot

### 4 .2`lookup`/`full-compaction` 模式
```
总延迟 ≈ checkpoint interval + Compaction 延迟 + continuous.discovery-interval

```


- changelog 在 Compaction 阶段异步生成
- checkpoint 是数据提交的前提，是间接依赖

### 4 .3 关键参数


```
-- 上游：控制提交频率
'execution.checkpointing.interval' = '30s'

-- 下游：控制发现新 snapshot 的间隔
'continuous.discovery-interval' = '10s'

-- full-compaction 模式：控制触发间隔
'changelog-producer.compaction-interval' = '30min'

```


---

## 5 . Compaction 触发机制

> [!note] 补充批注：Checkpoint 与 Snapshot 不保证一一对应
> Checkpoint 决定流任务状态和提交时机，但一次提交是否生成 Snapshot、是否附带 Compaction 结果，还受是否有可提交变更及 Compaction 调度影响。排查消费延迟时，应直接查看 `$snapshots` 中的提交类型与时间。

Compaction **不只在 Checkpoint 时执行**，有多种触发条件：

1. Checkpoint 时检查是否需要 compact
2. 后台线程定期检查（异步 compaction）
3. 文件数量达到阈值（`num-sorted-run.compaction-trigger`）
4. 写入数据量达到阈值

### 5 .1 关键参数


```
'write-only' = 'false'                    -- false：Writer 触发；true：独立 Compaction Job
'num-sorted-run.compaction-trigger' = '5' -- L0 文件数达到阈值触发
'compaction.min.file-num' = '5'
'compaction.max.file-num' = '50'

```


---

## 6 . 离线能查到但流读消费不到的问题


> [!note] 批注：先检查“读取契约”再调低 Compaction 阈值
> Batch 查询读取选定 Snapshot 的完整逻辑状态；Streaming 查询读取增量提交或 Changelog。两者能看到不同内容并不直接证明数据丢失。应依次检查 `$snapshots`、`changelog-producer`、消费者位点、Compaction 积压和 Snapshot 保留窗口，再决定是否调整阈值。
### 6 .1 问题场景

在`lookup`/`full-compaction` 模式下，会出现以下情况：


```
T1: 写入数据 → Checkpoint 提交 → 形成 Snapshot A（含 L0 文件）
T2: 离线查询 → 读取 Snapshot A → ✅ 能查到数据
T3: 流读任务 → 扫描 Changelog → ❌ 无 Changelog（Compaction 未执行）
T4: Compaction 执行 → 生成 Changelog
T5: 流读任务 → ✅ 现在能消费到

```


### 6 .2 根本原因

| 读取模式 | 读取内容 | 是否依赖 Compaction |
| --- | --- | --- |
| 离线查询（Batch Read） | 直接读取所有数据文件（L0 + LSM） | 不依赖 |
| 流读（Streaming Read） | 读取 changelog 文件 | **依赖** |

### 6 .3 解决方案

**方案1：使用`input` 模式（推荐，上游为 CDC 流时）**


```
CREATE TABLE t (
    id BIGINT,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'changelog-producer' = 'input'
);

```


**方案2：调整 Compaction 触发参数**


```
'num-sorted-run.compaction-trigger' = '2'
'changelog-producer.compaction-interval' = '5min'

```


**方案3：独立 Compaction Job**


```
flink run \
  -c org.apache.paimon.flink.action.CompactAction \
  paimon-flink-action.jar \
  compact \
  --warehouse hdfs:///path/to/warehouse \
  --database mydb \
  --table mytable

```


---

## 7 . 核心结论

1. **Changelog 生成时机**：`input` 模式在写入时生成；`lookup`/`full-compaction` 在 Compaction 时异步生成
2. **Append-only table（非主键表）不支持 changelog-producer**；`lookup`/`full-compaction` 适合的是 append-only **数据流**写入主键表的场景
3. **Compaction 不只在 Checkpoint 时执行**，还受文件数量等条件触发
4. **下游暴露延迟**由 checkpoint interval 和`continuous.discovery-interval` 共同决定
5. **离线能查到但流读消费不到**是`lookup`/`full-compaction` 模式的典型问题，根本原因是离线读不依赖 Compaction，流读依赖 Changelog 文件

## 复核资料

- [Changelog Producer](https://paimon.apache.org/docs/master/primary-key-table/changelog-producer/)
- [Primary-Key Compaction](https://paimon.apache.org/docs/master/primary-key-table/compaction/)
- [Concurrency Control](https://paimon.apache.org/docs/master/concepts/concurrency-control/)

## 系列导航

- 上一篇：[[Paimon 精讲（一）：架构、写入、读取与核心元数据（文章批注版）]]
- 下一篇：[[Paimon 精讲（三）：Partition、Snapshot 与数据生命周期（文章批注版）]]
