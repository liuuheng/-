---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 29
source_sections: "优化第3章、7.14节"
---

# Flink 2.3 反压、火焰图与外部 I/O 瓶颈

反压表示当前任务因为下游接收能力不足而受限，是流量控制现象，不是直接的根因。数据向下游流，反压可以向上游传。

## 用子任务指标定位边界

`busyTimeMsPerSecond、idleTimeMsPerSecond、backPressuredTimeMsPerSecond` 从任务运行状态划分时间，合计约为每秒 1000 毫秒。busy 高不等于 CPU 高：同步等待外部服务也可能让任务看起来一直忙。

从反压严重的上游向下游检查，找持续接近饱和、输出受限或外部调用慢的节点，再下钻具体 subtask。链内多个函数共享节点时，使用线程采样识别函数，而不是只给整个节点加并行度。

## 区分四类根因

| 证据 | 可能原因 | 后续核验 |
|---|---|---|
| 某个子任务输入远高于其他实例 | 热 key/分区倾斜 | key 分布、每实例状态与流量 |
| 全体 CPU 忙、热点集中在解析或序列化 | 计算成本高 | CPU profile、对象分配 |
| CPU 低但线程等待数据库/HTTP | 同步 I/O 阻塞 | 调用 P99、连接池、超时 |
| GC 暂停与反压同时升高 | 堆压力或大对象 | GC 日志、存活对象和状态增长 |

火焰图的宽度反映采样占比，颜色本身不是错误等级。On-CPU 与等待状态采样回答不同问题；不能只凭“顶部很平”就认定该函数必须重写。

## 异步 I/O 需要真正异步

```java
AsyncDataStream.unorderedWait(
    input, new DimensionLookup(),
    2, TimeUnit.SECONDS, 100
);
```

`DimensionLookup` 是需要实现的异步查询函数：在 `asyncInvoke` 调用异步客户端，并在完成回调中调用 `resultFuture.complete(...)`；失败调用异常完成，超时走明确策略。不能在 asyncInvoke 里同步 get 等待结果。

容量 100 约束同时在途请求，超时 2 秒不是外部系统的容量保证。按 Little 定律，可用“请求速率 × 平均等待时间”估算基础并发，但最终须受下游 QPS、连接池和长尾约束。无限加 capacity 可能压垮维表服务。

有序输出可能让一个慢请求阻塞后续已完成结果；无序输出减少这种等待，但仍要遵守 watermark 等流语义边界。缓存提高命中时需接受一致性和过期策略。

## 延迟指标分层

LatencyMarker 主要反映标记在算子网络中的传递，不能覆盖业务函数、外部事务提交以及最终消费者看见数据的全部耗时。业务端到端延迟应由事件携带生成/采集时间，在结果实际可见处测量；机器时钟须同步。

验证时固定吞吐，比较外部请求数量、P50/P99 延迟、超时率、状态大小、检查点和恢复后的重复请求。async 算子会恢复在途请求，外部写操作仍需幂等。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：优化第3章、7.14节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [back pressure](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/monitoring/back_pressure/)
- [flame graphs](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/debugging/flame_graphs/)
- [asyncio](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/asyncio/)
- [metrics](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/metrics/)

前篇：[[04-Flink 2.3 Checkpoint慢的分阶段诊断]]

后篇：[[06-Flink 2.3 数据倾斜与两阶段聚合正确性]]
