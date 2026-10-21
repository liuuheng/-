---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 3
source_sections: "主教材第3、4章；优化第1.2、1.3节"
---

# Flink 2.3 执行图、算子链与 Slot

API 代码描述逻辑计算，调度器实际部署的是并行子任务。理解中间这层映射，才能解释“写了五个算子却只看到两个节点”，以及“有十个 slot 却没有十核 CPU”。

## 从转换到运行任务

DataStream 转换组成 StreamGraph，再生成 JobGraph；运行时据并行度建立执行实例，并分配到 TaskManager。每个算子的并行度是实例数量，不是数据 key 的数量。百万个用户 key 可以由几十个子任务管理。

`keyBy` 决定路由，不单独启动一个计算函数；它会形成网络分区边界。满足条件的相邻转换可以组成算子链，在同一任务线程中直接传递记录，减少序列化、网络和调度开销。

```java
DataStream<Event> normalized = source
    .map(new NormalizeEvent()).name("normalize").uid("normalize-v1");
normalized.keyBy(e -> e.userId)
    .process(new UserTotals()).name("user-totals").uid("user-totals-v1")
    .setParallelism(4);
```

`NormalizeEvent` 和 `UserTotals` 是业务函数占位名称。这里关键的是 keyed 边界和算子 UID，不是把 `keyBy` 算成额外一组 slot。

## 算子链的收益和代价

链内函数通常在同一线程顺序执行。上游 map 很快、链尾同步写数据库很慢，整条链的任务吞吐都会下降。在 Web UI 看到链节点反压时，还要通过线程采样识别具体耗时函数。

需要独立调并行度、隔离资源或定位瓶颈时，可以在选定转换上使用 `startNewChain()` 或 `disableChaining()`。拆链增加缓冲和序列化，不能把它当成固定性能优化。

## Slot 是调度资源单位

一个 TaskManager 可以有多个 slot。默认 slot sharing 允许同一作业的不同算子子任务共享 slot，但同一算子的并行实例仍需要分别调度。单个共享组中，流水线常按该组最大并行度估算所需 slot；多个共享组、批调度和精细资源配置会改变估算条件。

slot 通常不提供独立 CPU 隔离。同一 JVM 内多个任务线程会竞争 CPU、堆、磁盘和网络。把 slot 从 2 改成 8 可能只是让更多任务争抢原来的两核 CPU。

## key group 决定扩缩容边界

相同 key 先映射到 key group，key group 再分配给子任务。`maxParallelism` 定义 key group 总数，也是 keyed state 扩缩容的重要边界；实际并行度不必是 2 的幂。不能按简单的 `hash(key) % 当前并行度` 理解恢复后的状态重分配。

上线前稳定设置 UID、key 的计算方式和最大并行度。变更 key 的结构或提取规则，即使 Java 能编译，也可能破坏旧状态的业务含义。

## 观察方法

在同一作业中只改变 map 的并行度或链设置，比较 Web UI 的节点、边和子任务数。再固定 TaskManager CPU，仅增加 slot，观察 CPU 利用率和每子任务吞吐。资源不足的现象应能由实际任务分配解释，而不是只按源代码算子数量猜测。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：主教材第3、4章；优化第1.2、1.3节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [flink-architecture](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/flink-architecture/)
- [parallel](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/execution/parallel/)

前篇：[[10-Flink 2.3 Java项目与最小作业]]

后篇：[[04-Flink 2.3 Source读取、Split与恢复位置]]
