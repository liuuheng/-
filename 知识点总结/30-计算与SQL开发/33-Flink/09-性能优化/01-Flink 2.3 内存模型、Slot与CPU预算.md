---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 25
source_sections: "优化第1.1、1.2、7.1—7.6节"
---

# Flink 2.3 内存模型、Slot 与 CPU 预算

TaskManager 的进程内存不等于 Java 堆。调内存先确认哪个区域受限，否则增加总量后仍可能把预算分到无关区域。

## 总进程内存的组成

总进程包括 Flink 内存、JVM Metaspace 和 JVM Overhead；Flink 内存再分框架堆/堆外、Task 堆/堆外、网络内存、Managed Memory。网络缓冲与 RocksDB 受管缓存通常不计入 Java 业务对象堆。

```yaml
taskmanager.memory.process.size: 4g
taskmanager.memory.managed.fraction: 0.4
taskmanager.memory.network.fraction: 0.1
taskmanager.memory.network.min: 64mb
taskmanager.memory.network.max: 1gb
taskmanager.numberOfTaskSlots: 2
```

这是计算示例，不是每种作业的推荐值。只在明确理解推导约束时同时设置多个总量和分量，避免配置相互冲突。

## 用教材数值理解剩余预算

假设总进程 4096 MiB、Metaspace 256 MiB、Overhead 为进程的 10% 且未触及上下限，则 Flink 内存为 3430.4 MiB。再假设网络占 10%、Managed 占 40%、框架堆和堆外各 128 MiB，剩余 Task Heap 为：

`3430.4 - 343.04 - 1372.16 - 128 - 128 = 1459.2 MiB`。

这些是假设下的预算，不是 JVM 实测使用量。实际值会受显式 size、fraction、上下限和其他内存项影响。Managed 设置过高时，HashMap 状态和大对象需要的 Task Heap 可能不足；减少 Managed 又可能压缩 RocksDB 缓存。

## Slot 数不能代替 CPU 核数

同一 TaskManager 增加 slot，会让更多任务分摊现有内存和 CPU。YARN vcore 的请求、调度器是否综合 CPU 与内存、容器是否实际限核，是三件分别需要检查的事。

采用考虑多资源的调度策略可帮助分配，但不应为了某个作业直接把整个集群 AM 资源比例改到 100%。队列参数属于共享资源治理，需要按集群负载和隔离策略评估。

## 从错误定位区域

`Java heap space` 查对象、堆状态与全量窗口；`Direct buffer memory` 查直接内存和网络/用户 native 分配；`Metaspace` 查类加载和依赖；容器内存超限还要核对 RSS、RocksDB、线程栈与容器限额。每种错误都可能来自泄漏或无界增长，不能统一靠加内存掩盖。

## 验证预算是否有效

对同一负载记录进程 RSS、堆峰值、GC 暂停、Managed/网络实际使用、容器限制、吞吐和 P99 延迟。只改变一类预算再压测；若资源增加但状态仍线性增长，回到状态生命周期设计。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：优化第1.1、1.2、7.1—7.6节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [mem setup tm](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/memory/mem_setup_tm/)
- [mem tuning](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/memory/mem_tuning/)
- [mem trouble](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/memory/mem_trouble/)

前篇：[[11-Flink 2.3 集群部署与Application生命周期]]

后篇：[[02-Flink 2.3 并行度规划与可对账压测]]
