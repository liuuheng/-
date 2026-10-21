---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 30
source_sections: "优化第4章"
---

# Flink 2.3 数据倾斜与两阶段聚合正确性

倾斜要先判断发生在 keyBy 前还是后。无状态阶段的分发不均可以重分区；keyed 状态的热 key 需要改变可合并的计算结构。

## keyBy 前的倾斜

若 Source 分区负载不均，昂贵的无状态 map 可以在适当位置经 `rebalance()` 分发。它把读取后的计算分散开，但不能让一个单分区 Source 同时由多个 Reader 读取，也不能自动保留原分区处理顺序。

`rescale` 只在上下游局部子集间轮询，网络成本可能更低，但不能承诺任意输入倾斜都能完全均衡。

## 热 key 为什么扩容仍在

keyBy 要让同 key 进入同一状态实例。一个 key 占 60% 输入时，从 8 扩到 64 个并行实例，热 key 的实例仍承担这 60%。真正有效的拆分要求聚合满足可合并性质。

## 两阶段输出不能重复相加

第一阶段先输出累计值 1，再输出累计值 2，第二阶段若直接相加，会得到 3，真实事件却只有 2 条。这是把“累计快照”错当成“互不重叠增量”。

```java
// 如果第一阶段发送 shard 的完整累计，第二阶段要先扣旧值再加新值。
Long previous = shardTotals.get(partial.shardId);
long next = (total.value() == null ? 0L : total.value())
          - (previous == null ? 0L : previous)
          + partial.count;
shardTotals.put(partial.shardId, partial.count);
total.update(next);
```

这只是快照替换的核心片段。生产实现还需要分片版本校验、窗口隔离和清理；旧分片结果晚到时，不能覆盖更高版本。若第一阶段每条输入仍发一次完整结果，下游压力也没有减少。

## 窗口内的确定分桶

对可合并计数，可按 `原key + floorMod(hash(eventId),桶数)` 分组开相同窗口，第一阶段输出每桶窗口计数，第二阶段按 `原key+窗口` 合并。确定分桶便于重放和对账；随机分桶若没有固定协议，更难检查恢复一致性。

只有每个分片窗口最终输出一次时，第二阶段简单求和才成立。允许迟到修正时，应按分片替换旧累计、处理撤回，或直接使用 planner 支持的两阶段 SQL 聚合。

## UV 不能把分片 UV 随意相加

同一用户落在两个桶，两个桶的 UV 相加会重复计数。精确 UV 的分桶依据应保证同一个去重对象只进同一桶，或合并集合；近似 UV 要使用可合并 sketch 并明确误差。平均数应合并 sum/count，不能合并各桶平均值。

## 本地攒批也需要容错

在普通 HashMap 里先攒 1000 条再输出，若攒到 999 条时源位置已经随检查点前进，而缓存没有进入一致快照，恢复后这 999 条贡献可能丢失。`close()` 时 flush 不能覆盖进程故障。优先使用托管状态和有完整快照协议的实现。

验证同时比较每子任务输入、忙碌时间、状态大小和最终业务对账。分布更均匀且吞吐更高，但计数不对，仍然是失败的优化。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：优化第4章。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [parallel](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/execution/parallel/)
- [tuning](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/tuning/)
- [state](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/state/)

前篇：[[05-Flink 2.3 反压、火焰图与外部IO瓶颈]]

后篇：[[07-Flink 2.3 对象复用、滑动窗口与延迟测量]]
