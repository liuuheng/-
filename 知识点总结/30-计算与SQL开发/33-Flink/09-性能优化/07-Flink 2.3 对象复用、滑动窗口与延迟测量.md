---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 31
source_sections: "优化第5.2—5.5节"
---

# Flink 2.3 对象复用、滑动窗口与延迟测量

作业级优化应围绕减少不必要的复制、状态和重复计算，但必须保持对象所有权与聚合语义。

## 对象复用不是任意可变对象通行证

```java
env.getConfig().enableObjectReuse();
```

开启前需要确认每个函数不会长期保留随后可能被复用的输入引用，不会在输出后继续修改同一对象，也不会让多分支共享一个会被修改的对象。即使关闭该选项，业务函数主动把同一可变实例反复输出，也不应假设运行时替你修复全部别名问题。

对希望保存的记录，构造新对象或保存不可变业务字段；使用 Heap 状态时尤其注意对象引用，使用 RocksDB 时则注意每次显式更新。减少 GC 的收益必须与结果对账一起验证。

## 滑动窗口的重复倍数

最近 24 小时每 3 分钟更新，窗口长度/步长为 480。朴素实现可能让一条事件更新数百个窗口状态，并带来多个触发/清理定时器。扩大并行度只能分摊部分开销，不能消除这类重复工作。

可以先按 3 分钟小窗生成时间片摘要，再组合最近 480 个片段。若长度不能整除步长，要推导可对齐的时间片粒度与边界，不能把“任意小窗相加”当通用等价变换。

## 哪些摘要能合并

SUM/COUNT 可以相加；AVG 要合并 sum/count；MAX/MIN 可按片段合并但滚动移除旧片段需要保留足够候选；精确 UV 必须合并用户集合或保证去重对象唯一分桶，不能把各片段 UV 直接求和。

```sql
SELECT window_start, window_end, user_id, SUM(amount)
FROM TABLE(HOP(TABLE events, DESCRIPTOR(et),
               INTERVAL '3' MINUTE, INTERVAL '24' HOUR))
GROUP BY window_start, window_end, user_id;
```

优先评估当前 SQL TVF 的优化计划，减少自己维护大量窗口状态的复杂度。是否有共享切片、实际状态大小及吞吐收益，要以 EXPLAIN 和运行指标确认；不能仅因为用了 HOP 就承诺所有函数都同样高效。

## 稳定 UID 和观测标签

给有状态算子设稳定 UID，方便恢复，也便于长期关联性能指标。UID 应表达稳定算子身份，而不是每次发布变化的随机 UUID；对展示友好的名称另设 name。

## 测量真实可见延迟

```yaml
metrics.latency.interval: 30 s
metrics.latency.granularity: operator
```

这类标记用于观察框架内传播。业务延迟还包括输入积压、watermark 等待、MiniBatch 缓冲和 Sink 事务可见时间。应在结果实际可读的位置计算业务时间差，分清“事件发生到结果可见”和“进入 Flink 到结果可见”。

优化验收记录相同输入的结果、复制/分配量、GC、状态大小、P99 延迟及恢复时间。吞吐提高而迟到数据丢失或窗口 UV 被重复累计，不能接受。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：优化第5.2—5.5节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [execution configuration](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/execution/execution_configuration/)
- [windows](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/windows/)
- [window-tvf](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-tvf/)
- [metrics](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/metrics/)

前篇：[[06-Flink 2.3 数据倾斜与两阶段聚合正确性]]

后篇：[[08-Flink 2.3 SQL MiniBatch、Local-Global与Distinct优化]]
