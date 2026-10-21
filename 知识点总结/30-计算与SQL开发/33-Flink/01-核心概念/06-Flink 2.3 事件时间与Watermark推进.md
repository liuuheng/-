---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 6
source_sections: "主教材第6.1、6.2节；优化第7.10节"
---

# Flink 2.3 事件时间与 Watermark 推进

事件时间回答“业务事件何时发生”，处理时间回答“当前机器何时处理”。回放历史数据仍希望按原业务时间汇总时，需要事件时间；只关心机器处理后十秒触发时，可使用处理时间。

## Watermark 是进度声明

watermark `W` 声明系统认为时间戳不大于 W 的数据基本已到达。它是由数据源和策略生成的判断，并不是“所有早期记录都已到齐”的事实保证。因此，watermark 推进后仍可能有迟到事件。

有界乱序策略会维护已见最大事件时间 `maxTs`，周期性发出近似 `maxTs - 乱序容忍 - 1ms` 的水位线。它不会对每条记录排序，也不会随着墙上时钟无条件前进。

```java
WatermarkStrategy<Event> wm = WatermarkStrategy
    .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
    .withTimestampAssigner((event, oldTimestamp) -> event.ts)
    .withIdleness(Duration.ofMinutes(1));
DataStream<Event> timed = input.assignTimestampsAndWatermarks(wm);
```

这里的 `event.ts` 必须是毫秒。若误传秒，十秒窗口在业务时间尺度上就完全失真；若混入遥远未来时间戳，水位线可能被推进过远，后续正常事件大量迟到。应在赋时间戳前做范围校验。

## 多输入为什么会等最慢的一侧

下游对活跃输入的 watermark 取最小值。快分区到了 12:10，慢分区还在 12:00，下游不能仅凭快分区就宣布 12:05 的窗口已完成。双流 Join 也受两侧进度共同约束。

空闲输入检测可把长期无数据的输入排除在最小值计算之外，但这改变了进度判断条件。空闲分区恢复并发送较早事件时，它们可能已经迟到；不能只开启 idleness 却不设计迟到策略。所有输入都空闲时，通常也不会凭空产生更大的事件时间。

## Alignment 解决另一种问题

Watermark alignment 通过限制快源领先慢源过多，减少快侧数据在 Join 等算子中的积压。它与 idleness 不同：前者约束过快输入，后者识别无数据输入。支持情况受 Source 实现影响，应尽量在 Source 处配置策略，以保留分片级时间信息。

## 如何定位窗口不出结果

先看输入是否有有效时间戳，再检查 Source 与目标算子的 `currentInputWatermark`。若个别输入长期停滞，查对应分区、空闲检测及反压；若 watermark 已过窗口边界却无输出，再查 key、窗口分配和触发器。

验证时使用时间戳 1000、4000、3000、13000 的输入，记录每次水位线及窗口结果。有限集合结束时发出的终止 watermark 会关掉剩余窗口；持续源没有这个结束信号，不能用“集合实验能出结果”推断线上会自然关窗。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：主教材第6.1、6.2节；优化第7.10节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [generating watermarks](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/event-time/generating_watermarks/)
- [built in](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/event-time/built_in/)

前篇：[[05-Flink 2.3 转换、分区与数据质量]]

后篇：[[06-Flink 2.3 窗口分配与增量聚合]]
