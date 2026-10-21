---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 8
source_sections: "主教材第6.3.6、6.3.7节"
---

# Flink 2.3 迟到处理与窗口结果修正

事件时间窗口首次输出不一定就是最终结果。是否允许迟到修正、何时清理状态、超期数据去哪儿，必须和下游写入规则一起设计。

## 三种措施解决不同阶段的问题

调大乱序容忍会推迟 watermark，窗口首次输出更晚；`allowedLateness` 在首次触发后继续保留窗口，允许修正；侧输出接收已超出可接纳范围的数据，供补偿或离线对账。三者不是同一个延迟参数。

```java
OutputTag<Event> tooLate = new OutputTag<Event>("too-late") {};
SingleOutputStreamOperator<WindowCount> counts = timed.keyBy(e -> e.url)
    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
    .allowedLateness(Duration.ofSeconds(5))
    .sideOutputLateData(tooLate)
    .aggregate(new CountAgg(), new AddWindow());
counts.getSideOutput(tooLate).print("reconcile-late");
```

这里的 `CountAgg`、`AddWindow`、`Event` 都在实验工程中有完整实现。允许迟到仅适用于事件时间窗口，不会让处理时间窗口重新接纳过去的业务时间。

## 用毫秒边界推演生命周期

对窗口 `[0,10000)`：默认事件时间触发点为 9999。watermark 到 9999 时首次输出；允许迟到 5000 毫秒时，清理时间为 `9999+5000=14999`。

在 watermark=10000 后，收到时间戳 9000 的事件，窗口仍存在，默认触发器会再输出修正后的聚合。watermark 到清理时间后再来同一时间范围的事件，它进入迟到侧输出。如果一个滑动窗口事件还可以被另一个尚未清理的所属窗口接纳，不能仅凭某一个窗已过期就断言它必然进入侧输出。

## 下游最容易发生重复累计

首次输出 `count=3`，迟到后输出 `count=4`，第二条通常是新的完整累计值。把两条都执行“旧值加本条”会得到 7。写入主键为 `(url,windowStart,windowEnd)` 的结果表时，应覆盖旧结果或使用明确的版本/撤回协议。

中间流没有自动替你附加数据库事务、撤回消息或版本号。若需要防乱序覆盖，应增加确定的结果版本并在下游比较版本，而不能只按机器接收时间覆盖。

## 级联计算要传递修正语义

窗口结果接到 Top-N 时，上游允许迟到意味着排名也可能更新。只在首次窗口结束后排一次、清掉全部排名状态，随后直接忽略修正，会让最终排名错误。可以选择禁用迟到并进入补偿链路，或保留排名状态到上游最终清理点并支持更新输出。

## 一个可复现的验证序列

依次发送：属于前窗的三条事件、推进至首次触发的 watermark、前窗迟到事件、推进至清理点的 watermark、另一条前窗事件。应观察“首次计数、修正计数、侧输出”三个阶段。测试需显式控制 watermark；仅按终端输入速度等待几秒无法稳定验证这些边界。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：主教材第6.3.6、6.3.7节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [windows](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/windows/)
- [side output](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/side_output/)

前篇：[[06-Flink 2.3 窗口分配与增量聚合]]

后篇：[[08-Flink 2.3 ProcessFunction与可恢复定时器]]
