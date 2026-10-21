---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 9
source_sections: "主教材第7.1—7.3、7.5节"
---

# Flink 2.3 ProcessFunction 与可恢复定时器

窗口适合固定的时间分组；当业务要求“用户最后一次活动后十秒未再活动就提醒”，需要按 key 维护截止时间并管理定时器。状态和定时器共同构成这个业务状态机。

## 选择处理函数

普通 `ProcessFunction` 适合读取时间戳、分流等处理。需要 keyed state 和定时器时，先 `keyBy`，再使用 `KeyedProcessFunction`；两条 keyed 输入使用 `KeyedCoProcessFunction`；广播规则使用 `KeyedBroadcastProcessFunction`。`ProcessWindowFunction` 获取窗口上下文，但不提供任意注册定时器的同一种接口。

## 核心实现：事件时间不活跃检测

```java
public static class Inactivity extends KeyedProcessFunction<String,Event,String> {
    private transient ValueState<Long> deadline;
    @Override public void open(OpenContext ctx) {
        deadline=getRuntimeContext().getState(new ValueStateDescriptor<>("deadline-v1",Types.LONG));
    }
    @Override public void processElement(Event e, Context ctx, Collector<String> out)
            throws Exception {
        Long old=deadline.value();
        if(old!=null) ctx.timerService().deleteEventTimeTimer(old);
        long next=e.ts+10000;
        // Keep the latest event time when records arrive out of order.
        if(old!=null) next=Math.max(old,next);
        deadline.update(next);
        ctx.timerService().registerEventTimeTimer(next);
    }
    @Override public void onTimer(long ts, OnTimerContext ctx, Collector<String> out)
            throws Exception {
        if(Objects.equals(deadline.value(),ts)) {
            out.collect(ctx.getCurrentKey()+":inactive:"+ts);
            deadline.clear();
        }
    }
}
```

接入方法为 `timed.keyBy(e -> e.userId).process(new Inactivity())`。每次事件删除旧定时器，记录较大的新截止时间，再注册新定时器；这样乱序较旧事件不会把截止时间倒退。回调时再核对状态，避免过时回调发出错误提醒。

这里表示“事件时间已经走过最后活动时间十秒”。若所有输入都停止且 watermark 不推进，提醒不会因为墙上时钟过去十秒就自动发生。真实“在线等待十秒超时”的需求应基于处理时间，或设计外部调度与业务时间转换。

## Timer 的去重和恢复

同一 key、同一时间域、同一时间戳的定时器会去重。事件时间回调由 watermark 触发，处理时间回调由系统时间触发。定时器跟随托管状态进入检查点；恢复后已过期的处理时间定时器可能立即触发。

`onTimer(timestamp,...)` 中的 timestamp 是注册的触发时间，不是“当前 watermark 的精确值”。watermark 一次跨过多个时间点时，会触发多个已到期定时器。

## 回调线程不能长期阻塞

记录处理和定时器回调由任务线程协调执行。不需要给普通 keyed state 访问自行加锁，但也不能在回调里同步等待一个很慢的 HTTP 请求。大量定时器同一刻触发会造成延迟尖峰，应减少定时器数量或在业务允许时合并时间精度。

## 清理也是正确性的一部分

输出提醒后清理 deadline，防止长期保留失效用户状态。若业务还保存订单、匹配标志或补偿标记，应分别定义它们的生命周期。外部通知可能在失败重放时重复执行，需以用户、截止时间等稳定业务键实现幂等。

实验工程的 `timers` 模式验证两个用户分别输出对应的截止时间。它验证事件时间定时行为，不等于已验证外部告警系统的投递一致性。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：主教材第7.1—7.3、7.5节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [process function](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/process_function/)

前篇：[[07-Flink 2.3 迟到处理与窗口结果修正]]

后篇：[[09-Flink 2.3 双流Join的时间与状态边界]]
