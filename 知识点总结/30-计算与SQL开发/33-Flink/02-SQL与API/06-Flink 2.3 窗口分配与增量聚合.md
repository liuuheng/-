---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 7
source_sections: "主教材第6.3.1—6.3.5节"
---

# Flink 2.3 窗口分配与增量聚合

窗口分配器确定事件属于哪些统计区间，窗口函数确定如何计算结果，触发器确定何时输出。把这三层分清，才能判断状态开销和结果更新方式。

## 选择窗口

滚动窗口互不重叠；滑动窗口会让一条事件属于多个窗口；会话窗口按相邻活动的间隔合并。计数窗口按到达条数触发，不能代替“每天结算”；全局窗口需要自行提供触发与清理逻辑。

时间窗口采用左闭右开区间。例如 `[0,10000)` 接收 9999 毫秒的事件，10000 属于下一个窗口。窗口事件时间触发点是该窗口的最大时间戳 9999，不能把边界事件错放回前窗。

## 核心实现：增量计数加窗口信息

```java
public static class CountAgg implements AggregateFunction<Event,Long,Long> {
    public Long createAccumulator() { return 0L; }
    public Long add(Event event, Long acc) { return acc+1; }
    public Long getResult(Long acc) { return acc; }
    public Long merge(Long a, Long b) { return a+b; }
}
public static class AddWindow extends ProcessWindowFunction<Long,WindowCount,String,TimeWindow> {
    public void process(String key, Context ctx, Iterable<Long> values,
                        Collector<WindowCount> out) {
        out.collect(new WindowCount(key,ctx.window().getStart(),
            ctx.window().getEnd(),values.iterator().next()));
    }
}
static SingleOutputStreamOperator<WindowCount> windows(DataStream<Event> input) {
    return input.keyBy(e->e.url)
        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
        .aggregate(new CountAgg(), new AddWindow()).uid("url-window-v1");
}
```

`CountAgg` 每到一条就更新一个计数；`AddWindow` 只在触发时将计数和窗口边界组装为输出。这里 `Iterable<Long>` 通常是预聚合的一个结果，不是窗口内全部原始事件。

相比仅使用 `ProcessWindowFunction<Event,...>` 缓存所有记录，增量聚合能显著减小状态。它仍会为每个活跃 `(key,window)` 维护状态，不能称为完全无状态。

## 累加器要反映数学性质

求平均数的累加器应保存 `(sum,count)`，合并后再做 `sum/count`。平均数的平均通常错误：两个局部组大小不同就不能直接平均各组均值。会话窗口和两阶段聚合尤其需要正确的 `merge`；返回 `null` 不是可复用的实现。

精确 UV 必须记住不同用户。即使写成增量聚合，使用 Set 的状态量仍随不同用户数增长；“增量”只表示逐条更新，不保证常量内存。

## Trigger 和 Evictor 的边界

自定义 Trigger 会替换默认触发器，需要自行处理事件时间、提前输出及清理。`FIRE` 输出后可以继续保留内容，`PURGE` 清除内容；二者并不等同于关闭整个业务窗口生命周期。加入 Evictor 可能要求保留原始元素，使原本的预聚合优势消失。

`windowAll` 将整体窗口集中到一个任务，适合很小的最终汇总，不能用增加上游并行度掩盖它的单点限制。

## 验证结果和内存代价

同一窗口 `/a` 三次、`/b` 一次，应得到计数 3 和 1；再加入下一窗口事件，验证边界没有串窗。比较增量聚合和全量窗口的状态大小时，保持输入条数、key 基数、窗口长度一致。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：主教材第6.3.1—6.3.5节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [windows](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/windows/)

前篇：[[06-Flink 2.3 事件时间与Watermark推进]]

后篇：[[07-Flink 2.3 迟到处理与窗口结果修正]]
