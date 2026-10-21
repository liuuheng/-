---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 10
source_sections: "主教材第5.3.6、6.4节"
---

# Flink 2.3 双流 Join 的时间与状态边界

双流 Join 先问匹配范围，再选 API。仅按 key 匹配且可能无限期等待，会长期保留历史；按业务时间限定区间，才能在时间进度足够后安全清理。

## Window Join 和 Interval Join

Window Join 要求两条记录落入相同窗口且 key 相等。相差两秒的两条记录若分别处在滚动窗口两侧，也可能无法匹配。

Interval Join 以左记录的事件时间为中心判断右记录。设下界为 -5 分钟、上界为 1 分钟，则 `left.ts-5min <= right.ts <= left.ts+1min`；默认边界包含端点，需要时可设置排除。

```java
left.keyBy(e -> e.userId)
    .intervalJoin(right.keyBy(e -> e.userId))
    .between(Duration.ofMinutes(-5), Duration.ofMinutes(1))
    .process(new ProcessJoinFunction<Event, Event, String>() {
        @Override
        public void processElement(Event a, Event b, Context ctx,
                                   Collector<String> out) {
            out.collect(a.eventId + ":" + b.eventId);
        }
    });
```

两侧都要正确分配时间戳及 watermark。DataStream Interval Join 是事件时间的内连接；未匹配的左记录不会自动产生带 null 的右字段。若需要“支付超时仍未匹配”，应使用 CoProcess 的状态与定时器，或选合适的 SQL 外连接语义。

## 匹配不保证一对一

同一 key 的左侧有 3 条、右侧有 4 条且时间条件都满足，就可能输出 12 个匹配对。增加并行度不会消除这种业务上的乘法增长。订单与支付若应一对一，需要在入流时约束业务标识或处理重复消息。

## 状态何时能被删除

算子保留尚可能匹配未来输入的记录，随 watermark 推进清理。任一侧长时间不推进，下游时间进度停住，另一侧记录可能持续积压。先查 watermark 与匹配范围，再考虑增加内存。

用 TTL 删除历史虽然能压住体积，但如果业务还允许晚到的匹配，它就改变了结果。因此“加 TTL 后不再 OOM”不能直接当作正确修复。

## connect 需要自行实现协议

`connect` 只把两路输入交给同一个双输入函数。自己实现订单支付匹配时，至少需要左右状态、超时 timer、成功匹配后的清理、超时后晚到的处理，以及业务重试去重。用一个普通 HashMap 保存等待记录无法自动获得故障恢复。

## 验证用边界而不是随机数据

固定左记录时间 10:00，分别放入 09:54:59、09:55、10:01、10:01:01 的右记录。默认闭区间应只接纳中间两条。再让左右两侧交换到达顺序，最终匹配对集合应相同；若结果依赖到达顺序，检查迟到、清理和非确定逻辑。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：主教材第5.3.6、6.4节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [joining](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/joining/)
- [process function](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/process_function/)

前篇：[[08-Flink 2.3 ProcessFunction与可恢复定时器]]

后篇：[[05-Flink 2.3 Keyed State、去重与TTL]]
