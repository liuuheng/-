---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 17
source_sections: "主教材第7.4节；优化第4.2.3节"
---

# Flink 2.3 两阶段 Top-N 与排名修正

热门页面 Top-N 分成两步：先按页面和窗口统计访问量，再按窗口收集各页面结果并排序。这样昂贵的原始事件聚合可以并行，最终排名只处理较小的聚合结果。

## 第一阶段输出必须携带窗口标识

使用前文的 `CountAgg + AddWindow`，输出 `url、windowStart、windowEnd、count`。第二阶段按 `windowEnd` 分组，避免不同窗口混在一起。若窗口长度和类型不唯一，单独 windowEnd 不足以辨识窗口，还需将起点或窗口定义加入 key。

## 第二阶段核心实现

```java
public static class TopN extends KeyedProcessFunction<Long,WindowCount,String> {
    private transient MapState<String,Long> counts;
    @Override public void open(OpenContext ctx) {
        counts=getRuntimeContext().getMapState(
            new MapStateDescriptor<>("url-counts-v1",Types.STRING,Types.LONG));
    }
    @Override public void processElement(WindowCount value, Context ctx, Collector<String> out)
            throws Exception {
        counts.put(value.url,value.count);
        ctx.timerService().registerEventTimeTimer(value.windowEnd);
    }
    @Override public void onTimer(long ts, OnTimerContext ctx, Collector<String> out)
            throws Exception {
        List<Map.Entry<String,Long>> rows=new ArrayList<>();
        for(Map.Entry<String,Long> e:counts.entries())
            rows.add(new AbstractMap.SimpleEntry<>(e.getKey(),e.getValue()));
        rows.sort(Comparator.<Map.Entry<String,Long>>comparingLong(Map.Entry::getValue)
            .reversed().thenComparing(Map.Entry::getKey));
        out.collect(ts+":"+rows.subList(0,Math.min(2,rows.size())));
        counts.clear();
    }
}
```

接入方式：`windows(timed).keyBy(w -> w.windowEnd).process(new TopN())`。使用 `MapState<url,count>` 保存最新累计值，可避免同一个页面的更新被当成两条不同候选。相同访问量用 URL 排序，保证输出确定性。

## 定时器为什么设在窗口边界之后

上游十秒事件时间窗口在 `windowEnd-1` 触发，先输出聚合记录，再向后转发相应 watermark。下游在 `windowEnd` 注册事件时间 timer，需等输入时间进度继续前进后才出排名。

这依赖上游和下游遵守 watermark 传播语义，并以本例**不允许迟到修正**为前提。不能概括成“加 1 毫秒就保证未来任何迟到结果都收齐”；watermark 不是绝对无迟到证明。

## 允许迟到时需要完整修正链

若上游保留允许迟到，第二阶段也要保留候选到最终清理时间，接受页面 count 覆盖，并更新排名结果。最终存储需要按 `(window,rank)` 或明确业务主键写入；排名变动时还要删除失去名次的旧行。

只把上游 `allowedLateness` 调大、第二阶段仍在首次排名后清空，得到的是不一致链路。另一种清晰方案是主链只出关闭窗口结果，超期修正进入补偿链路。

## 容量与复杂度

完整排序开销约 `O(U log U)`，U 是窗口内页面数。只需 N 个结果时可用固定大小最小堆减少排序成本，但仍需考虑更新与撤回时如何维护候选集合。最终单窗口排名依然有集中点，可进一步局部 Top-N 再全局合并，前提是排名与更新协议可合并。

实验工程 `topn` 模式验证首窗 `/a=3、/b=1`，次窗 `/b=1`。该实验采用无迟到修正的有限输入，覆盖分窗、排序和定时触发；允许迟到的在线排名需要另做更新与删除验证。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：主教材第7.4节；优化第4.2.3节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [process function](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/process_function/)
- [window-topn](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-topn/)

前篇：[[10-Flink 2.3 Savepoint、UID与状态演进]]

后篇：[[11-Flink 2.3 动态表、Changelog与流表转换]]
