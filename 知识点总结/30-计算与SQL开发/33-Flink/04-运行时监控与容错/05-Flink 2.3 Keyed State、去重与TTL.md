---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 11
source_sections: "主教材第8.1、8.2节；优化第6.1节"
---

# Flink 2.3 Keyed State、去重与 TTL

Keyed state 的句柄在一个算子实例中初始化，但每次访问自动对应当前记录的 key。它不是“一个成员变量保存所有用户的同一个值”，也不是多个子任务共享的远程数据库。

## 按访问模式选状态

| 状态 | 适合保存 | 需要注意 |
|---|---|---|
| `ValueState<T>` | 一个用户的当前累计或标志 | 无值时通常返回 null |
| `ListState<T>` | 多个待处理对象 | 无限制追加会持续膨胀 |
| `MapState<K,V>` | 当前外层 key 下的内部索引 | 内外两层 key 含义不同 |
| `ReducingState<T>` | 同型归约结果 | 归约逻辑须适合增量累积 |
| `AggregatingState<IN,OUT>` | 不同输入与输出的聚合 | 累加器序列化同样重要 |

状态描述器名称和序列化类型是恢复协议的一部分，不应为了代码美观随意重命名。拿到对象后修改字段也不应替代显式 `update/put`，否则不同后端的对象引用行为可能让结果不一致。

## 核心实现：按事件标识去重

```java
public static class Dedup extends KeyedProcessFunction<String,Event,Event> {
    private transient ValueState<Boolean> seen;
    @Override public void open(OpenContext ctx) {
        StateTtlConfig ttl=StateTtlConfig.newBuilder(Duration.ofHours(24))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build();
        ValueStateDescriptor<Boolean> d=new ValueStateDescriptor<>("seen-v1",Types.BOOLEAN);
        d.enableTimeToLive(ttl);
        seen=getRuntimeContext().getState(d);
    }
    @Override public void processElement(Event e, Context ctx, Collector<Event> out)
            throws Exception {
        if (seen.value()==null) { seen.update(true); out.collect(e); }
    }
}
```

接入方式为 `input.keyBy(e -> e.eventId).process(new Dedup())`。这里外层 key 就是 eventId，每个事件只需一个布尔标志；如果按 userId 分组，则应使用 `MapState<eventId,...>`，不能只给用户保存一个 seen 标志。

## TTL 规定“记住多久”

该示例基于处理时间，首次写入后保留 24 小时。`OnCreateAndWrite` 在创建与写入时更新过期时间；重复记录只读不写，因此不会不断续期。若选择读取也续期，热门重复事件可能长期存在。

`NeverReturnExpired` 让逻辑读取看不到过期值；物理字节的清理仍可能等待访问、后台增量清理或 RocksDB compaction。观察到状态文件尚未立刻变小，不足以证明 TTL 没有生效。

TTL=24h 的业务含义是“在这个处理时间范围内去重”。一条业务上两天前的事件，今天刚第一次到达，会从今天开始计时；若要求按事件时间到某个业务日结束，需要自己记录业务过期时间并用事件时间 timer 清理。

## TTL 与恢复兼容性按当前版本判断

官方当前文档说明：RocksDB 和 Heap 已支持已有状态启用/禁用 TTL 的迁移，分别自 2.1 和 2.2 起提供相关支持；ForSt 不在同一支持范围。不能把旧版“切换 TTL 必然恢复失败”的结论原样保留。

这也不表示任意状态类型、TTL 参数和跨版本组合都能无条件迁移。恢复时需核对 serializer；从未启用 TTL 的旧状态恢复后，过期逻辑不是对全部历史条目做一次追溯批量删除。

## 验证

实验工程 `dedup` 模式输入两条相同 eventId 和一条新 eventId，预期只留下两个标识。TTL 到期与故障恢复需分别用可控处理时间和真实快照验证；本次有限输入断言不声称验证了等待 24 小时后的行为。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：主教材第8.1、8.2节；优化第6.1节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [state](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/state/)
- [state migration](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/state_migration/)

前篇：[[09-Flink 2.3 双流Join的时间与状态边界]]

后篇：[[06-Flink 2.3 算子状态与广播规则一致性]]
