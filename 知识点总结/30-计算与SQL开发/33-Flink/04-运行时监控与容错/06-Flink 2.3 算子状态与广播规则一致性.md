---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 12
source_sections: "主教材第8.3节"
---

# Flink 2.3 算子状态与广播规则一致性

Operator state 归属于子任务实例，适合保存一组分配给该实例的工作项；broadcast state 在每个并行实例中保存一份规则副本。二者都由 Flink 快照管理，但扩缩容语义不同。

## List 与 Union 的重分配

普通算子 `ListState` 恢复或扩缩容时，把旧列表元素重新分配给新实例；每个元素应是可独立恢复的最小单元。`UnionListState` 则让每个新实例得到所有旧分片内容的并集，需要业务自己选择当前负责部分。

因此，Union 不是减少状态的优化：它会增加每实例拿到的信息，元数据也可能增大。不要把一整张超大映射当作一个 list 元素，期待扩容时 Flink 自动把映射内部拆开。

## 广播规则的核心写法

```java
MapStateDescriptor<String, Long> ruleDesc =
    new MapStateDescriptor<>("rule-version-v1", Types.STRING, Types.LONG);
BroadcastStream<Tuple2<String, Long>> broadcastRules = rules.broadcast(ruleDesc);
events.keyBy(e -> e.userId).connect(broadcastRules)
    .process(new KeyedBroadcastProcessFunction<String, Event,
             Tuple2<String, Long>, String>() {
        public void processBroadcastElement(Tuple2<String, Long> r, Context ctx,
                                             Collector<String> out) throws Exception {
            Long old = ctx.getBroadcastState(ruleDesc).get(r.f0);
            if (old == null || r.f1 > old)
                ctx.getBroadcastState(ruleDesc).put(r.f0, r.f1);
        }
        public void processElement(Event e, ReadOnlyContext ctx,
                                   Collector<String> out) throws Exception {
            Long version = ctx.getBroadcastState(ruleDesc).get(e.url);
            if (version != null) out.collect(e.eventId + ":rule=" + version);
        }
    });
```

事件侧只能读取广播状态，规则侧更新。示例用“每个规则键只保留最大版本”实现确定性更新，避免规则消息乱序造成不同实例最终保存不同版本。

## 一致副本不等于同步生效

广播记录在不同实例的到达顺序可能不同，事件与规则也是两路独立输入。即使最终规则副本一致，同一条事件在某一时刻看到哪个版本仍可能不同。需要严格业务时点时，应携带规则版本、按生效时间保存历史规则，或设计可回放的版本关联。

广播数据会复制到每个下游并行实例。百万级维表不应仅因为“查询方便”就整表广播；需计算并行度倍增后的内存、快照和更新成本。

## 自定义快照的完整约束

使用 `CheckpointedFunction` 时，`initializeState` 获取状态并恢复内存结构，`snapshotState` 写入当前可恢复内容。若自己在普通 Map 中攒批，快照不能漏掉尚未发往下游的数据；否则从 Source 已推进的 offset 恢复时会永久丢失这部分输入的贡献。

验证广播规则至少包含版本乱序、重复更新、规则删除和重启恢复。删除也需要版本或 tombstone 协议，否则旧的迟到更新可能把已删除规则复活。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：主教材第8.3节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [broadcast state](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/broadcast_state/)
- [state](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/state/)

前篇：[[05-Flink 2.3 Keyed State、去重与TTL]]

后篇：[[07-Flink 2.3 状态后端与异步状态访问]]
