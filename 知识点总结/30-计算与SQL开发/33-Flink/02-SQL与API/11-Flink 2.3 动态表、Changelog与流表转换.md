---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 18
source_sections: "主教材第10.1—10.4节"
---

# Flink 2.3 动态表、Changelog 与流表转换

Flink SQL 将输入看作持续变化的表，并持续维护查询结果。运行时通常通过增量更新维护结果，不是每来一条都重新扫描全部历史。

## 查询形状决定输出协议

过滤通常产生 insert-only 流；按用户累计求和可能先插入一个用户，再更新其累计值；外连接还可能撤回此前的空匹配行。因此结果表不一定能直接写进只接收追加记录的 Sink。

| RowKind | 意义 |
|---|---|
| `INSERT` | 新行进入动态表 |
| `UPDATE_BEFORE` | 更新前的旧行 |
| `UPDATE_AFTER` | 更新后的新行 |
| `DELETE` | 旧行退出动态表 |

不要求每种执行计划都输出完整的四类消息；planner 会结合主键和下游能力选择编码。不能靠打印时恰好看到 `+I` 就推断查询永远只追加。

## 流表转换的核心调用

```java
StreamTableEnvironment t = StreamTableEnvironment.create(env);
t.createTemporaryView("events", eventStream);
Table totals = t.sqlQuery("SELECT userId, SUM(amount) total "
    + "FROM events GROUP BY userId");
DataStream<Row> changes = t.toChangelogStream(totals);
changes.map(r -> r.getKind() + ":" + r).print();
env.execute("dynamic-totals");
```

`toDataStream` 适合能证明仅追加的结果；更新表使用支持 changelog 的转换和 Sink。把 `UPDATE_AFTER` 直接再当一笔新金额求和，会重复累计。

## Upsert 的逻辑主键

Upsert 以主键标识当前行，更新覆盖旧值，删除移除旧值。`PRIMARY KEY ... NOT ENFORCED` 表达给 planner 的约束不由 Flink 自动逐条校验，输入需要真实满足该约束。

普通 Kafka 表、upsert-kafka 表和 CDC 格式承担不同协议。value 为 null 的 tombstone 与 JSON 中某个字段为 null 不同；消息传递语义和数据库表的最终状态也不是同一个维度。

## Flink 2.3 的显式变更转换

```sql
SELECT * FROM TO_CHANGELOG(input => TABLE user_totals);

SELECT * FROM FROM_CHANGELOG(
  input => TABLE raw_changes,
  op => DESCRIPTOR(operation)
);
```

`TO_CHANGELOG` 把原变更类型放进操作码列，使输出成为显式变更事件的追加表；`FROM_CHANGELOG` 反向解释操作码。追加的是“变更事件”，不表示业务结果从此不再更新。后续消费者仍要解释这些操作码。

当前版本的 FROM_CHANGELOG 对更新要求完整 before/after 语义。只有一个 `u` 码且缺旧值的 CDC 数据，不能未经建模就冒充完整撤回流。未知或空操作码也需要按官方约束处理，不能静默当 INSERT。

## 验证一条更新

让 u1 先产生 10，再产生 30，总额应从 10 更新为 40。若采用全变更编码，观察旧值撤回和新值；若采用主键 upsert，则观察覆盖。最后按协议还原物化表后对账 40，而不是简单累计打印出来的所有数值。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：主教材第10.1—10.4节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [dynamic tables](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/sql-table-concepts/dynamic_tables/)
- [data stream api](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/data_stream_api/)
- [changelog](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/changelog/)

前篇：[[10-Flink 2.3 两阶段Top-N与排名修正]]

后篇：[[12-Flink 2.3 SQL建表、时间属性与时区]]
