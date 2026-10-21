---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 19
source_sections: "主教材第10.2、10.4、10.5节"
---

# Flink 2.3 SQL 建表、时间属性与时区

SQL DDL 不仅定义字段类型，还定义字段来自哪里、什么时间可以用来关窗、查询结果应怎样写回。它是数据契约的一部分。

## 可直接在 SQL Client 运行的输入表

```sql
SET 'table.local-time-zone' = 'Asia/Shanghai';
CREATE TEMPORARY TABLE events (
  event_id BIGINT,
  user_id BIGINT,
  amount BIGINT,
  ts_ms BIGINT,
  et AS TO_TIMESTAMP_LTZ(ts_ms, 3),
  pt AS PROCTIME(),
  WATERMARK FOR et AS et - INTERVAL '2' SECOND
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '10',
  'number-of-rows' = '100',
  'fields.event_id.kind' = 'sequence',
  'fields.event_id.start' = '1',
  'fields.event_id.end' = '100',
  'fields.user_id.min' = '1', 'fields.user_id.max' = '5',
  'fields.amount.min' = '1', 'fields.amount.max' = '100',
  'fields.ts_ms.min' = '1767225600000',
  'fields.ts_ms.max' = '1767225619999'
);
```

这是有限随机输入，适合观察语法和窗口，不能用它期望固定的每用户金额。精确对账使用实验工程的固定 VALUES 输入。随机时间戳会制造乱序；在持续实时业务中应来自实际事件，而不是把 DataGen 时间当作生产时间模型。

## 物理列、计算列、元数据列

`event_id` 是由连接器读取的物理列，`et` 是从物理列计算出的列，`pt` 是处理时间属性。支持元数据的连接器还可读取 partition、offset 等 metadata；不同连接器可用的键和类型不同。

`VIRTUAL` 常用于仅从源读取而不向 Sink 写入的元数据列。不要把 Kafka 消息元数据与 CDC payload 里的数据库日志字段混成同一层。

## TIMESTAMP 与 TIMESTAMP_LTZ

`TIMESTAMP` 表达不带时区的本地日期时间；`TIMESTAMP_LTZ` 表达时间线上的瞬时点，显示和相关运算受会话时区影响。Unix 毫秒转换优先明确使用 `TO_TIMESTAMP_LTZ(ts_ms,3)`。

更改 `table.local-time-zone` 不会修改事件的 epoch 毫秒，但会影响展示及某些日历边界计算。按中国自然日统计时，必须用明确时区验证午夜前后数据，不能只看一天中间的样本。

## 普通时间戳不自动等于时间属性

事件时间列需要 watermark 定义或从输入正确继承；处理时间来自 `PROCTIME()`。把时间列用于任意计算或物化后，可能失去可继续传播的时间属性。后续窗口应检查 `DESCRIBE` 中的 rowtime/proctime 标记。

## 空闲检测与秒毫秒问题

多输入最慢 watermark 会限制 SQL 窗口，可按业务配置 Source 空闲超时，但空闲恢复后仍要处理迟到。13 位毫秒当成秒、字符串时区不明和未来时间戳，是“窗口怎么一直不出/一下出了很多”的常见原因。

运行后先查询原始 `ts_ms、et、pt`，再开窗。先验证数据含义，能避免在错误时间列上反复调参数。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：主教材第10.2、10.4、10.5节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [time attributes](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/sql-table-concepts/time_attributes/)
- [timezone](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/timezone/)
- [datagen](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/datagen/)

前篇：[[11-Flink 2.3 动态表、Changelog与流表转换]]

后篇：[[13-Flink 2.3 SQL聚合、窗口TVF与去重排名]]
