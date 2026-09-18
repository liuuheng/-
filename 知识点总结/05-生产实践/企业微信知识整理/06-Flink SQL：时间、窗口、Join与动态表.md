---
tags:
  - flink
  - flink-sql
  - event-time
status: active
---

# Flink SQL：时间、窗口、Join 与动态表

返回：[[README|企业微信知识地图]]

## 1. 开始写 SQL 前先看输入数据长什么样

流式 SQL 的结果是否正确，首先取决于输入是追加流还是更新流、时间字段来自哪里、主键是否真实唯一，以及 connector 能提供哪些 metadata。只看字段名写 SQL，容易把 CDC 更新当成新增、把 Kafka 时间当成业务时间或让 sink 无法接收回撤消息。

## 2. 事件时间与处理时间

- Event Time 是业务事件自己携带的发生时间，适合按“事情什么时候发生”统计，可处理乱序和重放。
- Processing Time 是记录被算子处理时的机器时间，延迟低，但结果受运行速度、故障恢复和重放时机影响。

事件时间字段通常声明为 `TIMESTAMP(3)` 或 `TIMESTAMP_LTZ(3)` 并定义 Watermark。Epoch 毫秒表示绝对时刻时，通常先转换为 `TIMESTAMP_LTZ`：

```sql
CREATE TABLE orders (
  order_id STRING,
  ts_ms BIGINT,
  event_time AS TO_TIMESTAMP_LTZ(ts_ms, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (...);
```

`TIMESTAMP` 表示没有时区的墙上时间；`TIMESTAMP_LTZ` 内部表示绝对时刻，并按会话时区展示。选错类型会让跨时区数据偏移。

处理时间是计算列：

```sql
proc_time AS PROCTIME()
```

它不需要 Watermark，但作业重跑时会得到不同处理时刻，因此不适合需要历史可重复的事件归属。

## 3. Watermark 到底是什么

Watermark 是算子对“事件时间已经推进到哪里”的估计。`event_time - 5s` 表示系统大致认为时间戳小于当前 watermark 的大部分事件已经到达，并据此触发窗口、清理部分状态。

它不是等待 5 秒的定时器，也不保证旧事件绝不会再来。更晚到达的记录如何处理取决于窗口、允许迟到、side output 或 SQL 能力。Watermark 延迟越小，结果越快，但迟到风险越高；延迟越大，状态和端到端延迟越高。

多 Kafka partition 下游 watermark 常受最慢输入约束。某个 partition 长时间无数据会卡住整体进度，应配置 source idleness。不同分区速度差异大时可评估 watermark alignment，但 connector 是否支持 pushdown 与 Flink 版本有关。

## 4. 物理列、计算列和 Metadata 列

```sql
CREATE TABLE source_table (
  old_col STRING,
  new_col AS old_col,
  total_price AS price * 1.1,
  kafka_partition BIGINT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
  kafka_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (...);
```

- 物理列来自消息 payload 或外部表，需要显式类型。
- 计算列由表达式推断类型，不物理存储。
- Metadata 列来自 connector；`VIRTUAL` 表示插入别的表时不把它当普通可写列。

Kafka metadata 与 Debezium envelope metadata 不是同一层。Kafka partition/offset/timestamp 是消息系统元数据；Debezium 的 `source.table`、`source.ts_ms` 等通常在消息 value 的 envelope 中，需要 format 支持和正确 metadata key。具体名称必须查所用 connector/format 版本。

## 5. Dynamic Table 与 Changelog

Flink SQL 把流解释为持续变化的动态表。普通 `GROUP BY`、去重、Regular Join 可能产生：

- `INSERT/+I`
- `UPDATE_BEFORE/-U`
- `UPDATE_AFTER/+U`
- `DELETE/-D`

例如 LEFT JOIN 中左记录先到、右记录未到，可能先输出 `[L,NULL]`；右记录到达后撤回旧行，再输出 `[L,R]`。因此“SQL 能解析”不代表 sink 能写，sink 必须支持查询产生的 changelog，或通过主键 Upsert 吸收更新。

## 6. 窗口 TVF

窗口表值函数返回原表列并附加：

- `window_start`
- `window_end`
- `window_time`

滚动窗口示例：

```sql
SELECT window_start, window_end, supplier_id, SUM(price) AS total_price
FROM TABLE(
  TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES)
)
GROUP BY window_start, window_end, supplier_id;
```

窗口是左闭右开 `[window_start, window_end)`。`window_start` 和 `window_end` 是普通 timestamp；`window_time` 是可继续向下游传播的时间属性，通常等于窗口能包含的最大时间点，即 `window_end - 1ms`（精度为 3 时）。需要级联窗口或后续时间语义时应保留 `window_time`。

TVF 窗口聚合优于旧式 Group Window，是因为返回关系更容易组合、支持窗口 Top-N/Join，并能传播时间属性。旧语法是否可用取决于版本，但新代码优先 TVF。

## 7. 分组聚合与状态

无界流上的 `GROUP BY key` 必须记住每个 key 的聚合状态。如果 key 数持续增长，状态也会持续增长。可用窗口限定范围、设置 State TTL、使用 MiniBatch 减少状态访问，或定期输出到外部聚合系统。

TTL 不是纯性能开关。TTL 到期后旧状态被删除，之后迟到更新可能得到不同结果，因此必须大于业务允许的最大回看周期，并接受超期数据不再修正的语义。

## 8. OVER 窗口

流式 OVER 聚合通常要求时间属性升序排序，上边界为 `CURRENT ROW`：

```sql
SUM(amount) OVER (
  PARTITION BY user_id
  ORDER BY event_time
  RANGE BETWEEN INTERVAL '30' MINUTE PRECEDING AND CURRENT ROW
)
```

`RANGE` 按时间值范围，`ROWS` 按物理行数。两者在同一时间戳多行时结果不同。Flink 支持范围和边界随版本变化，应按集群版本核对。

## 9. Regular Join

Regular Join 适用于动态表，可以接收追加或更新。为了匹配未来任意时刻到达的另一侧记录，通常要保存两边历史状态，因此无界流可能无限增长。

```sql
SELECT *
FROM orders o
JOIN payments p ON o.order_id = p.order_id;
```

优先确认是否能加入时间范围改成 Interval Join，或事实与维度关系是否应使用 Temporal/Lookup Join。只能使用 Regular Join 时，设置 TTL 前必须定义允许匹配的最大时间跨度。

## 10. Interval Join

Interval Join 通常需要至少一个等值条件，以及同时引用两侧时间属性的有界范围：

```sql
SELECT *
FROM Orders o
JOIN Shipments s
  ON o.id = s.order_id
 AND o.order_time BETWEEN s.ship_time - INTERVAL '4' HOUR
                      AND s.ship_time;
```

两侧都可能驱动匹配，不存在固定“左驱动右”。时间范围和 Watermark 让系统知道某些记录未来不再可能匹配，从而清理状态。它通常只支持 append-only 输入；CDC 更新流是否支持必须核对版本。

## 11. Temporal Join 与 Lookup Join

两者常使用相似语法：

```sql
SELECT o.order_id, o.price, c.rate
FROM orders o
LEFT JOIN currency_rates FOR SYSTEM_TIME AS OF o.order_time AS c
  ON o.currency = c.currency;
```

### Event-time Temporal Join

维表本身是版本化 changelog，按事实 `order_time` 找到 `rowtime <= order_time` 的最新维度版本。它需要维表主键、正确时间属性和 Watermark，才能还原事实发生时看到的维度。

### Processing-time Temporal/Lookup Join

如果维表来自 JDBC 等外部系统，事实到达时发起查询，得到“当前”维度值。它不保留历史版本，重放时可能查到新值，因此不是确定性历史回放。缓存能降低外部 QPS，但 cache TTL 会影响新鲜度。

判断执行为内部状态 Temporal Join 还是外部 Lookup Join，不能只看 SQL，要看 connector 和 planner 计划。

## 12. Window Join、Semi Join 与 Anti Join

Window Join 先让两侧通过兼容窗口 TVF，再按窗口边界和 key 关联，使状态可按窗口结束清理。

Semi Join 只输出左表在右表存在匹配的行，可用 `EXISTS`；Anti Join 输出不存在匹配的左表行，可用 `NOT EXISTS`。流上 Anti Join 何时能确定“不存在”与 Watermark/时间范围密切相关，没有边界时可能需要长期等待。

## 13. Top-N 与去重

Top-N 的标准模式是 `ROW_NUMBER()`：

```sql
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY category
    ORDER BY score DESC, item_id
  ) AS rn
  FROM items
)
WHERE rn <= 10;
```

排序中加入唯一字段，避免并列时结果不确定。窗口 Top-N 把 `window_start/window_end` 放入 PARTITION BY。`QUALIFY` 是否支持及语法位置取决于 Flink 版本，不能把 Flink 2.x 能力直接套到旧集群。

去重本质上也是 Top-1，但必须定义保留最早还是最新一条，并理解更新流会输出回撤。

## 14. Lateral 与 UNNEST

```sql
SELECT u.user_id, tag
FROM users u
CROSS JOIN LATERAL TABLE(UNNEST(u.tags)) AS T(tag);
```

空数组时 CROSS JOIN 不输出左行。需要保留时：

```sql
SELECT u.user_id, tag
FROM users u
LEFT JOIN LATERAL TABLE(UNNEST(u.tags)) AS T(tag) ON TRUE;
```

对 JSON 数组应先用 JSON 函数解析并 CAST 成 `ARRAY<...>`，解析失败和 NULL 应有脏数据策略。

## 15. 确定性

会破坏可重复性的因素包括 `RAND()`、`UUID()`、处理时间、外部 Lookup 当前值、无稳定 tie-breaker 的 Top-N，以及 TTL 清理后的迟到更新。并发和状态后端通常不会凭空改变确定性逻辑，但会暴露顺序依赖或非确定 UDF。

exactly-once 也不等于业务结果必然确定；它解决故障恢复时状态一致性，不能修复本身使用随机函数或当前时间的 SQL。

## 参考依据

- [Flink Watermark](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/generating_watermarks/)
- [Flink SQL JOIN（2.2 版本文档）](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/joins/)
- [Flink SQL CREATE TABLE（2.2 版本文档）](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/create/)

