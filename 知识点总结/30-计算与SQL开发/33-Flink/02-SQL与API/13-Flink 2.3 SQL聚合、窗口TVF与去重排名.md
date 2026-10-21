---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 20
source_sections: "主教材第10.6.1—10.6.11节；优化第5.5节"
---

# Flink 2.3 SQL 聚合、窗口 TVF 与去重排名

无窗口 GROUP BY 持续更新每个分组的累计；窗口聚合按时间边界给出有限区间结果；OVER 为每条输入增加相对范围的计算值。它们的输出行数、状态生命周期和下游协议不同。

## 窗口 TVF 的主路径

```sql
SELECT window_start, window_end, user_id,
       COUNT(*) AS pv, SUM(amount) AS total
FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(et), INTERVAL '10' SECOND))
GROUP BY window_start, window_end, user_id;
```

TVF 给每行附加 `window_start、window_end、window_time`。在流式模式中，`window_time` 可继续承担时间属性；普通起止字段不能直接替代它进行所有后续事件时间操作。

`HOP(TABLE events, DESCRIPTOR(et), INTERVAL '5' SECOND, INTERVAL '1' MINUTE)` 表示每五秒更新最近一分钟。`CUMULATE` 有固定统计周期内不断扩大的前缀，适合周期内累计，不等同于每条记录都更新的永久 GROUP BY。

## 当前版本的 Session TVF

```sql
SELECT user_id, window_start, window_end, COUNT(*) AS cnt
FROM SESSION(TABLE events PARTITION BY user_id,
             DESCRIPTOR(et), INTERVAL '10' SECOND)
GROUP BY user_id, window_start, window_end;
```

Session 依活动间隔合并，晚到的桥接事件可能把两个会话连成更大会话。2.3 文档支持该 TVF，但不支持其批模式，且 Session 窗口聚合不具备通用 TVF 聚合页面列出的全部优化；Session Join/Top-N/去重仍有 beta 边界，不能当成熟滚动窗等价替换。

## OVER、Top-N 和去重

```sql
SELECT user_id, amount,
       SUM(amount) OVER (
         PARTITION BY user_id ORDER BY et
         RANGE BETWEEN INTERVAL '1' MINUTE PRECEDING AND CURRENT ROW
       ) AS recent_amount
FROM events;
```

`ROWS` 按行数，`RANGE` 按排序值范围；同时间戳多行时两者结果可能不同。流式 OVER 受支持的排序与边界约束，不能把批查询任意排序直接搬入。

窗口 Top-N 先完成按 URL 的窗口聚合，再计算：

```sql
SELECT * FROM (
  SELECT window_start, window_end, url, cnt,
         ROW_NUMBER() OVER (
           PARTITION BY window_start, window_end
           ORDER BY cnt DESC, url ASC
         ) AS rn
  FROM url_window_counts
) WHERE rn <= 2;
```

按业务键取第一条/最后一条常用 `ROW_NUMBER()` 和时间属性升序/降序，再筛 `rn=1`。仅写 `DISTINCT` 是按全行去重，不表示按订单号保留最新版；普通时间字段和时间属性也影响 planner 能否识别专用去重算子。

## 多维、集合和全局排序

`GROUPING SETS、ROLLUP、CUBE` 会增加分组层级，区分汇总占位 null 和原始 null 时使用 grouping 信息。`UNION ALL` 保留重复，`UNION/INTERSECT/EXCEPT` 可能需要状态和撤回。无界全局排序没有天然的最终时刻；Top-N 应明确分组与排序范围，不能用一次 LIMIT 假装持续精确排名。

对相同数据分别手算 PV、UV、金额、窗口 Top-2，并检查执行计划是否识别 WindowAggregate/Rank/Deduplicate 等相应结构；SQL 能解析只是第一步。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：主教材第10.6.1—10.6.11节；优化第5.5节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [window-tvf](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-tvf/)
- [over-agg](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/over-agg/)
- [window-topn](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-topn/)
- [deduplication](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/deduplication/)

前篇：[[12-Flink 2.3 SQL建表、时间属性与时区]]

后篇：[[14-Flink 2.3 SQL Join选型与历史维度一致性]]
