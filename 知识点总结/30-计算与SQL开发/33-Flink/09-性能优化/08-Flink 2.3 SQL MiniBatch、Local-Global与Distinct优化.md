---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 32
source_sections: "优化第6章；官方补充 Regular Join、Multi-Join"
---

# Flink 2.3 SQL MiniBatch、Local-Global 与 Distinct 优化

SQL 优化先问瓶颈是状态访问、网络数据量、热 key，还是状态无限保留。每个开关只针对其中部分问题，并可能增加延迟或改变清理语义。

## MiniBatch 和两阶段聚合

```sql
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '2 s';
SET 'table.exec.mini-batch.size' = '5000';
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';
EXPLAIN PLAN FOR
SELECT user_id, SUM(amount) AS total FROM events GROUP BY user_id;
```

MiniBatch 缓冲一组输入，减少同 key 的重复状态访问；Local-Global 在上游先产生局部累加器，下游合并，降低 shuffle 和热聚合负载。后者依赖 MiniBatch，UDAF 还要具备可用的 merge 等能力。

两个阈值表达等待时间与缓冲数量，不是“必须同时满足才触发”。低流量时仍应能按时间刷新；高基数下，同批同 key 很少重复，收益可能有限。实际端到端延迟还要叠加 watermark 与 Sink 提交等待。

## 精确 DISTINCT 分桶

```sql
SET 'table.optimizer.distinct-agg.split.enabled' = 'true';
SET 'table.optimizer.distinct-agg.split.bucket-num' = '128';
SELECT user_id, COUNT(DISTINCT event_id) FROM events GROUP BY user_id;
```

按 distinct key 的确定哈希分桶，让相同去重对象进入同一桶，再合并桶计数。这与按随机事件分桶后直接相加 UV 不同。桶数增加会扩大中间分组和状态，不应把默认值或最大值当作最佳值。

## 多维 DISTINCT 使用 FILTER

```sql
SELECT day_key,
       COUNT(DISTINCT user_id) AS all_uv,
       COUNT(DISTINCT user_id) FILTER (WHERE platform='app') AS app_uv,
       COUNT(DISTINCT user_id) FILTER (WHERE platform='web') AS web_uv
FROM visits GROUP BY day_key;
```

相同 distinct key 的多种过滤条件有机会共享去重状态。对相同查询比较计划、状态体积和结果，不能仅凭语法变短就宣布状态一定减少到某个比例。

## TTL 是语义约定

```sql
SET 'table.exec.state.ttl' = '1 d';
```

它限制受支持算子的空闲状态保留，不等于“只计算最近一天”。长期累计清掉旧状态后，新事件可能从零开始；Regular Join 清掉一侧后，更晚记录可能无法关联。窗口由时间语义清理，不应给所有算子统一拍一个 TTL。

## 当前版本的 Join 优化

当前文档支持 MiniBatch Regular Join，通过缓存和减少中间更新降低输出与状态访问；Multi-Join 可用 `table.optimizer.multi-join.enabled` 对多表关联评估共享执行结构。两者都需要查看实际计划与适用限制，不能把旧教材只涉及聚合的介绍当作能力全集。

Session Window TVF Aggregation 不适用上述通用 TVF 聚合优化；精确 DISTINCT、UDAF、更新流等组合也需逐项检查 planner 是否采用预期路径。

## 本次已核对的执行计划

在 Flink 2.3.0 生成的计划中，SUM 聚合已出现 `MiniBatchAssigner → LocalGroupAggregate → Exchange → GlobalGroupAggregate`；精确 DISTINCT 已出现 `MOD(HASH_CODE(event_id),128)` 分桶、局部聚合与最终合并。计划原文保存在配套工程 `verification/plans/local-global.txt` 和 `split-distinct.txt`。这验证了两种优化被实际选中，没有把它扩大为已测得生产吞吐收益。

验收采用相同输入与快照，一项项启用优化，检查计划中的局部/全局聚合、分桶和 Join 结构，再比较状态、吞吐、P99 延迟及最终对账。设置成功但计划未变，不能算优化生效。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：优化第6章；官方补充 Regular Join、Multi-Join。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [tuning](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/tuning/)
- [config](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/config/)
- [window-tvf](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-tvf/)

前篇：[[07-Flink 2.3 对象复用、滑动窗口与延迟测量]]

后篇：[[09-Flink 2.3 故障排查与证据保留]]
