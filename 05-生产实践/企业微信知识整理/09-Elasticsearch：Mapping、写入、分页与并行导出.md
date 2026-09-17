---
tags:
  - elasticsearch
  - search
  - pagination
status: active
---

# Elasticsearch：Mapping、写入、分页与并行导出

返回：[[README|企业微信知识地图]]

## 1. Index、Primary Shard 与 Replica

一个 index 被拆成多个 primary shard；每个 primary 可以有若干 replica。写请求先根据 `_id` 或 `_routing` 计算目标 primary shard，在主分片执行后复制到副本。

可以类比 Kafka topic/partition 帮助入门，但不要完全等同：ES shard 是独立 Lucene index，负责搜索、segment 和文档版本；Kafka partition 是有序追加日志，消费和保留语义不同。

Primary shard 数决定索引的基础水平切分，建好后调整成本高；replica 数用于容灾和读并发，可动态调整，但也增加写放大和存储。

## 2. 一次写入发生什么

```text
客户端 → 协调节点
→ 路由到目标 primary shard
→ 更新 Lucene 内存结构并写 translog
→ 复制到 replica
→ 满足 ack 条件后返回
→ refresh 后可搜索
→ flush 形成 Lucene commit、开启新 translog generation
→ segment merge 合并小 segment、清理删除标记
```

写成功不一定立即能被 search 查到，因为 ES 是近实时搜索。

## 3. Refresh、Flush 与 Merge

- Refresh 将内存中的新 segment 打开给搜索，使文档可见，但不是持久化边界。
- Flush 执行 Lucene commit 并处理 translog generation，用于恢复和持久化管理。
- Merge 把多个小 segment 合并，回收被删除或旧版本文档占用的空间。

`refresh=true` 会让每次写都等待并制造大量小 segment，降低批量写吞吐。需要读己之写时可评估 `refresh=wait_for`，但仍要理解其等待机制和负载影响。

## 4. Translog 与写入持久性

默认 `index.translog.durability=request` 通常在主分片和已分配副本完成 translog fsync 后确认。改成 async 可提高吞吐，但节点崩溃时可能丢失最近已确认写入。不能把 translog 当搜索可见性机制；搜索可见由 refresh 决定。

## 5. text、keyword、match 与 term

- `text` 经过 analyzer 分词，适合全文检索。
- `keyword` 保存整体值，适合精确过滤、排序、terms 聚合。
- `match` 会分析查询文本，通常用于 text。
- `term` 查精确 term，通常用于 keyword、数值、日期。

一个名称既要全文搜索又要聚合时使用 multi-fields：

```json
"name": {
  "type": "text",
  "fields": {
    "keyword": { "type": "keyword" }
  }
}
```

不要在 text 上直接使用 term 期望匹配原字符串，也不要随意开启 fielddata 做聚合，因为会占用大量 heap。

## 6. index:true 的含义

字段要被正常 query，通常需要 mapping 中 `index: true`。大多数字段默认可索引，但 `_source` 存储与倒排索引是两回事：字段可以保留在 `_source` 中用于返回，却设置 `index:false` 禁止检索。

对从不查询的大对象关闭索引能降低存储和写入成本；需要聚合/排序的 keyword/数值还要关注 doc_values。

## 7. 对象数组与 nested

ES 没有独立 array 类型。普通对象数组会扁平化：

```json
"authors": [
  {"name":"Alice", "country":"CN"},
  {"name":"Bob", "country":"US"}
]
```

扁平后可能错误匹配 `name=Alice AND country=US`，因为元素间关系丢失。需要保持每个对象内部关联时使用 nested mapping 和 nested query。Nested 会把每个子对象存成隐藏 Lucene 文档，查询和写入成本更高，不应对所有数组默认使用。

动态键特别多时会造成 mapping explosion。可使用显式 mapping、dynamic template 或 `flattened`；但 flattened 叶子值主要按 keyword 语义处理，不适合真正数值范围和复杂 nested 关系。

## 8. 乐观并发控制

多个客户端读取后更新同一文档，简单最后写胜可能覆盖别人修改。当前推荐使用 `_seq_no` 与 `_primary_term`：

```http
PUT /index/_doc/1?if_seq_no=17&if_primary_term=3
```

只有文档仍是读取时版本才更新，否则返回冲突，由业务决定重试或合并。不要把旧式 `_version` 当成所有并发场景的首选。

## 9. PIT 与 search_after

PIT（Point In Time）固定一个相对稳定的搜索视图，适合深分页期间避免 refresh 导致结果漂移。它不是数据库事务快照，也不保证跨业务操作强一致。

```json
{
  "size": 1000,
  "pit": {"id":"PIT_ID", "keep_alive":"2m"},
  "sort": [
    {"update_time":"asc"},
    {"id.keyword":"asc"}
  ],
  "search_after": ["2026-07-14T10:00:12.345Z", "order_10086"],
  "query": {
    "range": {"update_time":{"gte":"...", "lt":"..."}}
  }
}
```

排序必须稳定且最好唯一。只按 `update_time`，相同时间的文档可能在分页边界重复或遗漏；加入唯一 ID 作为 tie-breaker。

每次查询更新 keep_alive 只延长上下文，并不是让旧 PIT 永久存在。客户端应及时关闭 PIT，并能处理过期重建。

## 10. Scroll 与 PIT 的选择

Scroll 维护有状态搜索上下文，适合传统批量导出；PIT + search_after 更接近无状态翻页，资源行为和恢复方式不同。现代大规模导出通常优先评估 PIT + search_after，但具体取决于 ES 版本和 connector 支持。

不能把 scroll 的 `_scroll_id` 和 PIT ID 混在同一查询中。两者都需要控制 keep_alive，避免长期占住旧 segment。

## 11. Sliced 查询的并行模型

Slice 在查询层把每个 shard 内文档逻辑切成若干片，多个客户端并行查询不同 `slice.id`：

```json
"slice": {"id":0, "max":4}
```

Slice 0 会读取所有相关 shard 中属于 slice 0 的文档，客户端并行度由 `max` 决定，不等于 shard 数。但实际吞吐仍受 shard 数、数据节点资源和 shard 大小倾斜约束。

Slice 数过多会增加搜索上下文、CPU 和合并结果的开销。若某 shard 特别大，最慢 slice 仍会拖累整体。

## 12. Flink 增量读取 ES 的正确性边界

一个实用方案是按时间窗口切 split，并在窗口内使用 slice 并行、PIT + search_after 分页：

- `upper.bound.delay` 不读取过新的数据，等待 refresh 和上游写入稳定。
- `lookback` 在下一轮回看上一段区间，覆盖迟到和可见性延迟。
- checkpoint 保存时间 split、slice 和 search_after。
- 下游按业务 ID 与 update_time/version 幂等覆盖。

Reader 故障后旧 PIT 可能过期，新 PIT 看到不同视图，无法保证严格从旧快照续读。因此语义应明确为“降低漏读概率、允许重复，通过回看和幂等达到最终一致”，而不是宣称端到端 exactly-once。

## 13. 批量导出检查清单

- 查询字段是否有合适 mapping 和 index/doc_values？
- 时间范围是否使用半开区间？
- 排序是否包含唯一 tie-breaker？
- PIT/scroll 是否及时续期和关闭？
- slice 数是否超过集群实际并行能力？
- 是否关闭不需要的 `track_total_hits`？
- 页面大小是否在网络吞吐与内存之间平衡？
- 恢复后是否允许重复，下游是否幂等？

## 参考依据

- [Elasticsearch 近实时搜索](https://www.elastic.co/docs/manage-data/data-store/near-real-time-search)
- [Refresh 参数](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/refresh-parameter)
- [Translog](https://www.elastic.co/docs/reference/elasticsearch/index-settings/translog)
- [Nested 类型](https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/nested)
- [乐观并发控制](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/optimistic-concurrency-control)
