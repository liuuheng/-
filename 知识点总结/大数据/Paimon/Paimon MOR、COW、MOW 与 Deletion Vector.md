# Paimon MOR、COW、MOW 与 Deletion Vector

## 结论

MOR、COW、MOW 决定“旧版本在写入、Compaction 还是查询阶段被处理”，不会改变 `merge-engine` 定义的逻辑结果。

| 模式 | 主要配置 | 版本合并发生在哪里 | 主要代价 |
| --- | --- | --- | --- |
| MOR | 默认 | 查询时合并重叠 Sorted Run | 查询 CPU、内存和延迟 |
| COW | `full-compaction.delta-commits=1` | 每次提交后的 Full Compaction | 写放大和提交延迟 |
| MOW | `deletion-vectors.enabled=true` | 写入侧 Lookup，读取时跳过失效行 | Lookup 状态、本地盘和可见性约束 |

不要把表模式和 `deduplicate`、`partial-update` 等 Merge Engine 混为一谈。Merge Engine 决定同一主键的多条记录如何得到最终行；表模式决定计算这个结果的成本落在哪个阶段。

## 1. MOR：写入保留多个版本，读取时合并

MOR（Merge On Read）是主键表的默认方式。Writer 把新记录写入 Level 0，后台 Compaction 逐步合并 Sorted Run。只要同一主键的多个版本仍分布在重叠文件中，普通查询就需要读取并按主键、sequence 和 Merge Engine 合并。

MOR 的好处是写入无需每次都重写该 Bucket 中的大量旧数据。代价是 Compaction 追不上时，查询要打开更多文件、合并更多版本。`num-sorted-run.compaction-trigger` 调大可以减少 Compaction 频率，但会把更多工作转移给查询；它不能消除工作量。

MOR 适合更看重持续写入吞吐、能够接受查询端合并，或者已有独立 Compaction 维持 Sorted Run 数量的场景。

## 2. COW：每次增量提交后做 Full Compaction

Paimon 没有独立的 `table-mode=COW` 选项。配置下面的参数，表示每 1 次 delta commit 触发同步 Full Compaction：

```sql
ALTER TABLE orders SET (
  'full-compaction.delta-commits' = '1'
);
```

在 Flink 流写入中，commit 通常随成功的 checkpoint 产生，所以这里的“1”是一次增量提交，不是每写入一行就合并一次。Full Compaction 后，查询可以读取充分合并的文件，减少 MOR 的版本合并成本。

COW 会反复重写已存在的数据。Bucket 很大而单次变化很小时，读取收益可能小于写放大、对象存储请求和 checkpoint 等待成本。它更适合写入频率低、批量更新明显、读延迟比写成本更敏感的表。

`full-compaction.delta-commits` 与 `changelog-producer=lookup` 不兼容。需要 Lookup Changelog 时不能用 COW 参数同时表达另一套 Changelog 生成方式。

## 3. MOW：用 Deletion Vector 标记旧物理行

MOW（Merge On Write）通过 Deletion Vector（DV）记录数据文件中哪些物理行已经失效。更新主键 `id=7` 时，新版本写入新文件，旧版本所在文件可以保留，但旧行的位置会被 DV 标记；读取时扫描数据文件并跳过这些位置。

```sql
CREATE TABLE orders_mow (
  order_id BIGINT,
  status STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'bucket' = '8',
  'deletion-vectors.enabled' = 'true'
);
```

为了找到旧物理行，Writer 需要 Lookup 之前的数据，并在 Lookup Compaction 中维护 DV。因此，MOW 把 MOR 的查询合并成本换成写入侧 Lookup、本地缓存、Compaction 和 DV 应用成本。它通常更适合更新较多且分析查询频繁的 `deduplicate` 表。

DV 应在建表时确定。现有表直接把 `deletion-vectors.enabled` 从 `false` 改为 `true` 不是完整迁移：旧文件还没有对应 DV，处理不当可能暴露重复版本。官方文档要求显式允许修改，并通过 Full Compaction 完成迁移；生产操作应按部署版本执行。

## 4. MOW 的 Level-0 可见性

MOW 的新记录先进入 Level 0。Lookup Compaction 完成前，旧行位置尚未全部转化为可用的 DV。默认情况下，Batch Read 会跳过这些待处理的 Level-0 文件，Writer 也会等待必要的 Lookup Compaction 后再提交，以保证查询看到可解释的结果。

如果使用异步 Compaction、`write-only=true` 或独立 Compaction，Level-0 数据可能要等 Compactor 追上后才对默认 Batch Read 可见。这种现象不是“snapshot 已提交却随机丢数据”，而是所选表模式的可见性约束。

可以配置 `deletion-vectors.merge-on-read=true`，让 Batch Read 把未完成 Lookup Compaction 的 Level-0 数据也按 MOR 方式合并。这样能提高新数据可见性，但查询重新承担合并成本；该选项不改变流式 Changelog 的生成方式。

## 5. `first-row` 与 DV 的边界

`first-row` 依赖 Lookup 判断某个主键是否已经存在，也可能使用 DV 相关路径。它保留的是首次到达的记录，不是事件时间最早的记录。

如果异步或独立 Compaction 尚未处理 Level-0，Batch Read 同样可能暂时看不到待确认的首条记录。普通 `first-row` 表不能因为想提升查询速度就随意套用所有 DV 配置；官方对 First Row、Primary-Key Index 和 PK Clustering Override 各有单独的 DV 限制，应按实际组合核对。

## 6. 查询过滤下推的限制

主键表的最终逻辑行可能来自多个物理版本。假设旧版本 `status='PAID'`，新版本把它改成 `CANCELLED`。如果在合并前只读取满足 `status='PAID'` 的物理行，查询可能错误地保留已失效的旧版本。

因此，非主键字段的过滤不能一律在 Merge 前下推。Paimon 只能在保证不改变主键合并结果时裁剪文件或记录。DV 让旧行失效位置显式化，能减少版本合并，但仍需使用 Paimon-aware Reader；直接扫描底层 Parquet/ORC 文件无法正确应用 Snapshot、DV 和 Merge Engine 语义。

## 7. 如何选择

- 写多读少、查询能承担合并：先用默认 MOR，并监控 Sorted Run 和 Level-0 文件数。
- 读多写少、每次重写成本可接受：评估 COW，不要仅为“文件整齐”每次 Full Compaction。
- `deduplicate` 表更新频繁且分析读取多：评估 MOW，同时给 Lookup Cache、本地盘和 Compaction 留足资源。
- 需要局部更新、聚合或特殊索引：先核对该 Merge Engine/Feature 与 DV 的兼容性，再决定模式。
- 无流式下游不等于不需要 Compaction。即便 `changelog-producer=none`，Compaction 仍负责控制文件数、重叠版本和查询成本。

## 8. 验证清单

1. 在 `$files` 中按分区和 Bucket 统计文件数、Level、平均文件大小。
2. 用 `$snapshots` 确认写入提交与 COMPACT 提交的时间关系。
3. 对同一批更新分别测写入延迟、checkpoint 时长和查询耗时，不只比较文件数量。
4. MOW 场景测试“提交后立即批查”，确认是否接受 Level-0 可见性延迟。
5. 用包含 UPDATE、DELETE、乱序和重复事件的数据校验最终逻辑行。
6. 确认所有查询引擎都支持当前 Paimon 版本和 DV；不要绕过 Catalog 直接读数据文件。

## 相关笔记

- [[Apache Paimon 表模型、存储组织与读取语义]]
- [[Paimon Snapshot、Manifest、Parquet 与 Compaction 的关系]]
- [[Paimon Sequence、RowKind 与删除语义]]
- [[Paimon 系统表、监控指标与故障排查]]

## 官方资料

- [Table Mode](https://paimon.apache.org/docs/master/primary-key-table/table-mode/)
- [Compaction](https://paimon.apache.org/docs/master/primary-key-table/compaction/)
- [Changelog Producer](https://paimon.apache.org/docs/master/primary-key-table/changelog-producer/)
- [Connecting Engines](https://paimon.apache.org/docs/master/ecosystem/connecting-engines/)

> 核对日期：2026-09-16。链接指向 master 文档，生产配置应以实际部署版本为准。
