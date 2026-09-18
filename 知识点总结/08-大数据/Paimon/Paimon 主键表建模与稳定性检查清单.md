# Paimon 主键表建模与稳定性检查清单

## 核心结论

Paimon 主键表的稳定性首先由数据模型决定。主键、分区和变更语义设计错误后，TTL、并行度、内存和 Compaction 参数只能延缓故障，不能恢复正确性。

建表时按以下顺序决策：

```text
业务唯一键与分区是否会变化
→ 是否需要 Cross-Partition Upsert
→ 固定桶、动态桶或 Postpone Bucket
→ 流式下游需要哪种 Changelog
→ Checkpoint、文件和 Compaction
→ Snapshot、分区生命周期与资源预算
```

相关笔记：[[Apache Paimon 表模型、存储组织与读取语义]]、[[Paimon Snapshot、Manifest、Parquet 与 Compaction 的关系]]、[[Paimon 流式读写中的 Snapshot 与 Changelog]]。

## 一、主键与分区决定是否需要跨分区索引

假设业务唯一键为 `id`，表按 `dt` 分区：

```sql
PRIMARY KEY (id) NOT ENFORCED
PARTITIONED BY (dt)
```

如果同一个 `id` 的 `dt` 会改变，新记录只携带新分区，Paimon 还需要找到旧记录所在的分区和 Bucket。动态桶因此维护本地索引：

```text
id → (partition, bucket)
```

流式写入任务启动或恢复时，`IndexBootstrap` 需要读取当前表的有效主键、Partition 和 Bucket 来重建索引。没有 TTL 时，所有并行实例合计扫描当前表的全部有效主键。十亿级主键下，恢复成本随全表规模增长，而不是只处理 Checkpoint 之后的增量。

将主键改成：

```sql
PRIMARY KEY (dt, id) NOT ENFORCED
PARTITIONED BY (dt)
```

可以避免 Cross-Partition Upsert，但同时改变了唯一性：`(2026-09-15, 1)` 与 `(2026-09-16, 1)` 是两个不同主键。该方案只有在以下任一条件成立时才正确：

- `dt` 对同一实体不可变，例如创建日期；
- 上游提供完整 CDC，能够删除旧 `(dt,id)`，再写入新 `(dt,id)`；
- 业务本来就允许同一个 `id` 在不同分区各有一行。

不要为了省索引直接把分区字段塞进主键。先定义业务唯一性，再决定分区。

## 二、TTL 与 Bootstrap 并行度只能缓解

```sql
'cross-partition-upsert.index-ttl' = '7 d'
```

TTL 限制索引初始化和维护考虑的历史范围，可以控制 RocksDB 规模和启动扫描量。代价是超过 TTL 的旧主键无法定位原分区；它再次更新时，旧分区和新分区可能同时保留记录。重复量取决于过期数据的更新比例，不能预设为“少量”。

```sql
'cross-partition-upsert.bootstrap-parallelism' = '10'
```

该参数只并行读取 Bootstrap 数据，不减少总扫描量。当前版本默认值即为 `10`。继续调大需要同时观察对象存储请求、网络、本地盘和 RocksDB 写入，避免用更高并发制造新的瓶颈。

三种手段的性质不同：

- 使用不可变分区字段并建立正确主键：消除跨分区索引需求；
- 设置 TTL：限制索引范围，但降低历史更新正确性；
- 提高 Bootstrap 并行度：缩短扫描时间，不改变全表扫描复杂度。

## 三、分桶策略

### 固定桶

固定桶要求 `bucket > 0`：

```text
bucket = hash(bucket-key) % bucket-count
```

桶数表示每个分区下的逻辑 Bucket 数，不是文件数。每个 Bucket 是一棵独立 LSM Tree，内部可以包含多个 Sorted Run 和数据文件。

初始估算公式：

```text
bucket 数 ≈ ceil（单分区稳定后的有效数据量 ÷ 目标单桶数据量）
```

当前 Paimon master 对 MOR 主键表给出的初始参考是每桶 `200 MB～1 GB`；其他版本或实践可能使用 `1～5 GB`。最终值还要检查分区大小差异、Bucket Key 倾斜、写入吞吐、查询计划和 Compaction 时长。

### 动态桶

`bucket = -1` 是当前主键表默认模式。Paimon 维护 Key 到 Bucket 的映射，旧 Key 返回原 Bucket，新 Key 随数据增长进入新 Bucket。它适合分区大小难以提前估算的单写入作业，但有索引内存、本地盘和并发写入限制。

主键包含分区字段时仍可使用动态桶，只是不进入 Cross-Partition 模式。不要把“动态桶”和“跨分区索引”视为同一概念。

## 四、Changelog Producer

`changelog-producer` 只决定流式读取获得什么变化，不决定表类型和普通批查询结果。

- 没有流式读取：通常使用 `none`，避免生成额外完整 Changelog。
- 上游提供完整 Before/After CDC，流式下游需要原始变化：使用 `input`。
- `partial-update`、`aggregation` 的流式下游需要 Merge 后完整行：考虑 `lookup`。
- 下游能容忍较高延迟，且确实需要周期性完整变化：再评估 `full-compaction`。

`input` 不会补齐缺失旧值，也不会把 Partial Update 输入转换为最终完整行。不要只看到“CDC”标签就认定输入完整，应检查实际 `RowKind`、字段镜像和删除记录。

没有流式读取时，不要为了 Changelog 配置 `full-compaction.delta-commits`。普通 Compaction 仍需保留，用于控制 Sorted Run 和文件数量；历史分区可按需执行独立 Full Compaction。

## 五、Checkpoint、文件与 Compaction

Checkpoint 间隔同时影响提交可见性、Snapshot 频率和每次提交能积累的数据量。`1～3 min` 可以作为非秒级 SLA 的起点，但不能脱离实际流量直接套用。低流量、多分区、多 Bucket 场景即使使用较长间隔，也可能产生小文件。

主键表常用基线：

```sql
'target-file-size' = '128 mb',
'num-sorted-run.compaction-trigger' = '5',
'commit.force-compact' = 'false'
```

- `target-file-size` 是文件滚动目标，不保证每个文件都达到该大小；Checkpoint 到来时仍可能提交未达到目标的小文件。
- `num-sorted-run.compaction-trigger` 统计 Sorted Run，不是文件数；一个高层 Sorted Run 可以包含多个文件。
- `commit.force-compact=false` 避免每次提交前强制 Compaction。打开后可能拉长 Checkpoint 提交时间。

不要把 Compaction 完全关闭。它负责控制文件数量、合并同一主键的物理版本并降低批查询的读时合并成本。

## 六、写入内存与本地盘

```sql
'write-buffer-size' = '256 mb',
'write-buffer-spillable' = 'true'
```

`write-buffer-size` 当前默认是 `256 MB`。增大缓冲可能减少过早 Flush，但一个 TaskManager 还要承担 Compaction、网络、Flink 状态和其他算子的内存，不能按单个参数估算总内存。

Spill 默认开启，作用是内存不足时把排序数据写到本地盘，降低 OOM 风险。它不保证减少文件；频繁 Spill 会增加本地 I/O 和中间归并。

同一主键在一个 Checkpoint 内被高频更新时，可以尝试：

```sql
'local-merge-buffer-size' = '64 mb'
```

Local Merge 在 Shuffle 前折叠相同主键的更新，解决的是热点 Key 的重复更新，不是所有数据倾斜。当前官方文档说明该能力不适用于 CDC ingestion。

## 七、生命周期与资源预算

Snapshot 保留由三个参数共同决定：

```sql
'snapshot.time-retained' = '...',
'snapshot.num-retained.min' = '...',
'snapshot.num-retained.max' = '...'
```

只配置时间并不能保证精确保留窗口。最小 Snapshot 数会阻止过早删除，过小的最大 Snapshot 数又可能把实际时间窗口压缩。分区过期只会从新 Snapshot 中移除分区，旧文件还要等引用它们的 Snapshot 过期后才会物理删除。

按时间删除历史分区时，可以配置 `partition.expiration-time`，同时确认 `partition.expiration-strategy` 使用分区值时间还是最后更新时间。

每个 Writer Subtask 的资源预算至少包括：

```text
内存：Write Buffer + Compaction Sort + Local Merge + 动态桶/跨分区索引 + Flink 算子状态
本地盘：排序 Spill + Compaction 临时文件 + RocksDB/本地索引 + 故障期间峰值空间
```

固定桶场景下，`sink.parallelism` 通常不应大于 Bucket 数，初始可设置得与 Bucket 数接近。但多个活跃分区会让一个 Subtask 同时处理多个 Partition-Bucket，仍需根据实际负载调整。

## 建模阶段 Checklist

- [ ] 业务唯一键是什么？把分区字段加入主键后，是否改变实体唯一性？
- [ ] 分区字段对同一实体是否不可变？若会变化，谁负责删除旧分区记录？
- [ ] 是否真的需要 Cross-Partition Upsert？十亿级主键能否承受启动扫描？
- [ ] 使用 TTL 后，业务是否接受过期 Key 可能重复？
- [ ] 固定桶还是动态桶？是否满足动态桶的单写入作业约束？
- [ ] 桶数是否根据单分区有效数据量、倾斜和写入并行度估算？
- [ ] 流式下游是否存在？需要 Upsert、原始 CDC，还是 Merge 后完整 Changelog？
- [ ] Checkpoint 间隔是否同时满足可见性、恢复和小文件要求？
- [ ] 是否监控每个 Partition-Bucket 的文件数、平均文件大小和 Sorted Run 数？
- [ ] TaskManager 是否为 Compaction、Spill 和 RocksDB 预留了独立资源余量？
- [ ] Snapshot、Consumer、Tag 和 Partition Expiration 是否形成一致的保留策略？

## 参考资料

- [Paimon Data Distribution](https://paimon.apache.org/docs/master/primary-key-table/data-distribution/)
- [Paimon Changelog Producer](https://paimon.apache.org/docs/master/primary-key-table/changelog-producer/)
- [Paimon Streaming Writes and Small Files](https://paimon.apache.org/docs/master/learn-paimon/small-files/)
- [Paimon Write Performance](https://paimon.apache.org/docs/master/maintenance/write-performance/)
- [Paimon Manage Snapshots](https://paimon.apache.org/docs/master/maintenance/manage-snapshots/)
- [Paimon Manage Partitions](https://paimon.apache.org/docs/master/maintenance/manage-partitions/)

> 本笔记按 2026-09-16 的 Paimon master 文档整理。生产配置应以实际部署版本的文档和压测结果为准。
