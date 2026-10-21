---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 27
source_sections: "优化第2.1、7.8节"
---

# Flink 2.3 RocksDB 缓存、写放大与增量快照

RocksDB 性能来自“内存缓存 + LSM 文件组织 + 后台合并”的组合。读慢、写停顿和检查点上传慢对应不同瓶颈，不能用一组大参数同时覆盖。

## 先保证状态有边界

先检查 TTL、窗口清理、Join 范围和 key 基数。状态没有业务清理边界时，任何缓存最终都会被耗尽。选择 MapState 或合适累加器，避免每次为一个字段更新就序列化整个巨大对象。

## 内存先使用受管预算

```yaml
state.backend.type: rocksdb
state.backend.rocksdb.memory.managed: true
taskmanager.memory.managed.fraction: 0.4
execution.checkpointing.incremental: true
execution.state-recovery.from-local: true
execution.checkpointing.local-backup.enabled: true
```

RocksDB 的 block cache、write buffer 等受共享预算约束。只增大某个缓存选项，未必增加总可用内存；关闭受管限制后，则必须自己算清每个数据库实例、列族和 slot 的 native 内存总和。

本地恢复仍要求文件存在且可复用。它节省远端下载，不替代远端检查点。多目录只有落在不同物理设备或不同性能资源上才可能分担压力。

## 读路径和写路径分别调整

读多且 cache miss 高，观察 block cache 命中、磁盘随机读和热点；写多且频繁 flush/compaction，观察 memtable、pending compaction、磁盘写入和后台线程。调大 write-buffer 比例可能挤压读缓存，改善写入却恶化读取。

```yaml
# 仅作为一次受控对比，数值应由负载验证
state.backend.rocksdb.memory.write-buffer-ratio: 0.5
state.backend.rocksdb.memory.partitioned-index-filters: true
```

增加 write buffer 数量、大小、后台线程和 level 阈值会改变内存及 I/O 竞争。CPU、磁盘已满时继续加 compaction 线程，可能让记录处理与快照上传更慢。

## 增量快照不是固定小流量

增量检查点复用未改变的 SST，但 compaction 会生成新文件，即使业务净增量不大，也可能需要上传较多数据。观察“本次上传量”和“全量状态体积”要分别看，不要把 delta checkpoint size 当作整个状态只有这么大。

上传线程参数只在远端上传是瓶颈且还有 I/O/带宽余量时有价值。状态读取慢时调上传线程，不会提升每条记录 get/put 的性能。

## 一次优化需要的证据

保留相同输入和状态快照，比较状态访问延迟、读写 IOPS、compaction、RSS、检查点各阶段、恢复耗时。启用 native metrics 和采样也有成本，按诊断目标开启，避免高基数指标反过来拖慢任务。

不要把“扩大并行度总能解决 RocksDB 慢”当结论。热 key、慢共享盘、远端限流和过大的单 value 都可能在扩容后保留或进一步放大。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：优化第2.1、7.8节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [large state tuning](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/large_state_tuning/)
- [state backends](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/state_backends/)
- [config](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/config/)

前篇：[[02-Flink 2.3 并行度规划与可对账压测]]

后篇：[[04-Flink 2.3 Checkpoint慢的分阶段诊断]]
