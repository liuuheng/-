# Paimon 动态 Bucket 的索引机制与单 Writer 限制

## 核心结论

Paimon 主键表设置 `bucket=-1` 后，动态 Bucket 需要维护“主键属于哪个 Partition-Bucket”的索引。单个 Flink Job 可以通过按主键 Hash Shuffle，让相同主键始终由同一个 Assigner subtask 处理，因此支持 Job 内多并发；多个 Job 的内存索引或 RocksDB 状态彼此独立，无法实时共享新 Key 的 Bucket 分配结果，可能把同一主键写入不同 Bucket，破坏主键表的合并和唯一性语义。

因此，“只支持单写”准确指：同一张动态 Bucket 主键表在同一时间只能有一个写入 Job。它不表示 `sink.parallelism` 必须为 `1`，也不表示所有记录只能由一个线程串行处理。

## 两种动态 Bucket 模式

| 模式 | 触发条件 | 索引内容与位置 | 多 Job 写入 |
| --- | --- | --- | --- |
| `HASH_DYNAMIC` | 主键包含全部分区键 | `主键 Hash → Bucket`，历史映射持久化为 Hash Index 文件 | 不支持 |
| `CROSS_PARTITION` | 主键未包含全部分区键 | `主键 → Partition + Bucket`，运行时保存在本地 RocksDB | 不支持 |

例如表按 `dt` 分区：

```sql
-- 主键包含分区键，属于 HASH_DYNAMIC
PRIMARY KEY (id, dt) NOT ENFORCED
PARTITIONED BY (dt)

-- 主键不包含分区键，需要 CROSS_PARTITION
PRIMARY KEY (id) NOT ENFORCED
PARTITIONED BY (dt)
```

把 `dt` 加入主键会改变唯一性：不同日期的同一个 `id` 将成为两条不同主键记录。不能为了避开跨分区索引而直接修改主键，必须先确认业务唯一键和分区字段是否会变化。参见 [[Paimon 主键表建模与稳定性检查清单]]。

## 相同主键如何进入同一 Bucket

动态 Bucket 的写入链路可以拆成四步：

```text
输入记录
→ 按主键 Hash Shuffle
→ Assigner 查询或更新 Key-Bucket 索引
→ 按 Partition + Bucket 再次 Shuffle
→ Writer → Committer
```

第一次 Shuffle 保证相同主键进入同一个 Assigner subtask。Assigner 再根据索引处理记录：

1. 旧 Key：返回历史 Bucket。
2. 新 Key：选择一个未满的 Bucket，并记录映射。
3. 现有 Bucket 都达到容量条件：创建新 Bucket。
4. 按确定的 Partition-Bucket 将记录发送给 Writer。

`HASH_DYNAMIC` 在启动时读取历史 Hash Index 文件，把映射加载到内存；Checkpoint 时持久化索引更新。`CROSS_PARTITION` 使用 RocksDB 保存 Key 的历史分区和 Bucket，Job 启动或恢复时需要通过 Bootstrap 扫描表内有效 Key 重建索引。

动态 Bucket 的一致性不只依靠 Hash 公式，而是由“主键路由、状态索引和 Bucket 分配”共同保证。

## 多个 Job 为什么会产生错误

假设 Job A 和 Job B 同时写入新主键 `id=1001`：

- Job A 的索引中不存在该 Key，将它分配到 Bucket 3。
- Job B 无法实时看到 Job A 的分配，也可能把它分配到 Bucket 7。
- 同一主键随后分散到不同 Bucket，超出单个 Bucket 内的合并范围。

在 `HASH_DYNAMIC` 模式下，不同 Job 各自维护运行时 `hash2Bucket` 映射，持久化文件不能提供实时的跨 Job 分配协调。在 `CROSS_PARTITION` 模式下，每个 Job 有独立的本地 RocksDB；Bootstrap 只建立启动时的存量视图，不能同步另一个 Job 启动后的新增映射。

这与 Snapshot 能否原子提交是两个问题。原子提交可以防止读者看到半个 Snapshot，乐观并发控制也可以发现部分提交冲突，但它们不能自动把两个独立的动态 Bucket 索引合并成一个实时一致的分配器。参见 [[Paimon 多 Writer、Catalog 与原子提交]]。

## 单 Job 仍然可以多并发

单 Writer Job 内可以配置多个 Assigner 和 Writer subtask：

- 相同主键经过第一次 Shuffle 后只进入一个 Assigner。
- 不同 Assigner 负责不同的 Hash 范围。
- 记录确定 Bucket 后，再按 `partition + bucket` 路由到对应 Writer。

所以，排查“single write”报错时，应先找是否存在第二个写入 Job，而不是直接把 Sink 并行度降到 `1`。盲目降低并行度只会损失吞吐，不能解决两个 Job 各自维护索引的问题。

## 批写和流写的并发边界

`INSERT OVERWRITE` 等批写场景可能使用 `SimpleHashBucketAssigner`，在内存中重新构建映射。它不依赖相同的历史索引流程，不代表可以安全地与流作业并行修改同一张表。

批写与流写同时运行时，两边仍可能基于不同的 Key-Bucket 视图作出决定。生产环境应为 Overwrite、历史修复和 Bucket 调整设置互斥窗口，并明确实际覆盖的分区范围。

## 生产设计

多个数据源需要写入同一张动态 Bucket 表时，先在上游汇聚，再由一个 Flink Job 写入：

```text
数据源 A ─┐
数据源 B ─┼→ CDC / Kafka 汇聚 → 一个 Flink Job → Paimon 动态 Bucket 表
数据源 C ─┘
```

如果业务必须让多个互不协调的 Job 直接写入同一张表，动态 Bucket 与该写入模型冲突，需要重新评估分桶策略和表边界。

使用 `CROSS_PARTITION` 时，还要评估：

- Bootstrap 扫描存量 Key 的启动时间；
- RocksDB 的本地磁盘、内存和状态大小；
- Checkpoint 与故障恢复耗时；
- 索引 TTL 是否会导致过期 Key 无法定位旧分区。

## 排查清单

- [ ] 表是否为主键表，并配置了 `bucket=-1`？
- [ ] 主键是否包含全部分区键？实际进入哪种 BucketMode？
- [ ] 是否有两个 Flink Job、批任务或历史修复任务同时写表？
- [ ] 报错是否包含 `same dynamic bucket table, it only supports single write`？
- [ ] 是否把“单 Job”误解成了“并行度只能为 1”？
- [ ] 多数据源是否可以先汇入 Kafka/CDC，再统一写入？
- [ ] `CROSS_PARTITION` 的 Bootstrap、RocksDB 和恢复成本是否经过压测？
- [ ] Overwrite 与流式写入是否设置了互斥窗口？

## 相关笔记

- [[Paimon 多 Writer、Catalog 与原子提交]]
- [[Paimon 主键表建模与稳定性检查清单]]
- [[Apache Paimon 表模型、存储组织与读取语义]]
- [[Paimon Snapshot、Manifest、Parquet 与 Compaction 的关系]]

## 来源与证据边界

- 原文：[Paimon 精讲（七）：Paimon 动态 Bucket 踩坑——主键表为什么只能单写？](https://mp.weixin.qq.com/s/PDZA6b1FPtprbbo1LtSs1Q)
- 作者：胖泽的技术笔记
- 发布时间：2026-08-04
- 本笔记按原文机制与源码片段整理。原文标注的类名包括 `DynamicBucketSink`、`PartitionIndex`、`HashIndexMaintainer` 和 `GlobalIndexAssigner`；源码行号及不同 Paimon 版本的具体行为尚未在本次整理中独立核对，生产使用前应以部署版本源码和官方文档为准。
