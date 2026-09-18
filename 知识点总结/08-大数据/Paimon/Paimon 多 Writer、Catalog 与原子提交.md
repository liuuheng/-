# Paimon 多 Writer、Catalog 与原子提交

## 结论

Paimon 允许并发 Writer，但“能同时提交 Snapshot”不等于“任意多个作业都能安全修改同一批文件”。并发正确性依赖三层：Catalog/文件系统能原子发布 Snapshot，乐观并发控制能发现冲突，表的分桶和维护方式允许这些 Writer 同时工作。

生产设计应尽量让 Writer 修改不同分区。多个 Writer 写同一分区时，固定桶仍需协调 Compaction；动态桶则要求同一分区只能由一个作业写入，独立 Compaction 不能解除这个限制。

## 1. 一次提交如何发生

Writer 的提交过程可以拆成四步：

1. 写出新的数据文件，准备本次要新增和逻辑删除的文件列表。
2. 读取最新 Snapshot，验证这些文件变更仍适用于当前表状态。
3. 生成新 Snapshot，通过 Catalog 或文件系统原子发布。
4. 如果其他 Writer 抢先提交，重新读取最新状态；变化仍兼容时重试，否则拒绝提交。

数据文件已经出现在存储中，不代表它对读者可见。只有被已提交 Snapshot 引用后，它才进入表状态。Compaction 标记旧文件为删除，也只是让新 Snapshot 不再引用；旧 Snapshot 和 Tag 仍可能引用这些文件，物理清理由 Snapshot Expiration 等维护操作决定。

## 2. Snapshot Conflict 与 File Conflict

Snapshot Conflict 是两个 Writer 都想发布 `N+1`。Writer A 先成功后，Writer B 重新读取最新 Snapshot；如果 B 的文件变更仍有效，可以改为提交 `N+2`。这是可重试的发布竞争。

File Conflict 是两个操作修改了同一批输入文件。例如两个 Compactor 都计划用新文件替换 A、B。第一个提交后，A、B 已不属于最新表状态；第二个提交仍要删除 A、B，因此验证失败。仅换一个 Snapshot ID 重试不能让过期的文件计划重新有效，作业需要重新计算或从 checkpoint 恢复。

流作业提交失败可能触发重启。若多个 Writer 持续对同一批文件做 Compaction，就可能反复冲突、恢复、再次冲突，表现为“表没坏，但作业一直重启”。

## 3. 为什么独立 Compaction 能减少冲突

默认每个 Writer 都会在写入过程中做 Compaction。多个 Writer 写同一分区时，它们可能同时选择相同旧文件。

可以让所有摄入 Writer 设置：

```sql
'write-only' = 'true'
```

再运行一个独立 Compaction 作业。这样只有一个 Compactor 负责替换旧文件，摄入 Writer 只追加新文件，能减少文件级冲突，并把 Compaction 的 CPU、内存和 I/O 从写入作业中隔离出来。

`write-only=true` 还会跳过 Writer 内的 Snapshot Expiration。启用后必须安排独立 Compaction 和历史清理，否则文件、Snapshot 与元数据会持续增长。若还希望跳过自动 Manifest Merge，可按版本核对 `manifest.merge.skip-on-write-only`。

Flink SQL 可以运行指定表的 Compaction：

```sql
CALL sys.compact(
  `table` => 'default.orders',
  partitions => 'dt=2026-09-16',
  options => 'sink.parallelism=8'
);
```

Batch 模式通常一次性处理当前文件；Streaming 模式持续发现新变化。不要让两个独立 Compaction 作业覆盖同一分区。

## 4. 独立 Compaction 不能解决什么

它不能消除 Snapshot 发布竞争，也不能让所有表特性支持并发写入。最典型的是动态桶：同一分区只允许一个作业写入。

动态桶 Writer 维护 Key 到 Bucket 的索引。两个作业无法共享这份实时分配状态，可能把同一主键分配到不同 Bucket，最终形成重复逻辑行。`write-only=true` 只改变谁做 Compaction，不会让两个动态桶索引变成一个全局一致索引。

固定桶通过稳定 Hash 规则计算 Bucket，更适合多个 Writer，但仍要检查 Merge Engine、sequence、跨分区更新和 Overwrite 是否兼容。

## 5. 分区是最有效的并发边界

推荐让流作业写当前分区，批作业只 Overwrite 已封闭的历史分区。只要二者不修改同一分区和文件，冲突面会小得多。

`INSERT OVERWRITE` 的危险不在 SQL 名称，而在实际覆盖范围。静态分区 Overwrite 只替换明确分区；动态分区 Overwrite 可能根据输出数据决定要替换哪些分区。上线前要用执行计划和测试表确认范围，不要让批作业覆盖仍在流式写入的分区。

对同一分区并发执行流写、Overwrite、Bucket Rescale 或两个 Compactor，需要逐项验证支持关系。乐观并发控制可以拒绝冲突提交，但不能保证所有作业都自动变成无损串行执行。

## 6. 原子发布取决于存储和 Catalog

Paimon 的读者通过 Snapshot 获得一致视图，因此“发布新 Snapshot”必须是原子的：读者只能看到旧 Snapshot 或完整的新 Snapshot，不能看到写到一半的元数据。

Filesystem-managed Snapshot 默认先写临时文件再 Rename。HDFS 提供原子 Rename；S3 一类对象存储不能假设具有相同语义。某些 Paimon 文件系统实现使用原子条件创建，例如官方 OSS 实现。

如果所选存储实现无法在不覆盖现有 Snapshot 的情况下原子发布，需要使用所有 Writer 共享的锁。Hive 或 JDBC Catalog 可以配置：

```sql
'lock.enabled' = 'true'
```

具体还要配置相应锁类型、Metastore 或 JDBC 后端。锁配置必须在创建 Catalog 时统一，不是给单个 SQL Session 临时加一个互不相干的本地锁。

## 7. 所有 Writer 必须看到同一个表

多个 Writer 即使表名相同，只要 Warehouse、Catalog Backend、Metastore URI、认证身份或锁配置不同，就可能没有在协调同一份元数据。

上线前逐项比对：

- Catalog 类型与名称。
- Warehouse 的规范化路径。
- Hive Metastore/REST/JDBC Endpoint。
- 文件系统实现、Region、Endpoint 与认证。
- `lock.enabled`、锁类型和共享锁后端。
- Paimon 与引擎 Connector 版本。

不要让一个 Writer 通过 Hive Catalog 提交，另一个绕过它使用不兼容的 Filesystem Catalog 写同一目录。它们可能各自认为提交成功，却不共享相同的原子发布协议。

## 8. 多 Writer 设计选择

| 写入形态 | 推荐方式 | 主要风险 |
| --- | --- | --- |
| 多作业写不同分区 | 可并发，固定边界 | 分区范围配置错误 |
| 多作业写同一分区、固定桶 | `write-only=true` + 单独 Compactor | Snapshot 竞争、主键顺序 |
| 多作业写同一分区、动态桶 | 不允许 | 同一 Key 被分配到不同 Bucket |
| 流写当前分区 + 批 Overwrite 历史分区 | 推荐 | 动态 Overwrite 越界 |
| 多个 Compactor 处理同一分区 | 避免 | File Conflict 与反复重启 |
| 对象存储多 Writer | 统一 Catalog 与共享锁/原子实现 | Rename 非原子、锁不一致 |

## 9. 排查提交冲突

1. 从 Flink 日志区分 Snapshot ID 竞争和文件变更冲突。
2. 查看 `$snapshots`，确认相同时间有哪些 APPEND、OVERWRITE、COMPACT 提交。
3. 查看 Commit 指标中的 `lastCommitAttempts` 和 `lastCommitDuration`。
4. 列出所有活跃 Writer、Compactor、维护作业及其分区范围。
5. 核对这些作业使用的 Catalog、Warehouse 和锁配置。
6. 动态桶表确认同一分区是否被两个作业写入。
7. 反复重启时不要只增加 restart 次数；先移除持续制造冲突的并发关系。

## 10. 上线检查

- 为每个写作业写明允许修改的分区范围。
- 同分区多 Writer 优先使用固定桶，并验证 sequence/merge 语义。
- `write-only=true` 后明确唯一 Compactor 和 Snapshot 清理作业。
- 对象存储先验证原子提交与共享锁，不按 HDFS Rename 假设设计。
- Batch Overwrite、Rescale 和历史修复设置互斥窗口。
- 监控提交尝试次数、提交耗时、作业重启和 Compaction 积压。
- 故障演练至少包含两个 Writer 同时提交和 Compactor 冲突。

## 相关笔记

- [[Paimon 主键表建模与稳定性检查清单]]
- [[Paimon Snapshot、Manifest、Parquet 与 Compaction 的关系]]
- [[Paimon MOR、COW、MOW 与 Deletion Vector]]
- [[Paimon 系统表、监控指标与故障排查]]

## 官方资料

- [Concurrency Control](https://paimon.apache.org/docs/master/concepts/concurrency-control/)
- [Dedicated Compaction](https://paimon.apache.org/docs/master/maintenance/dedicated-compaction/)
- [Data Distribution](https://paimon.apache.org/docs/master/primary-key-table/data-distribution/)
- [Compaction](https://paimon.apache.org/docs/master/primary-key-table/compaction/)

> 核对日期：2026-09-16。原子提交和锁能力依赖实际 Catalog、文件系统实现及 Paimon 版本。
