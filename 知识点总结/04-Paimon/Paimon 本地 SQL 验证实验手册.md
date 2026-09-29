# Paimon 本地 SQL 验证实验手册

## 目标与环境

这套实验用于把现有 Paimon 笔记中的概念落实为可重复执行的 SQL。当前本地环境为：

- Flink `1.18.1`
- Paimon `1.3.1`
- Paimon Catalog：filesystem
- Warehouse：`s3://paimon/warehouse`
- 对象存储：MinIO
- 实验数据库：`paimon_lab`

所有持久化 Paimon 实验表都位于独立数据库，不修改现有的 `demo.dwd_withdraw`。流式脚本还会在 SQL Client 会话的 `default_catalog.default_database` 中创建 DataGen 源表。

## 文件

- `实验/00-paimon-batch-core.sql`：核心表模型、乱序更新、时间旅行、Schema Evolution。
- `实验/01-paimon-metadata-inspection.sql`：系统表、Snapshot、Manifest、File、Bucket、Level。
- `实验/02-paimon-streaming-checkpoint.sql`：流式写入、Checkpoint 与 Snapshot。
- `实验/03-paimon-cleanup.sql`：逐表清理本套实验数据。
- `实验/04-paimon-streaming-verify.sql`：从独立批查询会话验证流式可见数据和 Snapshot。
- `实验/05-paimon-merge-engine-advanced.sql`：`first-row` 与 `partial-update sequence-group`。
- `实验/06-paimon-dml-overwrite.sql`：批量 `UPDATE`、`DELETE`、Audit Log 与动态/静态分区覆盖。
- `实验/07-paimon-tag-incremental.sql`：Tag、按 Tag/快照时间旅行、增量扫描与 Audit Log。

部署到本地集群后，对应容器内目录为：

```text
/opt/flink/sql/paimon-lab/
```

## 一、运行批处理核心实验

在 Mac 终端执行：

```bash
docker exec jobmanager \
  /opt/flink/bin/sql-client.sh \
  -f /opt/flink/sql/paimon-lab/00-paimon-batch-core.sql
```

脚本中的检查项应全部返回 `PASS`：

| 检查项 | 验证知识点 |
| --- | --- |
| `01_append_keeps_duplicates` | Append Table 保存重复事件，不按业务字段去重 |
| `02_pk_sequence_deduplicate` | 主键表按主键合并，`sequence.field` 决定新旧顺序 |
| `03_snapshot_1_time_travel` | 历史 Snapshot 是历史完整表状态，不只是一次增量 |
| `04_partial_update` | `partial-update` 将不同记录中的非空字段合并 |
| `05_aggregation_sum_max` | `aggregation` 对贡献值执行 `sum`、`max` |
| `06_schema_evolution` | 新增字段后，旧数据该字段为 `NULL`，新数据可写入 |

脚本会先删除同名实验表再重建，因此可以重复运行。删除范围仅限 `paimon_lab` 数据库中的四张实验表。

本实验的 Append Table 使用固定桶，因此同时设置了 `bucket = 2` 和 `bucket-key = event_id`。在 Paimon 1.3.1 中，固定桶 Append Table 不能只设置正数 `bucket` 而省略 `bucket-key`。

## 二、运行高级表模型实验

```bash
docker exec jobmanager \
  /opt/flink/bin/sql-client.sh \
  -f /opt/flink/sql/paimon-lab/05-paimon-merge-engine-advanced.sql
```

该脚本补充两个容易被普通 Upsert 表掩盖的 Paimon 语义：

1. `merge-engine = first-row` 保留同一主键最先到达的数据；它与默认 `deduplicate` 的后到覆盖先到相反，不能同时配置 `sequence.field`。
2. `fields.<sequence-field>.sequence-group` 为 `partial-update` 的不同字段组分别维护版本顺序。画像流的旧版本不会回滚姓名和城市，同时不妨碍更高版本的积分流更新积分。

检查项 `07_first_row_keeps_earliest_arrival` 和 `08_partial_update_sequence_groups` 应返回 `PASS`。

## 三、运行批量 DML 与分区覆盖实验

```bash
docker exec jobmanager \
  /opt/flink/bin/sql-client.sh \
  -f /opt/flink/sql/paimon-lab/06-paimon-dml-overwrite.sql
```

这组 SQL 展示三种特殊边界：

- `UPDATE`、`DELETE` 只能在批模式执行；`UPDATE` 不能修改主键，且 Merge Engine 必须支持相应变更语义。
- `$audit_log` 在业务字段之前增加 `rowkind`，用于审计实际产生的 `+I`、`-U`、`+U`、`-D`。默认 `changelog-producer = none` 不保证提供 UPDATE 的旧值 `-U`；普通批查询也不会把删除记录作为结果行返回。
- `INSERT OVERWRITE` 对分区表默认使用动态分区覆盖，只替换输入中实际出现的分区。通过 `/*+ OPTIONS('dynamic-partition-overwrite' = 'false') */` 可只对当前语句切换为静态覆盖；配合空结果可清空指定分区。

检查项 `09_batch_update_delete`、`10_dynamic_partition_overwrite`、`11_static_partition_purge` 应返回 `PASS`。

## 四、运行 Tag 与增量读取实验

```bash
docker exec jobmanager \
  /opt/flink/bin/sql-client.sh \
  -f /opt/flink/sql/paimon-lab/07-paimon-tag-incremental.sql
```

`CALL sys.create_tag` 给当前 Snapshot 建立长期可读的名字。`scan.tag-name` 和 `scan.snapshot-id` 读取的是该版本的完整表状态；`incremental-between = start,end` 读取的是 `(start, end]` 之间的变化，两者不能混为一类查询。

脚本同时查询 `$tags` 查看 Tag 与 Snapshot 的绑定，并用 `$audit_log` 展开增量的 RowKind。检查项 `12_latest_snapshot`、`13_read_by_tag` 应返回 `PASS`。

以上三个进阶脚本已按 Paimon 1.3 文档核对语法，但尚未在当前本地集群执行。若本地 `1.3.1` 与官网当前 `1.3.2` 补丁版本存在行为差异，以实际 SQL Client 报错和连接器版本为准。

## 五、巡检 Paimon 元数据

```bash
docker exec jobmanager \
  /opt/flink/bin/sql-client.sh \
  -f /opt/flink/sql/paimon-lab/01-paimon-metadata-inspection.sql
```

重点理解四组结果：

1. `DESCRIBE` 和 `SHOW CREATE TABLE` 展示当前逻辑结构与显式表参数。
2. `$schemas` 展示 Schema 历史；`ALTER TABLE` 不会改写旧数据文件。
3. `$snapshots` 展示提交历史；Snapshot 通过 Manifest 引用一组有效文件。
4. `$files` 展示物理文件、Bucket 和 Level；主键表的物理记录数不等于最终逻辑行数。

`$options` 只展示显式设置的参数。没有出现的参数并不代表不存在，而是使用当前 Paimon 版本的默认值。

## 六、观察流式 Checkpoint 与 Snapshot

重跑本实验前，先用 `flink list -r` 确认不存在旧的 `streaming_events` 写入作业；如果存在，必须先取消。否则脚本重建表后，旧 Writer 和新 Writer 可能同时操作同一路径。

在 Mac 终端直接提交流式 SQL 作业：

```bash
docker exec jobmanager \
  /opt/flink/bin/sql-client.sh \
  -f /opt/flink/sql/paimon-lab/02-paimon-streaming-checkpoint.sql
```

然后观察：

1. 打开 `http://localhost:8081`，确认作业处于 `RUNNING`。
2. 进入作业的 Checkpoints 页面，确认约每 10 秒产生一次成功 Checkpoint。
3. 另开一个终端执行验证脚本：

```bash
docker exec jobmanager \
  /opt/flink/bin/sql-client.sh \
  -f /opt/flink/sql/paimon-lab/04-paimon-streaming-verify.sql
```

预期现象：成功 Checkpoint 后出现新的 Paimon Snapshot，表中可见行数持续增长。Writer 已接收记录但 Checkpoint 尚未成功时，新数据不会成为可见的已提交 Snapshot。

DataGen 的 `event_id` 使用 `random(1..100000)`，而不是超大范围的 `sequence`。Flink 1.18 的 `SequenceGenerator` 会为序列范围维护内部状态；范围过大会在当前小内存 TaskManager 中触发 OOM。随机 ID 还会自然制造少量重复主键，用于观察 `deduplicate` 更新。

实验结束后，在 Flink Web UI 取消作业，或者执行：

```bash
docker exec jobmanager /opt/flink/bin/flink list -r
docker exec jobmanager /opt/flink/bin/flink cancel <JobID>
```

## 七、本地实测结果（2026-09-27）

- 批处理脚本的 6 个断言全部返回 `PASS`。
- `pk_orders` 最终逻辑行数为 2，最新 Snapshot 中物理 `total_record_count` 为 3，验证了主键表“物理记录不等于逻辑最终状态”。
- 流式作业连续完成 8 次 Checkpoint，失败数为 0，最新 Checkpoint 写入 `s3://paimon/flink-checkpoints/.../chk-8`。
- 批量读在某一时刻看到 190 行已提交数据；`$snapshots` 同时显示后续 APPEND 已提交到 210 条物理记录，中间还有 COMPACT 提交。这个短暂差异是持续写入期间两个查询的取样时刻不同，不是数据丢失。
- 验证结束后已取消 DataGen 作业；JobManager、两个 TaskManager 和 MinIO 均保持运行，实验表数据保留。

## 八、清理实验数据

确认流式实验作业已经停止，再执行：

```bash
docker exec jobmanager \
  /opt/flink/bin/sql-client.sh \
  -f /opt/flink/sql/paimon-lab/03-paimon-cleanup.sql
```

该脚本只逐个删除本套实验创建的 10 张 Paimon 表，不会删除 `demo.dwd_withdraw`。它不使用 `CASCADE`，并保留 `paimon_lab` 数据库本身；你之后加入的其他对象也不会被清理。

## 九、建议你独立完成的验证题

1. 删除 `pk_orders` 的 `sequence.field` 后重新制造乱序数据，结果是否还稳定？为什么？
2. 对同一主键重复写入同一个 `request_count` 贡献值，`aggregation` 是否具备幂等性？
3. 对比 `pk_orders$snapshots.total_record_count` 与 `SELECT COUNT(*)`，解释两者不同的原因。
4. 增加写入次数后查询 `$files`，观察 Level 0 文件是否增加，以及 Compaction 后如何变化。
5. 在 Snapshot 1 和最新 Snapshot 中分别查询订单 `1001`，解释“历史完整状态”和“增量事件”的区别。
6. 把 `first_seen_users` 改成 `deduplicate` 后重跑，用户 `101` 为什么变成 `web`？
7. 将 `profile_sequence_groups` 的两个 Sequence Group 合并成一个全局 `sequence.field`，两路独立更新会发生什么？
8. 比较 `scan.tag-name = baseline` 与 `incremental-between = baseline,after_price_change` 的结果集，指出“历史状态”和“区间变化”的区别。

## 官方资料

- [Paimon 1.3 Flink Quick Start](https://paimon.apache.org/docs/1.3/flink/quick-start/)
- [Paimon 1.3 Primary Key Table](https://paimon.apache.org/docs/1.3/primary-key-table/overview/)
- [Paimon 1.3 Merge Engine](https://paimon.apache.org/docs/1.3/primary-key-table/merge-engine/)
- [Paimon 1.3 First Row](https://paimon.apache.org/docs/1.3/primary-key-table/merge-engine/first-row/)
- [Paimon 1.3 Partial Update](https://paimon.apache.org/docs/1.3/primary-key-table/merge-engine/partial-update/)
- [Paimon 1.3 Changelog Producer](https://paimon.apache.org/docs/1.3/primary-key-table/changelog-producer/)
- [Paimon 1.3 Flink SQL Write](https://paimon.apache.org/docs/1.3/flink/sql-write/)
- [Paimon 1.3 Flink SQL Query](https://paimon.apache.org/docs/1.3/flink/sql-query/)
- [Paimon 1.3 Manage Tags](https://paimon.apache.org/docs/1.3/maintenance/manage-tags/)
- [Paimon 1.3 System Tables](https://paimon.apache.org/docs/1.3/concepts/system-tables/)

## 相关笔记

- [[Apache Paimon 表模型、存储组织与读取语义]]
- [[Paimon Sequence、RowKind 与删除语义]]
- [[Paimon Snapshot、Manifest、Parquet 与 Compaction 的关系]]
- [[Paimon 系统表、监控指标与故障排查]]
