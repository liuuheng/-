# Paimon 本地 SQL 验证实验手册

> 2026-11-07 更新实验范围说明：本目录保留基础 `00—06、99` 八个脚本，数据库为 `paimon_lab`；IDEA 学习项目另有 `00—22、99` 等脚本，数据库为 `paimon_sql_learning`。两套编号和数据库不同，不能直接互换。本文补充映射与历史验证记录，本次没有重跑集群。

这套实验用可重复执行的 SQL 回答四类问题：表模型怎样合并记录，版本对象怎样组织历史，批量 DML 怎样改变表状态，流式数据何时对外可见。每个脚本只负责一个主题；除元数据巡检和流式验证外，其余实验都可独立重跑。

## 环境与安全边界

- Flink `1.18.1`
- Paimon `1.3.1`（官方 `1.3` 文档当前展示 `1.3.2`）
- Catalog：filesystem
- Warehouse：`s3://paimon/warehouse`
- 对象存储：MinIO
- 实验数据库：`paimon_lab`
- 容器内脚本目录：`/opt/flink/sql/paimon-lab/`

所有持久化实验表都位于 `paimon_lab`，不会修改 `demo.dwd_withdraw`。各脚本开头的 `DROP TABLE IF EXISTS` 只用于保证本实验可重跑；执行前仍应确认该数据库没有同名自建表。

`05-paimon-streaming-writer.sql` 是持续运行的 Writer。重跑或清理前必须先取消旧作业，不能让旧 Writer 与新 Writer 同时操作 `streaming_events`。

## 文件职责与执行顺序

| 顺序 | 文件 | 要验证的问题 | 前置条件 |
| --- | --- | --- | --- |
| 00 | `00-paimon-table-models.sql` | Append Table 与主键表如何保存、合并记录 | 无 |
| 01 | `01-paimon-merge-engines.sql` | `partial-update`、`aggregation`、`first-row`、sequence-group 的差异 | 无 |
| 02 | `02-paimon-dml-overwrite.sql` | `UPDATE`、`DELETE`、动态覆盖和静态覆盖分别改动什么 | 无 |
| 03 | `03-paimon-versions.sql` | Schema、Snapshot、Tag、增量区间分别表示什么 | 无 |
| 04 | `04-paimon-metadata-inspection.sql` | 系统表如何连接逻辑结构、提交与物理文件 | 已运行 00、03 |
| 05 | `05-paimon-streaming-writer.sql` | Checkpoint 如何成为 Paimon Snapshot 的提交边界 | 无旧 Writer |
| 06 | `06-paimon-streaming-verify.sql` | 独立读者何时看到流式提交 | 05 正在运行且已有成功 Checkpoint |
| 99 | `99-paimon-cleanup.sql` | 清理本套实验对象 | 05 的作业已取消 |

批处理脚本中的自动检查统一返回 `check_name` 与 `check_result`。只有全部为 `PASS`，且人工查询的行内容与说明一致，才算实验通过。系统表中的行数多为物理文件统计，不能直接代替业务表 `COUNT(*)`。

## 00：Append Table 与主键表

执行：

```bash
docker exec jobmanager /opt/flink/bin/sql-client.sh \
  -f /opt/flink/sql/paimon-lab/00-paimon-table-models.sql
```

输入先向 `append_events` 写入两条完全相同的 `event_id=1`，再向 `pk_orders` 写入订单 `1001` 的版本 `1、3、2`。版本 `2` 虽然后到，但 `sequence.field=update_version` 使版本 `3` 胜出。

预期：

- `00_01_append_keeps_duplicates=PASS`：Append Table 共 4 行，`event_id=1` 保留 2 行。
- `00_02_sequence_resolves_disorder=PASS`：主键表共 2 行，订单 `1001` 为 `PAID/version=3`。
- `00_03_snapshot_time_travel=PASS`：Snapshot 1 有两张订单，订单 `1001` 仍为 `CREATED/version=1`。

这个实验区分三件事：业务字段相同不会让 Append Table 自动去重；主键定义逻辑合并范围；`sequence.field` 只在同一主键内部决定哪一版数据生效。

## 01：Merge Engine 与 sequence-group

```bash
docker exec jobmanager /opt/flink/bin/sql-client.sh \
  -f /opt/flink/sql/paimon-lab/01-paimon-merge-engines.sql
```

预期四个检查都为 `PASS`：

- `partial-update`：两条稀疏记录合成 `Alice / Shanghai / 90`，输入中的 `NULL` 默认不覆盖已有值。
- `aggregation`：`order-api` 的请求量累加为 `30`，最大延迟为 `180`。重复贡献会再次参与 `sum`，不能把它当成天然幂等的状态覆盖。
- `first-row`：用户 `101` 保留最先到达的 `app`，后到的 `web` 不覆盖；该模式不能配置 `sequence.field`。
- sequence-group：旧画像版本 `9` 不回滚姓名和城市，新积分版本 `101` 仍可更新积分，之后画像版本 `11` 独立生效。

选择规则取决于输入语义：完整状态覆盖用 `deduplicate`；多来源补列用 `partial-update`；贡献值累积用 `aggregation`；只认首次到达用 `first-row`。名称相似不构成可互换关系。

## 02：批量 DML 与覆盖范围

```bash
docker exec jobmanager /opt/flink/bin/sql-client.sh \
  -f /opt/flink/sql/paimon-lab/02-paimon-dml-overwrite.sql
```

`UPDATE`、`DELETE` 只在批模式执行，且 `UPDATE` 不能修改主键。脚本先把订单 `2001` 改成 `PAID/120`，删除订单 `2002`；随后分别验证两种分区覆盖。

- `02_01_batch_update_delete=PASS`：最终只剩订单 `2001、2003`。
- `02_02_dynamic_partition_overwrite=PASS`：输入只出现 `2026-09-27`，所以该分区被替换，`2026-09-28` 保留。
- `02_03_static_partition_purge=PASS`：关闭动态覆盖后，用空输入明确清空 `2026-09-27`，其他分区不变。

`mutable_orders$audit_log` 用 `rowkind` 展示增量记录。默认 `changelog-producer=none` 不承诺提供完整的 `-U/+U` 镜像，因此这条查询用于观察实际变化形状，不作为固定行数断言。

## 03：Schema、Snapshot、Tag 与增量

```bash
docker exec jobmanager /opt/flink/bin/sql-client.sh \
  -f /opt/flink/sql/paimon-lab/03-paimon-versions.sql
```

`schema_events` 先写旧结构数据，再增加 `source_system`。预期 `03_01_schema_evolution=PASS`：旧两行的新字段为 `NULL`，新行可写 `app`。这说明 Schema Evolution 建立字段映射，不代表旧数据文件已被改写。

`versioned_products` 在 Snapshot 1 建立 `baseline` Tag，改价并新增商品后建立 `after_price_change` Tag。预期：

- `03_02_latest_snapshot=PASS`：最新状态有 3 个商品，键盘价格为 `120`。
- `03_03_read_historical_state=PASS`：`baseline` 是完整历史状态，只有 2 个商品，键盘价格为 `100`。
- `incremental-between=baseline,after_price_change` 返回区间 `(start,end]` 的变化，不等于 end Tag 的完整表状态。
- `$tags` 应显示两个 Tag 绑定的 Snapshot；`scan.snapshot-id=1` 应与 `baseline` 的业务状态一致。

## 04：从系统表连接逻辑层和物理层

先完成 00、03，再执行：

```bash
docker exec jobmanager /opt/flink/bin/sql-client.sh \
  -f /opt/flink/sql/paimon-lab/04-paimon-metadata-inspection.sql
```

按结果顺序验证：

1. `DESCRIBE`、`SHOW CREATE TABLE` 和 `$options` 表示当前逻辑结构与显式参数；缺失参数通常表示使用版本默认值。
2. `schema_events$schemas` 应至少有两个 `schema_id`；每行是一份完整 Schema。
3. `pk_orders$snapshots` 表示成功提交历史；`snapshot_id` 不是 Flink Checkpoint ID，`commit_time` 也不是业务事件时间。
4. `$partitions`、`$files` 展示当前 Snapshot 引用的物理文件统计。Bucket 是分布单元，Level 是 LSM 层级，一个 Bucket 可有多个文件。
5. `$manifests` 记录数据文件引用的 ADD/DELETE；Manifest 文件不是业务数据文件。
6. `SELECT COUNT(*)` 经过 Merge-Read 得到逻辑行数，`total_record_count` 是未合并物理记录统计，两者允许不同。

### `$后缀` 是什么

`orders$snapshots` 这类名称由“业务表名 + `$` + 系统表类型”组成。它不是 warehouse 中另一张普通业务表，而是 Paimon 根据 `orders` 的元数据动态暴露的只读视图。Flink SQL 中应把包含 `$` 的完整名称放进反引号，例如 `` `orders$snapshots` ``；系统表使用批模式查询。

系统表描述的是不同层次，不能互相替代：`$schemas` 回答“结构是什么”，`$snapshots` 回答“哪个表版本已发布”，`$manifests` 回答“该版本通过哪些元数据文件引用 Data File”，`$files` 回答“最终引用哪些数据文件”，`$audit_log` 回答“增量记录是什么 RowKind”。

### `$options`、`$schemas`、`$snapshots`

| 后缀与字段 | 含义 | 不能据此推断 |
| --- | --- | --- |
| `$options.key` | DDL 中显式保存的参数名 | 未出现的参数并非不存在，可能使用默认值 |
| `$options.value` | 参数值的字符串表示 | 不代表值已经按目标数据类型解析 |
| `$schemas.schema_id` | Schema 版本号 | 不等于 Snapshot ID |
| `$schemas.fields` | 该版本完整字段定义，包含稳定字段 ID、名称和类型 | 不是本次只新增或删除的字段 |
| `$schemas.partition_keys` | 分区字段列表 | 不代表当前实际存在的分区值 |
| `$schemas.primary_keys` | 主键字段列表；Append Table 通常为空 | 不代表数据库会像 OLTP 一样强制唯一约束 |
| `$schemas.options` | 该 Schema 版本保存的显式表参数 | 不包含所有默认参数 |
| `$schemas.comment` | 该版本的表注释 | 不是提交说明 |
| `$schemas.update_time` | Schema 创建或更新时间 | 不是业务事件时间 |
| `$snapshots.snapshot_id` | 已发布表版本的标识；通常递增，过期后允许有缺口 | 不等于 Flink Checkpoint ID |
| `$snapshots.schema_id` | 该 Snapshot 使用的 Schema | 不表示本次一定发生了 Schema 变更 |
| `$snapshots.commit_user` | 提交者唯一标识 | 不一定是登录用户名 |
| `$snapshots.commit_identifier` | 同一提交者内部的提交标识 | 不应当作全局 Snapshot ID |
| `$snapshots.commit_kind` | `APPEND`、`COMPACT`、`OVERWRITE`、`ANALYZE` 等提交类型 | `COMPACT` 不表示新增业务事件 |
| `$snapshots.commit_time` | Snapshot 成功提交时间 | 不是行级事件时间 |
| `$snapshots.base_manifest_list` | 描述既有文件状态的 Manifest List 文件 | 不是历史事件全集 |
| `$snapshots.delta_manifest_list` | 描述本次文件 ADD/DELETE 的 Manifest List 文件 | 不是业务行级增量 |
| `$snapshots.changelog_manifest_list` | Changelog File 对应的 Manifest List；未生产时可为空 | 不保证一定有完整 Before/After |
| `$snapshots.total_record_count` | 当前 Snapshot 引用的数据文件物理记录总数 | 不等于主键 Merge-Read 后的 `COUNT(*)` |
| `$snapshots.delta_record_count` | 本次提交造成的物理记录净变化 | 不等于本次收到的业务事件数 |
| `$snapshots.changelog_record_count` | 本次 Changelog File 记录数 | 不代表 Changelog 一定完整 |
| `$snapshots.watermark` | 提交携带的事件时间 Watermark | 没有上游 Watermark 时可为空 |

### `$partitions`、`$buckets`、`$files`

| 后缀与字段 | 含义 |
| --- | --- |
| `$partitions.partition` | 分区字段值组成的 Row |
| `$partitions.record_count` | 该分区当前文件中的未合并物理记录数 |
| `$partitions.file_size_in_bytes` | 该分区有效文件总字节数 |
| `$partitions.file_count` | 该分区当前有效 Data File 数 |
| `$partitions.last_update_time` | 分区元数据最近更新时间，不是最大业务时间 |
| `$buckets.partition` | Bucket 所属分区 |
| `$buckets.bucket` | 逻辑 Bucket 编号；不是文件数量或 Writer 并行度 |
| `$buckets.record_count` | 该分区 Bucket 的物理记录数 |
| `$buckets.file_size_in_bytes` | 该分区 Bucket 的文件总字节数 |
| `$buckets.file_count` | 该分区 Bucket 的有效文件数 |
| `$buckets.last_update_time` | 该分区 Bucket 最近元数据更新时间 |
| `$files.partition` | Data File 所属分区 |
| `$files.bucket` | Data File 所属逻辑 Bucket |
| `$files.file_path` | Data File 路径或文件名 |
| `$files.file_format` | ORC、Parquet 等文件格式 |
| `$files.schema_id` | 写该文件时采用的 Schema |
| `$files.level` | LSM 层级；L0 键范围可重叠，高层文件通常由 Compaction 产生 |
| `$files.record_count` | 单个文件的物理记录数 |
| `$files.file_size_in_bytes` | 单个文件大小 |
| `$files.min_key` / `max_key` | 完整主键元组的排序边界，不是每个主键列的独立 Min/Max |
| `$files.null_value_counts` | 文件内各 Value 字段的 NULL 数统计 |
| `$files.min_value_stats` / `max_value_stats` | 各 Value 字段独立的文件级 Min/Max；缺失时应保守读取 |
| `$files.min_sequence_number` / `max_sequence_number` | 文件内 Sequence 值边界 |
| `$files.creation_time` | 文件创建时间，不是业务事件时间 |

`$files` 默认只列出所选 Snapshot 当前引用的文件。对象存储目录中还可能存在旧 Snapshot 或 Tag 引用的文件以及 Orphan File，所以目录 `LIST` 结果不能替代 `$files`。反过来，`$files` 只能证明具备哪些文件统计和索引条件，不能单独证明某条查询实际跳过了多少文件。

### `$manifests`、`$tags`、`$audit_log`、`$consumers`

| 后缀与字段 | 含义 |
| --- | --- |
| `$manifests.file_name` | Manifest 元数据文件名，不是业务 Data File |
| `$manifests.file_size` | Manifest 文件自身大小 |
| `$manifests.num_added_files` | 其中状态为 ADD 的数据文件条目数，不是新增业务行数 |
| `$manifests.num_deleted_files` | 其中状态为 DELETE 的文件条目数；表示从新表状态移除引用，不等于立即物理删除 |
| `$manifests.schema_id` | 写这些 Manifest 条目时关联的 Schema |
| `$manifests.min_partition_stats` / `max_partition_stats` | Manifest 覆盖的分区统计边界；一个 Manifest 可跨多个分区和 Bucket |
| `$tags.tag_name` | Tag 名称 |
| `$tags.snapshot_id` | Tag 固定的 Snapshot |
| `$tags.schema_id` | 该 Snapshot 使用的 Schema |
| `$tags.commit_time` | 被固定 Snapshot 的提交时间 |
| `$tags.record_count` | 该版本的数据文件记录统计，不是主键逻辑行数 |
| `$tags.branches` | Paimon 1.3.2 文档中的关联 Branch 列表；本地 1.3.1 是否存在以 `DESCRIBE` 为准 |
| `$audit_log.rowkind` | `+I` 插入、`-U` 更新前、`+U` 更新后、`-D` 删除 |
| `$audit_log` 其余字段 | 与原业务表字段一一对应，字段含义不变 |
| `$consumers.consumer_id` | 使用 `consumer-id` 的命名流式消费者 |
| `$consumers.next_snapshot_id` | 该消费者下一次应读取的 Snapshot；是读取位置，不是业务计数 |

系统表字段受版本和 Feature 影响，实验脚本先执行 `DESCRIBE`。如果本地 `1.3.1` 与官网 `1.3.2` 不一致，应保留本地输出并调整后续显式字段列表，不能用文档字段硬套运行结果。

下面这些 `$后缀` 已恢复到 `04-paimon-metadata-inspection.sql` 的对应说明旁。由于当前实验没有启用相关 Feature，或本地 `1.3.1` 尚未确认支持，查询默认保持注释；满足条件后先执行 `DESCRIBE`，再取消查询注释：

| 后缀 | 字段及含义 | 当前为什么不验证 |
| --- | --- | --- |
| `$binlog` | `rowkind` 表示 `+I/+U/-D`；其余列对应业务字段，UPDATE 时会把 Before/After 打包在同一列值中 | 需要能产生对应 Binlog 的表配置；计算列还有展示限制 |
| `$ro` | 与原业务表字段相同，没有额外元数据字段；只读取无需 Merge 的最高层文件 | 当前没有可控的 Full Compaction 边界，结果可能是跨 Bucket 的不同历史时点 |
| `$branches` | `branch_name` 是 Branch 名；`create_time` 是创建时间 | 当前没有创建 Branch |
| `$aggregation_fields` | `field_name` 字段名；`field_type` 类型；`function` 聚合函数；`function_options` 函数配置；`comment` 字段注释 | 当前 04 的前置实验没有依赖 aggregation 表，避免引入额外顺序依赖 |
| `$statistics` | `snapshot_id/schema_id` 标识统计版本；`mergedRecordCount/mergedRecordSize` 是合并后的记录规模；`colstat` 是列统计 | 需要显式统计收集流程，空统计无法说明机制 |
| `$table_indexes` | `partition/bucket` 定位范围；`index_type` 区分 HASH、DELETION_VECTORS；`file_name/file_size/row_count` 描述索引文件；`dv_ranges` 描述 DV 覆盖的数据文件范围 | 当前只使用 Fixed Bucket，未启用动态桶或 Deletion Vector |
| `$file_indexes`、`$file_key_ranges` | 用于检查 File Index 或文件主键范围；字段集合随版本变化 | Paimon 1.3 官方系统表页未把它们列为稳定通用接口，不能假定本地 1.3.1 一定存在 |

## 05—06：流式 Checkpoint 与可见 Snapshot

先检查旧作业：

```bash
docker exec jobmanager /opt/flink/bin/flink list -r
```

确认没有旧 `streaming_events` Writer 后，提交持续作业：

```bash
docker exec jobmanager /opt/flink/bin/sql-client.sh \
  -f /opt/flink/sql/paimon-lab/05-paimon-streaming-writer.sql
```

打开 `http://localhost:8081`，确认作业为 `RUNNING`，并在 Checkpoints 页面观察约每 10 秒一次成功 Checkpoint。另开终端执行只读验证：

```bash
docker exec jobmanager /opt/flink/bin/sql-client.sh \
  -f /opt/flink/sql/paimon-lab/06-paimon-streaming-verify.sql
```

间隔 10 秒以上重复执行 06。成功 Checkpoint 后，最大 `snapshot_id` 和已提交数据通常增长；Writer 已收到但尚未进入成功 Checkpoint 的记录，对独立批读不可见。随机 `event_id` 会制造少量重复主键，所以逻辑行数不要求等于 `total_record_count`。

结束实验时先列出作业并取消目标 Job：

```bash
docker exec jobmanager /opt/flink/bin/flink list -r
docker exec jobmanager /opt/flink/bin/flink cancel <JobID>
```

## 99：清理

只有确认流式 Writer 已停止后才执行：

```bash
docker exec jobmanager /opt/flink/bin/sql-client.sh \
  -f /opt/flink/sql/paimon-lab/99-paimon-cleanup.sql
```

脚本逐个删除本套实验创建的 11 张 Paimon 表和 SQL Client 默认 Catalog 中的 DataGen 源表，不删除 `paimon_lab` 数据库，不使用 `CASCADE`，也不清理整个 warehouse。

## 知识点覆盖检查

当前 8 个脚本覆盖了可在现有单机 Flink SQL 环境中稳定复现的基础链路：

| 知识点 | 实验 |
| --- | --- |
| Append Table、主键表、Fixed Bucket、分区主键约束 | 00 |
| `deduplicate`、`sequence.field`、Snapshot 完整状态 | 00 |
| `partial-update`、`aggregation`、`first-row`、sequence-group | 01 |
| `UPDATE`、`DELETE`、RowKind、动态/静态覆盖 | 02 |
| Schema Evolution、Tag、时间旅行、批增量区间 | 03 |
| `$options/$schemas/$snapshots/$partitions/$buckets/$files/$manifests/$consumers`，以及其他条件式 `$后缀` | 04 |
| Checkpoint 提交、Snapshot 可见性、独立读者验证 | 05、06 |
| 对象级清理边界 | 99 |

### 学习项目已有的扩展实验

此前把下列主题列为后续实验，是针对本目录八个基础脚本的判断。2026-10-04 学习项目已扩展，不应继续把它们全部当作“尚无实验”。项目位置为 `/Users/nanan/Desktop/bigdata-projects/bigdata240620/Flink/sql/paimon-learning`；该目录的 README 和具体脚本共同说明验证范围。

| 主题 | 项目脚本编号 | 已有记录能够证明什么 | 尚不能证明什么 |
| --- | --- | --- | --- |
| RowKind 与 Changelog Producer | 09 | input/lookup/full-compaction 可建表并得到预期最终状态，另有 Audit Log 观察查询 | 持续下游对所有更新、删除和聚合的处理均正确 |
| MOR/COW/MOW、DV 与 Compaction | 10 | 相应表配置、逻辑结果、Level 和 DV 元数据可观察 | 大数据量下的性能、成本和长期稳定性 |
| 动态桶、跨分区 Upsert、Bootstrap | 11、16 | HASH 索引、分区迁移、Bootstrap 参数与 Snapshot Ordering 的指定用例 | 大规模恢复耗时、TTL 之外历史更新的完整正确性 |
| File Index、Snapshot 与 Tag | 12 | 三类索引配置、补建、查询结果、普通 Snapshot 过期与 Tag 保留 | 实际扫描裁剪效果；Orphan 物理清理仅有模板 |
| 持续 Writer 与命名 Consumer | 07、08、13、14 | Writer/Reader 成功 Checkpoint，Consumer 进度可查询 | 停机超出保留期或旧 Savepoint 的恢复结果 |
| 生命周期与写缓冲 | 16 | 参数可配置、自动 Tag 生成 | 自然分区过期、压力下 Spill 和磁盘峰值 |
| 多 Writer 与独立 Compactor | 17—21 | 不同分区的两个 Writer 与 Compactor 可协作，指定结果通过 | 同分区同键胜出、文件冲突与完整重试行为 |
| 外部 CDC 与 Catalog | 15、22 | 目标 DDL 或安全 SET 已检查；外部命令与 DDL 保持模板 | 真实 MySQL/Kafka/Pulsar、Hive/JDBC/REST 与恢复已接通 |

这里的“已有记录”来自项目 README 的 2026-10-04 记录和当前脚本内容，本次未再次运行。09 的 PASS 主要检查最终状态，12 的索引查询主要检查结果；25 个 PASS 不能扩展成全部生产语义或性能已经通过。

### 后续演练应补的证据

| 待验证问题 | 现有起点 | 本次补充的说明 |
| --- | --- | --- |
| Checkpoint 故障与旧 Savepoint 恢复 | 07、13 和恢复说明 | [[20-Paimon Checkpoint、Savepoint 与一致性恢复]] |
| Changelog 下游 SUM/COUNT 与删除 | 09 的结果断言与 Audit Log | [[21-Paimon Changelog 下游聚合与消费恢复验证]] |
| 慢 Consumer、独立 Changelog 保留与安全清理 | 12、16 的生命周期模板 | [[22-Paimon Snapshot 回滚、Branch 与安全文件清理]] |
| 同分区同键、Compaction/Overwrite 竞争与 Rescale | 17—21 的不同分区实验 | [[23-Paimon 多 Writer 冲突验证与 Bucket 扩缩容]] |
| 实际裁剪、读写放大、资源和积压收敛 | 12 的索引与 16 的参数 | [[24-Paimon 查询裁剪、Compaction 与容量调优]] |
| 外部 Catalog、多引擎读取与数据对账 | 15、22 的外部模板 | [[25-Paimon 多引擎读写、Catalog 与存储部署]] |
| MySQL 初始快照、DDL 与恢复 | 15、22 的 CDC 模板 | [[05-Paimon CDC 入湖、Schema Evolution 与恢复语义]] |

这些新增文章给出机制、操作前提和判据，均不是新的集群实测记录。恢复、并发和清理演练使用独立库与 Warehouse，并保留输入及预期，不在已有实验表上直接执行破坏性操作。

## 验证状态与版本边界

2026-09-27 的原始实测记录保留如下：

- 批处理核心脚本的 6 个断言全部返回 `PASS`。
- `pk_orders` 最终逻辑行数为 2，最新 Snapshot 的物理 `total_record_count` 为 3，验证了“物理记录数不等于主键合并后的逻辑行数”。
- 流式作业连续完成 8 次 Checkpoint，失败数为 0，最新 Checkpoint 写入 `s3://paimon/flink-checkpoints/.../chk-8`。
- 批量读在某一时刻看到 190 行已提交数据；`$snapshots` 随后的查询已经看到 APPEND 提交到 210 条物理记录，中间还出现 COMPACT。两条查询取样时刻不同，不是数据丢失。
- 验证结束后已取消 DataGen 作业；当时 JobManager、两个 TaskManager 和 MinIO 仍保持运行，实验表数据保留。

2026-09-29 的重构保留了原数据与断言语义，但重新拆分文件，并新增 Merge Engine、DML、Tag、增量读取和系统表字段说明。

上段描述的是 2026-09-29 基础脚本重构时的状态：当时按 Paimon `1.3` 官方文档静态核对，未逐个重跑本目录八个脚本。不能把它理解为后续学习项目也一直没有执行。

2026-10-04 的学习项目记录包含 25 个 PASS，以及 Writer/Reader、双 Writer 和独立 Compactor 的成功 Checkpoint；外部 CDC、外部 Catalog、同分区冲突与恢复流程仍未验证。该记录对应 `paimon_sql_learning`，不追认本目录不同编号、不同库的脚本全部已经重跑。

2026-11-07 本次仅对照项目 README 与 SQL 回填进度，没有启动或取消 Flink 作业，没有重新执行断言，也没有清理实验数据。官网 `1.3` 分支展示的补丁版本与本地 `1.3.1` 仍需区分；实际系统表字段和过程签名以本地连接器为准。

## 建议独立完成的验证题

这些问题来自重构前手册，保留用于确认是否真正理解实验现象：

1. 删除 `pk_orders` 的 `sequence.field` 后重新制造乱序数据，结果是否还稳定？为什么？
2. 对同一主键重复写入相同 `request_count` 贡献值，`aggregation` 是否具备幂等性？
3. 对比 `pk_orders$snapshots.total_record_count` 与 `SELECT COUNT(*)`，解释两者不同的原因。
4. 增加写入次数后查询 `$files`，观察 L0 文件是否增加，以及 Compaction 后如何变化。
5. 在 Snapshot 1 和最新 Snapshot 中分别查询订单 `1001`，解释“历史完整状态”和“增量事件”的区别。
6. 把 `first_seen_users` 改成 `deduplicate` 后重跑，用户 `101` 为什么会变成 `web`？
7. 将 `profile_sequence_groups` 的两个 sequence-group 改成一个全局 `sequence.field`，两路独立更新会怎样互相阻塞？
8. 比较 `scan.tag-name=baseline` 与 `incremental-between=baseline,after_price_change` 的结果集，指出“历史状态”和“区间变化”的区别。

## 官方资料

- [Paimon 1.3 Primary Key Table](https://paimon.apache.org/docs/1.3/primary-key-table/overview/)
- [Paimon 1.3 Partial Update](https://paimon.apache.org/docs/1.3/primary-key-table/merge-engine/partial-update/)
- [Paimon 1.3 First Row](https://paimon.apache.org/docs/1.3/primary-key-table/merge-engine/first-row/)
- [Paimon 1.3 SQL Write](https://paimon.apache.org/docs/1.3/flink/sql-write/)
- [Paimon 1.3 SQL Query](https://paimon.apache.org/docs/1.3/flink/sql-query/)
- [Paimon 1.3 Manage Tags](https://paimon.apache.org/docs/1.3/maintenance/manage-tags/)
- [Paimon 1.3 System Tables](https://paimon.apache.org/docs/1.3/concepts/system-tables/)

## 相关笔记

- [[02-Apache Paimon 表模型、存储组织与读取语义]]
- [[10-Paimon Sequence、RowKind 与删除语义]]
- [[12-Paimon Snapshot、Manifest、Parquet 与 Compaction 的关系]]
- [[18-Paimon 系统表、监控指标与故障排查]]
