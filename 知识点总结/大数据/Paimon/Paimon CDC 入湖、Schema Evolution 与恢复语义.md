# Paimon CDC 入湖、Schema Evolution 与恢复语义

## 结论

CDC 入湖是否正确，要同时检查四层：Source 是否读取了完整变更，传输格式是否保留主键和 Before/After，Paimon Sink 如何提交，目标表的 Merge Engine 如何解释重复、乱序和删除。

Paimon CDC Action 可以自动建表并执行一部分 Schema Evolution，但它不是数据库 DDL 的完整复制工具。新增列和兼容的类型扩大可以传播；删除列、重命名列、重命名表、主键和分区键变更不能按“源端改了，目标自动完全一致”理解。

## 1. 先区分四条入湖路径

| 路径 | Schema 从哪里来 | 适用场景 | 需要特别检查 |
| --- | --- | --- | --- |
| Flink SQL `INSERT INTO` | SQL 中声明 | 自定义转换、已有 Flink 作业 | 源新增列不会自动改 SQL 和目标表 |
| Paimon CDC Action | Source 元数据与事件 | 单表或整库同步 | Action 参数、路由、支持的演进范围 |
| Flink CDC YAML Pipeline | Pipeline 定义与 Connector | Flink CDC Pipeline 体系 | 使用该体系自己的 Schema Change 规则 |
| Kafka/Pulsar CDC Action | 消息格式中的 Schema 和变更 | 已有 CDC 消息总线 | Format 是否真的携带类型、主键和删除 |

Paimon Action 的 `--table_conf`、`--catalog_conf` 等参数属于 Paimon Action CLI，不能原样当作 Flink CDC YAML 的配置。普通 JSON 通常只表达 Insert-only；Canal、Debezium、Maxwell、OGG 等格式能表达哪些变更，也取决于消息实际包含的字段。

## 2. 初始快照与增量日志如何衔接

数据库 CDC Source 通常先读取已有数据，再持续读取 binlog/WAL。正确衔接要求 Source 在一致性边界上记录日志位点，并把位点纳入 Flink checkpoint。Paimon 不会在 Sink 端补救 Source 漏读的日志区间。

验证时不要只看“初始数据条数正确”。至少要覆盖：初始快照期间发生 UPDATE/DELETE、Source 切换到增量后是否重复或漏失、故障恢复是否从 checkpoint 位点继续。

Source 侧的一致性快照算法、启动模式和 Connector 版本属于 Flink CDC 或源 Connector 的能力，不由 Paimon 表参数决定。Paimon 负责接收已经产生的变更并在 checkpoint 成功时提交 Snapshot。

## 3. Checkpoint 与 Paimon Snapshot

Flink 流式写 Paimon 时，完成的 checkpoint 驱动提交，数据只有进入已提交的 Paimon Snapshot 后才对普通读取可见。看到 Source 已消费某个 binlog offset，不代表相应数据已经提交到表；还要检查 checkpoint 和 `$snapshots`。

发生失败时，Flink 从最近成功 checkpoint 恢复 Source、算子状态和 Sink 事务边界。未完成 checkpoint 对应的数据可能被重新处理。对于 `deduplicate` 表，如果同一主键、sequence 和最终行相同，重放通常能被覆盖；对于 `aggregation`，重复贡献可能再次累加，主键不会自动按事件 ID 去重。

所以“Flink exactly-once”不等于所有业务 Merge Engine 都天然幂等。要区分：传输与提交没有丢失/重复提交，以及同一业务事件被源端重复产生后目标结果是否仍然正确。

## 4. CDC 事件是否真的完整

检查实际记录，而不是根据“CDC”标签推断：

- INSERT 是否包含完整新行。
- UPDATE 是否包含 `-U/+U`，还是只有更新后镜像。
- Before/After 是完整行，还是仅包含变化字段。
- DELETE 是否包含完整旧行，还是只有主键。
- 事件是否携带可比较的 binlog offset、LSN 或源端版本。
- DDL 和数据事件是否共用同一条有序通道。

完整 `RowKind` 不代表完整字段镜像。只有主键的 DELETE 对 `deduplicate` 可能足够定位记录，但 partial-update、aggregation、跨分区更新或基于 sequence 的乱序判断可能需要更多字段和版本信息。

## 5. Schema Evolution 能做什么

Paimon CDC Action 会比较输入 Schema 与目标 Schema，对支持的变化修改目标表：

| 源端变化 | Paimon 目标行为 |
| --- | --- |
| 新增列 | Source 能把 Schema 传到 Sink 时，新增目标字段 |
| 字符串、二进制长度扩大 | 扩大目标类型 |
| 整数或浮点类型扩大 | 在同类型族内扩大，如 `INT` 到 `BIGINT` |
| Decimal、时间精度变化 | 兼容且通过 Paimon 校验时演进 |
| 非字符串改字符串 | 默认禁用，需要 `allow-non-string-to-string` |
| 删除列 | 目标旧列保留 |
| 重命名列 | 新名称按新列处理，旧列保留 |
| 重命名表 | 不自动重命名原 Paimon 表 |

类型缩小不会自动把已有目标列缩小；不兼容的变化可能使作业失败，不能假设 Sink 会静默忽略。

`to-string` 影响初始类型映射，`allow-non-string-to-string` 允许已有列后续演进到字符串，两者不是同一个开关。

## 6. 主键和分区键不是普通列演进

Schema Evolution 不负责迁移主键和分区键。目标表已存在时，Action 会基于目标 Schema 和参数检查兼容性；需要改变主键、分区键或 Bucket 布局时，通常要建新表、重写数据并切换作业。

如果分区键来自 computed column，例如从 `create_time` 计算 `dt`，还要确认时区、格式和空值行为。源字段类型变化可能同时影响路由到哪个分区，不能只检查目标列能否新增。

## 7. 多表合并与路由

多个源表或分表可以同步到同一张 Paimon 表，但它们的 Schema 必须兼容，目标主键必须在所有来源之间唯一。

假设 `shop_1.orders` 和 `shop_2.orders` 都有 `id=100`。如果目标主键只有 `id`，两条记录会被当作同一逻辑行互相覆盖。应把来源标识加入目标主键，或确保源端主键已经全局唯一。

整库同步保留多张目标表时，要明确表名映射、包含/排除规则以及新表发现机制。源表重命名后是否继续被路由，取决于 Source 选择与映射规则，不等于原目标表被自动 Rename。

## 8. Merge Engine 与 CDC 的匹配

- 上游每次提供完整最终行：通常使用 `deduplicate`，并用可靠 sequence 处理乱序。
- 上游只提供变化字段：使用 partial-update 前确认 `NULL` 是“不更新”还是“清空”，并设计 sequence group。
- 上游字段代表可加减贡献：才使用 aggregation，同时处理重放和 retract。
- 只保留第一次到达：使用 first-row，但它不是按事件时间取最早记录。

存在流式下游时，还要单独选择 Changelog Producer。只有 Writer 实际收到的记录已经是完整最终变化，`input` 才合适；partial-update 或 aggregation 的合并后完整行通常需要 `lookup` 或 `full-compaction`。

## 9. 恢复测试

上线前至少执行这些故障用例：

1. 初始快照中途杀掉作业，从 checkpoint 恢复后核对行值和主键唯一性。
2. 在快照切换增量期间持续 UPDATE/DELETE，确认没有日志空洞。
3. Sink 写完但 checkpoint 未完成时故障，确认重放不会造成聚合重复。
4. 新增列后恢复旧 checkpoint，确认 Source 状态和目标 Schema 兼容。
5. 发送乱序 UPDATE 与迟到 DELETE，确认 sequence 能阻止旧事件覆盖新状态。
6. 多表合并制造相同源主键，验证是否发生目标主键冲突。

排查顺序是：Source offset 与反序列化日志、Flink checkpoint、Paimon `$snapshots`、目标 Merge Engine 结果。只查最终表很难判断问题发生在哪一层。

## 10. 上线检查

- 固定并记录 Source Connector、Flink、Paimon 的兼容版本。
- 明确启动模式和初始快照期间的资源需求。
- 抽样保存真实 INSERT、UPDATE、DELETE 事件，确认 RowKind 和字段镜像。
- 定义主键在分表、租户和数据库之间是否全局唯一。
- 把每种 DDL 变化分成“自动支持、作业失败、人工迁移”三类。
- 给 aggregation 等非幂等语义增加事件级去重或对账。
- 同时监控 checkpoint 成功时间和 Paimon Snapshot 提交时间。

## 相关笔记

- [[Paimon Sequence、RowKind 与删除语义]]
- [[Paimon 流式读写中的 Snapshot 与 Changelog]]
- [[Paimon 主键表建模与稳定性检查清单]]
- [[Paimon 系统表、监控指标与故障排查]]

## 官方资料

- [CDC Ingestion](https://paimon.apache.org/docs/master/cdc-ingestion/)
- [Schema Evolution and Type Mapping](https://paimon.apache.org/docs/master/cdc-ingestion/schema-evolution/)
- [Kafka CDC](https://paimon.apache.org/docs/master/cdc-ingestion/kafka-cdc/)
- [Sequence and Row Kind](https://paimon.apache.org/docs/master/primary-key-table/sequence-rowkind/)

> 核对日期：2026-09-16。Source 的增量快照与恢复细节还应以实际 Flink CDC/Connector 版本文档为准。
