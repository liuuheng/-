# Paimon Sequence、RowKind 与删除语义

## 核心结论

Paimon 主键表是否能得到正确结果，取决于三个彼此独立的问题：

1. `sequence`：同一主键的多条记录冲突时，哪一条更新应该获胜。
2. `RowKind`：当前记录表示写入、更新还是撤回。
3. `merge-engine`：收到这条变更后，如何计算表中的最终逻辑行。

不能只看到 CDC、主键或 `RowKind` 就判断语义完整。`RowKind` 完整不代表字段镜像完整；字段完整不代表带有旧值；既有新旧值，也不代表输入已经是 merge 后的最终逻辑行。

## 1. 三层语义如何配合

假设同一主键先后到达两条记录：

| 记录 | sequence | RowKind | 含义 |
| --- | ---: | --- | --- |
| A | 10 | `+U` | 版本 10 的更新后镜像 |
| B | 9 | `-D` | 版本 9 的删除 |

`RowKind` 说明 B 是删除，但 `sequence` 说明它比 A 旧。对于按 sequence 去重的表，旧删除不应覆盖新更新。随后，`merge-engine` 决定“保留整行”“只更新非空字段”或“把数值当作增量累加”。

因此，排查错误结果时要按顺序检查：实际输入记录是什么、记录如何排序、merge engine 如何解释记录。三者中任意一层与上游语义不一致，都可能产生旧值覆盖新值、误删、重复累加或字段无法清空。

## 2. `sequence.field`：决定更新顺序

主键表默认依据内部记录顺序解决同一主键的冲突。如果上游可能乱序、并发写入或重放，仅依赖到达顺序通常不够稳定。可以把源端版本号、binlog position 或业务更新时间声明为 sequence：

```sql
CREATE TABLE order_status (
    order_id       BIGINT,
    status         STRING,
    source_version BIGINT,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'sequence.field' = 'source_version'
);
```

默认使用升序比较，较大的 sequence 获胜。只有源字段的顺序含义相反时，才配置 descending。sequence 必须表达同一主键内可靠的先后关系；处理时间只能表达“谁先被当前任务看到”，不能自动代表源端事件顺序。

多个 sequence 字段按声明顺序进行字典序比较。它适合给相同业务时间补充稳定的次级排序条件：

```sql
'sequence.field' = 'update_time,binlog_offset'
```

如果所有 sequence 字段都相同，Paimon 仍需退回内部记录顺序。因此，同一主键可能出现相同时间戳时，应补充能唯一确定顺序的 offset、版本号等字段。

`sequence.field` 不能与 `first-row`、跨分区更新或 `sequence.snapshot-ordering` 组合使用。建表前还要以当前 Paimon 版本文档校验字段类型和 merge engine 限制。

## 3. `RowKind`：说明这条记录要做什么

Flink/Paimon 常见的四种 `RowKind`：

| RowKind | 简写 | 语义 |
| --- | --- | --- |
| INSERT | `+I` | 插入 |
| UPDATE_BEFORE | `-U` | 更新前镜像，撤回旧值 |
| UPDATE_AFTER | `+U` | 更新后镜像，加入新值 |
| DELETE | `-D` | 删除 |

如果变更类型存放在普通字段中，可以用 `rowkind.field` 将字符串解码为 RowKind。该字段必须是非空字符串，值使用 `+I`、`-U`、`+U`、`-D`，而不是 `INSERT`、`DELETE` 等长名称；字段本身仍会保存在表中。

```sql
CREATE TABLE user_change (
    id      BIGINT,
    name    STRING,
    op      STRING NOT NULL,
    version BIGINT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'rowkind.field' = 'op',
    'sequence.field' = 'version'
);
```

`-U` 与 `-D` 能否正确执行，不能只看 RowKind 名称，还要看 merge engine 是否支持这种撤回方式。尤其是 partial-update 和 aggregation，两者对删除、旧值及重放的处理完全不同。

## 4. Deduplicate：整行覆盖

默认 `deduplicate` merge engine 把同一主键的新记录作为新的整行状态。它适合上游每次都提供完整更新后镜像的 upsert 流。

判断是否适用时，要检查输入是否满足两个条件：每次更新都能构造完整逻辑行；sequence 能正确处理乱序。若输入只带发生变化的字段，直接使用整行覆盖可能把未提供字段写成 `NULL`；此时应考虑 partial-update，或者先在 Flink 中补齐完整行。

删除记录还必须携带足够的主键和排序信息。若删除事件缺少可比较的 sequence，迟到删除与后续更新的顺序就无法可靠判断。

## 5. Partial Update：按字段合并

`partial-update` 将同一主键的多条记录按字段拼成最终行。默认情况下，非空字段覆盖旧值，`NULL` 表示“本次不更新该字段”。这意味着默认 partial-update 无法用普通 `NULL` 把旧值清空。

```sql
WITH (
    'merge-engine' = 'partial-update'
)
```

如果不同字段来自不同上游，且各自有版本号，可用 sequence group 保护一组字段：

```sql
WITH (
    'merge-engine' = 'partial-update',
    'fields.profile_version.sequence-group' = 'name,address',
    'fields.score_version.sequence-group' = 'score'
)
```

sequence group 中，ordering field 为 `NULL` 时跳过该组；版本小于当前值时保留旧字段；版本大于或等于当前值时，用新值替换受保护字段，此时 `NULL` 也能参与替换，因此可以清空字段。不同 sequence group 独立排序，适合把多个更新节奏不同的数据源合并到一行。

删除语义必须显式选择：

- 默认且未配置 sequence group 时，`DELETE` 和 `UPDATE_BEFORE` 会被拒绝。
- `ignore-delete=true` 会忽略两者，适用于明确只关心新增与更新的场景。
- `partial-update.remove-record-on-delete=true` 让 `DELETE` 删除整行，但仍忽略 `UPDATE_BEFORE`。
- 使用 sequence group 时，撤回通常只作用于对应字段组；可以再用 `partial-update.remove-record-on-sequence-group` 指定某个字段组的合格 `DELETE` 删除整行。

这些选项存在互斥关系：整行删除配置不能与 `ignore-delete` 随意组合，`partial-update.remove-record-on-delete` 也不能与 sequence group 同时使用。配置前应按当前版本文档核对。

## 6. Aggregation：把记录当作增量贡献

`aggregation` merge engine 不把输入记录视为完整最终行，而是把字段值视为对当前状态的贡献，例如 `sum`、`max`、`last_value`：

```sql
CREATE TABLE account_balance (
    account_id BIGINT,
    balance    DECIMAL(18, 2),
    remark     STRING,
    PRIMARY KEY (account_id) NOT ENFORCED
) WITH (
    'merge-engine' = 'aggregation',
    'fields.balance.aggregate-function' = 'sum',
    'fields.remark.aggregate-function' = 'last_non_null_value'
);
```

这里的 `balance=10` 通常表示“贡献 10”，不是“最终余额等于 10”。如果相同事件被重放两次，`sum` 可能累计两次；主键本身不会为聚合贡献提供事件级去重。需要 exactly-once 之外的业务幂等时，应在上游按事件 ID 去重或重新设计表模型。

默认情况下，`UPDATE_BEFORE` 与 `DELETE` 会尝试撤回字段贡献，但只有部分聚合函数支持撤回；不支持时会报错，除非配置相应的 `ignore-retract=true`。`aggregation.remove-record-on-delete=true` 可以让 `DELETE` 删除整行，但它不把 `UPDATE_BEFORE` 当作整行删除。

`last_value` 或 `last_non_null_value` 的撤回也不等于恢复历史上的前一个值。Paimon 没有仅凭当前撤回记录自动保存完整值历史，因此需要“撤回后恢复前值”的场景不能只靠这两个函数推断正确性。

Flink SQL 写 aggregation 表时，官方文档要求将 `table.exec.sink.upsert-materialize` 设置为 `NONE`，避免 Sink 上游额外物化改变预期的增量贡献语义。

## 7. First Row：保留首次到达，而非最早业务事件

`first-row` 只保留同一主键首次到达的记录，不允许配置用户 sequence 字段。它表示“首次被表看到”，并不等价于“业务事件时间最早”。如果源端乱序，先到的晚事件仍可能成为最终结果。

该模式默认拒绝 `DELETE` 和 `UPDATE_BEFORE`；只有明确允许丢弃删除时才使用 `ignore-delete=true`。如果业务要求删除首条记录后恢复下一条候选记录，`first-row` 并不保存足够的完整历史来完成这种回溯。

## 8. `sequence.snapshot-ordering`：按提交快照排序

多 writer 写同一张表时，可以启用 `sequence.snapshot-ordering=true`，让后提交的 snapshot 在跨 writer 冲突时获胜。但 snapshot 提交顺序不等于源端事件时间，它只能解决“哪个已提交快照更新”的排序。

该模式要求主键表开启 `write-only=true`，并通过独立 compaction 维护文件；它必须在建表时确定，不能与 `sequence.field` 同时使用。同一 snapshot 内部仍没有由该配置提供的顺序保证。

因此，源端已有可靠版本号时优先使用 `sequence.field`；只有排序目标确实是提交快照、并接受独立 compaction 运维成本时，才考虑 snapshot ordering。

## 9. Changelog Producer 与下游看到的内容

merge engine 决定表内最终结果，不代表流式下游一定收到完整最终行：

- `changelog-producer=input` 转发 writer 实际收到的输入。partial-update 输入若只带局部字段，下游也可能只看到局部记录；aggregation 下游看到的可能是增量贡献。
- `changelog-producer=lookup` 会结合表内旧状态生成更接近完整结果的变更，适合 partial-update、aggregation 等需要读取合并后行的流消费。
- `changelog-producer=full-compaction` 在 full compaction 时比较结果生成 changelog，延迟与 `full-compaction.delta-commits` 相关。
- 无流式读取需求时通常使用 `none`，独立 compaction 只负责文件组织与读性能，不会因为启用 compaction 就自动获得业务 changelog。

因此，“上游是 CDC”不能直接推出 `input` 正确。只有 writer 收到的每条记录已经等价于最终表状态的完整变化时，`input` 才成立。

## 10. 建表前检查

1. 抓取实际输入，确认存在的 `RowKind`，不要只根据连接器名称推断。
2. 检查 UPDATE 是完整行还是局部字段；检查 DELETE 是否携带主键和可比较的版本。
3. 明确 sequence 的来源、单调范围、相同值处理和重放行为。
4. 根据输入语义选择 merge engine：完整最终行用 deduplicate，局部字段拼接用 partial-update，增量贡献用 aggregation，首次到达去重才用 first-row。
5. 单独定义 `-U` 与 `-D` 的处理，确认是拒绝、忽略、字段撤回还是删除整行。
6. 存在流式下游时，再检查 changelog producer 能否输出下游需要的完整逻辑行。
7. 用乱序、重复、迟到删除、字段清空和故障重放五类用例验证最终状态。

## 11. 常见误区

“有主键就不会重复”不成立。主键约束的是表的最终逻辑行，aggregation 的同一增量事件重放仍可能重复累计。

“时间戳能做 sequence”只在该时间戳能稳定区分同一主键的版本时成立。秒级时间戳或多个系统各自产生的时钟值，经常需要额外 offset 作为 tie-breaker。

“partial-update 的 `NULL` 会清空字段”默认不成立。默认 `NULL` 表示不更新；只有 sequence group 等明确语义下，`NULL` 才可能覆盖旧值。

“收到 `-U` 就一定能恢复前一个值”不成立。撤回是否支持、能否恢复历史值，取决于 merge engine、聚合函数以及表中是否保存了足够状态。

“表内结果正确，流式下游就一定正确”不成立。下游看到的是 changelog producer 生成的记录，`input`、`lookup` 与 `full-compaction` 的完整性和延迟不同。

## 相关笔记

- [[Apache Paimon 表模型、存储组织与读取语义]]
- [[Paimon 流式读写中的 Snapshot 与 Changelog]]
- [[Paimon 主键表建模与稳定性检查清单]]
- [[Paimon Snapshot、Manifest、Parquet 与 Compaction 的关系]]

## 官方资料

- [Sequence Field and Row Kind](https://paimon.apache.org/docs/master/primary-key-table/sequence-rowkind/)
- [Partial Update](https://paimon.apache.org/docs/master/primary-key-table/merge-engine/partial-update/)
- [Aggregation](https://paimon.apache.org/docs/master/primary-key-table/merge-engine/aggregation/)
- [First Row](https://paimon.apache.org/docs/master/primary-key-table/merge-engine/first-row/)
- [Primary Key Table](https://paimon.apache.org/docs/master/primary-key-table/)

> 核对日期：2026-09-16。以上链接指向 Paimon master 文档；生产配置应以实际部署版本的对应文档为准。
