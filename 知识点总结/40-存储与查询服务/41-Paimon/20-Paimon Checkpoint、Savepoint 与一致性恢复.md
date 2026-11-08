# Paimon Checkpoint、Savepoint 与一致性恢复

> 核对日期：2026-11-07。Paimon 行为以官方 `2.0` 文档为参考，Flink 恢复命令参考 `1.18` 文档。本地学习环境曾使用 Flink `1.18.1`、Paimon `1.3.1`；本文没有执行集群任务，也没有验证两个 Paimon 版本之间的状态兼容性。文中 SQL、命令和故障场景均为待执行示例。

## 核心结论

普通故障后从最近成功 Checkpoint 恢复，与主动回到较早 Savepoint，是两种操作。前者由 Flink 和 Paimon Connector 的恢复协议处理，不应看到作业失败就先手工回滚表；后者可能让 Source 和算子回到过去，而 Paimon 仍停留在未来，需要对齐两边的恢复边界。

Checkpoint/Savepoint 保存作业状态，Paimon Snapshot 保存表状态，Tag 保存选定表版本的长期引用。三者必须建立对应关系，不能根据编号相同、时间接近或者“都是快照”就认为已经一致。

计划停机、升级时，Paimon 官方推荐 `stop-with-savepoint`。需要长期保留可回退点时，官方还提供 Savepoint 自动关联 Tag 的方式：保存对应关系，在主动恢复前将表回滚到对应 Tag。[Paimon Savepoint](https://paimon.apache.org/docs/2.0/flink/savepoint/)

## 一、先区分三种被保存的状态

| 对象 | 保存什么 | 不能代替什么 |
| --- | --- | --- |
| Flink Checkpoint | Source 读取位置、算子状态、Sink 恢复所需状态 | 长期保存任意业务时刻的 Paimon 表 |
| Flink Savepoint | 可供主动停止、恢复或升级使用的作业状态 | 自动保存外部数据库、表和全部依赖环境 |
| Paimon Snapshot/Tag | 指定表版本引用的元数据和数据文件 | 恢复 Flink 的 Join、Normalize、聚合等算子状态 |

例如，作业消费 Kafka 后写入 Paimon。Kafka Offset 说明读到了哪里，Flink 聚合状态说明已经算到了哪里，Paimon Snapshot 说明哪些输出已经进入表。三者属于同一条处理链，但不是同一个编号体系。

`snapshot_id=100` 不等于 `checkpoint_id=100`。Paimon 提交还可能生成不同类型的 Snapshot，不能用一个固定加减公式推算两者关系。排查时应结合提交者、提交标识、日志和实际输出建立映射。

Flink 的 Checkpoint 需要能访问其元数据及引用的状态文件；只保留一个 `_metadata` 文件名，或者只复制 Paimon Warehouse，都不足以保证作业可恢复。[Flink Checkpoints](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/ops/state/checkpoints/)

## 二、普通故障恢复为什么通常不用人工回滚

假设作业已经成功完成 Checkpoint C10，接着处理 C11 对应的数据时失败。正常恢复流程是：

1. Flink 选择可用的成功 Checkpoint，恢复 Source、算子和 Sink 状态。
2. Source 从恢复位置继续读取，需要重放的记录由上游重新提供。
3. Paimon Connector 根据恢复状态处理待提交内容并继续提交。
4. 对齐 Source 进度、Checkpoint、Paimon 提交和业务结果，确认链路重新推进。

Writer 已经生成数据文件，不代表文件已经进入可见表状态。相反，Flink 页面出现成功 Checkpoint，也不应被当作所有独立查询在同一毫秒已经看到提交的证明；还要检查 Paimon `$snapshots` 和 Committer 的进展。

人工先回滚 Paimon，可能删除已经有效提交的版本，并影响其他 Writer、消费者和历史查询。这会把一次连接器可处理的故障，变成需要协调全链路状态的主动历史回退。

如果恢复一直失败，应先区分文件冲突、状态不兼容、历史文件过期、Source 日志过期和权限问题。重试次数增加，不能补回已经不存在的 Binlog，也不能修复持续发生的 Compactor 冲突。

## 三、主动恢复旧 Savepoint 为什么需要额外处理

假设保存 Savepoint S10 后，原作业又继续运行数小时，Paimon 已写入更多结果。此时只把 Flink 恢复到 S10：

- Source 会从旧位置重放。
- 算子会用旧聚合值、旧 Join 状态继续计算。
- Paimon 却可能仍包含 S10 之后的输出。

对于完整行 `deduplicate`，某些重放最终可能被覆盖，但这不足以证明整个过程正确。删除、乱序、外部副作用、非确定计算和 `aggregation` 增量贡献，都可能使结果不同。

例如，表内指标已经累计到 150，恢复旧 Source 后又重放贡献 50。如果将该贡献继续交给 `aggregation/sum`，主键本身不会识别“这个业务事件已经加过一次”。

因此，主动回退的目标应明确为一组状态：**指定 Savepoint 的 Source/算子/Sink 状态，以及与它对应的 Paimon 表版本**。不能把“读取一个旧 Tag”当作已经修改了当前表，也不能把“回滚一张表”当作已经回退整条链路。

## 四、方案一：计划停机使用 Stop with Savepoint

Paimon 官方推荐这种方式，是因为它将停止作业与完成最后一致性边界结合，减少遗留未提交元数据的情况。它适合计划重启、资源调整和经兼容性验证的升级，不等于任意版本都能直接恢复。

以下命令只展示 Flink 1.18 CLI 的提交形式，`RECOVERY_JOB_ID` 必须替换成实验作业 ID；目标路径必须能被 JobManager、TaskManager 和恢复后的作业访问：

```bash
bin/flink stop \
  --savepointPath s3://paimon/recovery-savepoints/ \
  "$RECOVERY_JOB_ID"
```

命令返回后，记录完整 Savepoint 路径，确认作业已经停止，再检查最后提交和业务结果。不能在旧作业仍运行时，用同一份状态启动第二个写入同表的作业。

`flink savepoint` 只触发运行中的中间 Savepoint，和 `flink stop --savepointPath` 的停止语义不同。Flink 1.18 文档说明，中间 Savepoint 从 1.15 起不作为普通自动恢复点，也不会按普通成功 Checkpoint 的方式提交全部副作用。[Flink Savepoints](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/ops/state/savepoints/)

恢复 JAR 作业的命令形式如下；SQL Client、SQL Gateway 或平台托管作业需要使用各自的恢复入口，不能把 `.sql` 文件直接当成这个 JAR 参数：

```bash
bin/flink run \
  -s s3://paimon/recovery-savepoints/savepoint-example \
  /path/to/original-compatible-job.jar
```

Savepoint 文件存在只是前提。作业的算子标识、拓扑、状态序列化、Connector 及依赖都需要兼容。不要把 `--allowNonRestoredState` 当作通用修复开关；丢弃无法映射的状态，可能同时丢掉正确性所依赖的聚合或去重历史。

## 五、方案二：Tag 与 Savepoint 建立明确对应

这适用于需要保留旧恢复点，并且未来可能主动回退到它的场景。Paimon 2.0 文档提供 `sink.savepoint.auto-tag=true`，关联 Tag 名称形如 `savepoint-${savepointID}`。

下面只创建独立实验库中的表。前提是当前已经选中正确的 Paimon Catalog，并确认本地目标版本支持所列选项；不要直接对既有业务表照抄。

```sql
CREATE DATABASE IF NOT EXISTS paimon_recovery_lab;
USE paimon_recovery_lab;

CREATE TABLE entity_state (
    id BIGINT,
    amount DECIMAL(18, 2),
    source_version BIGINT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'bucket' = '2',
    'merge-engine' = 'deduplicate',
    'sequence.field' = 'source_version',
    'sink.savepoint.auto-tag' = 'true'
);
```

`source_version` 只有在同一主键内部可靠表达先后时才有效。示例中的 Bucket 数只服务小规模隔离演练，不是生产推荐值。

对既有表修改该选项后，也要确认实际 Writer 已重新加载配置。是否支持自动 Tag、Tag 何时生成，以及本地 1.3.1 的行为，均需独立验证，不能根据 2.0 示例推定。

自动 Tag 也有生成时点。官网 `1.3` 文档当前展示 `1.3.2`，明确说明：Stop with Savepoint 的 Tag 随其完成流程创建；运行中的其他中间 Savepoint，要等下一次 Checkpoint 成功后才创建对应 Tag。不能收到 Savepoint 路径就立即取消 Writer，并假定表侧恢复点已经保存。本地 `1.3.1` 仍需实测这一时序。[Paimon 1.3：Savepoint](https://paimon.apache.org/docs/1.3/flink/savepoint/)

触发 Savepoint 后，等待对应提交和 Tag 实际出现，再查询并把以下信息保存在同一份演练记录中：

```sql
SET 'execution.runtime-mode' = 'batch';

SELECT tag_name, snapshot_id, commit_time
FROM paimon_recovery_lab.`entity_state$tags`;

SELECT snapshot_id, commit_user, commit_identifier, commit_time
FROM paimon_recovery_lab.`entity_state$snapshots`
ORDER BY snapshot_id DESC;
```

首次查询前先 `DESCRIBE` 系统表确认字段。记录至少包括 Job ID、代码版本、Connector 版本、Savepoint 路径、Savepoint/Checkpoint 标识、Tag 名和 Snapshot ID。

自动 Tag 的名称对应 Flink 保存点标识，不应根据 Savepoint 目录的随机后缀猜测。Tag 是否生成、指向哪个版本，都以 `$tags` 和运行日志为准。[Paimon Savepoint](https://paimon.apache.org/docs/2.0/flink/savepoint/)

## 六、真正回到旧保存点时，按什么顺序操作

以下是主动历史回退的演练流程，不用于普通失败后的自动恢复：

1. 停止该表的 Writer、Compactor 及相关维护任务，暂停依赖其版本链的消费者。
2. 核对源数据仍能从 Savepoint 中的位点重放，检查日志保留和访问权限。
3. 核实保存点文件、对应 Tag、代码和依赖全部可用；涉及多个目标表时逐表核对。
4. 将需要保留的较新结果备份到独立位置，并明确所有受影响的下游状态。
5. 回滚实验表到对应 Tag，然后从匹配的 Savepoint 恢复原作业。
6. 等待成功 Checkpoint 和新提交，验证回放区间及下游对账结果。

下面使用 Flink 1.18 对应的位置参数形式。`savepoint-42` 仅为示意，必须替换为已查到且已确认对应关系的 Tag：

```sql
CALL sys.rollback_to(
    'paimon_recovery_lab.entity_state',
    'savepoint-42'
);
```

这不是只读时间旅行。官方说明，`rollback_to` 回到 Tag 时会删除目标版本之后的 Snapshot/Tag，并删除相关数据。给较新版本再建一个普通 Tag，不能被当作回滚操作之外的独立备份。[Manage Tags：Rollback to Tag](https://paimon.apache.org/docs/2.0/maintenance/manage-tags/#rollback-to-tag)

Procedure 的位置参数与命名参数受 Flink 版本影响；此处语法参考 Paimon 2.0 Procedures，不代表本地 Paimon 1.3.1 已验证支持同一签名。[Paimon Procedures](https://paimon.apache.org/docs/2.0/flink/procedures/)

## 七、流式 Reader 和 Consumer 也有恢复边界

Reader 从 Checkpoint/Savepoint 恢复时，读取位置通常由恢复状态决定。不能指望修改 `scan.mode=latest`，就把一个已经恢复的 Source 自动切到任意新位置。

`consumer-id` 保护消费仍需的 Snapshot，并可保存表侧消费进度；它不保存下游聚合、Join 和 Sink 状态。新作业只沿用 Consumer 位置而丢掉聚合状态，可能跳过构建累计结果所必需的历史。

以下是 Reader 配置示意，必须先配置表侧 Consumer 生命周期，并确认实际 Writer 已加载它：

```sql
ALTER TABLE paimon_recovery_lab.entity_state SET (
    'consumer.expiration-time' = '7 d'
);

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10 s';

SELECT *
FROM paimon_recovery_lab.entity_state
/*+ OPTIONS('consumer-id' = 'recovery-lab-reader') */;
```

7 天只是演练值，应覆盖真实停机、排队和修复窗口。Paimon 2.0 文档还说明，`consumer.mode` 两种模式的 Flink 状态不兼容，切换不能当作普通热更新。[Consumer ID](https://paimon.apache.org/docs/2.0/flink/consumer-id/)

## 八、如何设计一次能判断正确性的演练

先使用可重放、可确定顺序的输入，例如固定事件序列或已配置持久化的 Kafka Topic。随机 DataGen 适合观察吞吐和提交，不适合单独证明故障前后没有漏失或重复。

在 `paimon_recovery_lab` 准备包含新增、更新、删除和乱序的有限样本，事先给出最终主键集合及每个键的期望值。分别演练普通故障恢复、Stop with Savepoint，以及主动回到旧 Savepoint。

普通恢复测试至少覆盖 Writer 已处理但尚未形成成功提交、以及提交边界附近的故障。通过运行日志确定故障时点，不根据“睡了几秒”宣称命中了某个边界。

主动回退测试则要先证明 Savepoint 后确实产生过新表版本，再按配对 Tag 回退和恢复。只在保存后立刻停止，无法验证“作业状态在过去、表状态在未来”的问题。

每次演练按同一组判据验收：

| 判据 | 需要保存的证据 |
| --- | --- |
| 恢复来源正确 | 实际加载的 Checkpoint/Savepoint 路径与日志 |
| 源能继续重放 | Offset/Binlog 位点、保留范围及输入事件清单 |
| 表重新推进 | 新 `$snapshots`、提交日志、成功 Checkpoint |
| 最终状态正确 | 主键集合、逐键字段、删除结果和聚合对账 |
| 下游同步正确 | Reader 位点、下游状态恢复来源与输出结果 |

仅有作业 `RUNNING`、逻辑 `COUNT(*)` 相同，或 Snapshot ID 增长，都不足以证明恢复正确。两行金额交换后，条数和总额都可能相同，所以需要逐键检查。

## 九、保留策略需要同时覆盖哪些对象

恢复能力取决于整条依赖链中最短的保留窗口：Source 日志、Flink 状态文件、Paimon Snapshot/Tag、Consumer，以及下游的恢复资料。

Flink 默认 Checkpoint 并不是永久备份；取消作业后的保留行为需要按外部化 Checkpoint 配置确认。Paimon 普通 Snapshot 也按时间和数量规则过期，Tag 则用于固定需要较长期保留的版本。

Tag 只能保护仍被它引用的表版本，不能补回已经过期的 Source 日志，也不能修复已经被手工删除的状态文件。长期留存还需要监控实际可读性和物理存储，而不只是记录一个路径。[Manage Snapshots](https://paimon.apache.org/docs/2.0/maintenance/manage-snapshots/)

## 相关笔记

- [[05-Paimon CDC 入湖、Schema Evolution 与恢复语义]]
- [[10-Paimon Sequence、RowKind 与删除语义]]
- [[15-Paimon 多 Writer、Catalog 与原子提交]]
- [[17-Paimon 流式读写中的 Snapshot 与 Changelog]]
- [[21-Paimon Changelog 下游聚合与消费恢复验证]]
- [[Paimon 实时与离线共用维表设计：Tag、Lookup 与历史重跑]]

## 官方资料

- [Paimon 2.0：Savepoint](https://paimon.apache.org/docs/2.0/flink/savepoint/)
- [Paimon 1.3：Savepoint 与自动 Tag 时序](https://paimon.apache.org/docs/1.3/flink/savepoint/)
- [Paimon 2.0：Consumer ID](https://paimon.apache.org/docs/2.0/flink/consumer-id/)
- [Paimon 2.0：Manage Tags](https://paimon.apache.org/docs/2.0/maintenance/manage-tags/)
- [Paimon 2.0：Manage Snapshots](https://paimon.apache.org/docs/2.0/maintenance/manage-snapshots/)
- [Paimon 2.0：Procedures](https://paimon.apache.org/docs/2.0/flink/procedures/)
- [Flink 1.18：Checkpoints](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/ops/state/checkpoints/)
- [Flink 1.18：Savepoints](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/ops/state/savepoints/)
