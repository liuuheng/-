# Paimon Snapshot 回滚、Branch 与安全文件清理

## 结论

先确定要改变的是“某次查询读哪个版本”“表当前指向哪个状态”，还是“哪些文件可以回收”。Time Travel、Rollback、Branch 和文件清理分别处理这些问题，不能用一个删除命令代替整套恢复方案。

Tag 固定历史状态，Branch 提供独立修改空间，Consumer 保护流式消费需要的进度。它们都可能影响文件保留，但保护对象和生命周期不同。能够读到一个历史 Tag，不代表该 Tag 之后的逐条 Changelog 仍然完整。

> [!info] 版本与验证范围
> 2026-11-07 按 Apache Paimon `2.0` 官方文档核对。现有本地学习环境是 Flink `1.18.1` + Paimon `1.3.1`；旧实验已验证部分 Tag、Snapshot 过期和命名 Consumer，不代表本文新增的回滚、Branch、独立 Changelog 保留和清理流程已经执行。本文所有新增流程均为文档核对，未在集群执行，也未物理删除数据。
>
> 示例使用隔离数据库 `paimon_maintenance_lab`，默认已进入指向隔离 Warehouse 的 Paimon Catalog。Flink `1.18` 的 Procedure 使用位置参数；`1.19+` 才支持命名参数。不能把 `2.0` 的 Procedure 存在性直接当作本地 `1.3.1` 的兼容证明。[官方 Procedures](https://paimon.apache.org/docs/2.0/flink/procedures/)

## 1. 五种对象分别保护什么

| 对象或操作 | 直接作用 | 不能据此推出 |
| --- | --- | --- |
| Snapshot | 一次提交后的表状态与文件引用 | 所有旧版本会永久存在 |
| Time Travel | 让本次查询选择历史 Snapshot 或 Tag | 当前最新版本已经回滚 |
| Tag | 为某个历史状态保留命名引用 | 中间所有变更事件也被永久保存 |
| Branch | 从 Tag 或空状态形成可独立读写的分支 | 自动合并主分支随后收到的新数据 |
| Consumer | 记录命名流读者的表侧位置，参与保留判断 | 已经替代 Flink Checkpoint 和下游事务 |

例如订单在三个版本中依次为 `CREATED → PAID → CLOSED`。一个 Tag 可以保留 `PAID` 的完整表状态；如果业务要求审计每次状态迁移，还需要确认 Changelog 的生产方式、保留区间及读者位置。

上例是语义区分，不是对某个 Producer 必然输出三条事件的保证。`full-compaction` 可能合并中间变化；`input` 则依赖上游输入完整。[Changelog Producer](https://paimon.apache.org/docs/2.0/primary-key-table/changelog-producer/)

## 2. 先建立可核验的恢复基线

下面只准备小表和业务状态，不固定假定每条 `INSERT` 恰好对应一个 Snapshot。

```sql
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
CREATE DATABASE IF NOT EXISTS paimon_maintenance_lab;
USE paimon_maintenance_lab;

CREATE TABLE maintenance_orders (
  order_id BIGINT,
  amount INT,
  update_version BIGINT,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'bucket' = '2',
  'sequence.field' = 'update_version'
);

INSERT INTO maintenance_orders VALUES (1, 100, 1), (2, 200, 1);
CALL sys.create_tag('paimon_maintenance_lab.maintenance_orders', 'before_change');
INSERT INTO maintenance_orders VALUES (1, 120, 2), (3, 300, 1);

SELECT * FROM maintenance_orders ORDER BY order_id;
SELECT * FROM maintenance_orders
/*+ OPTIONS('scan.tag-name' = 'before_change') */ ORDER BY order_id;
```

准备阶段应记录：当前 Schema、Tag 对应的 Snapshot、最新业务结果，以及上下游作业是否仍在运行。预期最新状态有三行、订单 1 金额为 120；Tag 状态只有订单 1、2，金额分别为 100、200。

```sql
DESCRIBE `maintenance_orders$snapshots`;
DESCRIBE `maintenance_orders$tags`;
SELECT * FROM `maintenance_orders$snapshots`;
SELECT * FROM `maintenance_orders$tags`;
SHOW CREATE TABLE maintenance_orders;
```

历史读只改变本次读取范围，不修改当前表。Tag 的状态也应通过业务查询确认，不能只看 Tag 元数据里存在一个名称。[SQL Query](https://paimon.apache.org/docs/2.0/flink/sql-query/)、[Manage Tags](https://paimon.apache.org/docs/2.0/maintenance/manage-tags/)

## 3. Rollback 与历史查询怎样区别

| 选择 | 当前表是否改变 | 后续历史如何处理 | 适用问题 |
| --- | --- | --- | --- |
| `scan.tag-name` / `scan.snapshot-id` | 否 | 保持现状 | 对账、历史报表、定位错误 |
| `rollback_to` | 是 | 目标之后的 Snapshot、Tag 会被移除 | 接受撤销后续历史的表级恢复 |
| `rollback_to_as_latest` | 是 | 以新最新版本呈现目标状态，保留之后的 Snapshot、Tag | 需要保留后续历史的恢复 |

以下是 Paimon `2.0` 文档中的位置参数写法，列出的是两种替代方案，不能连续执行后就当成同一场景的验证结果。

```sql
-- 方案 A：回到 Tag；会改变表历史，仅在隔离表停写后演练。
-- CALL sys.rollback_to(
--   'paimon_maintenance_lab.maintenance_orders', 'before_change'
-- );

-- 方案 B：把 Tag 状态作为新最新版本；先核对目标版本是否支持。
-- CALL sys.rollback_to_as_latest(
--   'paimon_maintenance_lab.maintenance_orders',
--   'before_change', CAST(NULL AS BIGINT)
-- );
```

`rollback_to` 不是撤销一条 SQL，也不是只把某一个订单改回去。若需要保留订单 3，只修正订单 1，应采用符合业务顺序的补偿写入或在 Branch 中重建正确状态。

恢复表状态也不会自动恢复 MySQL 位点、Flink 算子状态和 Doris 已输出结果。重启旧 Writer 后，它可能继续写入或重放已处理的数据，所以表侧恢复必须与上游恢复点和下游对账一起设计。

Paimon `2.0` 文档还提示：仅有 Deletion Vector 差异的 `rollback_to_as_latest`，不能保证被流式 Overwrite Reader 观察到。批读恢复正确不等于流式下游自动修正。[Manage Snapshots](https://paimon.apache.org/docs/2.0/maintenance/manage-snapshots/)

## 4. Branch 用来隔离修改，Fast Forward 不是自动合并

可以从已确认正确的 Tag 创建分支，在该分支修改和核验数据；主分支原有读写流程可以继续运行。但分支不会天然接收主分支后续提交。

```sql
-- Paimon 2.0 文档示例：从 Tag 创建分支。
CALL sys.create_branch(
  'paimon_maintenance_lab.maintenance_orders',
  'repair_candidate', 'before_change'
);

SELECT * FROM `maintenance_orders$branch_repair_candidate` ORDER BY order_id;
INSERT INTO `maintenance_orders$branch_repair_candidate` VALUES (1, 110, 2);
SELECT * FROM `maintenance_orders$branch_repair_candidate` ORDER BY order_id;
SELECT * FROM maintenance_orders ORDER BY order_id;
```

验证时，分支订单 1 应为 110；主分支应保持原结果。只看到两个查询不同还不够，应核对其他订单、Schema 及分支起点。

`fast_forward` 会让分支内容进入主分支；官方文档明确说明，会移除主分支在分支初始 Tag 之后产生的 Snapshot、Tag、Schema，再复制分支对应历史。因此不能把它理解成 Git 式自动解决冲突、保留两边所有新数据的合并。

建议在切换前停止受影响的主分支 Writer，确认分支已包含切换点之前应保留的全部业务数据，再核对读者与恢复状态。这里是针对生产切换的操作建议，不是“创建分支必须停主分支”的限制。

```sql
-- 完成全量与增量对账、明确切换边界后，才考虑执行。
-- CALL sys.fast_forward(
--   'paimon_maintenance_lab.maintenance_orders', 'repair_candidate'
-- );
```

删除 Branch 只移除分支元数据，不等于当场回收其专属数据文件；后续仍需按引用和清理年龄判断。不要直接删除 `branch`、`snapshot` 或数据目录。[Manage Branches](https://paimon.apache.org/docs/2.0/maintenance/manage-branches/)

## 5. Snapshot 与 Changelog 要分别设计保留窗口

Snapshot 回答“某时刻表是什么”，Changelog 回答“期间发生哪些变化”。Paimon `2.0` 提供 `changelog.time-retained`、`changelog.num-retained.min/max`，也提供 `expire_changelogs`；不能只检查 `snapshot.time-retained` 就声称事件回放窗口足够。

```sql
-- 仅为隔离表的策略示例；不是通用生产推荐值。
ALTER TABLE maintenance_orders SET (
  'snapshot.time-retained' = '1 d',
  'snapshot.num-retained.min' = '10',
  'changelog.time-retained' = '7 d',
  'changelog.num-retained.min' = '10'
);
```

这段配置没有启用 Changelog Producer，也不会补出原本未生成的 Before Image。独立保留参数与当前 Producer、表模式的组合仍需验证；本地 `1.3.1` 对这些参数的实际支持和行为也尚未在本文重跑。

如果下游只消费已生成的 Changelog，`consumer.changelog-only=true` 表示 Consumer 只影响 Changelog 过期，不阻止 Snapshot 过期。它不能用于保护全量 Snapshot 扫描；应先确认 Reader 读取路径，再选择保护范围。[Configurations](https://paimon.apache.org/docs/2.0/maintenance/configurations/)、[Procedures](https://paimon.apache.org/docs/2.0/flink/procedures/)

## 6. 慢 Consumer、失效 Consumer 与恢复窗口

有效 Consumer 会参与历史保留判断。慢读者可能让文件持续保留；永久停掉但未处理的读者也可能造成存储增长。反过来，过早失效的 Consumer 会失去保留保护，不能保证很久以后还能从原位置补读。

一个可执行的规划方法是：先估算最长合法停机、排障、恢复和追赶时间，再设置保留窗口和 Consumer 生命周期，并对消费位置长期不推进告警。不能只按正常运行时几秒延迟设置一天之外的所有故障策略。

```sql
ALTER TABLE maintenance_orders SET ('consumer.expiration-time' = '7 d');
DESCRIBE `maintenance_orders$consumers`;
SELECT * FROM `maintenance_orders$consumers`;
```

表侧 `next_snapshot_id` 是消费位置，必须结合最近更新、Reader Checkpoint 和下游已提交数据判断。共享同一个 Consumer ID 的两个独立下游，不能当作两份独立保护记录管理。

若需要重置或删除 Consumer，先停止使用该 ID 的任务，保存旧位置并确认所需历史仍存在，再按目标版本调用 `reset_consumer`。不要为了释放空间直接清除所有 Consumer，再让旧任务按原状态启动。

`consumer.mode` 的 `exactly-once` 与 `at-least-once` 实现不同，官方提示切换模式后的 Flink 状态不兼容。表侧命名消费能力也不能单独证明外部 Sink 的端到端 Exactly Once。[Consumer ID](https://paimon.apache.org/docs/2.0/flink/consumer-id/)

## 7. Orphan 清理的危险来自尚未提交的文件

对象存储中存在、当前 `$files` 中不存在的文件，不一定是垃圾：它可能属于历史 Snapshot、Tag、Branch、保留的 Changelog，或正在写入但尚未提交的 Job。

因此清理工具要结合可达引用和文件年龄判断。`older_than` 是年龄截止时间，不是“希望删掉哪个业务日期分区”。官方默认仅考虑一天以前的孤儿文件，目的是避开其他 Writer 新写入的文件；一天不是对所有长作业都安全的保证。[Manage Snapshots](https://paimon.apache.org/docs/2.0/maintenance/manage-snapshots/)

例如一次批任务运行了 30 小时，早期生成的未提交文件可能已经超过默认年龄。若仍在运行，仅凭“文件超过一天且不在最新 Snapshot”就删除，会破坏待提交任务。应把截止时间设在最长写入／提交／恢复窗口之外，并识别遗留运行和待恢复任务。

本笔记只给 dry-run，不给实际删除调用。以下日期是固定示例，执行前必须替换为经过上述计算的截止时间；命令不会因为写在笔记中就自动保证安全。

```sql
-- Paimon 2.0 Procedure 的位置参数：第三项明确为 true。
-- 只列候选，不物理删除；本次未执行。
-- CALL sys.remove_orphan_files(
--   'paimon_maintenance_lab.maintenance_orders',
--   '2026-11-01 00:00:00',
--   'true'
-- );
```

`dry_run` 默认是 `false`，因此不能省略该参数。先在单表验证，不使用 `database.*` 扩大范围；保留候选路径、文件年龄、关联 Job 和引用检查结果。dry-run 是某次扫描结果，也不构成未来执行时引用关系不变的证明。[Procedures](https://paimon.apache.org/docs/2.0/flink/procedures/)

## 8. 一次维护演练怎样算通过

| 阶段 | 应保存的证据 | 失败或停止条件 |
| --- | --- | --- |
| 基线 | 版本、Schema、Tag、Snapshot、业务明细 | 无法明确哪个版本正确 |
| 历史查询 | 最新与 Tag 的订单及金额差异 | 只有元数据名称，没有业务结果 |
| Branch 修正 | 分支改变、主分支保持原状态 | 改动意外进入主分支 |
| 回滚演练 | 最新业务状态和后续历史是否符合所选语义 | 误以为保留了被回滚的全部提交 |
| 消费保护 | Reader 位置、Checkpoint、历史可读性 | 读者位置未核实就缩短保留 |
| dry-run | 候选清单、年龄与引用核验 | 候选可能属于活跃或待恢复 Job |

这些阶段应在可重新建立的小表上分别演练。Snapshot 和 Tag 不是独立存储的备份：如果底层数据文件丢失，仅有引用元数据不能恢复原数据。清理和恢复策略仍需配合存储备份、访问权限与灾备方案。

## 相关笔记

- [[09-Paimon Partition 与 Snapshot：写入、读取和过期机制（文章注释版）]]
- [[11-Paimon Snapshot 历史分区查询]]
- [[17-Paimon 流式读写中的 Snapshot 与 Changelog]]
- [[18-Paimon 系统表、监控指标与故障排查]]
- [[Paimon 实时与离线共用维表设计：Tag、Lookup 与历史重跑]]

## 官方资料

- [Paimon 2.0 Manage Snapshots：过期、回滚与 Orphan 清理](https://paimon.apache.org/docs/2.0/maintenance/manage-snapshots/)
- [Paimon 2.0 Manage Tags：历史状态引用与回滚到 Tag](https://paimon.apache.org/docs/2.0/maintenance/manage-tags/)
- [Paimon 2.0 Manage Branches：分支读写、Fast Forward 与删除边界](https://paimon.apache.org/docs/2.0/maintenance/manage-branches/)
- [Paimon 2.0 Consumer ID：保留、消费模式和重置](https://paimon.apache.org/docs/2.0/flink/consumer-id/)
- [Paimon 2.0 Configurations：Changelog 和 Consumer 保留参数](https://paimon.apache.org/docs/2.0/maintenance/configurations/)
- [Paimon 2.0 Procedures：参数顺序、版本和 dry-run](https://paimon.apache.org/docs/2.0/flink/procedures/)
- [Paimon 2.0 SQL Query：历史读与增量读语义](https://paimon.apache.org/docs/2.0/flink/sql-query/)
