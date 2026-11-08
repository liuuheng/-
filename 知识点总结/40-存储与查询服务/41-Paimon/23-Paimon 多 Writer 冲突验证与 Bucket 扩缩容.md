# Paimon 多 Writer 冲突验证与 Bucket 扩缩容

## 结论

“两个作业都完成 Checkpoint”只能证明这次运行提交成功，不能证明同分区、同主键、Compaction、Overwrite 和故障恢复的所有组合都正确。并发实验需要同时验证发布竞争、文件修改和业务版本胜出规则。

固定 Bucket 扩缩容也不是调高 Flink 并行度：前者改变数据的桶布局，需要批处理重写；后者改变作业处理资源。重写同一表或分区时必须排除其他 Writer，完成数据对账后再切回持续写入。

> [!info] 版本与验证范围
> 2026-11-07 按 Apache Paimon `2.0` 官方文档核对。现有本地环境为 Flink `1.18.1` + Paimon `1.3.1`。本地项目已有不同分区的双 Writer 与独立 Compactor 运行记录；本文新增同分区同键验证、冲突观察和 Bucket 扩缩容流程仅做文档核对，未执行任何作业或数据重写。
>
> 示例使用隔离数据库 `paimon_concurrency_lab`，默认已进入隔离 Warehouse 的 Paimon Catalog。命令面向明确标注的文档版本；上线前还需核对实际 Connector、Catalog、锁和对象存储实现。Procedure 在 Flink `1.18` 使用位置参数。[Procedures](https://paimon.apache.org/docs/2.0/flink/procedures/)

## 1. 旧的双 Writer 实验究竟证明了什么

本地 `paimon-learning/17—21` 采用：

- 固定桶 `bucket=2`；
- 主键 `(writer_id, event_id, dt)`；
- Writer A 写 `2026-10-04`，Writer B 写 `2026-10-05`；
- 摄入 Writer 使用 `write-only=true`，另起一个专用 Compactor。

`21_01_two_writers_partition_isolated` 断言检查两方在各自分区存在数据，没有写入对方分区。它已经验证不同分区并发写的一条有效路径，不能据此推出同分区同键已经完成验证。

即使把两方日期改成相同，由于 `writer_id` 不同，完整主键仍然不同。这样的实验可观察同分区不同键的提交，无法验证订单同一版本冲突。再加上摄入端关闭了 Compaction，也不能要求它必然发生 File Conflict。

后续实验应明确标注“改变了哪一项条件、要观察什么”，保留旧实验的有效结论，不把新场景悄悄覆盖到旧的 PASS 上。

## 2. 三类问题要分别验证

| 问题 | 发生机制 | 应查看的证据 | 不能混淆的结论 |
| --- | --- | --- | --- |
| Snapshot 发布竞争 | 多个提交竞争下一版本 | `commit_user`、提交尝试、成功 Snapshot | 出现重试不等于业务写失败 |
| File Conflict | 待删除的输入文件已被其他提交替换 | 冲突日志、Failover、重算后的提交 | 换一个 Snapshot ID 不会修复旧文件计划 |
| 同键业务顺序 | 多 Writer 的到达或提交先后不代表业务新旧 | 同键不同版本的最终行和下游变化 | 没有报错不等于业务版本选对 |

Paimon 的乐观并发检查在提交阶段发现不兼容的文件变化；流任务可能通过 Failover 重新计算。多个作业持续争抢同一批 Compaction 输入文件时，反复恢复本身就是需要处理的运行问题。[Concurrency Control](https://paimon.apache.org/docs/2.0/concepts/concurrency-control/)

## 3. 先选合法且可解释的并发拓扑

| 布局与操作 | 本文采用的边界 |
| --- | --- |
| 固定桶，不同分区写入 | 可作为并发基线，仍需统一元数据与原子提交配置 |
| 固定桶，同分区追加新 L0 | 可验证发布竞争与同键顺序；不保证出现文件冲突 |
| 固定桶，多个摄入作业 + 单专用 Compactor | 摄入端统一 `write-only=true`，维护职责明确 |
| 同分区 Overwrite / Rescale | 维护窗口内排除其他 Writer 与重叠维护作业 |
| 动态桶，同分区多个独立写入 Job | 不支持，不作为允许上线的实验组合 |

动态桶需要维护 Key 到 Bucket 的映射。官方 `2.0` 文档明确禁止多个 Job 写同一分区；开启 `write-only=true` 和独立 Compactor 也不能解除限制。无需为了“证明会重复”主动在正式表上启动不支持的组合。[Data Distribution](https://paimon.apache.org/docs/2.0/primary-key-table/data-distribution/)

固定桶多 Writer 的前提也包括共享正确的 Catalog 和锁。对象存储上的 Rename 不应直接类比 HDFS 的原子 Rename；应遵循目标存储及 Catalog 的并发配置，再在隔离环境验收。[Dedicated Compaction](https://paimon.apache.org/docs/2.0/maintenance/dedicated-compaction/)

## 4. 同分区、同键：先把业务顺序单独测清楚

建立不含 `writer_id` 的业务主键，让来源身份仅作为普通字段保留。

```sql
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
CREATE DATABASE IF NOT EXISTS paimon_concurrency_lab;
USE paimon_concurrency_lab;

CREATE TABLE concurrent_orders (
  order_id BIGINT,
  amount INT,
  update_version BIGINT,
  source_job STRING,
  dt STRING,
  PRIMARY KEY (order_id, dt) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
  'bucket' = '2',
  'merge-engine' = 'deduplicate',
  'sequence.field' = 'update_version',
  'write-only' = 'true'
);

INSERT INTO concurrent_orders VALUES
  (1001, 120, 20, 'writer-a', '2026-11-07');
INSERT INTO concurrent_orders VALUES
  (1001, 100, 19, 'writer-b', '2026-11-07');

SELECT * FROM concurrent_orders
WHERE dt = '2026-11-07' AND order_id = 1001;
```

预期同键只保留版本 20、金额 120。这是先后两次批写的语义基线，尚未构成并发或故障恢复实验。

真正的双 Job 演练应由两个独立作业持续写入这张表，使用可重放、有明确预期的输入：A 写版本 20、22，B 延迟写 19、21。先以不相等版本验证最新业务版本胜出，再单独设计同版本不同内容的处理规则。

业务版本必须来源可靠。两个源各自递增、但彼此不可比较的计数器不能直接组成全局顺序；相同版本的冲突也不能靠“较后提交一定正确”解决。Sequence 的具体类型和相等值边界应按目标版本核对。[Sequence & Rowkind](https://paimon.apache.org/docs/2.0/primary-key-table/sequence-rowkind/)

## 5. 并发验证应保存哪些证据

```sql
DESCRIBE `concurrent_orders$snapshots`;
SELECT snapshot_id, commit_user, commit_kind, commit_time
FROM `concurrent_orders$snapshots` ORDER BY snapshot_id;

SELECT order_id, dt, COUNT(*) AS logical_rows,
       MAX(update_version) AS max_version
FROM concurrent_orders GROUP BY order_id, dt;

SELECT * FROM concurrent_orders
WHERE dt = '2026-11-07' ORDER BY order_id;
```

不要把 Snapshot ID 当作 Checkpoint ID。应同时保留两个 Job ID、各自 Checkpoint、输入版本清单、提交者身份、重试日志，以及批读最终行。

如果验证目标包含“流式下游看见了哪些变化”，还需独立 Reader 和审计 Sink；批读只有最终状态，无法证明所有中间事件完整。物理文件中的记录数也不能用来判断是否存在重复逻辑主键。

本场景的验收至少包括：两个 Writer 均有成功提交；订单 1001 最终只有一条逻辑行且为版本 22；停止并恢复其中一个 Writer 后，对账结果仍符合其输入和恢复语义。恢复部分应单独执行、单独记录，不能从正常运行推断。

## 6. File Conflict 不应靠随意叠加维护作业制造

同分区不必然等于文件冲突。新增独立 L0 文件和同时替换同一批旧文件，是不同操作。观察不到冲突时，应检查这次运行是否存在重叠的文件删除计划，而不是直接宣布并发控制失效。

学习中先掌握健康拓扑：多个固定桶摄入作业只写入，一个 Compactor 替换旧文件。Flink `1.18` 位置参数示例：

```sql
-- 无界维护作业，必须单独提交和管理；本次未执行。
-- SET 'execution.runtime-mode' = 'streaming';
-- CALL sys.compact(
--   'paimon_concurrency_lab.concurrent_orders',
--   '', '', '', 'continuous.discovery-interval=10s'
-- );
```

如果必须研究文件冲突，优先使用目标版本源码测试或可控的隔离故障测试，明确选中的旧文件和提交时序。不能把两个不支持的维护操作直接叠到生产表上，再用自动重启是否成功来代替设计审查。

同时要安排 Snapshot 过期等维护：`write-only=true` 会跳过摄入端 Compaction 和 Snapshot Expiration。没有专用维护时，正常追加也会积累文件和历史元数据。[Dedicated Compaction](https://paimon.apache.org/docs/2.0/maintenance/dedicated-compaction/)

## 7. Bucket 数与 Flink 并行度怎样区别

Bucket 是表或分区内部的数据分布单元；Sink 并行度是当前 Job 的 Writer 任务数量。同一 Job 内会把一个 Partition-Bucket 的记录确定性路由给对应任务，调大并行度不会自动把该 Bucket 拆成更多桶。

| 调整项 | 直接改变 | 主要约束 |
| --- | --- | --- |
| `sink.parallelism` | Job 中 Writer 数量 | 任务可能空闲，受活跃 Partition-Bucket 分布影响 |
| 固定表 `bucket` | 新布局下 Hash 的桶数量 | 旧布局要经过 Rescale 重写 |
| 动态桶目标行数 | 后续 Bucket 分配策略 | 不是把所有既有 Key 自动均匀重分布 |
| Checkpoint 间隔 | 提交频率与可见性边界 | 还影响文件规模、延迟与恢复窗口 |

假设只有一个活跃分区、2 个固定 Bucket，把 Sink 并行度从 2 调成 8，不会得到 8 个独立桶的写入能力。多个活跃分区则可能覆盖更多任务，因此不能简单宣称“表的总并行度永远等于桶数”。

选择桶数应结合每个活跃分区的数据规模、写入热点、目标文件大小和 Compaction 负荷。桶过少可能形成热点，过多可能增加小文件和管理成本。[Data Distribution](https://paimon.apache.org/docs/2.0/primary-key-table/data-distribution/)、[Write Performance](https://paimon.apache.org/docs/2.0/maintenance/write-performance/)

## 8. 固定 Bucket 扩缩容：先停同范围写入，再重写

官方 `2.0` 的 Rescale 路径是先修改 `bucket` 元数据，再用批模式 `INSERT OVERWRITE` 按新桶数重建目标表或分区。仅 `ALTER TABLE` 不会重排旧文件；新启动的追加 Job 写入尚未重排的已有分区，可能报桶数不匹配。

文档同时指出，修改元数据本身不影响已有读和运行中的写 Job。这不意味着它们可以与同范围 Overwrite 并发：官方明确要求重写期间没有其他 Job 写同一表或分区。[Rescale Bucket](https://paimon.apache.org/docs/2.0/maintenance/rescale-bucket/)

下面对无其他写入的小表演示 `2 → 4`；缩容也需要相同的重写和对账过程。语句不是连续复制到活跃生产表执行的操作单。

```sql
-- 前提：停止所有会写目标分区的 Writer 和重叠维护 Job。
-- 确认最终成功 Checkpoint/Savepoint、源端位点和当前表基线。
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';

ALTER TABLE concurrent_orders SET ('bucket' = '4');

INSERT OVERWRITE concurrent_orders PARTITION (dt = '2026-11-07')
SELECT order_id, amount, update_version, source_job
FROM concurrent_orders
WHERE dt = '2026-11-07';
```

静态分区 Overwrite 的 `SELECT` 不再输出已经固定的分区字段。本例的目标字段顺序与建表顺序一致；换成其他表时必须重新核对，不能依赖 `SELECT *` 猜测映射。

分区表可以存在不同桶数的历史分区，不要求一次重写所有历史。需要继续追加的分区必须完成对应布局调整；重启 Writer 时也要确认它加载了正确的表 Schema 和桶配置。

## 9. 扩缩容切换的检查顺序

1. **确定范围**：当前分区还是全表；列出会修改该范围的 Writer、Overwrite、Compactor 和调度任务。
2. **建立基线**：记录版本、Schema、桶配置、业务明细、逻辑行数、金额汇总和源端位点；按需创建恢复 Tag，并另行安排可恢复的备份。
3. **停写封口**：完成计划中的 Checkpoint／Savepoint，确认没有遗漏的独立 Writer 或待启动调度。
4. **修改与重写**：批模式完成目标布局重建；失败时保持维护窗口，不自动开启新配置的持续追加。
5. **对账验收**：对比明细、删除、空值和版本；检查 `$files` 的 Bucket 分布，再启动新 Writer。
6. **切回观察**：确认新增数据能提交，热点、Checkpoint 和 Compaction 均可接受，最后再恢复其他维护任务。

```sql
DESCRIBE `concurrent_orders$files`;
SELECT `partition`, bucket, COUNT(*) AS file_count
FROM `concurrent_orders$files`
GROUP BY `partition`, bucket ORDER BY `partition`, bucket;

SELECT dt, COUNT(*) AS logical_rows, SUM(amount) AS total_amount
FROM concurrent_orders GROUP BY dt;
SHOW CREATE TABLE concurrent_orders;
```

并非每个配置 Bucket 都一定有文件，小数据只命中部分桶是正常现象。元数据里 `bucket=4` 也不单独证明所有旧文件已经重排；应结合目标分区文件布局、重写成功记录和后续追加验证。

若已完成重写，不要只把 `bucket` 改回 2 就称为回滚。恢复旧布局同样需要受控重写，或采用经过验证的表状态恢复，并重新核对元数据、文件布局和上游恢复点。Tag 能保留旧状态，但不能自动把运行中作业的状态改回去。

## 10. 最小验收记录

| 场景 | 必须回答的问题 |
| --- | --- |
| 不同分区双写 | 双方输入是否都进入正确分区？ |
| 同分区不同键 | 提交竞争能否完成，是否出现持续重试？ |
| 同分区同键 | 胜出版本是否符合业务顺序，是否只有一条逻辑行？ |
| 独立 Compactor | 谁负责替换文件与过期，是否存在重叠维护？ |
| 固定桶 Rescale | 停写范围是否明确，旧数据和新增数据是否对账通过？ |
| 动态桶需求 | 是否遵守同分区单 Writer，能否通过合并输入或划分分区满足？ |

无冲突日志、作业 RUNNING、参数读回正确都只是一部分证据。并发与扩缩容的完成标准是数据正确、恢复路径明确，并且文件维护和资源负荷能够持续稳定。

## 相关笔记

- [[13-Paimon 主键表建模与稳定性检查清单]]
- [[14-Paimon 动态 Bucket 的索引机制与单 Writer 限制]]
- [[15-Paimon 多 Writer、Catalog 与原子提交]]
- [[18-Paimon 系统表、监控指标与故障排查]]
- [[22-Paimon Snapshot 回滚、Branch 与安全文件清理]]

## 官方资料

- [Paimon 2.0 Concurrency Control：Snapshot 与文件冲突](https://paimon.apache.org/docs/2.0/concepts/concurrency-control/)
- [Paimon 2.0 Dedicated Compaction：多 Writer 与维护职责](https://paimon.apache.org/docs/2.0/maintenance/dedicated-compaction/)
- [Paimon 2.0 Data Distribution：固定桶、动态桶和限制](https://paimon.apache.org/docs/2.0/primary-key-table/data-distribution/)
- [Paimon 2.0 Rescale Bucket：修改元数据和批量重写](https://paimon.apache.org/docs/2.0/maintenance/rescale-bucket/)
- [Paimon 2.0 Write Performance：并行度、缓冲与 Checkpoint](https://paimon.apache.org/docs/2.0/maintenance/write-performance/)
- [Paimon 2.0 Sequence & Rowkind：同键版本和操作类型](https://paimon.apache.org/docs/2.0/primary-key-table/sequence-rowkind/)
- [Paimon 2.0 Procedures：Flink 1.18 参数调用规则](https://paimon.apache.org/docs/2.0/flink/procedures/)
