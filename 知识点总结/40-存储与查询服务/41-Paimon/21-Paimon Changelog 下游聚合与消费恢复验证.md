# Paimon Changelog 下游聚合与消费恢复验证

> 核对日期：2026-11-07。机制与配置参考 Paimon 官方 `2.0` 文档，Flink 动态表语义参考 `1.18` 文档。本地学习环境曾使用 Flink `1.18.1`、Paimon `1.3.1`；本文没有运行作业，SQL 是待执行的隔离演练示例，不构成跨版本兼容或性能实测结论。

## 核心结论

Changelog 是否够用，要根据下游计算判断。按订单主键覆盖最新状态，与按品类持续计算 `SUM/COUNT`，对旧值的需求不同。订单金额从 100 变为 120 时，聚合需要表达净增加 20；订单从 A 类移到 B 类时，还必须撤回 A 类的旧贡献。

四种 Producer 不承诺同一种历史：`input` 保存 Writer 实际收到的变更；`lookup` 基于旧状态生成变化；`full-compaction` 比较相邻全量合并结果，中间更新可能折叠；`none` 没有额外完整旧值，需要具备相应能力的消费者维护历史状态。[Changelog Producer](https://paimon.apache.org/docs/2.0/primary-key-table/changelog-producer/)

因此，应分别验证“最终表状态”“下游聚合结果”“需要保存的事件历史”和“变化可见延迟”。不能要求所有模式都输出每一次业务中间状态，也不能仅凭最终源表正确，就推断下游已经正确恢复。

## 一、为什么主键表正确，SUM 仍可能错误

假设源表有两张订单：`(1,A,100)`、`(2,A,50)`。当前 A 类金额为 150、订单数为 2。

当订单 1 更新为 120，下游如果只收到一个新值 120 并直接累加，就会得到 270。正确过程是撤回旧贡献 100，再加入 120，或者由消费者保存旧值后计算差额。

删除订单 2 时，下游必须将金额减去 50，并将数量减去 1。将订单 1 的分类改成 B 时，则需要从 A 撤回一行，再向 B 加入一行；只比较全表总金额，会漏掉分组错误。

Flink 将持续查询理解为对动态表不断维护结果。对应变更处理完成后，持续查询的逻辑结果应与同一输入状态上的批查询一致。[Flink Dynamic Tables](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/table/concepts/dynamic_tables/)

## 二、实验分别比较什么

| Producer | 为正确聚合提供旧值的方式 | 不能据此承诺的内容 |
| --- | --- | --- |
| `none` | Flink 可能插入 Normalize，按主键保存旧值 | 无状态消费者也能正确做任意聚合 |
| `input` | 输入本来就有完整业务 Before/After | 自动补齐上游遗漏的旧字段和删除 |
| `lookup` | Lookup Compaction 查找已有状态并生成变化 | 保存每一次被同轮合并折叠的业务事件 |
| `full-compaction` | 比较相邻全量合并后的状态 | 保留两次全量合并之间的全部中间状态 |

例如，两次 Full Compaction 之间同一订单从 100 变为 120，再变为 110，下游只看到 100 到 110 的净变化，也可能满足最终聚合要求。若业务要审计“曾经到过 120”，应保留事件日志，不能把完整撤回语义等同于完整事件历史。

不要为了让执行计划看起来更轻，直接启用 `scan.remove-normalize`。先证明上游变化和下游算法在移除旧值状态后仍然正确，再讨论这类优化。

## 三、建立隔离表，先固定输入语义

以下 SQL 的前提是已选中正确的 Paimon Catalog。实验对象全部位于 `paimon_changelog_lab`，先确认库中没有同名自建表；本文不提供自动删除重建流程。

为了手工构造变更，用 `op` 映射 RowKind，用 `event_seq` 表达同一订单的顺序。`op/event_seq` 是演练控制字段，业务 Before/After 的比较对象是 `order_id/category/amount`。

```sql
CREATE DATABASE IF NOT EXISTS paimon_changelog_lab;
USE paimon_changelog_lab;

CREATE TABLE orders_none (
    order_id BIGINT,
    category STRING,
    amount DECIMAL(18, 2),
    event_seq BIGINT,
    op STRING NOT NULL,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'bucket' = '2',
    'merge-engine' = 'deduplicate',
    'sequence.field' = 'event_seq',
    'rowkind.field' = 'op',
    'changelog-producer' = 'none',
    'consumer.expiration-time' = '7 d'
);

CREATE TABLE orders_input LIKE orders_none;
ALTER TABLE orders_input SET ('changelog-producer' = 'input');

CREATE TABLE orders_lookup LIKE orders_none;
ALTER TABLE orders_lookup SET ('changelog-producer' = 'lookup');

CREATE TABLE orders_full LIKE orders_none;
ALTER TABLE orders_full SET (
    'changelog-producer' = 'full-compaction',
    'full-compaction.delta-commits' = '1'
);
```

`LIKE` 复制结构和表属性，不复制已有业务数据；上述修改发生在新表尚未写入、尚未启动 Reader 时。创建后用 `SHOW CREATE TABLE` 检查四张表的主键、桶数和 Producer，确认没有复制显式业务路径。[Flink SQL DDL](https://paimon.apache.org/docs/2.0/flink/sql-ddl/)

RowKind 字符串使用 `+I/-U/+U/-D`，不是 `INSERT/DELETE`。`event_seq` 只在相同订单内比较；真实 CDC 应使用来源可靠的顺序字段，并提供真实旧行，不能照搬手工序列。[Sequence and Rowkind](https://paimon.apache.org/docs/2.0/primary-key-table/sequence-rowkind/)

建立四张聚合结果表。它们保存持续聚合的完整最新结果，所以使用 `deduplicate`，而不是再次用 `aggregation/sum` 累加聚合算子输出的总额。

```sql
CREATE TABLE agg_none (
    category STRING,
    order_count BIGINT,
    total_amount DECIMAL(18, 2),
    PRIMARY KEY (category) NOT ENFORCED
) WITH (
    'bucket' = '2',
    'merge-engine' = 'deduplicate'
);

CREATE TABLE agg_input LIKE agg_none;
CREATE TABLE agg_lookup LIKE agg_none;
CREATE TABLE agg_full LIKE agg_none;
```

## 四、先写入基线，再启动真正持续的 Reader

在 Writer 会话使用 batch 模式，将以下初始数据分别写入四张 `orders_*` 表。示例展示 `orders_none`，其他三张只替换目标表名；每次等待有界写入作业成功结束，不并发启动同表 Writer。

```sql
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';

INSERT INTO orders_none VALUES
    (1, 'A', 100.00, 1, '+I'),
    (2, 'A',  50.00, 1, '+I');
```

在独立 Reader 会话执行下列 SQL。四组各使用自己的源表、目标表、Consumer ID 和 Job ID，不能共用同一个 Consumer ID。

```sql
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10 s';

EXPLAIN INSERT INTO agg_none
SELECT category, COUNT(*),
       CAST(SUM(amount) AS DECIMAL(18, 2))
FROM orders_none
/*+ OPTIONS('consumer-id' = 'agg-none-v1') */
GROUP BY category;

INSERT INTO agg_none
SELECT category, COUNT(*),
       CAST(SUM(amount) AS DECIMAL(18, 2))
FROM orders_none
/*+ OPTIONS('consumer-id' = 'agg-none-v1') */
GROUP BY category;
```

对应关系为 `orders_input → agg_input / agg-input-v1`、`orders_lookup → agg_lookup / agg-lookup-v1`、`orders_full → agg_full / agg-full-v1`。它们与 `none` 的查询结构相同。

首次新建且没有恢复状态、没有旧 Consumer 进度时，默认流读先输出启动时的完整表状态，再跟随后续变化。这使聚合先得到 `A / 2 / 150.00`。若改用 `scan.mode=latest` 跳过初始全量，空聚合状态就无法知道已有订单贡献。[Flink SQL Query](https://paimon.apache.org/docs/2.0/flink/sql-query/)

记录四组执行计划、Normalize 是否出现、状态大小、Checkpoint 和提交耗时。Producer 配置不会保证任意查询计划都完全没有状态；聚合本身仍然需要维护分组结果。

## 五、按阶段发送更新、删除和分组变化

以下事件均在 Writer 的 batch 会话执行，并逐阶段分别写入四张源表。阶段之间先确认源表提交和 Reader 处理进度，再发送下一阶段，避免把尚未处理完成误判成结果错误。

```sql
-- 阶段 2：订单 1 的金额从 100 改成 120。
INSERT INTO orders_none VALUES
    (1, 'A', 100.00, 1, '-U'),
    (1, 'A', 120.00, 2, '+U');

-- 阶段 3：删除订单 2，携带其完整业务旧行。
INSERT INTO orders_none VALUES
    (2, 'A', 50.00, 2, '-D');

-- 阶段 4：订单 1 从 A 类移动到 B 类。
INSERT INTO orders_none VALUES
    (1, 'A', 120.00, 2, '-U'),
    (1, 'B', 120.00, 3, '+U');
```

这里的 `INSERT INTO ... VALUES` 是有界故障样本注入，并不代表 INSERT-only 业务流；`rowkind.field` 让 Writer 按 `op` 解释行的操作。用于 `input` 的 Before/After 必须与真实业务旧状态吻合，不能只伪造一个 `-U` 标记。

预期逻辑结果如下。它描述每阶段变化处理完成后的状态，不要求四种 Producer 在同一时刻可见，也不固定中间消息条数。

| 阶段 | 当前订单 | 聚合结果 |
| --- | --- | --- |
| 初始 | `1/A/100`、`2/A/50` | A：2 单，150 |
| 金额更新 | `1/A/120`、`2/A/50` | A：2 单，170 |
| 删除订单 2 | `1/A/120` | A：1 单，120 |
| 分类移动 | `1/B/120` | B：1 单，120；A 不再有组 |

对 `full-compaction`，必须确认发生了对应 Full Compaction，不能等待任意一个 APPEND Snapshot 后就要求聚合立即变化。有界注入也不能直接用来推导流式 Checkpoint 的触发频率。

如需显式推进隔离表的全量合并，可在该阶段有界 Writer 已结束后，使用单独 batch 会话调用；先确认目标版本支持 Procedure，并避免多个 Compactor 同时覆盖该表。

```sql
SET 'execution.runtime-mode' = 'batch';
CALL sys.compact('paimon_changelog_lab.orders_full');
```

Paimon 文档说明，独立 Compaction 的 batch 模式默认选择全部当前文件，streaming 模式则持续发现和执行按条件选出的合并。记录 COMPACT 提交和实际 Reader 输出，以验证全量边界。[Dedicated Compaction](https://paimon.apache.org/docs/2.0/maintenance/dedicated-compaction/)

## 六、怎样证明下游与源表一致

暂停样本注入，等 Reader 已越过目标变更并完成后续 Checkpoint、聚合表也形成提交，再用 batch 会话分别读取源表和结果表：

```sql
SET 'execution.runtime-mode' = 'batch';

SELECT order_id, category, amount
FROM orders_none;

SELECT category, COUNT(*) AS order_count,
       CAST(SUM(amount) AS DECIMAL(18, 2)) AS total_amount
FROM orders_none
GROUP BY category;

SELECT category, order_count, total_amount
FROM agg_none;
```

对其余三组执行相同对账。应比较分组键集合、每组数量、每组金额和已经消失的组；只比较总额 120 无法发现 A 类旧行未被正确删除的问题。

还可以另开独立诊断 Reader，观察 `SELECT order_id, category, amount FROM orders_input` 的真实结果变更。保存 SQL Client 显示的 RowKind；表内保存的字符串 `op` 是输入控制字段，不能当作 Reader 输出 RowKind 的替代证据。

批量增量查询普通业务表会过滤某些删除表示。需要观察区间中的删除时，官方推荐使用 `$audit_log`；先确认本地系统表字段，再指定确实存在的起止 Snapshot：

```sql
-- Snapshot 10、20 为示意编号，不可直接照抄。
SELECT *
FROM `orders_input$audit_log`
/*+ OPTIONS('incremental-between' = '10,20') */;
```

区间语义是 `(start,end]`，它不等于 end 的完整表状态。普通批查询没有显示删除行，不能推出流式 Reader 没有消费删除。[Flink SQL Query：Batch Incremental](https://paimon.apache.org/docs/2.0/flink/sql-query/#batch-incremental)

## 七、故障恢复必须把聚合状态一起恢复

在四组分别形成成功 Checkpoint 后，记录 Reader Job ID、Checkpoint 路径、源表和结果表的提交位置。再发送一个已知样本，并在处理或提交边界附近制造受控故障。

普通故障恢复应让 Source 位置、Normalize 状态、Group Aggregate 状态和 Sink 状态从同一 Checkpoint 恢复。恢复后再次做逐组对账，并检查删除组有没有重新出现。不要对每次普通故障先人工回滚源表或聚合表。

只使用原来的 `consumer-id`，但启动一个没有旧算子状态的新聚合作业，是另一种行为。它可能从已记录的后续 Snapshot 继续，却没有之前的累计状态；Consumer 的断点续读能力不能替新聚合补出旧贡献。

如果确实要重建聚合，较清晰的演练方式是创建独立的新结果表和新的 Consumer ID，从完整初始状态重新计算，再对账切换。不能把已有结果表当作 Flink 内部聚合状态，也不能只重置 Consumer 位置就宣称恢复完整。

Paimon 2.0 的 Consumer 文档区分严格与 `at-least-once` 模式，并说明切换模式的 Flink 状态不兼容。本实验不要为了避开恢复报错而随意切换模式。[Consumer ID](https://paimon.apache.org/docs/2.0/flink/consumer-id/)

## 八、如何比较成本，而不把小样本当压测

上述四阶段只验证语义，不证明某个 Producer 更快。成本比较需要相同输入量、主键规模、更新比例、Checkpoint 周期、桶数和机器资源，并记录可见延迟分位数、状态大小、Checkpoint/Commit 耗时及磁盘使用。

`none` 的成本可能主要在下游 Normalize；`lookup` 将旧值查找放到写入侧并使用本地缓存；`full-compaction` 的延迟和读写放大受全量频率影响。增加缓存或拉大全量间隔，都可能同时改变资源和新鲜度，不能只比较最终文件数。

若要验证“中间更新折叠”，需要另用持续 Writer，让多次更新确实落在同一个合并观察窗口，并保存输入事件、合并边界和输出。不能仅因本实验未看到每次 100→120→110，就断言漏数据；先判断需求是事件审计还是最终状态聚合。

## 相关笔记

- [[10-Paimon Sequence、RowKind 与删除语义]]
- [[17-Paimon 流式读写中的 Snapshot 与 Changelog]]
- [[18-Paimon 系统表、监控指标与故障排查]]
- [[20-Paimon Checkpoint、Savepoint 与一致性恢复]]
- [[实验/01-Paimon 本地 SQL 验证实验手册]]

## 官方资料

- [Paimon 2.0：Changelog Producer](https://paimon.apache.org/docs/2.0/primary-key-table/changelog-producer/)
- [Paimon 2.0：Sequence and Rowkind](https://paimon.apache.org/docs/2.0/primary-key-table/sequence-rowkind/)
- [Paimon 2.0：Flink SQL DDL](https://paimon.apache.org/docs/2.0/flink/sql-ddl/)
- [Paimon 2.0：Flink SQL Query](https://paimon.apache.org/docs/2.0/flink/sql-query/)
- [Paimon 2.0：Consumer ID](https://paimon.apache.org/docs/2.0/flink/consumer-id/)
- [Paimon 2.0：Dedicated Compaction](https://paimon.apache.org/docs/2.0/maintenance/dedicated-compaction/)
- [Flink 1.18：Dynamic Tables](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/table/concepts/dynamic_tables/)
