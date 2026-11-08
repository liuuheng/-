# Paimon 查询裁剪、Compaction 与容量调优

> 核对日期：2026-11-07。机制与参数入口参考 Paimon 2.0 官方文档；现有本地实验是 Flink 1.18.1 + Paimon 1.3.1。下文是基准设计和待执行示例，没有新增压测结果，也没有把 1.3.1 的功能断言当作 2.0 的性能验证。

## 核心结论

Paimon 调优要先确定瓶颈在规划、读取、合并、写入还是提交，再修改对应因素。一次查询变快，可能来自缓存、数据变少或读取了较旧的优化视图，不能直接归因于 File Index 或 Compaction。

证明优化有效，需要同时满足三个条件：逻辑结果一致，输入与运行条件可比较，目标指标改善且没有把成本转移到不可接受的写入延迟、内存或存储上。

`SHOW CREATE TABLE` 证明配置写入，`$files` 证明文件和统计条件，`EXPLAIN` 证明计划选择；实际扫描量、耗时和资源指标才能支持性能结论。

## 一、先把“查询慢”拆成不同阶段

| 阶段 | 可观察现象 | 优先寻找的证据 |
| --- | --- | --- |
| 规划与枚举 | 作业长时间停留在实际读文件之前 | Manifest 数量、Scan Planning 时间、JobManager CPU/堆、Split 数 |
| 数据读取 | 存储读取字节大、网络繁忙 | 分区/主键过滤、文件统计、实际读取文件和字节 |
| 主键合并 | 读取并行度提高但少数任务仍慢 | MOR Bucket 大小、重叠 Sorted Run、热点 Bucket |
| 写入与 Compaction | Writer 反压、Level-0 持续积压 | Write Buffer、Spill、本地盘、Compaction 输入/输出 |
| 提交 | Writer 有流量但新 Snapshot 更新慢 | Checkpoint、Commit Duration、Commit Attempts、Catalog/存储延迟 |

这几类问题可能同时出现。应先画出同一时间窗口内的变化关系：例如 Compaction 积压先上升，随后 Checkpoint 变慢，最后数据可见延迟扩大；单独调高 Checkpoint Timeout 只会推迟失败。

指标口径和挂载位置见[官方 Metrics](https://paimon.apache.org/docs/2.0/maintenance/metrics/)。没有暴露某项指标时，应记录缺口，结合引擎 Profile 和存储监控补证，不能把空值当成零。

## 二、建立可重复的基准数据

下面是实验设计，不是已经执行的性能报告。所有表放在独立库 `paimon_perf_lab`，Catalog 必须先指向隔离的测试 Warehouse。

```sql
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
CREATE DATABASE IF NOT EXISTS paimon_perf_lab;
USE paimon_perf_lab;

CREATE TABLE perf_orders (
    dt STRING,
    order_id BIGINT,
    user_id BIGINT,
    status STRING,
    amount BIGINT,
    update_version BIGINT,
    PRIMARY KEY (dt, order_id) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
    'bucket' = '4',
    'merge-engine' = 'deduplicate',
    'sequence.field' = 'update_version'
);
```

这里的 `4` 只是小型示例的固定桶数，不是生产推荐值。`dt` 表示订单不可变的创建日期；若业务允许日期变化，应先解决旧分区撤回或跨分区 Upsert 语义。

正式基准输入应保存为可以重放的有界数据集，并记录文件校验值、行数、字段编码与生成规则。至少包含：

- 多个分区和足够多的数据文件，避免所有数据落在一个文件中。
- 均匀 Key 与热点 Key 两种分布，分别测试，不能混成一个平均值。
- 固定数量的初始主键，以及分批提交的更新和删除事件。
- 低基数 `status`、高基数 `user_id`、范围过滤 `amount`，用于观察不同索引条件。
- 每一阶段的预期逻辑行数、金额总和和抽样主键最终状态。

初始规模可以从机器能稳定容纳的数据量起步，再逐档扩大。样本是否足够，取决于能否形成真实的文件、合并和资源压力，不由“达到某个行数”自动保证。

## 三、固定逻辑输入，也记录物理布局版本

同一轮重复查询应固定一个仍保留的 Snapshot，避免测试期间 Writer 又提交新数据：

```sql
SELECT snapshot_id, commit_kind, commit_time
FROM `perf_orders$snapshots`
ORDER BY snapshot_id DESC;

-- 42 仅为占位示例，必须替换为上一步实际存在的 Snapshot ID。
SELECT status, COUNT(*) AS orders, SUM(amount) AS total_amount
FROM perf_orders /*+ OPTIONS('scan.snapshot-id' = '42') */
WHERE dt = '2026-11-01'
GROUP BY status;
```

如果测试时间超过保留窗口，应提前为基线建立 Tag 或保留相应历史；不要在测试中途因 Snapshot 过期改成查询最新版本。时间旅行语法见[官方 Flink SQL Query](https://paimon.apache.org/docs/2.0/flink/sql-query/)。

Compaction 和补建索引会改变物理布局。比较这类变更时，不能强求两次都读取同一份旧布局；应记录各自使用的 Snapshot、Schema 和索引状态，并核实变更前后逻辑数据一致。可以使用两张由同一输入重放得到的实验表做对照。

如果两张表的提交批次、更新顺序和 Compaction 状态不同，即使最终行数相同，读写成本也可能不同，这些差异都要写进记录。

## 四、用查询组区分裁剪与读取成本

在固定 Snapshot 上使用同一组查询，分别改变分区、主键和非主键条件：

```sql
-- Q1：有分区条件的主键点查。
SELECT order_id, user_id, status, amount
FROM perf_orders /*+ OPTIONS('scan.snapshot-id' = '42') */
WHERE dt = '2026-11-01' AND order_id = 100001;

-- Q2：非主键低基数字段过滤。
SELECT order_id, amount
FROM perf_orders /*+ OPTIONS('scan.snapshot-id' = '42') */
WHERE dt = '2026-11-01' AND status = 'CLOSED';

-- Q3：非主键范围过滤与业务聚合。
SELECT status, SUM(amount)
FROM perf_orders /*+ OPTIONS('scan.snapshot-id' = '42') */
WHERE dt = '2026-11-01' AND amount BETWEEN 100 AND 200
GROUP BY status;
```

只测试 `COUNT(*)` 容易测到元数据聚合下推，不能代表宽表扫描或明细过滤性能。还应检查所需列数、过滤命中比例和结果输出开销，避免 SQL Client 打印大量结果主导耗时。

主键表非主键条件能否安全下推，受 MOR/COW/MOW、文件是否完整合并及引擎实现约束。索引存在，不表示每个层级都能跳过；见[主键表 Query Performance](https://paimon.apache.org/docs/2.0/primary-key-table/query-performance/)与[Table Mode](https://paimon.apache.org/docs/2.0/primary-key-table/table-mode/)。

## 五、EXPLAIN 与实际指标各证明什么

先检查计划是否识别分区和过滤条件：

```sql
EXPLAIN PLAN FOR
SELECT order_id, amount
FROM perf_orders /*+ OPTIONS('scan.snapshot-id' = '42') */
WHERE dt = '2026-11-01' AND status = 'CLOSED';
```

计划中出现过滤下推，只能说明优化器传递了条件。执行时文件值域是否足够集中、索引是否覆盖旧文件，以及条件是否有选择性，仍会决定实际收益。

建议每次只运行一个测量查询，并保存：

| 证据 | 记录内容 | 不能直接推出的结论 |
| --- | --- | --- |
| 表与文件元数据 | 配置、Snapshot、Level、文件数、字节和索引覆盖 | 查询一定实际使用了索引 |
| Scan Planning 指标 | `lastScanDuration`、`lastScannedManifests`、结果文件数与跳过文件数 | 已等于对象存储实际读取字节 |
| 引擎执行结果 | 总耗时、任务耗时分布、读字节、CPU、结果校验 | 一次耗时下降可以稳定复现 |
| 存储和系统监控 | 请求量、网络、本地盘与 Cache | 存储计数全部由本条查询产生 |

`last...` 通常描述最近一次操作，连续跑其他查询可能覆盖观察值。Planning 阶段留下的文件也可能在后续读取阶段进一步过滤，不能把一个阶段的文件数当作整条链路的全部 I/O。

## 六、冷缓存与暖缓存应分开报告

同一组查询至少区分首次执行和重复执行，分别记录 Paimon 本地 Cache、引擎缓存、操作系统页缓存和对象存储代理缓存的状态。

冷启动测试需要事先定义隔离方法，例如使用独立测试进程或可控的新执行节点。不要在共享生产节点直接清理系统缓存，也不要仅因“重开 SQL Client”就声称所有缓存都已清空。

暖缓存测试应保持查询和执行资源一致，预热后再收集多次稳定样本。比较中位数和尾部耗时，同时报告样本量；少数运行不足以证明可靠的 P99。

若无法控制缓存，就把结论限定为“当前缓存条件下的端到端耗时”，不要把它写成纯粹的文件索引收益。

## 七、Compaction 要同时看积压与代价

观察 Compaction 的意义不是“文件数变少了”，而是文件布局改善能否抵消整理成本。至少比较：

- 同一逻辑数据量下的 Level-0、Sorted Run、文件数和平均文件大小。
- 查询需要读取的文件、读取字节及主键合并 CPU。
- Compaction 输入/输出字节、运行时间和资源峰值。
- 写入吞吐、成功 Checkpoint 间隔和提交可见延迟。

独立 Compactor 会把部分整理工作从 Writer 移到另一个作业，整体集群仍承担这部分 CPU、I/O 和临时空间。`write-only` 后还要明确谁负责 Compaction 和历史清理；见[Dedicated Compaction](https://paimon.apache.org/docs/2.0/maintenance/dedicated-compaction/)。

不要让两个 Compactor 为了测速度同时修改同一分区。不同维护方式的合法并发边界，见 [[23-Paimon 多 Writer 冲突验证与 Bucket 扩缩容]]。

## 八、写入调优按一个因素一轮推进

| 现象 | 可验证的候选措施 | 必须同时检查的代价 |
| --- | --- | --- |
| 每次提交数据很少、小文件多 | 评估 Checkpoint 间隔、活跃分区/桶数 | 可见延迟和恢复重放窗口 |
| Write Buffer 经常提前刷出 | 评估缓冲预算和 Spill | TaskManager 总内存与本地盘 |
| 同一主键被高频重复更新 | 评估 Local Merge 的适用性 | 额外内存、输入语义；CDC 路径存在限制 |
| Compaction 追不上写入 | 定位热点后评估独立 Compactor 或资源 | 整体成本、冲突与维护覆盖范围 |
| 增加并行度没有收益 | 检查 Bucket 路由、倾斜与下游瓶颈 | 不能用更多空闲 Task 代替布局调整 |

上述措施是候选实验，不是一组需要同时开启的默认参数。每轮保留相同输入、运行时长和 SLA，只改变一个因素；确认变化方向后，再测试组合影响。[Write Performance](https://paimon.apache.org/docs/2.0/maintenance/write-performance/)提供参数入口，初始取值应由本机基准决定。

## 九、容量预算不要按 Snapshot 数乘整表大小

Snapshot 会共享数据文件。计算历史占用，应统计当前文件与仍被历史版本、Tag、Branch 引用的文件的并集；不能把每个 Snapshot 的整表大小相加。

规划容量时，将这些部分分别计量：

| 部分 | 主要增长来源 |
| --- | --- |
| 当前有效文件 | 主键规模、行宽、压缩和文件布局 |
| 历史独占文件 | 更新、删除、Overwrite 和 Compaction 后仍被历史引用的文件 |
| Changelog 与索引 | Producer、保留窗口、索引字段及更新频率 |
| 元数据 | Snapshot、Manifest、Schema、Tag、Consumer 和 Branch |
| 本地工作空间 | 排序 Spill、Lookup/动态索引 Cache、Compaction 临时文件 |

容量估计需增加故障停机、清理滞后和维护期间的峰值。旧文件回收依赖保留引用释放，不能用“执行了 Compaction”推断空间立即下降；见[Manage Snapshots](https://paimon.apache.org/docs/2.0/maintenance/manage-snapshots/)。

压测记录中可以定义“存储写出总字节 ÷ 业务输入字节”作为写放大观测值，但要声明两者采用什么编码和计数边界。Compaction 的输入/输出比值只描述整理过程，不等同于全链路写放大。

## 十、什么时候停止调参

一次变更可以接受，需要同时确认：

1. 逻辑对账通过，包括更新、删除和异常分布数据。
2. 目标吞吐、可见延迟或查询耗时达到事先约定的目标。
3. 持续运行后积压不会无限增长，尾部延迟和失败率可接受。
4. 内存、本地盘、远程存储成本和恢复时间仍在预算内。
5. 相同条件下可以重现收益，并有撤回配置或回退布局的方案。

若收益只出现在缓存已经预热、数据量下降或结果较旧的情况下，应把适用范围写明，再决定是否采用。

## 相关笔记

- [[06-Paimon File Index：存储、读写与选型]]
- [[07-Paimon L0、L1、Snapshot 与查询裁剪]]
- [[08-Paimon MOR、COW、MOW 与 Deletion Vector]]
- [[13-Paimon 主键表建模与稳定性检查清单]]
- [[18-Paimon 系统表、监控指标与故障排查]]

## 官方资料与验证边界

- [Paimon 2.0 Query Performance](https://paimon.apache.org/docs/2.0/primary-key-table/query-performance/)
- [Paimon 2.0 Table Mode](https://paimon.apache.org/docs/2.0/primary-key-table/table-mode/)
- [Paimon 2.0 Flink SQL Query](https://paimon.apache.org/docs/2.0/flink/sql-query/)
- [Paimon 2.0 Write Performance](https://paimon.apache.org/docs/2.0/maintenance/write-performance/)
- [Paimon 2.0 Metrics](https://paimon.apache.org/docs/2.0/maintenance/metrics/)
- [Paimon 2.0 Dedicated Compaction](https://paimon.apache.org/docs/2.0/maintenance/dedicated-compaction/)
- [Paimon 2.0 Manage Snapshots](https://paimon.apache.org/docs/2.0/maintenance/manage-snapshots/)

以上官方页面支持机制、指标与参数说明；基准数据设计、对照方法和接受条件是本文补充的工程验证方案。具体阈值应由实际业务 SLA 决定，本文没有提供未经测量的性能数字。
