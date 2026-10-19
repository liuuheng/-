# Paimon 实时与离线共用维表设计：Tag、Lookup 与历史重跑

> 适用范围：Paimon 2.0、Flink 实时 Lookup Join、Spark 或 Flink Batch 离线查询。具体参数仍需按实际使用的 Paimon、Flink、Spark 版本复核。

## 一、要解决的不是“能否共用一张表”，而是两种读取语义

一张维表同时服务实时和离线，在数据模型上没有冲突。冲突来自读取时间：

- 实时任务需要不断看到新的维度状态，例如用户等级发生变化后，后续到达的事实流应尽快关联到新等级。
- 离线任务需要在一次运行内固定维度版本。当天失败后隔天重跑，也应继续使用原来的维度状态，否则同一业务日期可能得到不同结果。

Paimon 的主键表负责保存持续变化的当前态，Snapshot 表示一次成功提交后的完整表状态，Tag 为选定的 Snapshot 提供稳定名称并延长其可读时间。因此，单表方案的实际关系是：

```text
实时写入任务 ──持续提交──> 最新 Snapshot ──供实时 Lookup 使用
                                  │
                                  └──每日固定一个 Tag ──供离线任务和重跑使用
```

Tag 不是日期分区，也不是每天复制一份全表。它固定的是“这一时刻整张表引用哪些 Manifest 和数据文件”。

## 二、方案选择

### 方案一：当前态主键表加每日 Tag

一张不按日期分区的主键表持续接收 CDC 或 Upsert。实时任务 Lookup 最新状态；离线任务显式读取指定 Tag。

这个方案适用于以下条件同时成立的场景：

- 维表表达的是按主键覆盖的当前状态，通常使用 `deduplicate`。
- 离线任务需要的是“某个日切时刻的整表状态”，不是每条事实发生时刻对应的历史维度。
- 离线计算引擎支持 Paimon Tag 或 Time Travel。
- 团队能够监控 Tag 是否生成，并让 Tag 保留窗口覆盖重跑窗口。
- 实时和离线可以共用表结构、主键和数据质量规则。

它减少了一套每日全量同步链路，也避免同一份维度数据在两张表之间产生口径差异。代价是离线任务必须理解 Tag 语义，历史 Tag 还会固定旧数据文件，增加存储和清理压力。

### 方案二：当前态表和每日全量快照表分开

当前态表只服务实时 Lookup；离线任务读取 `dt` 分区中的每日全量维表。

以下情况更适合拆表：

- 离线引擎不能稳定读取 Paimon Tag。
- 审计要求每天交付独立、可导出或可归档的物理数据集。
- 实时与离线的字段、脱敏、保留周期或者 SLA 差异很大。
- 每日快照需要被多个非 Paimon 引擎长期读取。
- 业务明确要求 `WHERE dt = '${bizdate}'` 这种物理分区接口，并愿意承担每日全量写入成本。

需要注意：只有“每个 `dt` 分区都保存完整维表”时，它才与每日 Tag 的完整表状态近似等价。如果 `dt` 分区只保存当天变化量，它只是增量日志，不能直接当作当天完整维表 Join。

## 三、推荐的单表模型

当前态维表一般不使用业务日期分区。主键直接表达业务实体，更新覆盖同一个 Key：

```sql
CREATE TABLE dim_user (
    user_id       BIGINT,
    user_name     STRING,
    user_level    STRING,
    region_id     BIGINT,
    update_time   TIMESTAMP(3),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'bucket' = '16',
    'merge-engine' = 'deduplicate',
    'deletion-vectors.enabled' = 'true',

    'tag.automatic-creation' = 'watermark',
    'tag.creation-period' = 'daily',
    'tag.creation-delay' = '10 m',
    'tag.num-retained-max' = '45',
    'tag.default-time-retained' = '45 d'
);
```

这份 DDL 是讨论起点，不是可以无条件复制的最终参数。

`PRIMARY KEY (user_id)` 和 `merge-engine='deduplicate'` 定义当前态语义。同一用户的新记录覆盖旧记录。如果输入可能乱序，应确认上游 CDC 顺序是否可靠；只有存在可比较且能稳定表达先后顺序的字段时，才配置 `sequence.field`。

`bucket='16'` 表示固定 Bucket，但 16 只是示例。桶数应根据维表大小、单桶数据量、写入并行度、主键倾斜、Lookup 方式和离线扫描并行度估算。固定 Bucket 不等于自动启用某种 PartialCache。Paimon 2.0 中，大维表 Shuffle Lookup 还要求 Flink 2.0+、显式开启 Shuffle Lookup，并且 Join Key 包含全部 Bucket Key；必须通过实际执行计划确认是否生效。

`deletion-vectors.enabled='true'` 将主键表切换到 MOW 路径。它可以减少批读时多路归并，但会增加写入阶段的 Lookup、Deletion Vector 和 Compaction 成本。Paimon 2.0 中，MOW 的 Level-0 文件通常要在 Lookup Compaction 发布后才可见；如果改成异步 Compaction，数据可见性可能延迟。这个参数与 Tag 没有依赖关系，也不能仅凭“使用 FullCache”就直接决定是否开启。

`tag.automatic-creation='watermark'` 只有在写入链路确实传递 Watermark 时才成立，并且需要核对 `sink.watermark-time-zone`。没有可靠 Watermark 时，可以使用 `process-time`，但它表达的是机器时间日切，无法证明业务数据已经完整。

## 四、实时 Lookup 如何使用

实时作业读取当前表，不指定 Tag：

```sql
SELECT
    o.order_id,
    o.user_id,
    d.user_level,
    d.region_id
FROM kafka_orders AS o
LEFT JOIN dim_user FOR SYSTEM_TIME AS OF o.proc_time AS d
    ON o.user_id = d.user_id;
```

这里的 `FOR SYSTEM_TIME AS OF o.proc_time` 是 Flink Lookup Join 语法，不表示对每条事实做历史 Snapshot Time Travel。Lookup 算子查询的是其当前已刷新到的维表状态，实际新鲜度还受 Paimon Commit、Lookup 刷新周期、本地 Cache 和故障恢复影响。

如果维表很大，应先确认使用的是普通 Lookup、Shuffle Lookup、Query Service，还是某个版本中的 PartialCache/FullCache 实现。固定 Bucket 只是部分优化路径的前提，不能代替执行计划和运行指标验证。

## 五、离线任务如何固定读取 Tag

离线调度不要直接执行普通的 `SELECT * FROM dim_user`。普通批读默认读取任务启动时的最新 Snapshot，任务重跑时间不同，维表结果也可能不同。

调度系统应分别传入：

```text
bizdate     = 2026-10-13    # 本次处理的事实数据日期
dim_tag     = 2026-10-13    # 本次固定使用的维表版本
snapshot_id = 18452         # Tag 实际指向的 Snapshot，用于审计
```

不要在代码中默认 `bizdate` 必然等于 `dim_tag`。两者的映射取决于日切时间、迟到数据策略和业务口径，应由调度配置明确记录。

任务启动前先检查目标 Tag：

```sql
SELECT
    tag_name,
    snapshot_id,
    commit_time
FROM paimon_catalog.dim_db.`dim_user$tags`
WHERE tag_name = '${dim_tag}';
```

Tag 不存在时，任务应该等待或失败，不能悄悄改读最新 Snapshot。否则同一业务日期第一次运行和重跑会关联到不同维度状态。

Spark 3.3+ 可以通过 `VERSION AS OF` 读取 Tag：

```sql
INSERT OVERWRITE TABLE dws_user_order PARTITION (dt = '${bizdate}')
SELECT
    f.order_id,
    f.user_id,
    d.user_level,
    d.region_id
FROM dwd_order AS f
LEFT JOIN paimon_catalog.dim_db.dim_user VERSION AS OF '${dim_tag}' AS d
    ON f.user_id = d.user_id
WHERE f.dt = '${bizdate}';
```

Flink Batch SQL 可以使用动态参数：

```sql
SET 'execution.runtime-mode' = 'batch';

INSERT OVERWRITE dws_user_order
SELECT
    f.order_id,
    f.user_id,
    d.user_level,
    d.region_id
FROM dwd_order AS f
LEFT JOIN dim_user
    /*+ OPTIONS('scan.tag-name' = '${dim_tag}') */ AS d
    ON f.user_id = d.user_id
WHERE f.dt = '${bizdate}';
```

初次运行和历史重跑必须复用同一个 `${dim_tag}`。重跑时重新计算“当前最新 Tag”，就失去了固定版本的意义。

## 六、日切时间、创建延迟和任务失败

`tag.creation-period='daily'` 按自然日周期生成 Tag。`tag.creation-delay='8 h'` 表示将生成动作延迟到日切后约 8 小时，它不会把业务日边界从 00:00 改成 08:00。

如果业务要求“每天 08:00 的状态才是正式日快照”，应由外部调度在 08:00 后选择经过确认的 Snapshot 并手动创建 Tag，而不是只依赖 `creation-delay` 改写业务含义。

使用 `watermark` 时，Tag 创建进度受输入 Watermark 驱动。上游没有推进 Watermark、写入任务失败或迟到数据未完成，都可能导致 Tag 延迟或缺失。使用 `process-time` 时，机器时间到了也不代表业务数据完整。

因此，日切链路应拆成可验证的三个条件：

1. 实时写入作业健康，并成功提交目标截止时间附近的 Snapshot。
2. `$tags` 中出现目标 Tag，而且 `commit_time`、Watermark 或业务校验符合日切口径。
3. 离线任务拿到 Tag 名称和 Snapshot ID 后再启动。

如果实时任务在日切时失败，早上恢复后不要假设系统一定会自动补出符合原截止时间的 Tag。先检查 `$tags` 和 `$snapshots`；若存在符合业务截止时间的 Snapshot，可以在确认后按 Snapshot ID 手动创建 Tag。若没有符合口径的 Snapshot，任务应继续阻塞，或者由业务负责人决定是否接受新的截止时间。

## 七、Tag 保留多少天

Tag 保留窗口至少覆盖最长历史重跑窗口，并为排队、修复和节假日留出缓冲：

```text
Tag 保留天数 >= 最大重跑跨度 + 最长故障恢复时间 + 运维缓冲
```

可采用下面的初始值，再根据真实使用记录调整：

| 使用场景 | 每日 Tag 初始保留量 |
| --- | ---: |
| 只支持日常失败重跑 | 30～45 |
| 支持跨月补数、月度结算 | 60～90 |
| 支持季度回溯 | 100～120 |
| 一年以上审计 | 先评估存储和合规要求，再决定是否继续使用在线表 Tag |

同时配置 `tag.num-retained-max` 和 `tag.default-time-retained` 时，应把它们理解为数量与时间的双重限制，不应承诺任何时刻一定存在完整的 N 个自然日 Tag。还要监控中间日期是否因写入或 Watermark 异常而缺 Tag。

## 八、过多 Tag 是否影响 Compaction

Tag 通常不会阻止最新表执行 Compaction，也不会要求 Compaction 把所有历史版本重新合并。它主要影响的是旧文件回收。

假设旧 Tag 引用了文件 `A、B`，后续 Compaction 将当前数据整理成文件 `D`。最新 Snapshot 可以只读取 `D`，但只要旧 Tag 仍然引用 `A、B`，这两个文件就不能物理删除。因此：

- 最新表仍然可以继续合并整理。
- 被替换的旧文件可能长期留在对象存储或 HDFS。
- 已创建的历史 Tag 固定在原来的文件布局上，不会自动享受后续 Compaction 的整理结果。
- 大量 Tag 集中过期时，Snapshot Expiration 可能集中删除大量文件，增加提交或存储系统压力。

真正危险的组合是“高更新率、频繁 Full Compaction、Tag 长期保留”。每次全量整理都会生成新的文件，历史 Tag 又固定旧文件，存储增长可能逐渐接近每日保存一份全量快照。

因此，不应只监控 Tag 数量，还要观察：

- Warehouse 总字节数和物理文件数的增长速度。
- `$files` 中最新 Snapshot 的 Level-0 文件数、平均文件大小和 Bucket 倾斜。
- `maxLevel0FileCount`、Compaction 时间和积压。
- Commit、Snapshot Expiration 的耗时。
- 历史 Tag 的 Scan Planning 时间和扫描文件数。
- 删除过期 Tag 后，旧文件是否经过 Snapshot Expiration 正常回收。

## 九、Tag 与日期分区快照的差异

| 对比项 | 每日 Tag | 每日全量分区快照 |
| --- | --- | --- |
| 表达的状态 | 某个 Snapshot 的整表状态 | 某个 `dt` 分区中的物理数据 |
| 数据写入 | 实时更新当前态，Tag 固定版本 | 每天执行一次全量写入 |
| 存储方式 | 相邻版本可复用未变化文件 | 通常每天复制一份全表 |
| 高更新或全表重写 | 大量旧文件被 Tag 固定，优势会缩小 | 成本稳定接近表大小乘以天数 |
| 查询方式 | `VERSION AS OF` 或 `scan.tag-name` | `WHERE dt = ...` |
| 引擎要求 | 需要支持 Paimon Time Travel | 普通分区读取更通用 |
| 实时服务 | 同一张当前态表即可 Lookup | 通常还要维护另一张当前态表 |
| 主要维护对象 | Tag 完整性、保留策略、旧文件回收 | 每日全量任务、分区完整性、分区过期 |

Tag 的存储量不是简单的“表大小 × Tag 数量”。相邻 Snapshot 可以共享未变化的数据文件；实际增量取决于更新比例、Compaction 重写量和 Overwrite 范围。低更新率维表通常比每日全量分区节省空间；每天全表 Overwrite 或频繁 Full Compaction 时，两者的物理成本可能逐渐接近。

## 十、不能用每日 Tag 解决的需求

每日 Tag 只能让一次离线任务统一使用一个固定维表版本。它不能让同一个 Join 根据每条事实的事件时间动态切换 Tag。

例如，订单 A 发生在 09:00，订单 B 发生在 17:00，如果要求两条订单分别关联各自发生时刻的用户等级，需要的是带 `effective_start_time`、`effective_end_time` 的 SCD2 历史维表，或者其他事件时间版本模型。每日 Tag 只能回答“这批订单统一按当天某个截止时刻的维度状态计算”。

## 十一、推荐落地流程

1. 将实时维表定义为不按日期分区的主键当前态表，确认主键、去重顺序和删除语义。
2. 根据表大小和 Lookup 方案估算固定 Bucket 数，不直接照搬 `16`。
3. 结合写入与批读压测决定是否启用 Deletion Vector，并验证数据可见延迟。
4. 选择 Watermark 或 Process Time 自动建 Tag；存在 08:00 等自定义业务日切时，改用外部调度创建并校验 Tag。
5. 离线调度同时记录 `bizdate`、`dim_tag`、`snapshot_id`，执行前检查 `$tags`。
6. 初次运行与重跑始终使用原 Tag；Tag 缺失时阻塞，不回退到最新 Snapshot。
7. 初始保留 45～90 个每日 Tag，再根据真实重跑跨度和物理存储增长调整。
8. 持续监控 Lookup 新鲜度、Compaction 积压、历史文件占用和 Snapshot Expiration。

## 十二、上线前检查表

- [ ] 业务需要的是日切固定版本，还是逐事实事件时间的历史维度？
- [ ] 离线引擎和实际 Paimon 版本是否支持 Tag 读取？
- [ ] 主键是否能唯一表达当前业务实体？
- [ ] 输入乱序时，是否有可靠的顺序字段或 CDC 顺序保证？
- [ ] Bucket 数是否根据数据量、倾斜、写入并行度和 Lookup 方式确定？
- [ ] Lookup 的实际策略是否通过执行计划和运行指标确认？
- [ ] Watermark 是否真实存在并持续推进？时区是否正确？
- [ ] 日切延迟与业务日边界是否被误认为同一配置？
- [ ] 离线任务是否在执行前检查 Tag，并记录 Snapshot ID？
- [ ] Tag 保留窗口是否覆盖重跑、排队和故障恢复？
- [ ] 旧 Tag 中的删除前数据是否符合安全、隐私和审计要求？
- [ ] 是否监控 Compaction、物理存储增长和旧文件回收？

## 相关笔记

- [[Paimon 主键表建模与稳定性检查清单]]
- [[Paimon Snapshot、Manifest、Parquet 与 Compaction 的关系]]
- [[Paimon 系统表、监控指标与故障排查]]
- [[Flink 实时维表性能优化（下）：用对 Paimon Lookup 的完整实践（文章批注版）]]
- [[Paimon MOR、COW、MOW 与 Deletion Vector]]

## 官方资料

- [Paimon 2.0：Manage Tags](https://paimon.apache.org/docs/2.0/maintenance/manage-tags/)
- [Paimon 2.0：Flink SQL Query](https://paimon.apache.org/docs/2.0/flink/sql-query/)
- [Paimon 2.0：Spark SQL Query](https://paimon.apache.org/docs/2.0/spark/sql-query/)
- [Paimon 2.0：Flink Lookup Join](https://paimon.apache.org/docs/2.0/flink/sql-lookup/)
- [Paimon 2.0：Primary Key Table Mode](https://paimon.apache.org/docs/2.0/primary-key-table/table-mode/)
- [Paimon 2.0：Manage Snapshots](https://paimon.apache.org/docs/2.0/maintenance/manage-snapshots/)
- [Paimon 2.0：Metrics](https://paimon.apache.org/docs/2.0/maintenance/metrics/)

> 文档核对时间：2026-10-14。涉及 Lookup 策略、Deletion Vector、Tag 自动创建和 Time Travel 的行为，应以生产环境实际版本和验证结果为准。
