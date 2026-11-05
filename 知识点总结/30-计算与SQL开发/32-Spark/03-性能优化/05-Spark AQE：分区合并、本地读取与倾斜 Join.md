##  AQE相关参数 （3.2默认开启）

### `spark.sql.files.maxPartitionBytes=256MB`

- 读取文件时，每个输入分区的目标上限是 256MB。
- 值越大：分区更少、task 更少、调度开销更低；但单 task 更重。
- 常见范围 128MB~512MB，256MB 属于偏稳妥。
- > ⚠️ 注意：此参数**只影响读取阶段**（scan 时的分区数），对 shuffle 后的分区数没有影响，两者独立控制。

---

### `spark.sql.adaptive.enabled=true`

- 开启 AQE 总开关。
- Spark 会在运行时根据真实统计信息重写执行计划（比如改 join 策略、合并分区）。
- > ⚠️ 补充：AQE 是 Spark 3.0 正式启用的特性，**Spark 3.2+ 默认已为 `true`**，若集群版本 ≥ 3.2，此配置可省略（显式写上也无害）。

---

### `spark.sql.adaptive.coalescePartitions.enabled=true`

- AQE 子功能：自动合并 shuffle 后的小分区。
- 作用是减少"很多很小的 task"，提升整体吞吐和稳定性。
- > ⚠️ 注意：此功能依赖 AQE 总开关开启，且**只作用于 shuffle 后的分区合并**，不影响读取阶段的分区数。

---

### `spark.sql.adaptive.advisoryPartitionSizeInBytes=256MB`

- AQE 在合并/调整 shuffle 分区时的"目标分区大小建议值"。
- > ⚠️ 勘误："`advisoryPartitionSizeInBytes` 和 `maxPartitionBytes` 对齐，整体分区策略更一致"的说法**容易产生误导**：
  >   - `maxPartitionBytes` 作用于**读取文件阶段**（输入端）
  >   - `advisoryPartitionSizeInBytes` 作用于 **AQE shuffle 后**的分区合并（中间/输出端）
  >   - 两者作用阶段完全不同，数值相同只是巧合，**并不代表"策略一致"**
- 推荐值通常在 64MB~256MB 之间，应结合下游 task 实际数据量调整，无需刻意与 `maxPartitionBytes` 对齐。

---

### `spark.sql.adaptive.localShuffleReader.enabled=true`

- 在可能情况下优先本地读取 shuffle 数据，减少跨节点网络拉取。
- 通常可降低网络开销、提高读取效率，尤其在 join/聚合阶段。
- > ⚠️ 补充：此优化在 **broadcast join 转换后效果最显著**，普通 sort-merge join 场景下收益相对有限。

---

### `spark.sql.adaptive.skewJoin.enabled=true`

- AQE 子功能：自动识别并处理倾斜 join（某些 key 数据量特别大）。
- 会把大分区拆分处理，避免个别 task 拖慢全局（长尾问题）。
- > ⚠️ 补充：skew join 的拆分会引入**额外的 shuffle 开销**，对于数据量不大、key 分布均匀的场景可能略有负担，并非适用所有情况的银弹，建议在确认存在数据倾斜时再依赖此功能。



---


# Spark AQE：分区合并、本地读取与倾斜 Join

注：AQE本质上是运行时优化，一些原数据是在运行时才能获取，然后在进行优化，和hint提示有区别

> spark.sql.adaptive.localShuffleReader.enabled=true 下面的话主要是针对此参数进行进一步描述

## localShuffleReader 与 Broadcast Join 的关系

### 疑问：broadcast 不是已经将小表放到 executor 了吗，读取是不是都在本地？

理解**部分正确**，但混淆了两个阶段。

**Broadcast Join 的过程：**

1. 小表被 broadcast 广播到所有 executor —— 这部分确实是"小表本地化"
2. 但大表在 join **之前**往往经历过 shuffle，shuffle 产生的中间数据块分散在各个节点上
3. `localShuffleReader` 的作用是：在读取**这部分 shuffle 中间数据**时，优先从本地磁盘读，而不是跨节点网络拉取

**为什么 broadcast join 转换后效果最显著？**

AQE 有一个能力：在运行时发现某张表足够小，**动态地把原本计划的 sort-merge join 转换成 broadcast join**。

转换发生后，原来为 sort-merge join 准备的 shuffle 数据已经写到磁盘了（这个 shuffle 无法撤销），此时 `localShuffleReader` 就能介入，让后续读取这批已有 shuffle 数据时尽量走本地，避免不必要的网络传输。

> **一句话总结：** broadcast 解决的是"小表不需要 shuffle"，`localShuffleReader` 解决的是"已经产生的 shuffle 数据尽量本地读"——两者作用对象不同，并不冲突。

---

## AQE 动态转换的完整流程

### 场景背景

假设有两张表做 join：

- **大表 A**：订单表，10GB
- **小表 B**：用户维度表，50MB

---

### 没有 AQE 时的 Sort-Merge Join 流程

```
大表 A ──► Shuffle ──► 按 join key 分区排序 ──┐
                                              ├──► Sort-Merge Join
小表 B ──► Shuffle ──► 按 join key 分区排序 ──┘
```

两张表都要经历完整的 shuffle，数据按 join key 哈希分区，**跨节点网络传输**，每个 executor 拉取自己负责的那部分数据，再做归并排序 join。

---

### AQE 介入后的动态转换流程

**第一阶段：shuffle 已经发生了**

```
大表 A ──► Shuffle 写出 ──► shuffle 文件落盘（分散在各节点）
小表 B ──► Shuffle 写出 ──► shuffle 文件落盘（分散在各节点）
```

> ⚠️ 注意：此时 shuffle 数据**已经写到磁盘了，无法撤销**。

---

**第二阶段：AQE 在运行时统计数据量**

AQE 在 shuffle 写出完成后，会收集真实的统计信息：

```
AQE 统计：小表 B shuffle 后实际只有 45MB
          ↓
判断：45MB < spark.sql.autoBroadcastJoinThreshold（默认 10MB，可调）
          ↓
决策：动态将 Sort-Merge Join 改为 Broadcast Join
```

---

**第三阶段：执行 Broadcast Join**

```
小表 B 的数据 ──► broadcast 广播到所有 executor（内存中）
                                    ↓
大表 A 的 shuffle 文件 ──► localShuffleReader 介入
                           ↓
                 优先读取本节点上的 shuffle 文件
                 （不再需要跨节点拉取大表数据）
                                    ↓
                        本地直接与广播的小表 B 做 join
```

---

### localShuffleReader 的关键作用

||没有 localShuffleReader|有 localShuffleReader|
|---|---|---|
|大表 shuffle 数据读取|跨节点网络拉取|优先读本地磁盘|
|网络开销|高|低|
|适用前提|—|AQE 已将 join 转为 broadcast|

**为什么转成 broadcast join 之后才有效？**

因为 sort-merge join 本身要求数据必须按 key 重新分区对齐，数据该去哪个节点是固定的，`localShuffleReader` 没有介入空间。而一旦转成 broadcast join，大表的 shuffle 数据**在哪个节点读都行**，这时候就可以优先读本地的，不用再跨节点拉取了。

---

### 完整流程总结

```
两表 Shuffle 写出（落盘）
        ↓
AQE 收集统计信息
        ↓
发现小表足够小 ──► 动态改为 Broadcast Join
        ↓
小表广播到所有 executor
        ↓
localShuffleReader：大表 shuffle 数据优先本地读
        ↓
本地完成 Join，无需跨节点网络传输大表数据
```

> **一句话记忆：** broadcast 负责把小表"推"到每个节点，localShuffleReader 负责让大表的 shuffle 数据"不用跑"——两个优化方向相反但相互配合，共同消灭网络传输。

---

## 为什么 AQE 的判断在 shuffle 之后？

AQE 的设计原则是**"运行时"优化**，它依赖的是真实执行后的统计数据：

```
执行计划制定时：Spark 只知道"预估"大小
                ↓ 预估可能严重不准（压缩、过滤、倾斜等）
Shuffle 写出后：Spark 才知道每个分区"实际"多大
                ↓ 此时统计数据才可靠
AQE 介入：基于真实数据做决策
```

所以 AQE 宁愿"先 shuffle 再改计划"，也不愿意基于不可靠的预估贸然决策。

---

## 显式指定广播小表（`BROADCAST hint`）

这走的是**完全不同的路径**，Spark 会在 shuffle 之前就决定用 broadcast join：

```sql
SELECT /*+ BROADCAST(b) */ *
FROM a JOIN b ON a.id = b.id
```

```
执行计划阶段：看到 BROADCAST hint
                ↓
直接规划为 Broadcast Join，跳过小表的 shuffle
                ↓
小表 B ──► 直接读取 ──► driver 收集 ──► broadcast 到所有 executor
大表 A ──► 正常 scan ──► 每个 executor 本地与广播数据 join
```

**小表完全不经过 shuffle**，大表也不需要 shuffle，整个 join 没有 shuffle 阶段。

---

## AQE 动态转换 vs 显式 BROADCAST hint 对比

||AQE 动态转换|显式 BROADCAST hint|
|---|---|---|
|决策时机|shuffle 写出后|执行计划阶段|
|小表是否 shuffle|✅ 已经 shuffle 了|❌ 完全跳过|
|大表是否 shuffle|✅ 已经 shuffle 了|❌ 完全跳过|
|localShuffleReader 是否有用|✅ 有用（大表 shuffle 已存在）|❌ 没有 shuffle 数据，无从介入|
|依赖统计信息|是（运行时真实数据）|否（用户强制指定）|
|风险|几乎没有|小表预估错误可能 OOM|

> **一句话总结：** 显式 `BROADCAST hint` 是"事前规划"，整个 join 没有 shuffle，性能最优但需要你自己保证小表够小；AQE 动态转换是"事后补救"，shuffle 已经发生了再改策略，`localShuffleReader` 才有用武之地。因此，如果**明确知道某张表很小**，直接用 hint 比依赖 AQE 效果更好。

---

## 判断 AQE 是否真正接管了一个 Shuffle

配置中出现 `spark.sql.adaptive.enabled=true`，或者物理计划根节点出现 `AdaptiveSparkPlan`，只能证明 AQE 已经开启，不能证明某个具体 Exchange 已被 AQE 改写。

AQE 合并 Shuffle 分区需要同时满足下面的执行条件：

1. 上游 Shuffle Map Stage 已经完成，Spark 已经获得各 Shuffle 分区的实际大小。
2. 下游读取仍处于 AQE 可以改写的查询计划中。
3. 最终计划在目标 Shuffle 的消费端出现 `ShuffleQueryStage` 和 `AQEShuffleRead`，分区合并时通常还能看到 `coalesced`。

正常被 AQE 接管的简化结构如下：

```text
Exchange
└─ ShuffleQueryStage
   └─ AQEShuffleRead coalesced
      └─ Join / Aggregate
```

如果最终计划仍然只是：

```text
Exchange hashpartitioning(join_key, 200)
└─ Sort
   └─ SortMergeJoin
```

并且 Exchange 的消费端没有 `AQEShuffleRead`，那么 `advisoryPartitionSizeInBytes`、`minPartitionSize` 等参数没有实际介入这个 Shuffle。此时 Stage 的 Task 数通常仍由 Exchange 声明的分区数决定。

因此，判断顺序应是：先找目标 Stage 对应的 Exchange，再检查最终计划中是否出现 `AQEShuffleRead`，最后才解释 AQE 参数。不能只根据 AQE 开关或 Stage Task 数下结论。

## 案例：`CACHE TABLE` 物化时 Join 的 200 个 Task 没有被 AQE 合并

### 运行信息

- Spark 版本：`3.2.2-dt`
- Application ID：`application_1783495977342_2319165`
- Attempt ID：`1`
- SQL Execution ID：`7`
- 目标 Stage：`Stage 3.0`
- 执行内容：物化临时缓存表 `tmp_order_quality_core_20261022`

业务查询先把三路数据按 `order_id` 进行 Shuffle，再执行两个 `SortMergeJoin LeftOuter`：

1. 季度审核订单 LEFT JOIN 驳回/退回记录聚合结果。
2. 上一步结果 LEFT JOIN 每个订单的最新设计记录。
3. 将 Join 结果写入 `InMemoryRelation`，供后续查询重复读取。

对应的 SQL 不是一个抽象 Join，而是下面这种可复现的缓存结构。这里省略字段清单和各子查询内部过滤，只保留会改变执行边界的部分：

```sql
DROP VIEW IF EXISTS tmp_order_quality_core_${bdp.system.bizdate};

CREATE TEMPORARY VIEW tmp_order_quality_core_${bdp.system.bizdate} AS
SELECT
        full_order.creator_id
        ,full_order.order_id
        ,full_order.created
        ,full_order.last_modified
        ,order_design.order_design_id
        ,order_design.design_id
        ,coalesce(reject_return.return_cnt, 0) AS return_cnt
        ,coalesce(reject_return.reject_cnt, 0) AS reject_cnt
FROM
(
        -- 当前季度审核订单
        SELECT order_id, created, creator_id, last_modified
        FROM order_info
) full_order
LEFT JOIN
(
        -- 按 order_id 聚合驳回和退回次数
        SELECT order_id, return_cnt, reject_cnt
        FROM reject_return_aggregate
) reject_return
ON full_order.order_id = reject_return.order_id
LEFT JOIN
(
        -- 每个 order_id 对应的最新订单方案
        SELECT order_id, order_design_id, design_id
        FROM latest_order_design
) order_design
ON full_order.order_id = order_design.order_id;

CACHE TABLE tmp_order_quality_core_${bdp.system.bizdate};
```

这里真正触发案例现象的是 `CACHE TABLE`：它立即物化临时视图，并把前面的两个 Join 封装为缓存构建计划。以后即使原 Application 已从 History Server 清理，只要看到“临时视图中包含 Shuffle Join，随后立即 `CACHE TABLE`”的结构，就可以按本案例检查缓存内部 Exchange 是否出现 `AQEShuffleRead`。

### Stage 指标

Stage 3 的直接运行事实如下：

| 指标 | 数值 |
|---|---:|
| 成功 Task 数 | 200 |
| Shuffle Read | 955,891,585 bytes |
| Shuffle Read Records | 46,485,272 |
| Shuffle Write Records | 200 |
| Stage 墙钟耗时 | 5.856 s |
| Memory Spill / Disk Spill | 0 / 0 |

三个上游 Stage 写出的记录数分别为：

- Stage 0：37,889,732
- Stage 1：8,320,930
- Stage 2：274,610

三者相加正好是 `46,485,272`，与 Stage 3 的 Shuffle Read Records 闭合。这证明 Stage 3 正在消费三路 Join 输入，而不是扫描原始文件。

Stage 3 的 200 个 Task 也不能解释为 200 个文件。它们对应缓存构建计划中三个 `Exchange hashpartitioning(order_id, 200)` 的接收分区。平均每个 Task 只读取约 `4.56 MiB` Shuffle，说明分区较碎，但不存在单 Task 数据过大的证据。

`Shuffle Write Records=200` 也不是最终业务结果只有 200 行。`CACHE TABLE` 的 eager 物化会为每个缓存分区产生一条局部计数记录；后续 Stage 8 使用 1 个 Task 汇总这 200 条记录，完成触发缓存的 `count`。

### 为什么 AQE 开启了却没有合并

SQL Execution 7 的最终计划可以简化为：

```text
AdaptiveSparkPlan
└─ HashAggregate
   └─ ShuffleQueryStage
      └─ Exchange
         └─ HashAggregate
            └─ Scan In-memory table tmp_order_quality_core_20261022
               └─ InMemoryRelation
                  └─ Project
                     └─ SortMergeJoin LeftOuter
                        ├─ SortMergeJoin LeftOuter
                        │  ├─ Exchange hashpartitioning(order_id, 200)
                        │  └─ Exchange hashpartitioning(order_id, 200)
                        └─ Exchange hashpartitioning(order_id, 200)
```

外层确实是 `AdaptiveSparkPlan`，但两个 Join 和三个 Exchange 已经被封装进 `InMemoryRelation` 的缓存构建计划。外层 AQE 实际看到的是 `InMemoryTableScan` 以及用于触发缓存物化的聚合，不是缓存内部 Join 的 Shuffle 消费端。

本次最终计划中，三个 Join Exchange 上方都没有出现 `AQEShuffleRead`。因此它们仍按 `hashpartitioning(order_id, 200)` 执行，Stage 3 保留 200 个 Task。

这里能够确认的是：在本次 Spark `3.2.2-dt` 的 `CACHE TABLE` eager 物化执行路径中，缓存内部的 Join Exchange 没有进入 AQE 的分区合并改写。不能据此泛化为“所有 Spark 版本中的缓存都会禁用 AQE”；不同 Spark 版本、缓存实现和查询结构需要重新检查最终计划。

### 为什么相关参数没有改变 Stage 3

- `spark.sql.files.maxPartitionBytes` 控制文件扫描分区，Stage 3 消费的是 Shuffle，不受它控制。
- `spark.sql.adaptive.coalescePartitions.initialPartitionNum` 是 AQE 合并前的初始分区基准，前提是该 Shuffle 进入 AQE 的读取路径。
- `spark.sql.adaptive.advisoryPartitionSizeInBytes` 是 AQE 合并后的目标大小建议值，不是强制分区大小。
- `spark.sql.adaptive.coalescePartitions.minPartitionSize` 是合并过程的下限约束，同样依赖 `AQEShuffleRead`。
- `spark.sql.adaptive.forceApply=true` 也不能解决本例，因为外层已经存在 `AdaptiveSparkPlan`；缺失的是缓存内部 Exchange 对应的自适应 Shuffle Reader。
- 环境中没有显式设置 `spark.sql.shuffle.partitions`，计划使用了默认的 200；最终物理计划中的 `hashpartitioning(order_id, 200)` 是直接证据。

### 是否需要优化

Stage 3 虽然有 200 个 Task，但墙钟耗时只有约 5.86 秒、没有 Spill，平均 Shuffle Read 约 4.56 MiB。Task 数偏多不等于它是性能瓶颈，本例不值得为了 Stage 3 单独修改全局参数。

如果后续同类缓存 Stage 的调度开销已经成为主要耗时，可以选择两类改法，但需要分别验证：

1. 在创建和物化缓存前调整 `spark.sql.shuffle.partitions`。它会改变该 Session 内相关 Exchange 的分区数，不只影响一个 Stage，不能随意降低。
2. 在公共核心查询的顶层使用 `COALESCE(n)` 控制缓存结果分区数。它主要影响缓存结果及后续读取，不代表上游 Join 的三个 Exchange 也会从 200 降为 `n`。

将缓存结果先写成临时磁盘表、再由下一条查询读取，也能形成新的查询边界，但会增加落盘和再次扫描成本。只有缓存内部 Join 本身是主要瓶颈、并且 AQE 收益高于额外 I/O 时才值得尝试。

### 优化后复验：静态分区从 200 调整为 22

优化后运行：`application_1783495977342_2319172`。SQL 结构和缓存边界没有改变，本次新增：

```properties
spark.sql.shuffle.partitions=22
```

最终计划中的三个 Exchange 由：

```text
Exchange hashpartitioning(order_id, 200)
```

变为：

```text
Exchange hashpartitioning(order_id, 22)
```

前后两个计划的目标 Join 上方都没有 `AQEShuffleRead`。因此 `200→22` 不是 AQE 合并结果，而是 `spark.sql.shuffle.partitions` 直接改变了缓存内部 Exchange 的静态接收分区数。

| Stage 3 指标 | 调整前 | 调整后 |
|---|---:|---:|
| 成功 Task | 200 | 22 |
| Shuffle Read | 911.61 MiB | 849.77 MiB |
| Shuffle Read Records | 46,485,272 | 46,442,042 |
| Task Shuffle Read P50 | 4.56 MiB | 38.62 MiB |
| Task Shuffle Read P95 | 4.57 MiB | 38.67 MiB |
| Task Duration P50 | 0.244 s | 3.147 s |
| Task Duration P95 | 1.867 s | 4.340 s |
| Peak Execution Memory P50 | 140.06 MiB | 288.50 MiB |
| 累计 Executor Run Time | 87.60 s | 63.78 s |
| 累计 CPU Time | 63.84 s | 54.02 s |
| Stage 墙钟时间 | 5.86 s | 5.75 s |
| Memory / Disk Spill | 0 / 0 | 0 / 0 |

Task 数下降 `89%`，累计 Executor Run Time 下降约 `27.2%`，但 Stage 墙钟只下降约 `1.9%`。这说明 200 个小 Task 确实带来了额外调度和固定执行开销；同时 Stage 原本只有约 6 秒，也不是应用的主要耗时，所以减少 Task 对关键路径的改善有限。

优化后每个 Task 读取约 `38.6 MiB`，Peak Execution Memory 约 `288.5 MiB`，没有 Spill。在本次 `4g / 2 cores` Executor 配置下，22 个分区没有形成明显内存压力。但 `spark.sql.shuffle.partitions` 是 Session 级配置，它还会影响其他未被 AQE 改写的 Exchange，不能因为本案例安全就作为所有任务的固定推荐值。

应用内全部 Job 的时间窗从 `248.65 s` 下降到 `227.92 s`，实测减少约 `20.73 s / 8.3%`；完成 Task 总数从 `8,285` 降到 `5,791`。不过两次源表记录并不完全相同，SQL Execution 7 本身还从 `22.516 s` 增加到 `34.465 s`，因此这些应用级变化只能作为实测结果，不能全部归因于分区配置。

这个案例最终证明了两件事：缓存内部固定 200 分区可以通过 `spark.sql.shuffle.partitions` 改变；是否由 AQE 完成必须看 `AQEShuffleRead`，不能根据 Task 数下降倒推 AQE 已生效。

业务验收结果：用户确认优化前后结果一致，本轮优化通过。当前保存的是用户确认；若需要可重复执行的字段级证据，还应另外保留双向 `EXCEPT ALL`、NULL、SUM 或稳定 HASH 校验结果。

### 验证清单

- 配置页确认 `spark.sql.adaptive.enabled=true` 及对应子开关。
- 从 SQL 最终计划定位目标 Stage 对应的 `Exchange`，不要只看初始计划。
- 检查消费端是否有 `ShuffleQueryStage`、`AQEShuffleRead` 和 `coalesced`。
- 用完整 Task 数据核对 Shuffle Read/Write Records、Bytes、Task 数、P50/P95、Spill 和墙钟耗时。
- 区分扫描 Task、Shuffle Reduce Task、缓存分区和输出文件，不能相互替代。
- 参数调整后重新确认最终计划和 Stage Task 数，而不是只确认配置值已经写入。

案例证据目录：`spark-runtime-model/real-runs/application_1783495977342_2319165/`。
