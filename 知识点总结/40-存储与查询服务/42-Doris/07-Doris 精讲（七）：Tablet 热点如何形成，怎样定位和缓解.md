---
aliases:
  - "06-Doris Tablet 热点：形成原因、限制与治理方法"
  - "Doris Tablet 热点：形成原因、限制与治理方法"
tags:
  - doris
  - doris-精讲
---

# Doris 精讲（七）：Tablet 热点如何形成，怎样定位和缓解

订单表按日期分区、按 `user_id` 分桶。假设一天写入 256 GB，其中用户 42 贡献 64 GB，并发查询也常查这个用户。这个例子同时存在数据倾斜和访问集中。两者可以独立发生，排查时要分别量化。

## 一、从热点用户追到 Tablet，再追到 BE

用户 42 按 Hash 路由到当天某个 Tablet。存算一体中，该 Tablet 的多个 Replica 按放置策略分散到不同 BE，一个 BE 又可能承载同一 Partition 中多个不同 Tablet。普通扫描通常为每个 Tablet 读一份可用副本，副本可用性与节点、版本等条件有关。读副本的实际选择受版本和配置影响，不能从 BE 迁移负载评分推断查询按最低 CPU 或网络耗时选副本。

| 可观察现象 | 该检查什么 |
| --- | --- |
| 某 Tablet 数据量远高于其余 Tablet | 分桶键频率、单副本大小、行数 |
| 数据量相近但少数 Tablet 的扫描耗时长 | 同一热点值的请求频率、读副本位置、实例耗时 |
| 写入慢且版本持续积累 | 同 Tablet 的导入速率、Compaction 和 BE CPU、磁盘 I/O |

存算分离使用共享存储和计算组，需要按对应架构看数据访问和资源。以下迁副本与多副本读的推演以存算一体为前提。

## 二、迁副本、分摊读与增加并发，分别改变哪个量

假设热点 Tablet 的一份副本从 BE-1 迁到 BE-4。BE-1 的空间和负载可能下降，迁移期间还会发生网络和磁盘读写。迁完后用户 42 的 64 GB 仍集中在同一个逻辑 Tablet。

若不同查询分别使用不同的健康副本，多次读请求可以分摊。增加副本也会增加需要维护的数据份数，单 Tablet 大小和 Hash 路由仍相同。Routine Load 或 Stream Load 提高并发能让多个 Tablet 同时写入，但落向同一热点 Tablet 的请求会继续竞争。若增量生成超过合并能力，还会积累更多版本。见 [[06-Doris 精讲（六）：从导入版本到 Rowset 与 Compaction|从导入版本到 Rowset 与 Compaction]]。

## 三、改变路由能否既分散负载又保住业务语义

`HASH(province)` 的取值较少，通常难以充分用上大量桶。`HASH(user_id)` 的取值更多，仍可能遇到 64 GB 全属于同一用户的情况。对于可按明细分散的记录，可以评估 `HASH(user_id, rand_bucket)` 或给用户键加 salt。若 salt 只由用户生成，每次都固定，就无法分散这个用户。若同一业务记录重试或更新时重新随机取 salt，Unique Key 可能把它视为另一个 Key。

可从稳定的订单明细标识导出辅助维度，但要验证聚合粒度、幂等、Join、桶裁剪和 Colocation。Unique／Aggregate 表的分桶列还必须满足模型的 Key 约束。桶数过少会扩大最大 Tablet，过多会增加 FE 元数据、调度和后台任务。规划时可用 1～10 GB 作为粗估，4.x 的一般压缩体积建议范围为 1～20 GB，Unique Key 建议不超过 10 GB。最终要结合单副本压缩体积、索引、行数、增长和恢复成本。见 [[03-Doris 精讲（三）：从订单分布反推分桶键与桶数|从订单分布反推分桶键与桶数]]。

按天或小时 `PARTITION BY RANGE(dt)` 能分散不同时间范围并便于保留策略。Dynamic Partition 创建和回收分区，小时粒度要用能表达小时的时间类型。分区内的热点 Key 仍会走固定路由。与 HBase 的 Region 自动 Split 不同，在这里讨论的 Doris 4.x 内表上，`BUCKETS AUTO` 为新分区推算桶数，Random 改变写入路由，二者均不会拆分已有的热点 Tablet。历史布局变化要评估重写或迁移，见 [[04-Doris 精讲（四）：已有分区、新分区与副本的变更边界|已有分区、新分区与副本的变更边界]]。

## 四、从 Tablet 和副本记录确认效果

使用 `SHOW TABLETS FROM t_order;` 查看 `TabletId`、`ReplicaId`、`BackendId`、`DataSize`、`RowCount` 和版本信息。同一 `TabletId` 的多条记录可能是多副本，应选一致的单副本口径比较大小。如果迁移后 BE 负载降低而同一 Tablet 的大小仍高于其他 Tablet，说明节点放置改善，数据分布依旧倾斜。继续结合查询 Profile 与 Compaction 趋势，确认读写长尾是否缓解。
