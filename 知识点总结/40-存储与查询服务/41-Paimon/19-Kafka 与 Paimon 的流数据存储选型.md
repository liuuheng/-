---
tags:
  - kafka
  - paimon
  - stream-processing
status: expanded
---

# Kafka 与 Paimon 的流数据存储选型

> 核对日期：2026-11-07。Paimon 机制参考 2.0 文档，Kafka 日志、事务与 Compaction 参考 4.1 文档；本文给出选型方法，没有对两个系统做性能实测。本地 Paimon 学习实验仍为 Flink 1.18.1 + Paimon 1.3.1。

## 核心结论

Kafka 适合把持续发生的事件按分区日志发布给多个消费者；Paimon 适合把数据组织为可批流读取、可保留版本的湖表。两者经常共同组成一条链路：Kafka 保留输入变化并解耦生产与消费，Flink 解释变化、维护计算状态，再把实体当前态或明细写入 Paimon。

选型首先要回答“下游需要保留每次发生过的事，还是查询某个版本的状态”。要求更新、删除或撤回，并不能单独推出必须选择 Paimon；Kafka 可以承载 CDC、Upsert 和撤回消息，关键是消息格式、消费者和结果存储是否共同理解这些变化。[Kafka 语义说明](https://kafka.apache.org/41/design/design/#message-delivery-semantics)、[Paimon 表模型](https://paimon.apache.org/docs/2.0/primary-key-table/)

## 一、事件历史与实体当前态有什么不同

订单 `1001` 先创建为 100 元，后来修改为 120 元，最后取消。这包含三个业务动作，却可能只对应一行当前订单状态。

如果审计要回答“什么时候改价、谁发起取消”，应保留动作标识、事件时间、顺序和载荷。把消息写进按订单 ID 合并的主键表后，仅查最新状态不能还原每个动作。Paimon Append 表也可以保存事件明细，但不能把它与主键当前态表混为一谈。

如果报表要回答“目前有多少有效订单、当前金额是多少”，则需要计算各订单的最新有效状态。Kafka 中存在三条消息并不自动得到一张能按 SQL 查询的当前态表；需要消费者、Flink 状态、Kafka Streams 的表语义或外部表存储来物化结果。

| 需求 | 优先考虑的组织方式 | 必须保留的语义 |
| --- | --- | --- |
| 审计每次订单操作 | 事件日志或不可变明细 | 事件 ID、事件顺序、原始载荷、保留窗口 |
| 查询订单最新状态 | Paimon 主键表等状态存储 | 实体主键、版本顺序、删除规则 |
| 长期历史分析 | 湖表明细或版本化状态 | Schema、数据版本、分区与历史保留 |
| 多个业务独立订阅输入 | Kafka 等日志服务 | 独立消费位置、积压预算、重放窗口 |
| 对某日结果重复计算 | 固定输入边界与表版本 | 日切规则、Tag/Snapshot、源位点与代码版本 |

这些是建模判断，并不意味着某个需求只能由一个产品实现。

## 二、Kafka 的 Compacted Topic 为什么不能直接当作湖表

Kafka Log Compaction 会逐步移除同一 Key 的旧值，删除可由 Key 加空载荷的 Tombstone 表达。清理不会把分区日志变成一个可以任意 SQL 点查、按业务日期时间旅行的表；消费者仍按日志位置读取，并负责重建自己的状态。

Compacted Topic 也不是永久保留所有变化的审计日志。旧值可能被压缩，Tombstone 也有保留边界；落后过久的消费者需要重新建立完整状态。反过来，Paimon 主键表保存最新实体状态，也不意味着全部中间事件都永久存在。[Kafka Log Compaction](https://kafka.apache.org/41/design/design/#log-compaction)

因此，以下两个设计解决的问题不同：

- Kafka 事件 Topic：下游逐条解释业务动作，保留策略决定可以重放多远。
- Paimon 主键表：下游读取合并后的实体状态，Snapshot、Tag 和 Changelog 策略决定可查询和可消费的历史。

## 三、需要撤回时应该检查哪一层

撤回表示“之前输出的贡献现在要取消”，它首先是计算与数据编码的约定。例如订单金额从 100 变为 120，下游总额不能直接再加 120；它需要先抵消旧贡献，或根据主键把旧值替换成新值。

| 层次 | 要检查的问题 | 常见错误 |
| --- | --- | --- |
| 输入消息 | 是否有 Key、操作类型、旧值或可重建旧值的信息 | 用普通 JSON 只发新值，却要求下游直接消费完整撤回 |
| Flink 计算 | 算子是否维护旧状态，输出哪种 Changelog | 把 Upsert 流当作独立事件不断累加 |
| 结果 Sink | 是否接受对应的更新或删除编码 | 把更新流写到只能追加的 Sink |
| 故障恢复 | 消费位置、算子状态、结果提交能否对齐 | 只看到源端重放成功，就认定外部结果不会重复 |

Kafka 可以运输上述变化，但不会替消费者补齐业务解释。Paimon 主键表可以维护状态，但 `changelog-producer=input` 也不会自动补造输入缺失的旧值。应结合 [[21-Paimon Changelog 下游聚合与消费恢复验证]] 观察最终聚合结果。

## 四、延迟应该按完整链路比较

Kafka 消息写入可见性受生产批次、确认、事务和消费配置影响；Flink 流式写 Paimon 的普通查询可见性还要经过 Checkpoint 和 Snapshot 提交。若使用 Full Compaction 生成下游变更，还要计入生成周期与处理积压。

因此，不能只比较一次写入 API 返回时间。业务真正关心的通常是：事件发生后多久能在最终查询或下游结果中看到它，以及高负载和重启时延迟能否恢复。

建议分别记录：

1. 源事件时间到进入 Kafka 或 CDC Source 的延迟。
2. Flink 处理与成功 Checkpoint 的延迟。
3. Paimon 最新 Snapshot 的提交延迟。
4. 下游扫描、Lookup 缓存刷新或查询引擎元数据刷新的延迟。

这些指标应使用同一业务标记关联，机器时钟也要同步。Checkpoint ID、Snapshot ID、Kafka Offset 是不同系统的标识，不能直接用数值大小比较。

## 五、事件流 Join 与维表 Join 怎样选

曝光关联点击、下单关联支付，通常需要按事件时间匹配两个持续变化的流。Kafka 可以作为输入载体，但窗口、Watermark、迟到处理和 Join 状态仍由 Flink 决定。把输入换成 Paimon 不会自动消除这些状态和时间语义。

订单关联用户等级，则要先定义希望看到哪个时刻的等级：

- 希望每条新订单尽快关联当前等级：可以评估 Paimon Lookup，并接受缓存刷新带来的可见性边界。
- 希望按照订单发生时刻还原等级：需要维护有效时间历史或相应的时态建模，不能仅查当前态。
- 希望离线重跑保持某个日切版本：可以固定 Tag，但日切状态仍不等于每条事实发生时刻的维度状态。

相关机制见 [[Paimon 实时与离线共用维表设计：Tag、Lookup 与历史重跑]]。选择 Lookup 还应检查延迟、热点 Key、本地磁盘和多 Job 缓存重复成本。

## 六、Exactly-once 需要明确输入与输出边界

Kafka 的幂等生产、事务和消费隔离可用于构建 Kafka 内部的精确一次处理。结果写入外部存储时，还要由计算框架和目标 Sink 协调恢复。Paimon 的提交机制也需要与 Flink Checkpoint、源位点和算子状态共同工作。

“业务事件被上游重复发送两次”与“同一条源记录因故障被框架重放”是两类问题。即使框架提交协议正确，`aggregation` 接收到两个独立的重复业务事件仍可能重复贡献；`deduplicate` 也需要正确主键和版本顺序。

对账至少覆盖最终实体行、删除状态和聚合金额。单独看到 Kafka Offset 前进、Checkpoint 成功或 Paimon Snapshot 增长，都不足以证明完整链路的业务结果正确。恢复步骤见 [[20-Paimon Checkpoint、Savepoint 与一致性恢复]]。

## 七、两者共用时如何划分职责

一条常见链路是：数据库 CDC 写 Kafka，Flink 消费并处理，Paimon 同时保存 Append 事件明细和主键当前态。Kafka 负责发布、订阅和短中期重放；湖表负责分析数据和版本化状态。具体保留窗口仍需按故障恢复和成本制定。

需要额外定义三个边界：

1. **源重放边界**：Kafka 消息或数据库日志必须保留到恢复点之后，Paimon Tag 不能替源端保存已经删除的 Binlog。
2. **表重跑边界**：历史重跑必须固定输入位点、目标分区或表版本，避免与实时 Writer 争抢同一范围。
3. **数据契约边界**：同一实体的 Key、Schema、操作类型和顺序标识需要贯穿链路，不能在传输时丢失。

如果需求只是一张规模较小、更新不频繁的维表，也应把直接数据库 Lookup 等方案纳入比较。引入 Kafka 与 Paimon 的数量不能作为架构成熟度的判据。

## 八、上线前的最小验证

构造一组有确定预期的数据：新增订单、连续改价、删除订单、乱序旧事件、重复业务事件。分别执行正常运行、消费者停机恢复和故障重放，并记录：

| 检查对象 | 通过判据 |
| --- | --- |
| 输入历史 | 需要审计的事件在约定保留期内完整可重放 |
| 当前状态 | 主键、金额、状态、删除结果符合业务版本规则 |
| 下游聚合 | 与从同一完成边界计算的基准结果一致 |
| 延迟 | 正常和恢复期间满足约定 SLA，积压能收敛 |
| 成本 | 日志保留、湖表历史、Compaction 和本地状态均有预算 |

上述验证是建议方案，本文未实际执行。性能对照方法见 [[24-Paimon 查询裁剪、Compaction 与容量调优]]。

## 原始选型提纲与本次补充边界

本篇原始提纲保留如下，便于追溯：

> “事件流”关联“事件流”（如曝光 Join 点击、下单 Join 支付）：两侧事件可进入 Kafka。
>
> “事件流”关联“维度或实体流”（如订单 Join 用户、物流 Join 仓库）：维度或实体数据可放入 Paimon，通过主键匹配。
>
> 需要回撤（Retraction）或更新结果时：优先考虑支持主键更新语义的 Paimon，而不是只依赖 Kafka 消息日志。

本次补充为机制解释和选型边界。前两条应先定义 Join 的时间语义；第三条只表达“状态结果需要更新能力”，不能单独作为替换 Kafka 的充分理由。

## 相关笔记

- [[10-Paimon Sequence、RowKind 与删除语义]]
- [[17-Paimon 流式读写中的 Snapshot 与 Changelog]]
- [[25-Paimon 多引擎读写、Catalog 与存储部署]]
- [[知识点总结/10-业务与数仓设计/14-需求沟通与方案设计/架构方案/01-flink-fluss-paimon-doris-架构补齐方案|Flink + Fluss + Paimon + Doris 架构]]

## 官方资料

- [Kafka 4.1 Design：消费位置、投递语义与日志压缩](https://kafka.apache.org/41/design/design/)
- [Paimon 2.0 Primary-Key Table](https://paimon.apache.org/docs/2.0/primary-key-table/)
- [Paimon 2.0 Changelog Producer](https://paimon.apache.org/docs/2.0/primary-key-table/changelog-producer/)
- [Paimon 2.0 Flink SQL Query](https://paimon.apache.org/docs/2.0/flink/sql-query/)
- [Paimon 2.0 Flink SQL Lookup](https://paimon.apache.org/docs/2.0/flink/sql-lookup/)
