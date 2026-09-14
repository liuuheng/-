# Apache Paimon 表模型、存储组织与读取语义

## 内容摘要

本笔记系统整理 Apache Paimon 建表基础，覆盖核心表类型、主键合并、Partition/Bucket/File 三级存储组织、Merge Engine、Changelog Producer、Flink Checkpoint、Paimon Snapshot、Compaction、批式与流式读取差异，以及常见配置误区。核心判断是：是否声明主键决定表是 Append Table 还是 Primary Key Table；Merge Engine 决定同一主键如何形成最终逻辑行；Changelog Producer 只决定主键表向流式下游输出变更的方式，不决定表类型，也不改变普通批查询的最终逻辑结果。

## 一、Paimon 的基本定位

Paimon 是支持批流统一、快照管理、增量读取、主键更新和 Schema Evolution 的湖表格式。流式写入的一般链路为：Flink 接收数据 → Paimon Writer 写入缓冲区 → 数据刷入文件 → Flink Checkpoint 成功 → Paimon 提交 Snapshot → Compaction 整理文件 → Snapshot 过期后清理不再引用的数据文件。

需要区分：

- Flink Checkpoint：作业级容错与一致性机制，也是 Paimon 流式写入的常见提交边界。
- Paimon Snapshot：表级已提交版本，查询只读取已提交快照。
- Compaction：文件和记录版本的物理整理机制，主要影响读写放大、文件数量和性能，原则上不改变逻辑查询结果。
- Changelog：用于描述新增、更新和删除的流式变化。
- Time Travel：主动选择历史 Snapshot 查询旧版本，不等同于普通查询返回完整变化过程。

## 二、核心表类型

### 2.1 Append Table

未声明 `PRIMARY KEY` 的表即 Append Table。每条输入均作为独立记录保存，相同字段值不会因重复而自动覆盖或去重。适用于行为日志、审计事件、Kafka 原始层、订单状态历史、批量 ETL 中间表和分区覆盖表。

判断原则：如果每次输入本身就是必须保留的业务事实，应选择 Append Table。

### 2.2 Primary Key Table

声明 `PRIMARY KEY` 的表即 Primary Key Table。同一主键可以存在多个物理版本，但查询时由 Merge Engine 合并成一条逻辑记录。适用于数据库 CDC、订单当前状态、用户最新资料、商品最新信息和实时宽表。

`PRIMARY KEY (...) NOT ENFORCED` 表示 Flink 不像关系数据库一样逐行检查唯一性；重复主键是更新语义的正常输入，Paimon 根据主键完成合并。

### 2.3 两类表的本质区别

- Append Table：保留每次输入；普通批查询返回全部正式记录。
- Primary Key Table：按主键合并；普通批查询返回指定 Snapshot 上每个主键的最终逻辑状态。
- `changelog-producer = none` 不等于 Append Table。表类型只由是否存在主键决定。

## 三、主键表的 Merge Engine

Merge Engine 决定同一主键的多条记录如何形成最终逻辑行，它不是独立表类型。

- `deduplicate`：默认模式，保留最后或序列值最大的记录；适用于 CDC 和最新状态表。
- `partial-update`：将同一主键不同记录中的字段进行部分更新；适用于多流拼接宽表。必须明确 NULL 是“不更新”还是“置空”，以及删除和字段级版本语义。
- `aggregation`：对同一主键的值字段执行 `sum`、`max`、`min` 等聚合；适用于指标累加。数据重放可能导致重复累计，该引擎不会自动根据独立事件 ID 去重。
- `first-row`：保留第一条到达记录；第一条到达不等同于业务时间最早，乱序场景需要额外评估。

对于可能乱序的 `deduplicate` 表，应使用 `sequence.field` 指定业务版本字段，例如 `event_time`。较大的 `sequence.field` 值优先，但相同序列值仍可能依赖输入顺序。

## 四、Partition、Bucket 与 File

Paimon 的典型物理层次为：Table → Partition → Bucket → Data Files。

- Partition：粗粒度数据划分，通常表现为文件系统目录或对象存储前缀；用于分区裁剪、组织数据和生命周期管理。
- Bucket：分区内部的逻辑存储单元；一个 Bucket 通常包含多个数据文件、索引文件和可能的 Changelog 文件，不等同于单个文件。
- File：实际数据文件；Compaction 会在相应存储边界内合并文件。

固定桶模式 `bucket > 0` 通常根据 `hash(bucket-key) % bucket-count` 分配记录。在同一分区、`bucket-key` 和桶数不变的前提下，同一桶键稳定进入相同编号的 Bucket。相同业务键位于不同分区时，仍属于不同分区中的不同 Bucket。

动态桶 `bucket = -1` 维护键到 Bucket 的映射，不应简单理解为固定哈希取模。动态桶可以支持特定跨分区 Upsert，但存在索引初始化、本地资源占用和并发写入约束。`cross-partition-upsert.index-ttl` 可以限制索引历史范围，但过期键可能无法定位旧分区，因此 TTL 是正确性与资源之间的权衡，不是无风险优化。

固定桶过少会限制并行度并形成大桶；固定桶过多会产生大量小文件和 Compaction 开销。桶数应根据单分区数据规模、查询过滤、Join 分布、写入并行度和文件数量评估。

## 五、主键与分区字段

主键不包含全部分区字段并非在所有情况下都属于非法语法，但可能触发 Cross-Partition Upsert。

当同一主键的分区值改变时，系统必须定位旧分区中的记录，再执行旧分区删除和新分区写入。动态桶可以维护相应映射，但大型历史表可能面临启动扫描和索引资源成本。

建模建议：

- 分区字段不可变，例如订单创建日期：可以将其纳入主键。
- 分区字段会随状态更新而改变：不宜直接用于普通最新状态表分区，应考虑不分区、使用不可变分区字段，或明确采用跨分区更新机制。
- 若业务目标是保存订单状态历史，应优先使用 Append Table，而不是以 `order_id` 为主键的状态表。

## 六、Changelog Producer

Changelog Producer 只控制 Primary Key Table 被流式读取时如何输出变化，不决定表类型，也通常不改变普通批查询结果。

常见 RowKind：

- `+I`：INSERT。
- `-U`：UPDATE_BEFORE。
- `+U`：UPDATE_AFTER。
- `-D`：DELETE。

配置语义：

- `none`：默认值，不额外产生完整旧值 Changelog；下游通常获得 Upsert 变化，若计算需要旧值，可能由 Flink Normalize 状态补齐。
- `input`：保存上游已经提供的完整 CDC；Paimon 不负责补造缺失旧值。
- `lookup`：查询表中旧值并生成较完整的变更流；会增加查询、缓存和 Compaction 成本。
- `full-compaction`：比较周期性 Full Compaction 的状态结果生成变化；延迟较高，中间多次更新可能被折叠，不等同于原始事件历史。

Append Table 通常天然产生 Insert-only 流；Primary Key Table 通常产生 Upsert 或完整 Changelog。Append Table/Primary Key Table 描述表内保存和合并方式，Insert-only/Upsert/Retract 描述流式输出形式，两者属于不同分类维度。

## 七、批式读取与流式读取

普通批查询读取某个已提交 Snapshot 的逻辑结果：

- Append Table 返回该快照中全部追加记录。
- Primary Key Table 返回该快照中按主键和 Merge Engine 合并后的最终结果。
- 普通批查询不会返回 `-U`、`+U` 等中间变更事件。
- `changelog-producer` 的选择不应改变同一 Snapshot 的普通批查询业务结果。
- Compaction 前可能由 Reader 在读取阶段合并多个版本；Compaction 后文件已物理整理，但逻辑结果应一致。

只有主动使用流式增量读取、Changelog 扫描、系统表或 Time Travel，才会观察增量变化、变更形式或历史快照状态。

## 八、Checkpoint、Snapshot 与 Full Compaction

`checkpoint.interval` 不是 Paimon 表属性。Flink SQL 应配置 `execution.checkpointing.interval`，例如通过 `SET 'execution.checkpointing.interval' = '30 s'`。

Checkpoint 间隔影响数据提交可见性、Snapshot 产生频率、元数据增长、小文件数量和 Compaction 压力。秒级间隔不必然错误，应选择系统能够长期稳定承受且满足可见性目标的最短周期。

`changelog-producer = none` 与 `full-compaction.delta-commits` 可以同时存在，两者不冲突：前者控制不额外生成完整 Changelog；后者控制每若干次增量提交执行 Full Compaction。周期性 Full Compaction 可以降低读时合并成本，但会增加写放大。若 `changelog-producer = full-compaction`，则 Full Compaction 还承担生成周期性完整状态变化的职责。

## 九、Snapshot 保留策略

`snapshot.time-retained`、`snapshot.num-retained.min` 和 `snapshot.num-retained.max` 共同决定历史快照保留范围。

当 Checkpoint 每 30 秒提交一次且 `max` 仅为 100 时，即使 `time-retained` 设置为 24 小时，按每次一个快照粗略估算也只能保留约 50 分钟；一次提交还可能形成多个快照，因此实际窗口可能更短。若目标是至少保留 24 小时，应根据实际每日 Snapshot 数量设置足够大的 `max`，或避免设置过小上限。

快照过期过快可能导致长时间批查询引用文件被清理、流式消费者无法从旧进度恢复、Time Travel 和回滚窗口不足。

## 十、文件与内存参数

`target-file-size` 控制目标数据文件大小；文件较大可以减少文件数量并改善大规模顺序扫描，但会增大 Compaction 重写粒度、降低细粒度查询和读取并行性。

`write-buffer-size` 控制 Writer 在内存中积累多少数据后形成有序文件，直接影响各并行 Writer 的内存压力。1 GB 缓冲不是整个作业只占 1 GB；多个并行实例、活跃分区、Bucket 和其他算子会放大总内存需求。新表应优先使用默认值，再根据 Writer 内存、Flush、Spill、小文件数量、Compaction 延迟和查询扫描文件数调整。

## 十一、扩展表形态

从更广义的 Paimon 2.x 能力看：

- Paimon Table：由 Paimon 管理 Schema、Snapshot、Manifest、数据文件、Time Travel 和 Compaction；内部核心分为 Append Table 与 Primary Key Table。
- Format Table：通过 Catalog 描述 Parquet、ORC、CSV、JSON 等普通格式文件，不等同于具备完整 Paimon 快照和主键合并能力的标准表。
- Object Table：面向图片、音频、文档等非结构化对象及其元数据。
- Multimodal/Data Evolution 相关能力：包括 BLOB、向量、全文索引和数据演化，主要面向 AI 与非结构化数据场景。

对于 Flink 实时数仓和 CDC 学习，应优先掌握 Append Table、Primary Key Table、Merge Engine、Changelog Producer、Partition/Bucket 和 Snapshot/Checkpoint 的关系。

## 十二、建表决策原则

1. 每次输入均为必须保留的业务事实：选择 Append Table。
2. 每个业务键只需要当前状态：选择 Primary Key Table + `deduplicate`。
3. 多条流分别更新实体不同字段：选择 Primary Key Table + `partial-update`。
4. 输入是可累加指标贡献：选择 Primary Key Table + `aggregation`，并评估重放。
5. 每个主键只保留首条到达记录：选择 Primary Key Table + `first-row`，并区分到达顺序与事件时间。
6. 先确定业务语义，再设计主键与不可变分区字段；最后才调整 Bucket、Changelog、Compaction、Snapshot 和文件参数。
7. 性能参数不应脱离数据规模和指标直接套用；优先采用默认值，通过监控和压测逐项调整。
