# Apache Paimon 表模型、存储组织与读取语义

> [!summary] 30 秒结论
> Paimon 是表存储，不是独立计算引擎。Flink、Spark 等引擎负责计算，Paimon 负责把记录组织成可提交、可追溯、可增量消费的表。Snapshot 通过 Manifest 决定某个版本可见哪些文件；主键表再用 LSM 和 Merge Engine 把同键物理版本合并成逻辑行。表类型、Merge Engine、Changelog Producer 和 Compaction 各自解决不同问题，不能互相替代。

## 5 分钟心智模型

Paimon 的物理组织可以记成一条关系：`Table → Partition → Bucket → Data File`。Partition 承担粗粒度裁剪和生命周期管理，Bucket 承担写入分布和局部合并，ORC/Parquet 等 Data File 保存实际记录。`schema/` 保存表结构版本，`snapshot/` 发布表版本，`manifest/` 记录数据文件和索引文件的 ADD/DELETE。

写入链路是：上游记录 → Writer Buffer/MemTable → flush 数据文件 → Checkpoint 成功 → Manifest 记录文件变化 → Snapshot 提交后对外可见。主键表的新记录通常先进入 L0，Compaction 再按主键和序列归并文件。Compaction 改写物理布局，原则上不改变同一表状态的逻辑结果。

读取链路是：选定 Snapshot → 读 Manifest List/Manifest → 得到有效文件 → 执行 Partition、Bucket 和 File Index 裁剪 → 读取数据文件。主键表在 Compaction 前可能仍有多个同键版本，Reader 需要 Merge-on-Read；L0 文件越多、主键范围重叠越大，读放大通常越高。

| 配置维度 | 它决定什么 | 不决定什么 |
| --- | --- | --- |
| 是否声明主键 | 是 Append Table 还是 Primary Key Table | 下游是否能拿到完整旧值 |
| `merge-engine` | 同一主键如何形成最终逻辑行 | Changelog 的产生时机 |
| `changelog-producer` | 主键表如何向流式下游输出变更 | 表类型和普通批查询结果 |
| Compaction | 何时归并物理文件和同键版本 | 原始业务事件是否被永久保留 |

Snapshot/Manifest 解决“这个版本读哪些文件”，LSM/Compaction 解决“同一主键的多个物理版本如何整理”，Changelog 解决“下游能看到哪些行级变化”。主键表能查出最终状态，不等于它必然能输出完整 `-U/+U`。

### 复用时按这个顺序判断

1. 每条输入是需要永久保留的事件，还是某个业务键的最新状态？这决定 Append Table 或 Primary Key Table。
2. 业务唯一性是什么？分区字段会不会变？主键不包含分区字段时，要评估 Cross-Partition Upsert 和索引恢复成本。
3. 同键记录应该保留最新值、更新部分字段、累加还是保留首条？这决定 Merge Engine 和 `sequence.field`。
4. 下游只需要按主键覆盖，还是必须获得旧值做撤回？先检查上游 `RowKind` 和行镜像，再选 `none`、`input`、`lookup` 或 `full-compaction`。
5. 性能问题发生在哪一层？用实际的 Snapshot 数、L0 文件数、单文件大小、Compaction 延迟、查询扫描文件数和 Writer 内存验证，不根据文章中的默认值直接调参。

> [!warning] 版本边界
> 来源文章没有声明 Paimon 版本。Snapshot 字段、参数默认值、Bucket 约束、File Index 和各引擎的读写能力可能随版本变化。建表或调参前，以目标版本官方文档和实际 `SHOW CREATE TABLE` 为准。“Paimon = RocksDB 的 LSM-Tree + Iceberg 的 Snapshot/Manifest”只是认知类比，不表示实现或兼容性等价。

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

## 十三、Changelog 的生成、保存与 Binlog 对比

### 13.1 核心定义

Paimon Changelog 是面向流式下游的表变更输出机制。`changelog-producer` 决定变更从哪里获得、何时生成、是否保存为额外的 Changelog 文件，以及下游得到 Upsert 还是完整的 Before/After 变更。

因此，它不只是“文件保存策略”，更准确地说是“变更生成与保存策略”。

完整 Changelog 使用 RowKind 描述一行数据的变化。假设订单金额从 100 更新为 150，完整更新通常表示为：

```text
-U (order_id=1, amount=100)
+U (order_id=1, amount=150)
```

`-U` 用于撤回旧值，`+U` 用于加入新值。下游执行分组聚合时，只有拿到旧值才能先撤回 100，再加入 150。

### 13.2 四种 Changelog Producer

#### `none`

默认模式，不额外生成完整 Changelog。下游从 Snapshot 增量中获得类似 Upsert 的最新值：

```text
+U (order_id=1, amount=150)
```

如果下游计算需要旧值，通常由 Flink Normalize 算子在状态中保存历史值并补齐。其本质是把维护旧值的成本从 Paimon 写入侧转移到下游 Flink 状态。

适用于下游能够按主键覆盖，或者只需要查询最终表状态的场景。

#### `input`

上游输入什么 RowKind，Paimon 就保存什么。例如 Flink CDC 已提供：

```text
-U (order_id=1, amount=100)
+U (order_id=1, amount=150)
```

Paimon 将这些输入写入 Changelog 文件，并随 Checkpoint 对应的 Snapshot 一起提交。Paimon 不会补造上游没有提供的旧值；如果上游只有 `+U`，保存后仍然只有 `+U`。

适用于 MySQL CDC、PostgreSQL CDC 或 Flink 状态计算已经产生完整 Before/After 的场景。

#### `lookup`

上游只提供新值时，Paimon 在 Lookup Compaction 中按主键查询历史旧值，再生成完整变化：

```text
输入：+U (order_id=1, amount=150)
查旧值：(order_id=1, amount=100)

输出：
-U (order_id=1, amount=100)
+U (order_id=1, amount=150)
```

为了避免逐条远程扫描存储，Lookup 会使用内存、本地磁盘缓存、主键索引及批量 Compaction。代价是增加索引查询、缓存空间、磁盘 IO 和 Compaction 压力。

适用于上游缺少旧值，但下游又必须执行撤回聚合的场景。

#### `full-compaction`

比较相邻两次 Full Compaction 得到的完整表状态，按主键计算 INSERT、UPDATE 和 DELETE。例如：

```text
旧状态：amount=100
新状态：amount=150

差异：
-U (order_id=1, amount=100)
+U (order_id=1, amount=150)
```

这种模式的延迟和读写成本较高，而且中间更新可能被折叠。例如 `100 → 120 → 150` 发生在两次 Full Compaction 之间，下游可能只看到 `100 → 150`，不会看到中间的 120。

适用于能够接受较高延迟和全量压缩成本、关注周期性表状态差异的场景。

### 13.3 Changelog 文件如何发布

Paimon 的流式写入过程可以概括为：

```text
上游变更
  → Paimon Writer 写数据文件/Changelog 文件
  → Flink Checkpoint 成功
  → Paimon 提交 Snapshot
  → Snapshot 引用本次新增文件
  → 流式下游读取新 Snapshot 的增量
```

因此，下游看见的不是一条独立、永久连续增长的中心日志，而是由 Snapshot 管理并按 Checkpoint 分批发布的变更文件。

### 13.4 与 MySQL Binlog 的区别

两者在消费语义上相似，都可以描述 INSERT、UPDATE 和 DELETE；但底层定位不同：

| 对比维度 | MySQL Binlog | Paimon Changelog |
| --- | --- | --- |
| 核心定位 | 数据库事务日志 | 湖仓表的流式变更输出 |
| 产生时机 | MySQL 事务提交 | Flink Checkpoint、Compaction 与 Snapshot 提交 |
| 存储形式 | 连续追加的 Binlog 文件 | 数据文件、Changelog 文件及 Snapshot 元数据 |
| 消费进度 | Binlog Position 或 GTID | Snapshot、Consumer ID 等表级进度 |
| 旧值来源 | 数据库执行更新时直接掌握 | 上游输入、Lookup 查询或 Full Compaction 比较 |
| 中间事件 | 通常可以逐条保留 | 可能因合并或 Compaction 被折叠 |
| 主要用途 | 主从复制、CDC、恢复和审计 | 流式表消费及下游增量计算 |

`input` 最接近保存 Binlog 变更，但准确链路是：

```text
MySQL Binlog
  → Flink CDC 解析成 +I/-U/+U/-D
  → Paimon changelog-producer=input
  → Changelog 文件
  → Snapshot 提交
  → 下游流式读取
```

Paimon Changelog 主要描述表状态变化，不应被当成永久保留每一条业务事件的原始日志。如果业务要求严格保存每个不可折叠的中间事件，应保留 MySQL Binlog、Kafka/Fluss 日志或另建 Append Table。

### 13.5 选择原则

1. 下游能够按主键覆盖或只查询最终状态：优先使用 `none`。
2. 上游已经提供完整 CDC：优先使用 `input`。
3. 上游缺少旧值，但下游必须执行撤回计算：考虑 `lookup`。
4. 只关心周期性表状态变化且能接受较高延迟：考虑 `full-compaction`。
5. Changelog 描述的是流式表变化，不天然等于完整业务事件历史。

## 十四、Hive 式每日全量快照表的 Paimon 建模

### 14.1 核心结论

如果目标是保留与 Hive 相同的“每天一个完整业务快照”逻辑，可以采用两种 Paimon 设计：

1. 不需要同日去重或更新：使用无主键 Append Table，只按 `dt` 分区。
2. 需要同日按用户去重或更新：使用 `(dt, user_id)` 联合主键，同时按 `dt` 分区。

联合主键 `(dt, user_id)` 保证的是“同一天内用户唯一”，不是整张表中 `user_id` 全局唯一。不同日期的同一用户属于两条独立快照记录，应同时保留。

### 14.2 方案一：Append Table + `dt` 分区

```sql
CREATE TABLE user_snapshot (
    user_id BIGINT,
    user_name STRING,
    status STRING,
    dt STRING
) PARTITIONED BY (dt);
```

因为没有声明 `PRIMARY KEY`，这是一张 Append Table，语义最接近传统 Hive 每日快照表：

- 每个 `dt` 分区保存当天完整数据。
- 相同 `user_id` 可以出现在不同日期分区。
- Paimon 不会根据 `user_id` 自动去重。
- 同一天重复执行 `INSERT INTO` 会继续追加，可能产生重复数据。

每日全量写入应使用覆盖分区：

```sql
INSERT OVERWRITE user_snapshot
PARTITION (dt = '2026-09-15')
SELECT
    user_id,
    user_name,
    status
FROM source_user;
```

这种设计适合每天一次性生成全量数据、分区写完后基本不再修改的场景。它不需要维护主键索引和同键版本，写入与维护路径相对简单。

### 14.3 方案二：`(dt, user_id)` 联合主键 + `dt` 分区

```sql
CREATE TABLE user_snapshot (
    user_id BIGINT,
    user_name STRING,
    status STRING,
    update_version BIGINT,
    dt STRING,
    PRIMARY KEY (dt, user_id) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
    'merge-engine' = 'deduplicate',
    'sequence.field' = 'update_version'
);
```

此时逻辑主键是 `(dt, user_id)`：

- 同一个 `dt`、同一个 `user_id`：属于同一逻辑记录，可以按 `sequence.field` 更新合并。
- 不同 `dt`、相同 `user_id`：属于不同主键，作为不同日期的快照同时保留。
- `dt` 已包含在主键中，不需要为了每日快照维护跨日期的全局主键映射。

这种设计适合当天数据会多次补写、可能存在重复记录，或者需要持续修正当天快照的场景。代价是增加主键索引、LSM 合并和 Compaction 成本；如果每天只覆盖写入一次，主键表的收益通常有限。

### 14.4 为什么有主键仍然需要 `INSERT OVERWRITE`

主键只能合并本次输入中实际出现的用户，不能自动删除新快照中已经不存在的用户。

例如目标分区原有：

```text
user_id=1
user_id=2
user_id=3
```

重新计算的全量结果只有：

```text
user_id=1
user_id=2
```

如果使用 `INSERT INTO`，`user_id=3` 不会因为新输入中缺失而自动删除；使用 `INSERT OVERWRITE` 替换整个目标分区，最终结果才是真正的当日全量快照。

因此，只要任务语义是“重新生成某日的完整结果”，无论表是否声明主键，都应优先使用分区覆盖写入。

### 14.5 应避免的设计

不建议用下面的结构承载每日全量快照：

```sql
CREATE TABLE user_snapshot (
    user_id BIGINT,
    user_name STRING,
    status STRING,
    dt STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) PARTITIONED BY (dt);
```

该结构表达的是“整张表中每个 `user_id` 只有一个最新状态”，但 `dt` 每天变化。每天写入全量数据时，Paimon 需要把大量用户从旧日期分区迁移到新日期分区，可能产生：

- 大规模跨分区索引查询；
- 旧分区删除与新分区写入；
- 较高写放大与 Compaction 压力；
- 索引初始化、本地内存和磁盘开销；
- 作业重启恢复时间增长。

每日全量快照强调“保留每一天的独立状态”，全局主键表强调“每个用户只保留当前状态”，两者不应混在一张表中实现。

### 14.6 业务日期快照与 Paimon Snapshot

`dt` 分区表示业务历史日期，例如“2026-09-15 的完整用户状态”；Paimon Snapshot 表示某次表提交版本，一天内可以产生很多 Snapshot。

- `dt`：业务数据维度，用于长期保存和按日查询。
- Paimon Snapshot：存储系统版本，用于原子提交、增量读取、Time Travel 和文件引用管理。

如果业务要求长期查询任意一天的全量状态，应保留 `dt` 字段或分区，不能只依赖可能过期的 Paimon Snapshot。

### 14.7 推荐选型

| 业务目标 | 推荐设计 |
| --- | --- |
| 每天生成一次全量数据，写完不再修改 | Append Table + `dt` 分区 |
| 当天会多次补写，需要按用户去重或更新 | Primary Key Table，主键为 `(dt, user_id)`，按 `dt` 分区 |
| 每日全量重跑，必须删除新结果中缺失的数据 | 对目标 `dt` 分区执行 `INSERT OVERWRITE` |
| 整张表只保存每个用户的当前状态 | Primary Key Table，主键为 `user_id`，不按每日快照日期分区 |
| 同时需要每日历史和实时最新状态 | 拆分为每日快照表与当前状态主键表 |

最终判断原则是：每日历史表的业务键是 `(dt, user_id)`；当前状态表的业务键才是 `user_id`。先明确要保存“每天的状态”还是“现在的状态”，再决定是否使用主键以及主键是否包含分区字段。

## 十五、专题笔记与来源

快照、物理文件与 Compaction：[[Paimon Snapshot、Manifest、Parquet 与 Compaction 的关系]]

流式读取起点与 Changelog：[[Paimon 流式读写中的 Snapshot 与 Changelog]]

LSM 层级、文件统计与查询裁剪：[[Paimon L0、L1、Snapshot 与查询裁剪]]

动态分桶与主键路由：[[Paimon 动态 Bucket 的索引机制与单 Writer 限制]]

File Index 的存储和选型：[[Paimon File Index：存储、读写与选型]]

文章来源：[《Paimon 精讲（一）：10分钟建立完整架构认知，老鸟也能查漏补缺》](https://mp.weixin.qq.com/s/VcO9_aEqneaoJp1Mu5NJtQ)，胖泽的技术笔记，2026-07-22。本笔记保留了文章的架构主线，但对未声明版本的默认值和实现细节保留核验边界。
