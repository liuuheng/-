##### ClickHouse 与 Kafka

##### ReplacingMergeTree 去重与幂等

Q：ReplacingMergeTree 能否保证不重复？版本列、分片和分布式写入会怎样影响去重？
A：不能把 ReplacingMergeTree 当实时唯一键约束。它只会在后台 merge 时，对同一分区内、`ORDER BY` 键相同的行保留一条；merge 时机不确定，所以查询阶段仍可能看到重复。指定版本列时通常保留版本最大的行；不指定版本时保留 merge 过程中的最后一行，但这个“最后”不适合作为业务确定性保证。跨分片的数据不会互相 merge，如果相同业务键被轮询写到不同分片，也无法完成全局去重。

线上我会让同一业务键稳定路由到同一分片，带明确版本号，并在关键查询用 `argMax`、窗口去重或聚合视图得到确定结果。`FINAL` 适合小范围核对，不适合所有大查询常态化使用。

##### 知识点补充

去重键由 `ORDER BY` 决定，不是普通主键语义。写入批次 token 可用于防止同一批数据因重试重复写，但不能解决业务记录的多版本更新。幂等方案通常由稳定分片、批次去重、业务版本和查询侧确定性聚合共同组成。

##### 查询性能、容量与适用场景

Q：ClickHouse 为什么查询快，适合哪些业务？数据增长后怎样做容量和慢查询治理？
A：ClickHouse 快不是只因为列存，而是按查询列读取、利用 `ORDER BY` 的稀疏索引跳过 granule、压缩减少 IO，并以 block 向量化、多核并行执行。它适合行为明细和聚合报表，不适合强唯一约束、高频单行更新和多行事务。容量上我用日增压缩字节乘保留期、副本数，再加 merge 临时空间和安全余量，并按单分片压测吞吐与并发确定节点数。慢查询先查 `system.query_log` 的扫描行数、字节、内存和线程，再验证分区裁剪、排序键及数据跳过；控制小 part，把高频 Join 改为字典、预关联或汇总表。优化后比较扫描量、P95、峰值内存和结果一致性。

##### 知识点补充

性能依赖分区和 `ORDER BY` 设计。分区不能过细，排序键应贴合高频过滤并兼顾基数；`FINAL` 和无裁剪大扫描不能成为常态。小批高频写会产生大量 part，增加 merge 压力。

##### Kafka 与 ClickHouse 的职责边界

Q：Kafka 和 ClickHouse 各自用在什么方向？
A：Kafka 和 ClickHouse 在链路里承担的职责完全不同。Kafka 保存按分区追加的事件日志，负责业务解耦、削峰、消费进度和故障后的重放；ClickHouse 保存可长期查询的列式数据，负责多维过滤、明细检索和聚合报表。实际交易链路是 MySQL CDC 与埋点先写 Kafka，保留 3～7 天供 Flink 重放；Flink 去重、关联维度后按批写入 ClickHouse，表按日期分区、按常用查询键排序，BI 查询只访问 ClickHouse。排障时也分开看：Kafka 看分区 lag、ISR 和消费速率，ClickHouse 看 part、merge、扫描行数和查询 P95。不会让业务直接扫 Kafka 做报表，也不会让 ClickHouse 代替消息总线承担多消费者订阅。

##### 知识点补充

两者都能保留数据，但访问模型不同。Kafka 以分区顺序日志和 offset 消费为核心；ClickHouse 以列式数据 part、排序键和 SQL 查询为核心。

##### ClickHouse 批量写入与可见延迟

Q：DWS 写入 ClickHouse 多长时间一次，批次大小怎么定？
A：我不会固定回答“每分钟写一次”，而会根据看板 SLA 和 ClickHouse part 压力设置多条件 flush。比如延迟要求 10 秒，先设置 5000～20000 行、8～16 MB 或 3 秒任一达到就提交；Flink Sink 使用有界缓冲和失败重试，并把批次 ID 或业务版本纳入幂等设计。压测时从峰值流量逐级增加，观察端到端 P95/P99、每分钟新 part、`system.parts` 活跃 part 数、merge backlog、写入失败和副本延迟。若出现 `Too many parts`，优先增大批次、降低并发或检查分区过细；若延迟超 SLA，再减小时间阈值。最终参数必须和流量、消息大小一起记录。

##### 知识点补充

ClickHouse 更适合批量插入。分布式表写入还涉及分片路由、异步发送队列和副本确认。Sink 的 flush 成功不等于业务查询已经满足一致性要求，需结合副本、物化视图和提交语义验证。

##### Kafka 顺序性与乱序治理

Q：Kafka 数据有序吗？出现乱序怎么处理？
A：Kafka 只保证单分区日志顺序，不保证 Topic 全局顺序。订单状态流要求同一订单有序时，ProducerRecord 的 key 固定使用 orderId，让同一订单进入同一分区；开启幂等生产者，配置 `acks=all` 和合理重试，避免重试产生重复或重排。消费者对单分区按 poll 顺序处理，若把消息提交异步线程池，必须按 key 串行，或者在落库端用 sequence/version 拒绝旧版本。扩分区会改变默认哈希映射，因此扩容前要评估同 key 新旧消息跨分区；Flink 的 watermark 只能处理事件时间乱序，不能恢复 Kafka 物理顺序。确需全局排序才使用单分区或下游全局归并，并明确吞吐损失。

##### 知识点补充

分区扩容会改变默认哈希映射，可能让同一 key 的新旧消息落到不同分区。事件时间乱序可由 Flink watermark 和允许迟到处理，但这解决的是时间语义，不等于 Kafka 物理顺序恢复。

##### Kafka ACK 与 ISR

Q：Kafka 的 `acks` 有哪些级别？ISR 是什么，两者如何共同保证可靠性？
A：`acks=0` 不等待 Broker，吞吐高但最容易丢；`acks=1` 等 Leader 写入，Leader 刚确认就宕机仍可能丢；`acks=all` 等所有当前 ISR 副本确认，是生产常用的可靠配置，但必须结合 `min.insync.replicas`。ISR 是与 Leader 保持在允许滞后范围内的副本集合。比如副本数 3、`min.insync.replicas=2`、`acks=all`，ISR 少于 2 时写入会失败，避免以可用性换掉数据安全。

##### 知识点补充

还应开启幂等生产者、合理重试，并禁用不安全的 leader 选举。`acks=all` 不是等待全部副本，只等待当前 ISR；如果 `min.insync.replicas=1`，可靠性仍可能不足。

##### Kafka 高效读写

Q：Kafka 为什么吞吐高，如何优化读写性能？
A：Kafka 高吞吐来自顺序追加、操作系统页缓存、批量请求、压缩和分区并行。实际调优先建立基线：记录单分区 MB/s、消息大小、Producer request latency、Broker 磁盘利用率和 Consumer lag。生产端从 `batch.size=64KB`、`linger.ms=5~20ms` 和 lz4/zstd 压缩开始压测，同时保留 `acks=all` 与幂等，不能为吞吐降可靠性；消费端调整 `fetch.min.bytes`、`fetch.max.wait.ms`、`max.partition.fetch.bytes`，并把业务处理批量化。只有单分区吞吐成为瓶颈且业务允许时才增加分区，同时扩消费者并行度。若 Broker 磁盘或网络已满，加分区只会恶化；最终用吞吐、P99 延迟、CPU、网络、磁盘和 lag 共同验收。

##### 知识点补充

分区数决定并行度，但过多会增加控制器元数据、文件句柄、选举和恢复成本。消费者慢不一定是 Kafka 拉取慢，也可能是下游处理或提交策略造成。

##### Kafka 消费位点与 Flink 恢复

Q：Kafka 的消费偏移量保存在哪里？Flink 消费 Kafka 时偏移量又保存在哪里？
A：普通 Kafka Consumer Group 的已提交 offset 保存在 Kafka 内部主题 `__consumer_offsets` 中，Broker 通过组协调器管理。Flink 场景要区分“外部已提交位置”和“作业真实恢复位置”：Flink 会把各分区 offset 作为 Source 状态随 checkpoint 一起保存，故障后从最近成功 checkpoint 中恢复，而不是简单读取 `__consumer_offsets`。Flink 仍可以把 checkpoint 完成后的 offset 提交给 Kafka，方便监控消费组进度，但这通常不是作业容错的权威依据。

##### 知识点补充

如果没有可用 checkpoint/savepoint，作业首次启动才会依据 `earliest`、`latest`、时间戳或已提交组位点等启动策略。修改 consumer group、Topic 分区或状态映射前要评估是否会造成跳读或重读。

##### Kafka 动态分区与扩容

Q：Kafka 如何动态增加分区？增加分区后会带来什么影响？
A：Kafka 支持在线增加 Topic 分区，生产和消费可以逐步发现新分区，但不能直接减少分区。扩容前我会先确认目的到底是提升吞吐还是解决数据倾斜，因为增加分区会触发消费者组 rebalance，而且默认哈希映射会变化，同一个 key 的新旧消息可能落到不同分区，从而破坏跨扩容时点的业务顺序。生产上会提前评估 key 路由、消费者并行度和下游状态，并选择低峰期操作和观察 lag。

##### 知识点补充

增加分区不会自动搬迁原分区中的历史数据，也不会自动消除热点 key。需要跨 Broker 均衡副本时还要执行分区副本重分配，并控制迁移带宽避免影响线上读写。

##### Topic 和分区数量过多的影响

Q：Kafka 创建大量 Topic 或分区会有什么影响？
A：真正消耗集群能力的是总分区数和副本数，不只是 Topic 名称。每个分区都对应日志段、索引、Leader/Follower 状态和控制器元数据；分区过多会增加 Broker 文件句柄、控制器加载与选举时间、消费者分配和故障恢复时长。实际建 Topic 时我先按峰值吞吐除以单分区压测吞吐估算下限，再结合消费者最大并行度、key 顺序要求和未来增长留 30%～50% 余量，而不是每个小业务默认建几十个分区。平台侧限制自动建 Topic，统一副本数、保留期和配额，并监控总分区、UnderReplicatedPartitions、控制器事件队列和单 Broker 分区分布；下线业务时同步删除无消费者 Topic，避免“空 Topic 不占资源”的误区。

##### 知识点补充

可承载分区数没有脱离硬件和版本的固定答案，需要通过 Broker heap、控制器性能、磁盘、故障切换和运维压测确定。空闲分区也不是零成本。

##### Kafka 分区倾斜对 Flink 窗口的影响

Q：Kafka 三个分区数据量严重不均，下游 Flink 做 5 秒窗口会有什么影响，怎么处理？
A：影响不只是某个 Source subtask 忙。热点分区会让对应消费子任务积压，其他分区虽然空闲却不能分担；如果 watermark 按分区生成，空闲或落后的分区还可能拖住全局 watermark，造成窗口迟迟不触发或延迟增大。先检查生产端 key 是否把热点集中到单分区，再结合业务顺序要求调整分区策略；Flink 侧可启用 idleness、Source 后重新均衡，并在真正的业务 key 上聚合。热点业务 key 仍需二阶段聚合或拆热点。

##### 知识点补充

Flink Source 并行度高于 Kafka 分区数时，多出的并行实例没有数据；低于分区数时，一个实例会消费多个分区。`rebalance` 只能均衡进入下游的数据，不能消除 Kafka Source 自身已经存在的分区热点。

##### MergeTree 写入与存储流程

Q：ClickHouse 的数据写入和存储流程是什么？MergeTree 在后台做了什么？
A：写入 MergeTree 时，一批数据先按分区键拆分，再按 `ORDER BY` 排序，生成不可变的数据 part。每个 part 内按列存储，包含压缩数据、标记文件、稀疏索引和校验信息；写入完成后通过原子重命名对查询可见。后台 merge 会把多个小 part 合并成更大的有序 part，并在不同 MergeTree 变体中执行去重、聚合或删除语义。线上我会重点控制批量大小、part 数量和 merge backlog，避免高频小写把后台合并拖垮。

##### 知识点补充

分区主要用于生命周期管理和裁剪，排序键决定数据局部性和主键索引效果。后台 merge 不保证立即发生，TTL 删除、Replacing 去重等结果也通常随 merge 逐步实现。

##### Distributed 表与本地表

Q：ClickHouse 的 Distributed 表存不存数据，查询和写入链路是什么？
A：Distributed 表通常是逻辑路由层，本身不按 MergeTree 方式保存业务数据；真正的数据落在各分片副本的本地 MergeTree 表。查询 Distributed 表时协调节点把请求下发到相关分片并合并结果；写入时可以由 Distributed 引擎按 sharding key 转发，也可以由上游直接写各分片本地表。生产中我更关注分片键是否均衡且支持常见查询裁剪、内部转发队列是否堆积、跨分片聚合的网络量，以及副本一致性。DDL 要通过集群方式保证各节点本地表与 Distributed 表结构一致，不能只在一个节点改表。

##### 知识点补充

是否“存数据”要区分业务 part 与待转发队列文件。ReplicatedMergeTree 负责副本复制，Distributed 负责分片路由，两者职责不同。

##### Kafka 端到端 Exactly-Once

Q：Kafka 如何实现 Exactly-Once，消费者怎样避免重复处理？
A：Kafka 生产端开启幂等后，broker 用 producer id、分区和序列号去掉重试产生的重复；跨分区原子写需要事务 producer。Kafka 到 Kafka 的 consume-transform-produce 链路可在同一事务中写输出并提交消费 offset，消费者设置 `read_committed` 避免读取未提交消息。若下游是数据库或 ClickHouse，Kafka 事务不能自动覆盖外部系统，我会使用业务唯一键、幂等 upsert、事务性 sink 或 checkpoint 两阶段提交，并把 offset 与结果提交绑定。故障演练要覆盖“结果已写、offset 未提交”的窗口，验证重放后结果不重复，而不是看到 `enable.idempotence=true` 就宣称全链路 exactly-once。

##### 知识点补充

Exactly-once 是处理语义，不代表网络只传一次。生产者幂等只在单个 producer 会话和分区序列规则内生效，事务还要合理设置 transaction id、超时和隔离级别。

##### Kafka 积压与消费者并行度

Q：Kafka 消息积压如何处理？消费者数量大于分区数会怎样？
A：同一消费组内一个分区同一时刻只分配给一个消费者，所以消费者数超过分区数时多出的实例会空闲，不能继续提升并行度。出现积压我先看各分区 lag 增长速度、生产速率、消费处理耗时、rebalance 和下游延迟，判断是分区热点、消费者资源不足还是外部 sink 变慢。短期可以扩消费者到分区数、提高批量和异步写并限流保护下游；长期吞吐不足再增加分区并调整 key。扩分区会改变 key 到分区的映射，可能影响顺序性，Flink 作业还要同步评估 source 和下游算子并行度，不能只改 Topic。

##### 知识点补充

积压恢复时间可粗略按 backlog 除以“消费能力减生产速率”估算。频繁 rebalance、单条毒消息和下游超时也会表现为 lag，不一定是 CPU 不够。

##### Kafka 日志文件、顺序写与零拷贝

Q：Kafka 为什么读写快？一个 Partition 对应多少文件，零拷贝和落盘是什么关系？
A：Partition 是逻辑追加日志，物理上按 segment 切成多组文件，每个 segment 至少包含 `.log` 数据文件和 `.index`、`.timeindex` 索引，不是“一个分区固定一个文件”。Producer 批量压缩写入，Broker 对活跃 segment 追加顺序写，操作系统 page cache 吸收写入；消息何时真正刷到磁盘由刷盘和 OS 策略控制，可靠性主要依赖副本确认，不能把 ack 等同于每条 `fsync`。消费者发送文件数据时可利用 `sendfile` 把 page cache 直接送到 socket，减少用户态复制和上下文切换；应用层仍负责协议、批次和校验，并非完全不处理。

##### 知识点补充

Kafka 性能来自顺序 I/O、page cache、批量、压缩、分区并行和零拷贝共同作用。segment 滚动受大小或时间配置控制，旧 segment 才便于按保留策略删除。

##### Kafka LEO、HW 与 ISR 收缩

Q：Kafka 的 LEO、HW 是什么？5 个副本只剩 1 个 ISR 会有什么影响？
A：LEO 是某副本日志下一条将写入的位置，各副本可能不同；HW 是消费者可见的高水位，通常由 ISR 中副本同步进度约束，HW 之前的消息才视为已提交。5 个副本只剩 Leader 在 ISR 时，说明其余副本落后或失联，冗余已经显著下降。若生产要求 `acks=all` 且 `min.insync.replicas=2`，新写会直接失败，这是用可用性换一致性的保护；若最小 ISR 设为 1，仍可写但 Leader 再故障可能不可用，允许非同步副本选主还可能丢数据。处理时先查网络、磁盘、GC 和副本 fetch lag，恢复 ISR 后再解除限流，而不是随手降低保障参数。

##### 知识点补充

ISR 不是“所有副本”，而是当前与 Leader 保持足够同步的副本集合。副本数增加提高容错，但也增加网络、磁盘和选主成本。

##### Doris、ClickHouse 与报表存储选型

Q：MySQL 已无法支撑大数据报表时，Doris、ClickHouse、Elasticsearch、HBase 怎么选？
A：先按查询模式选，不按组件名气选。ClickHouse 适合追加为主、宽表扫描和高吞吐聚合；Doris 提供 MPP SQL、明细/聚合/主键模型，对高并发报表、多表 Join 和更新场景通常更友好；Elasticsearch 擅长倒排全文、多条件检索，但精确大聚合成本和资源可控性要实测；HBase 擅长按 RowKey 点查和范围扫描，不是任意维度 OLAP。落地前用真实数据测试导入吞吐、更新语义、P95 并发、Join、存储成本和故障恢复。报表链路通常从 DWS 预聚合写 OLAP，热点结果缓存，权限和口径由指标层管理，不能把原始明细全部搬入新库就算完成。

##### 知识点补充

Doris 的 Duplicate、Aggregate、Unique/Primary Key 模型决定写入和更新语义；ClickHouse 的 MergeTree 变体也需按去重、聚合和明细选择。

##### Doris 数据模型与索引

Q：Doris 的明细、聚合、主键模型有什么区别，前缀索引和其他索引如何工作？
A：Duplicate Key 保留导入明细，适合日志和可重复聚合；Aggregate Key 按 key 聚合 value，可用 SUM、MIN/MAX、REPLACE 等聚合类型，适合固定汇总；Unique/Primary Key 按主键保留最新版本，适合更新事实，但要用 sequence/version 列解决乱序覆盖。Doris 数据按排序 key 组织，前缀索引截取排序键前缀形成稀疏索引，高频过滤列应放在前部；具体前缀字节规则随版本和类型变化，不死背固定字节。还可按场景使用 Bloom Filter、Bitmap、倒排等索引。设计通过真实 where/join、写入和 compaction 压测验证。

##### 知识点补充

模型决定数据合并语义，索引决定裁剪能力，二者不能互相替代。乱序 CDC 应携带 binlog position/业务版本作为 sequence，避免旧更新覆盖新状态。

##### Doris 写入、压测与故障处理

Q：Flink 如何写 Doris，怎样压测并处理写不进去或查询变慢？
A：Flink Sink 按批次和 flush 间隔使用 Stream Load，配置 label/事务语义、失败重试和脏数据处理；明细更新选择主键模型并传 sequence，窗口汇总按聚合键 upsert。压测使用与生产相同列数、分区、更新比例和并发，逐级提升写入 QPS 与查询并发，记录导入延迟、失败率、compaction、BE CPU/磁盘、查询 P95/P99 和资源余量。写不进先看 FE/BE load 日志、Schema/类型、分区、磁盘水位、批次和 compaction backlog；查询慢看扫描行数、分区裁剪、profile、Join 和并发。回放采用稳定 label 或业务键保证幂等。

##### 知识点补充

只测单条最快查询没有意义，必须混合持续写入和目标并发。集群扩容前也要确认热点分桶和数据副本是否均衡。

##### Kafka 高可用与副本同步

Q：Kafka 如何保证数据不丢，Leader/Follower 怎样同步，Leader 挂掉后怎么办？
A：Producer 设置 `acks=all`、开启幂等和有限重试，Topic 副本数至少 3，并把 `min.insync.replicas` 设为 2 等合理值；Broker 端 ISR Follower 持续 fetch Leader 日志，只有达到提交条件的数据推进 HW 对消费者可见。Leader 故障后 Controller 从 ISR 选择新 Leader，客户端刷新元数据继续读写；不允许 unclean leader election 能降低丢已确认消息风险，但 ISR 全失时可用性下降。消费者端关闭自动提交或把 offset 与处理结果绑定，下游幂等。生产还要监控 under-replicated/offline partitions、ISR shrink、磁盘和 controller 事件，并定期故障演练。

##### 知识点补充

“不丢”是端到端条件组合，只有副本或只有 `acks=all` 都不够。Follower 拉取而非 Leader 主动推送，ISR 是否同步由时间/进度规则判断。

##### Kafka Topic、分区与消息格式规划

Q：项目中 Kafka 应该设置多少 Topic、分区和什么消息格式？
A：Topic 按数据契约、保留期、权限和消费隔离划分，不为每张小表机械建一个，也不把所有业务塞进一个 Topic。分区数先用峰值吞吐除以单分区实测吞吐，并考虑最大消费者并行度、key 顺序和未来增长；扩分区会改变 key 映射，提前留余量但不过度创建。消息格式优先 Avro/Protobuf 配合 Schema Registry，或受控 JSON；都要包含业务主键、事件时间、操作类型、Schema 版本和 trace/源位点。上线监控单分区流量、消息大小、lag 和保留磁盘，容量数字以真实压测说明。

##### 知识点补充

Kafka 只保证单分区顺序。Topic/分区过多会增加 controller 元数据、文件句柄、选举和恢复成本，规划需要同时看吞吐与运维规模。

##### Kafka Consumer Group 与 Rebalance

Q：消费组怎样分配分区？Rebalance 为什么会造成重复或停顿？
A：同一消费组内一个分区同一时刻只给一个消费者，有效并行度不超过分区数。成员加入、退出、订阅变化或超时会触发 Rebalance；如果业务处理完成但位点未提交会重复，先提交再处理则可能丢。生产中由 Flink checkpoint 或受控手动提交管理位点，下游按业务键幂等，并合理设置 session timeout、heartbeat 和 max.poll.interval；长耗时处理要拆批或与 poll 隔离。cooperative sticky 分配可减少全量撤销的停顿。

##### 知识点补充

消费者多于分区时，多余实例空闲。静态成员身份能减少短暂重启引起的重平衡，但不能替代位点与幂等设计。

##### Kafka 内部 Topic 与日志索引

Q：Kafka 有哪些内部 Topic？分区日志为何能快速定位消息？
A：`__consumer_offsets` 保存消费组位点和组元数据，事务场景还有 `__transaction_state`；它们也按分区复制，不能随意删除。业务日志由 segment 组成，`.log` 存消息，`.index` 把相对 offset 映射到文件位置，`.timeindex` 把时间映射到 offset。读取先通过稀疏索引找到附近位置，再顺序扫描少量消息，因此不必为每条消息建索引。排障时会检查 segment、保留策略、磁盘和索引文件。

##### 知识点补充

Kafka 利用顺序写和页缓存提高吞吐。按时间查找只能得到近似起点，仍需扫描并核对消息时间戳。

##### ClickHouse 物化视图的边界

Q：ClickHouse 物化视图怎样工作？为何补历史时容易漏算？
A：增量物化视图只在源表收到新插入块时执行查询并写目标表，不会扫描创建前的数据。上线时我先确定切流点，再用同一逻辑回灌历史并去重，或在短暂停写窗口完成回灌与建视图。聚合目标常用 SummingMergeTree 或 AggregatingMergeTree，分组键要匹配查询维度。源数据更新、删除和 JOIN 右表后续变化不会自动修正旧结果，需要重算分区或另做版本化明细。

##### 知识点补充

物化视图处理的是本次插入的数据块，不是传统数据库中随查询实时刷新的结果。历史初始化和增量切点必须形成闭环。

##### ClickHouse 备份与恢复

Q：ClickHouse 怎样做备份，副本表能否代替备份？
A：副本解决单节点故障，但误删和错误写入会复制到所有副本，所以仍要独立备份。我会根据版本使用原生 BACKUP/RESTORE 或经过验证的备份工具，把数据 parts 与表、字典、用户等元数据备份到独立对象存储；全量配合增量，备份任务限速并保留清单、校验和。恢复先在隔离集群重建表和依赖，校验分区、行数、金额及抽样查询，再决定分区替换或切换服务。分布式表要逐分片保证同一恢复点，不能只备份入口表。

##### 知识点补充

备份能力和语法取决于 ClickHouse 版本与磁盘类型。没有定期恢复演练的备份不能视为可靠。

##### Kafka 扩缩容与分区下线

Q：Kafka 集群怎样扩容、缩容或处理 Broker 下线，如何避免影响业务？
A：扩容先部署并注册新 Broker，再用分区重分配工具把副本分批迁移到新节点，并限制迁移带宽，持续观察 ISR、UnderReplicatedPartitions、磁盘和客户端延迟；新节点加入不会自动均衡已有数据。缩容或计划下线前先把该 Broker 上的副本迁走，确认它不再承载分区 Leader/Replica，再停机。非计划故障则依赖 ISR 选主，恢复后检查副本追赶。整个过程按 Topic 优先级小批执行，保留重分配计划和回滚，不直接删除数据目录。

##### 知识点补充

分区本身通常不做“单独下线”，而是迁移副本或删除 Topic。副本因子不能超过可用 Broker 数，跨机架部署要设置 rack 感知。

##### Kafka 从 ZooKeeper 到 KRaft

Q：Kafka 为什么曾依赖 ZooKeeper，新版本 KRaft 改变了什么？
A：旧架构把 Broker 注册、Controller 选举和 Topic/分区元数据协调放在 ZooKeeper，Controller 再向 Broker 推送状态；数据日志始终存于 Broker 磁盘，不存 ZooKeeper。KRaft 使用 Kafka 自己的 Raft 元数据 quorum，由 Controller 节点维护元数据日志，移除外部 ZooKeeper，简化部署并提升元数据扩展与故障恢复。迁移前必须核对具体 Kafka 版本支持、Controller 数量、节点角色、元数据目录和迁移路径，先在测试环境演练，不能把普通滚动升级等同于架构迁移。

##### 知识点补充

KRaft quorum 通常部署奇数个 Controller 以形成多数派。功能成熟度和迁移工具随版本变化，应以生产版本文档为准。

##### Kafka 的 CAP 与脑裂

Q：Kafka 更偏 CAP 中哪两项？如何避免 Controller 或 Leader 脑裂？
A：网络分区发生时 Kafka 通常优先在可达 ISR 和配置约束内保证一致性，可能牺牲部分可用性，例如达不到 min.insync.replicas 时拒绝写入；但 CAP 不能用一个固定“CP/AP”标签概括所有配置。旧架构通过 ZooKeeper 会话和 controller epoch 防止旧 Controller 继续生效，分区 Leader 也用 epoch 拒绝过期请求；KRaft 用 quorum 任期和多数派提交元数据。生产关闭 unclean leader election、合理设置 ISR/min.insync.replicas，并监控控制器切换和副本落后。

##### 知识点补充

若允许非 ISR 副本当 Leader，可提高可用性但可能丢已确认数据。客户端 acks 和幂等配置共同影响实际一致性承诺。

##### Kafka 备份与回溯

Q：Kafka 有副本后还需要备份吗？怎样从历史数据回溯而不污染线上消费？
A：副本解决 Broker 故障，不防误删 Topic、错误消息和跨集群灾难。关键数据我会用 MirrorMaker 2/复制工具同步到独立集群，或长期落 HDFS/对象存储，并定期验证可恢复性。回溯时新建独立 consumer group，从指定 timestamp/offset 读取到影子结果表，不能重置线上消费组直接覆盖正式数据；结果按业务主键和版本幂等，完成行数、金额及抽样对账后再原子切换或修订。保留期必须覆盖发现故障与补算所需时间。

##### 知识点补充

Kafka 副本不是离线备份。跨集群复制通常异步，仍需定义 RPO/RTO，并保存 Topic 配置、ACL 和 Schema，而不只是消息正文。

##### ClickHouse 表引擎选型

Q：ClickHouse 常用表引擎有哪些，业务表应该怎样选择？
A：明细追加表通常从 MergeTree 开始；需要后台按排序键保留版本用 ReplacingMergeTree，汇总可用 SummingMergeTree，保存聚合状态用 AggregatingMergeTree，带正负抵消语义才考虑 Collapsing/VersionedCollapsing。副本能力由 ReplicatedMergeTree 系列提供，分片路由用 Distributed；Memory、File、Kafka 等更多用于临时或集成场景，不承担常规持久明细。选型前我明确更新、去重、聚合、复制和查询语义，并用重复、乱序、删除样例验证；后台 merge 都不是同步唯一约束，不能只看引擎名字。

##### 知识点补充

引擎的排序键、分区和版本/sign 字段共同决定结果。Summing/Aggregating 查询时仍可能需要最终聚合，不能直接假设每个键只有一行。

##### Kafka 容量规划与基准压测

Q：Kafka 分区数和集群规模怎样通过压测确定，而不是拍脑袋？
A：先明确峰值生产 MB/s、消费 MB/s、消息大小、保留期、副本因子和目标延迟；在与生产相近的磁盘、网络、压缩、acks 和批次配置下，用官方性能工具或自建回放分别测单分区生产和消费吞吐，再用 `max(生产峰值/单分区生产能力, 消费峰值/单分区消费能力, 最大消费并行度需求)` 估算分区并留增长余量。Broker 数还要满足副本分布、磁盘容量和故障后剩余吞吐。压测持续到稳定态，观察 P95/P99、CPU、网络、磁盘、ISR 和 GC，而不是只取几秒最高值。

##### 知识点补充

分区过多增加元数据、文件句柄、选举和恢复成本。压测结果只对当时硬件、消息大小、压缩和可靠性参数有效。

##### Kafka 日志保留与批量过期

Q：Kafka 保留七天后如何删除大量消息，会不会逐条扫描？
A：Kafka 按分区 segment 管理日志，不逐条删除过期消息。Broker 周期检查关闭的 segment，当该段最后修改时间超过 retention.ms，或分区总大小超过 retention.bytes 时，把整个 segment 的 `.log`、`.index`、`.timeindex` 等文件标记并延迟删除；当前活跃 segment 要先滚动后才能整体过期。因此 segment 大小和滚动时间影响删除粒度，磁盘紧张时不能只等默认周期。我会同时配置时间/容量、监控磁盘水位和日志目录，删除 Topic 前另做权限保护。

##### 知识点补充

compact 策略按 key 保留较新值，与 delete 时间/容量清理不同；两者可组合。副本也会执行对应日志清理。
