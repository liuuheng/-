##### Spark

##### RDD 核心特性与“弹性”

Q：RDD 的弹性体现在哪里？
A：RDD 的“弹性”首先体现在容错和分区计算，不只是动态加机器。它是不可变的分区集合，每次 transformation 记录 lineage；某个 executor 丢失缓存分区时，Spark 可以从上游依赖只重算丢失的分区，而不是重跑全部结果。实际 ETL 中，如果同一个清洗后的 RDD 被三个 Action 复用，我会 `persist(MEMORY_AND_DISK)`，执行后检查 Storage 页面命中率并及时 `unpersist`；如果 lineage 很长、上游代价高或含外部不稳定输入，会把结果 checkpoint 到 HDFS 截断血缘。扩缩分区则用 `repartition` 或 `coalesce`，前者有 shuffle、后者适合减少分区。回答时我会同时说明这些操作的代价，而不是只背“分区、容错、内存计算”。

##### 知识点补充

窄依赖可以按分区流水执行；宽依赖会产生 shuffle 并切分 stage。长血缘重算成本高时可 checkpoint 截断 lineage，热点数据可 persist，但缓存不是可靠持久化。

##### Transformation 与 Action

Q：Transformation 和 Action 有什么区别？
A：Transformation 如 `map`、`filter`、`reduceByKey` 只生成新的 RDD 和依赖，不立即计算；Action 如 `count`、`saveAsTextFile`、`collect` 才提交 Job。比如读取 1 TB 日志后做清洗和聚合，代码执行到 transformation 时 Spark UI 还没有 Job；调用 `count` 后 DAGScheduler 才按 shuffle 切 Stage 并启动 Task。如果随后又 `save`，未缓存的上游会再算一遍，因此我会在昂贵且复用的结果上 `persist(MEMORY_AND_DISK)`，用 Storage 页面确认缓存命中，最后 `unpersist`。`collect` 只用于已确认很小的结果，否则 Driver 可能 OOM。回答时会结合 Spark UI 的 Job/Stage 数验证惰性执行，而不是只背算子分类。

##### 知识点补充

一个 Action 通常对应一个 job；job 再按 shuffle 边界切 stage。`collect` 会把全部结果拉到 Driver，数据量不可控时风险很高。

##### Shuffle 机制

Q：Spark Shuffle 有哪些，过程和特点是什么？
A：从依赖看，`groupByKey`、`reduceByKey`、join、repartition 等都会触发 shuffle。现代 Spark 主要使用 sort-based shuffle：Map 端按目标分区组织并写出数据，Reduce 端拉取对应块，再聚合或排序。`reduceByKey` 能先做 Map 端聚合，通常比把全部值拉过去的 `groupByKey` 更省网络和内存。优化时我会看分区数、倾斜、spill、fetch wait 和序列化，而不是笼统说“避免所有 shuffle”。

##### 知识点补充

Hash Shuffle 是早期实现，文件数容易随 Mapper × Reducer 增长；Sort Shuffle 通过排序和合并文件改善管理。AQE 可动态合并 shuffle 分区、处理倾斜 join，但仍需合理设置初始分区和广播阈值。

##### Stage 划分与宽窄依赖

Q：Spark 的 Stage 怎么划分？`distinct` 是宽依赖还是窄依赖？
A：Spark 从 Action 生成 Job，再由 DAGScheduler 从最终 RDD 反向分析依赖，遇到 ShuffleDependency 就切开 Stage。没有 shuffle 的一串窄依赖可以流水放在同一个 stage；shuffle 前通常形成 ShuffleMapStage，最后形成 ResultStage。`distinct` 需要把相同值聚到同一分区，通常会触发 shuffle，所以属于宽依赖。理解 stage 的关键不是背算子名字，而是判断一个下游分区是否依赖多个上游分区。

##### 知识点补充

窄依赖下，上游单个分区只被少量确定的下游分区使用，丢失时可局部重算；宽依赖会形成全局数据交换，也是 stage 边界、倾斜和网络开销的主要来源。Spark UI 的 DAG、stage 和 SQL 指标可用于验证。

##### Spark 数据倾斜监控与定位

Q：离线任务的数据倾斜怎么监控和定位，除了看 YARN 还能看什么？
A：YARN 只能说明容器资源和任务状态，定位倾斜我主要看 Spark UI 和 History Server。先比较同一 stage 各 task 的 duration、input/shuffle read、shuffle write、records、spill 和 GC；如果少数 task 数据量或运行时间远高于中位数，就是明确证据。然后用采样 SQL、分区统计或 key 频次找出热点，判断是空值、默认值、业务大客户还是分区设计造成，再选择加盐、预聚合、广播、小表拆分或 AQE skew join。

##### 知识点补充

还要区分数据倾斜与资源异构、磁盘慢、fetch failure。可把 Spark event log 接入分析平台，持续监控 task 最大值/中位数比、spill、长尾 task 和 stage P95，而不是故障时才手工打开 UI。

##### Spark 与 MapReduce 的差异

Q：Spark 为什么通常比 MapReduce 快，什么情况下仍不应该简单替换？
A：Spark 把多步计算组织成 DAG，中间结果可以保留在内存或由流水线执行，减少 MapReduce 每一步都落 HDFS 的磁盘和调度开销；同时提供缓存、广播变量和丰富算子，迭代算法与交互分析优势明显。但 Spark 不是“全在内存”，shuffle、缓存淘汰和数据量超过内存时仍会落盘。超大规模顺序批处理、资源非常紧张或已有稳定 MR 作业时，MapReduce 的简单性和容错成本也可能更合适。实际迁移我会用同一数据和口径比较端到端时长、shuffle、峰值内存、失败恢复和维护成本，而不是只拿一个算子做结论。

##### 知识点补充

两者都基于分区并行和失败重算。Spark 的性能来自 DAG 优化、减少物化和更灵活的执行模型，不是因为“使用了内存”这一句话。

##### Spark 批处理、流处理与故障恢复

Q：Spark 批处理和 Spark Streaming 有什么区别，Streaming 作业如何保证故障恢复？
A：批处理处理有界数据，作业结束后退出；传统 Spark Streaming 把无界流切成一系列 micro-batch，每个批次生成 Job，延迟受 batch interval 约束。Structured Streaming 则用统一 DataFrame/Dataset API 表达增量查询，默认仍是微批，也可选择连续处理的受限模式。恢复时我会把 checkpoint 放在可靠存储，保存查询元数据、进度和状态；Kafka 等可重放源按已提交 offset 重新消费，状态算子从 checkpoint 恢复，输出端必须支持幂等键或事务，才能获得端到端效果。修改查询逻辑、状态结构或源参数时不能盲目复用旧 checkpoint，需按兼容性制定迁移方案。

##### 知识点补充

传统 DStream 的 checkpoint 包括元数据与状态数据，receiver 模式还涉及 WAL；Direct Kafka 方式直接管理 offset。Structured Streaming 的 exactly-once 仍依赖 source 可重放和 sink 语义。

##### RDD、DataFrame 与 Dataset 选型

Q：RDD、DataFrame、Dataset 有什么区别，项目中怎么选择？
A：RDD 是类型化的分布式对象集合，控制粒度高，适合不规则底层转换，但优化器看不到对象内部语义，序列化和 JVM 对象开销较大。DataFrame 本质是带 Schema 的 `Dataset[Row]`，Catalyst 能做谓词下推、列裁剪和 Join 重排，Tungsten 使用更紧凑的内存表示，SQL/ETL 场景通常优先选择。Dataset 在 Scala/Java 中通过 Encoder 同时提供类型安全和结构化优化，但某些强类型 lambda 会限制优化；PySpark 没有相同意义的 typed Dataset。实际项目我会尽量保持 DataFrame/SQL 链路，只在缺少表达能力或需要细粒度分区控制时下沉 RDD。

##### 知识点补充

DataFrame 并不是单机表格，它仍按分区分布式执行。频繁在 RDD 与 DataFrame 之间转换会增加序列化成本并打断优化，应控制边界。

##### Spark 统一内存模型与资源配置

Q：Spark 统一内存模型是什么？生产任务 executor、内存和并行度怎么设置？
A：Executor 堆内主要分执行内存和存储内存，两者共享统一区域：执行内存用于 shuffle、join、sort，存储内存用于 cache 和广播，空闲时可相互借用，但执行可以挤出缓存。配置任务时我先按输入量、shuffle 峰值和目标 Task 大小估算分区，再根据集群队列确定 executor 数；单 executor 通常不堆太多核，避免 GC 和并发 I/O 互相干扰。内存除 `executor-memory` 外还要给 memory overhead、堆外和 PySpark 留空间。上线后用 UI 检查 Task P95、spill、GC 比例、峰值内存和 CPU 利用率迭代，不能把“每个 executor 5 核 20G”当成固定答案。

##### 知识点补充

默认并行度、SQL shuffle partitions 和输入分区来源不同。分区太少资源闲置且单 Task 易 OOM，太多则调度与小文件开销上升。

##### Spark 综合调优与 Shuffle 规避

Q：Spark 任务如何系统调优？哪些算子会产生 Shuffle，能否避免？
A：我先从正确性与基线开始，再看 Spark UI 的扫描量、shuffle read/write、spill、GC、长尾和失败重试。`groupByKey`、`reduceByKey`、`distinct`、`repartition`、大表 Join 和按新 key 的聚合通常产生 shuffle；窄依赖的 map/filter 不会。能在 Map 端预聚合就用 reduceByKey/aggregateByKey，能广播的小表用 broadcast join，已有相同分区器的 RDD 保留分区器，避免无意义 repartition；但业务确实需要按 key 汇总时不能“消灭”必要 shuffle，只能减少数据量和治理倾斜。SQL 侧结合 AQE、谓词下推和列裁剪，优化后比较结果一致、运行时长与资源成本。

##### 知识点补充

判断是否 Shuffle 以执行计划和 Stage 边界为准，不只靠算子名称。缓存只对复用链路有价值，错误缓存大表会增加序列化、GC 和淘汰开销。

##### map、mapPartitions 与 flatMap

Q：Spark 的 `map`、`mapPartitions`、`flatMap` 有什么区别，实际如何选择？
A：`map` 对每条输入返回一条输出，适合纯转换；`flatMap` 每条可返回零到多条，例如把一行日志拆成多个词或过滤解析失败记录。`mapPartitions` 一次拿到整个分区迭代器，适合每分区创建一次昂贵资源，例如批量初始化解析器或数据库客户端，减少逐条连接开销；但必须流式处理迭代器，不能 `toList` 把整分区装内存，并在分区结束可靠关闭资源。写外部系统还要批量、限流和幂等，因为 Task 重试会重复执行。选择依据是输出基数和资源生命周期，不是认为 mapPartitions 永远更快。

##### 知识点补充

`mapPartitions` 改变调用粒度，不自动减少数据量。连接对象通常不可跨 Executor 随意共享，序列化边界和失败重试语义必须考虑。

##### Spark 持久化与故障排查

Q：Spark 什么时候持久化？作业从 20 分钟变成 2 小时或直接失败如何定位？
A：只有同一昂贵上游被多次 Action 复用时才持久化，并按内存容量选 `MEMORY_ONLY`、`MEMORY_AND_DISK` 或序列化级别；物化后用 Storage 页面确认缓存比例，用完 `unpersist`。异常慢时先定位时间花在队列等待还是运行，再比较历史与当前 Stage：输入/文件数增长、某 Task 长尾、shuffle fetch failure、spill、GC、Executor 丢失或外部存储变慢。失败则从最先出现的 executor/driver 异常和事件日志追根因，不只看最后的“stage aborted”。根据证据处理倾斜、分区、资源、小文件或依赖服务，并用相同数据回归。

##### 知识点补充

Checkpoint 会切断血缘并写可靠存储，cache/persist 主要用于性能且可能丢失后重算。两者不能互相替代。

##### Spark 提交流程与运行模式

Q：Spark 作业如何提交，client 与 cluster 模式有什么区别？
A：`spark-submit` 先解析主类、依赖和资源参数，创建 Driver 的 SparkContext；在 YARN client 模式 Driver 留在提交机，ApplicationMaster 主要协调资源，适合交互调试但提交机网络和生命周期成为风险。cluster 模式 Driver 运行在 YARN Container，客户端退出不影响作业，更适合生产。Driver 向资源管理器申请 Executor，DAGScheduler 按 Action 生成 Job 和 Stage，TaskScheduler 把 TaskSet 发到 Executor，Executor 拉取依赖、执行并汇报。排障按提交端、YARN application、Driver、Executor 日志逐层看，并保存 event log 供 History Server 复盘。

##### 知识点补充

local、Standalone、YARN、Kubernetes 是不同 master/deploy 组合。Driver 负责调度和结果元数据，过量 collect 或超大 task closure 会使 Driver OOM。

##### AQE 自适应查询执行

Q：Spark AQE 是什么，能解决哪些问题，不能解决哪些问题？
A：AQE 在运行时获得 Shuffle 统计后重新优化后续物理计划，常见能力包括合并过小 shuffle 分区、把运行时变小的 Join 一侧改为广播，以及拆分倾斜分区。它能减少静态估算不准造成的小 Task 和长尾，但前提是使用 Spark SQL/DataFrame、开启相关配置并产生查询阶段统计；RDD 自定义逻辑和外部系统慢不会被 AQE 自动修复。上线我会比较 explain 的 initial/final plan、Task 数、倾斜分区和 P95 时长，设置合理 advisory partition size 与 skew 阈值，防止过度合并降低并行度。

##### 知识点补充

AQE 是查询执行期间的计划调整，不等同于动态资源分配；后者根据 backlog 和空闲时间增减 Executor，解决的是资源数量。

##### RDD 创建与 DataFrame 转换

Q：Spark 如何创建 RDD，DataFrame 能否转换成 RDD？
A：RDD 可从驱动端集合 `parallelize` 创建，适合小测试数据；从 HDFS 等外部存储用 `textFile/objectFile` 读取；也可由已有 RDD transformation 产生。DataFrame 可以通过 `.rdd` 转为 `RDD[Row]`，Dataset 在 Scala/Java 可得到类型化 RDD；反向转换需要 Schema 或 case class/Bean。生产不会把大数据先 collect 到 Driver 再 parallelize，因为会造成单点内存和网络问题。转换到 RDD 后 Catalyst 无法继续理解对象内部逻辑，列裁剪和代码生成等优化边界会中断，因此只有结构化 API 表达不了的算法才下沉，完成后尽快回到 DataFrame。

##### 知识点补充

RDD 分区来自输入切片或显式并行度。DataFrame 的 `.rdd` 不会把数据拉到本地，但会改变执行接口和序列化表示。

##### SMB Join 与广播变量

Q：Sort-Merge-Bucket Join 和广播变量的原理是什么，分别适合什么场景？
A：SMB Join 要求两表按相同 Join key 分桶、桶数满足兼容关系，并在桶内排序；执行时对应桶可以做归并关联，减少全量 shuffle，适合反复关联的稳定大表，但建表写入和桶元数据必须真实可靠。广播 Join 则由 Driver 收集小表并通过块管理分发到各 Executor，Task 在本地构建哈希表与大表分区关联，省掉大表 shuffle；广播阈值看过滤后序列化大小而非表名，过大会使 Driver/Executor OOM。Spark 的广播变量是只读共享数据，不能在 Executor 修改后期望回传。

##### 知识点补充

SMB 的收益依赖引擎是否识别桶信息及数据是否按要求写入。广播对象在每个 Executor 保存副本，并非每个 Task 一份，但反序列化和 GC 成本仍需观察。

##### Spark SQL 与 Catalyst 执行流程

Q：一条 Spark SQL 从解析到执行经历哪些阶段，SQL 写错会在哪一步发现？
A：SQL 文本先由 Parser 生成未解析逻辑计划；Analyzer 结合 Catalog 解析表、列、函数和类型，字段不存在或歧义通常在这里报错；Optimizer 用规则做谓词下推、常量折叠、列裁剪和 Join 优化，形成优化逻辑计划；Planner 生成候选物理计划并选择，WholeStage Codegen 等生成执行代码。真正 Action 时，物理计划转为 RDD DAG，按 Shuffle 划分 Stage 并提交 Task。语法错误在 Parser，表列解析错误在 Analyzer，运行数据转换、OOM 等在执行阶段。排查会查看 extended explain 的各阶段，不把所有错误归为“物理执行器”。

##### 知识点补充

Catalyst 使用 TreeNode 和规则批次转换计划。逻辑正确不代表物理高效，统计信息、CBO 和 AQE 会影响 Join 顺序与策略。

##### reduceByKey 与 groupByKey

Q：Spark 中 reduceByKey 和 groupByKey 有什么区别，为什么通常优先前者？
A：groupByKey 会把同一个 key 的所有 value 全部拉到一个 Reduce 端并保存集合，Shuffle 数据量和内存压力都大；reduceByKey 能在 Map 端先局部聚合，再 Shuffle 合并后的中间结果，适合求和、计数、最大值等可结合操作。实际统计我优先 reduceByKey、aggregateByKey 或 DataFrame 聚合；只有业务确实需要一个 key 的全部原始 value，且单 key 数据可控时才用 groupByKey。即使使用 reduceByKey，超级热点 key 仍会集中到一个分区，需要加盐或两阶段聚合。

##### 知识点补充

局部聚合要求函数满足结合律，最好也满足交换律。排序、取 TopN 等需求可在每个 key 内维护有界结构，避免把全部 value 收集到内存。

##### Spark SQL 小文件合并

Q：Spark SQL 为什么产生小文件，怎样在不制造单点的情况下合并？
A：文件数通常由输出分区数、动态分区基数和频繁小批次写入共同决定。写前我先过滤、聚合，再按目标分区键 repartition 控制每个业务分区的并发；减少分区且无需全量重分布时可 coalesce，但不能一律 coalesce(1)，否则形成单任务瓶颈。开启 AQE 的分区合并和目标文件大小配置可以缓解 Shuffle 后小分区，湖表则使用 compaction。合并后检查文件数量、平均大小、任务拖尾和查询扫描，避免为了大文件造成倾斜或超大单文件。

##### 知识点补充

输入小文件可通过文件源分区打包参数减少 Task，输出小文件则要控制写出并行度与批次。定期合并只能治存量，源头频繁微批仍需治理。

##### 多个 Action 的执行过程

Q：同一个 Spark 程序有多个 Action，会生成几个 Job，前面的计算会不会复用？
A：每次 Action 通常触发一个独立 Job，DAG Scheduler 从该 Action 的依赖向上追溯并按宽依赖切 Stage。若两个 Action 依赖同一 RDD/DataFrame，但没有 cache/persist，前面的 lineage 很可能重复计算；我会对计算昂贵且确实被复用的中间结果持久化，并选择合适存储级别，最后 unpersist。SQL 的 exchange/subquery 复用受版本和执行计划影响，不能假定自动复用全部逻辑，应该通过 Spark UI 的 Job、Stage 和 DAG 验证。

##### 知识点补充

Action 数量不是 Stage 数量。缓存也有序列化、内存和重算成本，只被使用一次的数据不应盲目缓存。

##### Spark Streaming Direct 模式与 Offset

Q：Spark Streaming 为什么使用 Kafka Direct 模式，Offset 怎样维护才能避免丢失和重复？
A：Direct 模式由每个批次按 Kafka offset 范围直接读取分区，不需要 Receiver/WAL，Kafka 分区数决定单批可用读取并行度，数据位置和失败恢复更清晰。我会在结果成功写入后提交 offset，或把结果与 offset 放在同一可事务存储中；先提交 offset 再写结果会丢数据，先写后提交故障时会重复，因此下游必须按业务键幂等。重启读取已提交位置，回溯使用独立消费组或显式 offset 范围，不能直接重置线上进度覆盖正式结果。

##### 知识点补充

Spark Streaming 微批的 Exactly-Once 取决于输入 offset、确定性计算和输出幂等共同成立。消费者并行度不能超过 Kafka 分区的有效并行度。
