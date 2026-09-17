##### Hadoop

##### HDFS 读写流程与节点故障

Q：讲一下 HDFS 读写流程；读写过程中 DataNode 挂掉怎么办？
A：写入时客户端先向 NameNode 申请创建文件，NameNode 做权限和路径检查并返回目标 DataNode 管道。客户端把 block 切成 packet 沿 pipeline 发送，确认则反向返回；某个 DataNode 故障时会重建 pipeline、补写缺失副本，NameNode 后续安排副本恢复。读取时客户端从 NameNode 获取 block locations，优先读就近副本；当前 DataNode 失败就切换其他副本并上报坏块。NameNode 不传输实际数据，只管理命名空间和块元数据。

##### 知识点补充

写入是 packet 级流水线和 ACK，不是等完整 block 才复制。客户端有校验和，坏副本会被隔离。副本数、机架感知和心跳/块汇报共同影响容错。

##### MapReduce Shuffle 与排序次数

Q：MapReduce Shuffle 流程是什么，会发生几次排序？
A：Map 输出先进入环形缓冲区，达到阈值后按分区并在内存中排序，spill 到磁盘；多个 spill 文件在 Map 端归并。Reduce 端从各 Map 拉取属于自己的分区，边拉取边归并，最终按 key 有序后交给 reduce 函数。所以不能只背“固定两次排序”：Map spill 会排序，Map 和 Reduce 两侧都可能多轮归并，次数取决于 spill 文件数和归并因子。

##### 知识点补充

Partitioner 决定 key 去哪个 Reducer，排序保证同一 key 聚集，GroupingComparator 决定哪些 key 作为一组进入 reduce。Combiner 若满足结合律和交换律，可减少网络数据量，但不能保证执行。

##### HDFS Block 为什么常用 128 MB

Q：Hadoop 为什么默认使用较大的 block，例如 128 MB？
A：128 MB 是在元数据、顺序吞吐和并行度之间的工程折中，不是因为 HDFS 只能存这么大。假设每天落 1 TB 文件，128 MB 大约产生 8192 个 block；若改成 1 MB，会变成一百多万个 block，NameNode 要在内存维护更多块和副本信息，MapReduce/Spark 也会产生大量小任务，调度开销可能超过计算本身。反过来，如果设置成 2 GB，任务数太少会吃不满集群，单个 block 失败后的重算粒度也更大。实际项目会根据典型文件大小、磁盘顺序读带宽、期望并行度和 NameNode block 数监控调整；大量小文件则在采集或 compaction 阶段合并，单纯修改 block size 不能修复已有小文件。

##### 知识点补充

HDFS block 是逻辑切块，不等于操作系统块。NameNode 需要在内存维护块和副本元数据；大量小文件的问题主要是元数据和调度开销，单纯增大 block 不能合并已有小文件。

##### MapReduce 执行流程与 Combiner

Q：完整讲一下 MapReduce 作业从提交到完成的流程，Combiner 在哪里起作用？
A：客户端先切分输入并把作业资源提交给 YARN，ResourceManager 分配 ApplicationMaster，AM 申请容器启动 MapTask 和 ReduceTask。Mapper 读取 InputSplit，经 RecordReader 变成键值对，输出进入环形缓冲区；达到阈值后按分区排序并 spill 到磁盘，多次 spill 合并。Reducer 通过 shuffle 拉取属于自己的分区，归并排序后按 key 分组调用 reduce，最终由 OutputFormat 写 HDFS，并通过临时目录和提交协议避免失败任务留下正式结果。Combiner 发生在 Map 端，是可选的局部聚合，可能执行零次或多次，因此只能用于满足结合律、交换律且不改变语义的操作，如 sum，平均值必须传 sum/count，不能直接平均平均值。

##### 知识点补充

定位失败要区分容器资源、数据格式、shuffle fetch、磁盘和用户代码异常。推测执行会重跑 task attempt，业务输出仍应通过提交协议和幂等设计保证一致。

##### 分布式 TopK 的两阶段实现

Q：MapReduce 如何计算全局 Top10，数据量大时怎样避免单点排序全部记录？
A：第一阶段每个 Mapper 或 Reducer 只维护容量 10 的最小堆，输出本分片 Top10；由于不可能进入局部 Top10 的记录也不可能进入全局 Top10，可以安全丢弃。第二阶段汇总所有局部候选，再维护一个容量 10 的堆得到全局结果。若按类别求 Top10，第一阶段以类别分区并在每类维护堆，第二阶段仍按类别合并；若要求相同分值的稳定顺序，需要定义二级排序键。实际会用 Combiner 式局部裁剪降低网络量，但不会让单 Reducer 接收原始全量数据。

##### 知识点补充

局部 TopK 合并能得到全局 TopK，前提是比较规则一致。若 K 很大或要求完整有序结果，应采用采样范围分区和全局排序方案。

##### YARN 提交流程、部署模式与调度器

Q：YARN 上的应用如何提交和运行？FIFO、Capacity、Fair 调度器怎么选择？
A：客户端先向 ResourceManager 申请 Application ID，把程序包、配置和依赖上传到 HDFS 暂存目录，再提交 Application。ResourceManager 选择 NodeManager 启动 ApplicationMaster，AM 注册后按任务资源画像申请 Container，NodeManager 拉取资源并启动进程，AM 持续汇报进度，结束后注销并清理临时资源。Spark 可使用 client 或 cluster 模式，区别是 Driver 位于提交机还是 YARN Container；生产通常用 cluster，避免提交机断开导致任务失败。调度器方面，FIFO 按先来后到，适合简单小集群；Capacity 用多队列和容量保障隔离部门资源；Fair 追求运行中作业逐步公平共享。实际会配置队列容量、最大资源、用户限制和抢占，并通过队列利用率与等待时间验证。

##### 知识点补充

ResourceManager 管全局资源，NodeManager 管单机 Container，ApplicationMaster 管单个应用。调度器分配的是资源，不负责应用内部的 Task 调度逻辑。

##### HDFS 副本放置、磁盘均衡与小文件

Q：HDFS 默认副本如何写到多个 DataNode？磁盘不均衡和小文件问题怎么处理？
A：客户端向 NameNode 请求写文件后，NameNode 根据机架感知、节点空间、负载和故障域选择副本节点，客户端把 packet 以 pipeline 方式传给第一个 DataNode，再依次转发并反向确认；中途节点故障会重建 pipeline，NameNode 后续补足副本。副本数可用文件创建参数或 `dfs.replication` 指定，已有文件用 `hdfs dfs -setrep` 调整，不同应用不需要共用一个固定值。节点磁盘不均衡先排除坏盘、挂载缺失和新节点，再用 balancer 做跨节点均衡、diskbalancer 做节点内卷间均衡。小文件则在采集端批量、SequenceFile/Har/Parquet 合并或离线 compaction，不能只靠增加 block 大小。

##### 知识点补充

常见副本因子是 3，但以集群配置为准。Balancer 会产生网络和磁盘流量，应限速并避开高峰；低副本文件需要评估可靠性与机架故障风险。

##### 分布式系统处理海量数据的原理

Q：Hadoop、Spark、Flink 为什么能处理单机 MySQL 放不下的数据？
A：核心不是某个函数，而是把数据水平分区到多台机器，让存储容量、CPU、内存和网络可横向扩展。HDFS 把文件切块并保存副本，调度器尽量让计算靠近数据；MapReduce/Spark 把算子拆成多个并行 Task，按 key shuffle 后聚合；Flink 对无界流持续分区处理，并把中间状态做一致性快照。节点失败时通过副本读取、Task 重试或状态恢复继续执行。MySQL 单实例更擅长事务和索引点查，跨海量数据的全表聚合受单机资源与事务架构限制。实际系统仍要处理倾斜、网络 shuffle、小文件和一致性，不能把“加机器”理解成线性提速。

##### 知识点补充

可扩展性来自分区并行、容错和调度，但跨分区协调会带来网络与一致性成本。OLTP 与大数据计算解决的问题不同，不应简单互相替代。

##### NameNode 高可用与 HDFS 进程

Q：NameNode HA 如何实现主备切换，Active 与 Standby 的 FsImage/Edits 怎样同步？
A：HA 集群通常有两个 NameNode，共享 Edits 通过一组 JournalNode 以多数派写入；Active 处理客户端请求并写 EditLog，Standby 持续从 JournalNode tail edits，在内存应用相同命名空间状态，所以不是定期复制一份 FsImage 才同步。ZooKeeper 保存选主状态，ZKFC 监控 NameNode 并发起故障切换，fencing 防止旧 Active 继续写造成双主。DataNode 同时向两个 NameNode 汇报 block。常见进程还包括 NameNode、DataNode、JournalNode、ZKFC，SecondaryNameNode 只做 checkpoint 合并，不是热备。演练时验证切换时间、客户端 failover 和 JournalNode 多数派。

##### 知识点补充

FsImage 是某时点命名空间快照，EditLog 是之后的变更序列。Standby/Checkpoint 通过合并控制 EditLog 长度，但数据 block 仍由 DataNode 保存。

##### MapReduce 与 Spark Shuffle 差异及优化

Q：MapReduce 和 Spark Shuffle 有什么区别，数据量很大时怎么优化？
A：MapReduce Map 输出先在内存缓冲，分区排序后多次 spill/merge 成本地文件，Reducer 通过网络拉取并归并，再进入 reduce；每个 MR 阶段边界通常写磁盘且作业间落 HDFS。Spark Shuffle 同样按下游分区写本地 shuffle 文件、下游 fetch，但一个应用 DAG 内可以流水执行窄依赖，使用排序型 ShuffleManager、内存聚合和统一执行内存，Stage 间不必落 HDFS。优化共同点是先过滤/预聚合、合理分区、压缩、治理倾斜和减少小 spill；MR 调整缓冲与 reduce 数，Spark 看 spill、fetch、AQE 和 executor 内存。必要 shuffle 不能靠换引擎消失。

##### 知识点补充

排序次数不是固定常数，取决于 spill 次数和多路归并轮次。3 个不同 key、10 个 Reducer 时每个 key 只会按分区器进入其中一个 Reducer，其余可能空闲。

##### YARN 常见服务与端口认知

Q：面试问 YARN 端口号时应该怎样回答，生产访问如何管理？
A：我会先说明端口可配置、以集群配置和服务发现为准，不把默认值当协议保证。常见 Hadoop 3 默认中，ResourceManager Web UI 通常是 8088，调度器地址常见 8030，客户端提交 8032，NodeManager Web UI 常见 8042；HA 环境还可能通过反向代理或统一网关访问。实际排障使用 `yarn-site.xml`、管理平台和监听端口确认，不凭记忆直接放通防火墙。生产不把管理 UI 暴露公网，配置 Kerberos/TLS、网关和最小网络 ACL，并区分 Web、RPC 与 Shuffle 服务端口。

##### 知识点补充

不同发行版、容器平台和公司安全网关会改写默认端口。面试回答端口后应补充“以配置为准”和服务职责。

##### HDFS 并发写入边界

Q：HDFS 能否多个客户端同时写同一个文件？它如何避免并发冲突？
A：HDFS 设计为 write-once、read-many，通常一个文件同一时刻只有一个有效 writer，不支持多个客户端像本地文件一样随机覆盖。NameNode 在 create 时建立租约并检查路径是否已存在，客户端定期续约；写入失败后由租约恢复关闭未完成块。若多个生产者需要并发输出，我会让它们写不同文件或分区，结束后再合并；支持 append 也仍由单 writer 持有租约，不能当并发日志文件使用。Exactly-Once 要通过临时路径加原子 rename、任务提交协议或业务幂等保证。

##### 知识点补充

DataNode 的 pipeline 负责块副本写入确认，NameNode 管文件命名空间和租约。HDFS 一致性模型与 POSIX 文件系统不同。

##### Hadoop 3 纠删码

Q：HDFS 纠删码是什么？与三副本相比如何选型？
A：纠删码把数据拆成若干数据单元并计算校验单元，例如常见 RS-6-3 用 6 个数据块加 3 个校验块，可容忍最多 3 个单元丢失，空间开销约 1.5 倍；三副本是 3 倍。它适合大文件、冷数据和低频读取，能显著省存储，但编码、重建会消耗 CPU 和网络，小文件、热点数据及恢复延迟敏感场景仍优先副本。落地时我按目录配置 EC policy，确保节点和机架数量满足布局要求，监控坏块、重建队列和跨机架流量，并先做读写及故障压测。

##### 知识点补充

具体 policy 不是固定“4+2”，以集群启用策略为准。纠删码只解决存储冗余，不代替备份；误删除会同步影响所有编码单元。

##### NameNode 启动与元数据加载

Q：NameNode 启动时会加载哪些信息，完整流程是什么？
A：NameNode 先获取存储目录锁并校验 VERSION，加载最近的 fsimage 重建内存中的目录树、inode、权限、配额和块映射，再回放 edits 恢复到最新命名空间状态，随后生成新的检查点或进入正常服务。启动后 DataNode 通过注册、block report 和增量汇报上报实际块位置，NameNode 才逐步掌握副本分布；安全模式期间通常不接受普通写入，达到足够块安全比例后退出。HA 环境下 Active/Standby 还通过 JournalNode 共享 edits，ZKFC 负责健康检测和故障切换。

##### 知识点补充

fsimage 不保存每个块实时所在 DataNode 列表，该映射由块汇报重建。SecondaryNameNode 做 checkpoint，不是热备 NameNode。

##### Hadoop 2 与 Hadoop 3 的实际差异

Q：为什么集群选择 Hadoop 3，而不是 Hadoop 2？升级时关注什么？
A：我不会只回答版本号。Hadoop 3 的代表性变化包括纠删码、多 NameNode HA、YARN 资源类型扩展、Router-based Federation 和多项性能/运维改进，Java 与依赖基线也变化。选择 3.x 是为了长期维护、安全补丁和这些能力，但是否启用要看业务。升级前我核对 HDFS/YARN 客户端协议、JDK、Hive/Spark 兼容矩阵、废弃配置和第三方发行版补丁；搭建测试集群做 fsimage、作业、权限和滚动升级演练，保留元数据备份及回退方案。

##### 知识点补充

“3.x 一定更快”并不成立，收益取决于启用的功能与工作负载。小版本差异和发行版回补补丁也必须纳入选型。

##### YARN Task 失败监控与重试

Q：YARN 上某个 Task 失败后由谁发现，怎样重试和定位？
A：NodeManager 负责启动和监控 Container，并把状态心跳给 ResourceManager；具体应用的 ApplicationMaster 维护 Task/Executor 状态，收到 Container 退出或心跳异常后按框架策略重新申请资源并重试。MapReduce 的失败 Map Task 可在其他节点重跑，Spark Executor 丢失后由 Driver 根据 lineage 重算缺失分区。定位时我先看 YARN 最早失败 Container 的 exit code、stderr 和节点健康，再区分代码异常、OOM、磁盘、网络还是被抢占；不能只把最大重试次数调高。确定性坏数据先隔离，资源不足则校准内存开销和并发，节点故障交给运维下线。

##### 知识点补充

ResourceManager 管全局资源，不理解每个业务 Task 的计算语义；任务级重试由 ApplicationMaster/计算框架负责。无限重试会放大集群故障。

##### HDFS 常用文件操作与副本调整

Q：怎样上传、删除 HDFS 文件并修改副本数？生产操作如何避免误删？
A：上传使用 `hdfs dfs -put`/`-copyFromLocal`，查看用 `-ls`、`-du -h`，删除文件用 `-rm`，目录递归删除才使用 `-rm -r`；是否进入 Trash 取决于集群配置。修改已有文件副本数用 `hdfs dfs -setrep -w N path`，NameNode 调度补副本或删多余副本，`-w` 等待达到目标。生产执行前先 `-ls` 确认精确路径、检查权限和下游，避免宽泛通配符；大量副本调整要限批并监控欠副本、网络和磁盘，不能在峰值一次改整库。

##### 知识点补充

表是否删除应优先通过对应元数据系统操作，直接删 HDFS 目录会留下脏元数据。副本数提高可用性但不等于备份。
