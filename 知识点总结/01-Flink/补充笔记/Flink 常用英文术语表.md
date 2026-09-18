# Flink 常用英文术语表

## 1. 作业与运行架构

| 英文 | 中文 | 简要含义 |
|---|---|---|
| Job | 作业 | 一次提交运行的数据处理程序 |
| Task | 任务 | 算子并行实例执行的工作单元 |
| Subtask | 子任务 | 一个算子的并行执行实例 |
| Operator | 算子 | 对数据执行转换或计算的节点 |
| Transformation | 转换 | 构建 DataStream 拓扑的逻辑操作 |
| JobManager | 作业管理器 | 管理作业调度和协调工作的进程 |
| TaskManager | 任务管理器 | 执行具体 Subtask 的工作进程 |
| Dispatcher | 分发器 | 接收作业并启动 JobMaster |
| JobMaster | 作业主控 | 管理一个具体作业的执行 |
| ResourceManager | 资源管理器 | 管理 TaskManager 和 Slot 资源 |
| Slot | 槽位 | TaskManager 的资源调度单位 |
| Parallelism | 并行度 | 一个算子的并行实例数量 |
| Chaining | 算子链 | 将多个算子合并到同一 Task 中执行 |
| Pipeline | 流水线 | 从输入到输出的数据处理链路 |
| Topology | 拓扑 | 算子及其连接关系 |
| Graph | 图 | 作业结构的图模型 |
| Vertex | 顶点 | 执行图中的计算节点 |
| Edge | 边 | 执行图中节点之间的连接 |
| Execution | 执行 | 作业或任务的运行过程 |
| Deployment | 部署 | 将作业发布到运行环境 |
| Cluster | 集群 | 共同运行 Flink 的机器和进程集合 |
| Standalone | 独立集群 | Flink 自主管理资源的部署方式 |
| Session | 会话 | 多个作业共享一个长期运行的集群 |
| Application | 应用 | 一个应用对应一个专用 Flink 集群 |

## 2. DataStream 与数据转换

| 英文 | 中文 | 简要含义 |
|---|---|---|
| Stream | 数据流 | 持续或有限的数据序列 |
| DataStream | 数据流对象 | DataStream API 的核心数据抽象 |
| Record | 记录 | 流中的一条数据 |
| Element | 元素 | 数据流中的单个对象 |
| Event | 事件 | 表示某个业务行为的数据 |
| Key | 键 | 用于分区和状态隔离的标识 |
| Keyed Stream | 键控流 | 经过 `keyBy()` 的数据流 |
| Partition | 分区 | 数据的逻辑或物理划分 |
| Shuffle | 重分区 | 在上下游并行任务之间重新分配数据 |
| Forward | 前向分区 | 上下游一对一传递数据 |
| Rebalance | 重新均衡 | 轮询分发数据到下游实例 |
| Rescale | 局部缩放 | 在上下游局部范围轮询分发 |
| Broadcast | 广播 | 将数据发送给所有下游实例 |
| Union | 合并 | 合并多个相同类型的数据流 |
| Connect | 连接 | 连接两个类型可以不同的数据流 |
| Map | 映射 | 一条输入转换成一条输出 |
| FlatMap | 扁平映射 | 一条输入转换成零到多条输出 |
| Filter | 过滤 | 按条件保留数据 |
| Reduce | 归约 | 增量合并同类型数据 |
| Aggregate | 聚合 | 使用累加器汇总数据 |
| Process | 处理 | 使用底层函数和上下文处理数据 |
| Collector | 收集器 | 将函数结果发送到下游 |
| Side Output | 侧输出 | 主输出之外产生的附加数据流 |

## 3. Source、Sink 与 Connector

| 英文 | 中文 | 简要含义 |
|---|---|---|
| Source | 数据源 | 从外部系统读取数据 |
| Sink | 数据汇 | 向外部系统写出数据 |
| Connector | 连接器 | Flink 与外部系统交互的组件 |
| Reader | 读取器 | 从数据源读取记录或分片 |
| Writer | 写入器 | 将记录写入外部系统 |
| Split | 数据分片 | Source 分配和读取的数据单元 |
| Enumerator | 枚举器 | 发现并向 Reader 分配 Split |
| Commit | 提交 | 确认并完成写出结果 |
| Committer | 提交器 | 执行写出提交动作的组件 |
| Transaction | 事务 | 一组原子提交的写入操作 |
| Offset | 位点 | 消费数据的位置标识 |
| Topic | 主题 | Kafka 等消息系统的数据分类 |
| Partition Discovery | 分区发现 | 动态发现新增数据分区 |
| Bounded | 有界 | 输入数据存在确定终点 |
| Unbounded | 无界 | 输入数据持续产生、没有固定终点 |

## 4. 时间与 Watermark

| 英文 | 中文 | 简要含义 |
|---|---|---|
| Event Time | 事件时间 | 业务事件实际发生的时间 |
| Processing Time | 处理时间 | Flink 处理记录时的机器时间 |
| Ingestion Time | 摄入时间 | 数据进入 Flink 时的时间 |
| Timestamp | 时间戳 | 记录携带的时间数值 |
| Watermark | 水位线 | 事件时间处理进度标记 |
| Out-of-Order | 乱序 | 到达顺序与事件时间顺序不一致 |
| Bounded Out-of-Orderness | 有界乱序 | 乱序程度存在已知上限 |
| Monotonous | 单调递增 | 时间戳持续递增、不发生回退 |
| Idleness | 空闲状态 | 输入通道在一段时间内没有数据 |
| Alignment | 对齐 | 限制不同 Source 的 Watermark 进度差 |
| Timer | 定时器 | 在指定处理时间或事件时间触发回调 |
| Periodic | 周期型 | 按固定周期执行或发射 |
| Punctuated | 间断型 | 遇到特定记录时发射 Watermark |
| Late Data | 迟到数据 | 时间戳不超过当前 Watermark 的数据 |
| Allowed Lateness | 允许迟到 | 窗口触发后继续接收迟到数据的时间 |

## 5. Window

| 英文 | 中文 | 简要含义 |
|---|---|---|
| Window | 窗口 | 将数据按范围分组计算的机制 |
| Window Assigner | 窗口分配器 | 决定记录属于哪个窗口 |
| Tumbling Window | 滚动窗口 | 固定长度且互不重叠的窗口 |
| Sliding Window | 滑动窗口 | 固定长度并按步长滑动的窗口 |
| Session Window | 会话窗口 | 按活动间隔划分的动态窗口 |
| Global Window | 全局窗口 | 将相同 Key 的数据放入全局范围 |
| Trigger | 触发器 | 决定窗口何时执行计算 |
| Evictor | 清除器 | 在窗口函数前后移除部分元素 |
| Window Function | 窗口函数 | 处理窗口内数据的函数 |
| Incremental Aggregation | 增量聚合 | 数据到达时持续更新聚合结果 |
| Full Window Function | 全窗口函数 | 触发时遍历窗口全部数据 |
| Start | 开始 | 窗口起始时间 |
| End | 结束 | 窗口结束时间 |
| Offset | 偏移量 | 调整窗口边界的位置 |

## 6. State

| 英文 | 中文 | 简要含义 |
|---|---|---|
| State | 状态 | 跨记录保存的计算数据 |
| Keyed State | 键控状态 | 按 Key 隔离保存的状态 |
| Operator State | 算子状态 | 绑定到算子并行实例的状态 |
| Broadcast State | 广播状态 | 向所有并行实例广播的规则状态 |
| Value State | 单值状态 | 每个 Key 保存一个值 |
| List State | 列表状态 | 每个 Key 保存一组值 |
| Map State | 映射状态 | 每个 Key 保存一张 Map |
| Reducing State | 归约状态 | 按 ReduceFunction 聚合的状态 |
| Aggregating State | 聚合状态 | 按 AggregateFunction 聚合的状态 |
| Descriptor | 描述器 | 定义状态名称和数据类型 |
| Runtime Context | 运行时上下文 | 获取状态、指标和任务信息的入口 |
| State Backend | 状态后端 | 管理工作状态及快照行为的组件 |
| HashMap State Backend | 哈希表状态后端 | 在 JVM 堆中保存工作状态 |
| RocksDB State Backend | RocksDB 状态后端 | 使用 RocksDB 保存工作状态 |
| TTL | 生存时间 | 状态的过期时间限制 |
| Managed State | 托管状态 | 由 Flink 管理的状态 |
| Raw State | 原始状态 | 用户自行序列化管理的状态 |

## 7. Checkpoint 与恢复

| 英文 | 中文 | 简要含义 |
|---|---|---|
| Checkpoint | 检查点 | Flink 自动生成的一致性状态快照 |
| Savepoint | 保存点 | 用户主动触发、用于运维的状态快照 |
| Snapshot | 快照 | 某一时刻的状态副本 |
| Restore | 恢复 | 从快照重新加载状态 |
| Recovery | 故障恢复 | 作业失败后恢复执行 |
| Checkpoint Storage | 检查点存储 | 保存 Checkpoint 数据的位置和方式 |
| Externalized Checkpoint | 外部化检查点 | 可在作业取消后保留的检查点 |
| Barrier | 屏障 | 在数据流中标记 Checkpoint 边界的事件 |
| Alignment | 对齐 | 等待各输入通道 Barrier 到齐 |
| Aligned Checkpoint | 对齐检查点 | 通过 Barrier 对齐生成的检查点 |
| Unaligned Checkpoint | 非对齐检查点 | 将部分在途数据一并保存的检查点 |
| In-flight Data | 在途数据 | 网络缓冲中尚未处理的数据 |
| Timeout | 超时 | 操作允许执行的最长时间 |
| Interval | 间隔 | 周期任务两次触发之间的时间 |
| Concurrent | 并发 | 多个操作在同一时间段执行 |
| Tolerable Failure | 可容忍失败 | 作业失败前允许的连续检查点失败次数 |
| Retain | 保留 | 不自动删除检查点 |
| Cleanup | 清理 | 删除不再需要的检查点数据 |
| Local Recovery | 本地恢复 | 优先利用 TaskManager 本地状态恢复 |

## 8. 容错与一致性

| 英文 | 中文 | 简要含义 |
|---|---|---|
| Fault Tolerance | 容错 | 发生故障后保持或恢复计算正确性的能力 |
| Exactly Once | 精确一次 | 状态结果在逻辑上只生效一次 |
| At Least Once | 至少一次 | 数据不会丢失，但可能重复处理 |
| At Most Once | 至多一次 | 数据最多处理一次，但可能丢失 |
| Restart | 重启 | 任务失败后重新执行 |
| Restart Strategy | 重启策略 | 控制失败后是否以及如何重启 |
| Failover | 故障切换 | 将失败任务切换到新的执行实例 |
| Backoff | 退避 | 两次重试之间的等待时间 |
| Retry | 重试 | 操作失败后再次执行 |
| Consistency | 一致性 | 状态与数据处理进度保持匹配 |
| Idempotent | 幂等 | 重复执行产生与单次执行相同的结果 |

## 9. 序列化与类型

| 英文 | 中文 | 简要含义 |
|---|---|---|
| Serialization | 序列化 | 将对象转换为可存储或传输的数据 |
| Deserialization | 反序列化 | 将字节数据恢复为对象 |
| Serializer | 序列化器 | 执行序列化的组件 |
| Deserializer | 反序列化器 | 执行反序列化的组件 |
| Schema | 模式或格式定义 | 描述数据编码、解码或表结构 |
| Type Information | 类型信息 | Flink 对 Java 数据类型的描述 |
| Type Serializer | 类型序列化器 | 根据类型信息执行序列化 |
| POJO | 普通 Java 对象 | 符合 Flink POJO 识别规则的数据类 |
| Tuple | 元组 | 固定字段数量的复合数据类型 |
| Generic Type | 泛型类型 | 不能被专用序列化器识别的普通类型 |
| Kryo | Kryo 序列化 | Flink 可使用的通用序列化方案 |

## 10. 性能与运行状态

| 英文 | 中文 | 简要含义 |
|---|---|---|
| Throughput | 吞吐量 | 单位时间处理的数据量 |
| Latency | 延迟 | 数据从输入到输出所需时间 |
| Backpressure | 反压 | 下游处理能力不足导致上游减速 |
| Busy | 忙碌 | Task 正在执行计算 |
| Idle | 空闲 | Task 没有数据可处理 |
| Buffer | 缓冲区 | 暂存待传输或待处理的数据 |
| Network Buffer | 网络缓冲 | 用于 Task 间传输数据的内存 |
| Spill | 溢写 | 内存不足时将数据写入磁盘 |
| Skew | 数据倾斜 | 数据在分区间分布不均匀 |
| Metric | 指标 | 描述作业运行状态的数值 |
| Counter | 计数器 | 记录累计数量的指标 |
| Gauge | 瞬时值指标 | 返回当前数值的指标 |
| Meter | 速率指标 | 统计单位时间事件数量 |
| Histogram | 直方图指标 | 统计数值分布和分位数 |
| Reporter | 指标报告器 | 将 Metrics 输出到外部系统 |
| Accumulator | 累加器 | 聚合任务执行期间的统计值 |

## 11. Table、SQL 与动态表

| 英文 | 中文 | 简要含义 |
|---|---|---|
| Table | 表 | Table API 的逻辑数据对象 |
| Dynamic Table | 动态表 | 随流数据持续变化的表 |
| Catalog | 元数据目录 | 管理数据库、表和函数信息 |
| Database | 数据库 | Catalog 下的表命名空间 |
| Schema | 表结构 | 字段名称、类型和约束定义 |
| Row | 行 | 表中的一条记录 |
| RowKind | 行类型 | 标识插入、更新前、更新后或删除 |
| Changelog | 变更日志 | 描述动态表变化的数据流 |
| Append | 追加 | 只增加新记录的变更模式 |
| Retract | 撤回 | 撤销之前输出的结果 |
| Upsert | 插入或更新 | 按主键插入新行或更新已有行 |
| Planner | 规划器 | 将 SQL 和 Table API 转换为执行计划 |
| Optimizer | 优化器 | 优化逻辑计划和物理计划 |
| Expression | 表达式 | Table API 中的字段和计算表达式 |
| Function | 函数 | SQL 或 Table API 的计算逻辑 |

## 12. 常见缩写

| 缩写 | 完整英文 | 中文 |
|---|---|---|
| JM | JobManager | 作业管理器 |
| TM | TaskManager | 任务管理器 |
| DAG | Directed Acyclic Graph | 有向无环图 |
| API | Application Programming Interface | 应用程序编程接口 |
| SQL | Structured Query Language | 结构化查询语言 |
| UDF | User-Defined Function | 用户自定义函数 |
| POJO | Plain Old Java Object | 普通 Java 对象 |
| TTL | Time To Live | 生存时间 |
| UID | Unique Identifier | 唯一标识符 |
| HA | High Availability | 高可用 |

