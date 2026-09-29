# Flink API 命名前缀与类名后缀

## 1. 判断方法

阅读陌生 API 时，可以先将驼峰名称拆成：

```text
动作前缀 + 修饰词 + 核心对象
```

例如：

```text
setMaxConcurrentCheckpoints
set + Max + Concurrent + Checkpoints
设置 + 最大 + 并发 + 检查点数量
```

判断顺序：先看开头的动作前缀，再看末尾的核心名词，最后确认方法返回类型。

## 2. 常见方法前缀

| 前缀 | 常见含义 | Flink 示例 |
|---|---|---|
| `setXxx` | 设置或修改配置 | `setCheckpointTimeout()` |
| `getXxx` | 获取值或对象 | `getCheckpointConfig()` |
| `isXxx` | 查询是否处于某状态 | `isCheckpointingEnabled()` |
| `hasXxx` | 查询是否包含某属性或对象 | `hasTimestamp()` |
| `enableXxx` | 开启功能 | `enableCheckpointing()` |
| `disableXxx` | 关闭功能 | `disableCheckpointing()` |
| `withXxx` | 追加配置，通常支持链式调用 | `withIdleness()` |
| `forXxx` | 创建适用于某种规则的对象 | `forBoundedOutOfOrderness()` |
| `createXxx` | 创建具体实例 | `createWatermarkGenerator()` |
| `newXxx` | 创建新对象或进入构建流程 | `newBuilder()` |
| `fromXxx` | 从指定来源构造对象或数据流 | `fromSource()` |
| `of` / `ofXxx` | 使用参数快速构造对象 | `Duration.ofSeconds()` |
| `build` | 根据 Builder 配置生成最终对象 | `builder.build()` |
| `addXxx` | 添加组件、算子或配置 | `addSink()` |
| `removeXxx` | 删除关联项或配置项 | `removeJar()` |
| `registerXxx` | 注册定时器、状态或服务 | `registerEventTimeTimer()` |
| `assignXxx` | 为数据分配属性 | `assignTimestampsAndWatermarks()` |
| `emitXxx` | 发射数据或控制事件 | `emitWatermark()` |
| `collect` | 将处理结果发送到下游 | `collector.collect()` |
| `processXxx` | 处理数据、Watermark 或状态 | `processElement()` |
| `onXxx` | 某个事件发生时的回调 | `onTimer()` |
| `initializeXxx` | 初始化组件或状态 | `initializeState()` |
| `snapshotXxx` | 创建状态快照 | `snapshotState()` |
| `restoreXxx` | 恢复状态或组件 | `restoreReader()` |
| `open` | 初始化函数或算子 | `open()` |
| `close` | 关闭函数或释放资源 | `close()` |
| `configure` | 使用配置对象批量更新参数 | `configure()` |
| `toXxx` | 转换成另一种类型 | `toConfiguration()` |

## 3. 高频前缀区别

```text
setXxx      修改已有对象的配置
withXxx     在已有策略或 Builder 上追加配置
forXxx      创建适用于某种规则的策略
createXxx   创建实际运行组件
getXxx      获取对象或配置值
isXxx       获取布尔状态
```

示意关系：

```text
forBoundedOutOfOrderness(...)
    ↓ 创建基础 WatermarkStrategy
withTimestampAssigner(...)
    ↓ 追加时间戳提取配置
createTimestampAssigner(context)
    ↓ Flink 运行时创建实际分配器
extractTimestamp(...)
    ↓ 处理每条记录
```

## 4. DataStream 常见操作名称

| 名称 | 常见含义 |
|---|---|
| `map` | 一条输入转换为一条输出 |
| `flatMap` | 一条输入转换为零到多条输出 |
| `filter` | 按条件保留数据 |
| `reduce` | 增量合并同类型数据 |
| `aggregate` | 使用累加器增量聚合 |
| `process` | 使用 ProcessFunction 处理数据 |
| `keyBy` | 按 Key 重新分区 |
| `partitionCustom` | 使用自定义分区规则 |
| `rebalance` | 轮询重新分配数据 |
| `rescale` | 在上下游局部范围轮询分配 |
| `broadcast` | 将数据广播到所有下游并行实例 |
| `union` | 合并多个相同类型的数据流 |
| `connect` | 连接两个类型可以不同的数据流 |
| `window` | 创建窗口计算 |
| `trigger` | 指定窗口触发规则 |
| `evictor` | 指定窗口元素清除规则 |
| `sideOutputLateData` | 指定迟到数据侧输出 |
| `getSideOutput` | 获取侧输出流 |
| `sinkTo` | 使用 Sink 接口输出数据 |
| `addSink` | 添加 SinkFunction |
| `execute` | 提交并执行 Flink 作业 |
| `executeAsync` | 异步提交 Flink 作业 |

## 5. 常见类名后缀

| 后缀 | 常见职责 | Flink 示例 |
|---|---|---|
| `Config` | 运行参数配置对象 | `CheckpointConfig` |
| `Configuration` | 通用键值配置容器 | `Configuration` |
| `Options` | 一组配置项定义 | `PipelineOptions` |
| `Strategy` | 可替换的规则或算法组合 | `WatermarkStrategy` |
| `Generator` | 生成数据、对象或控制信号 | `WatermarkGenerator` |
| `Assigner` | 为数据分配属性或归属 | `TimestampAssigner` |
| `Supplier` | 提供或创建对象 | `WatermarkGeneratorSupplier` |
| `Factory` | 创建某类对象的工厂 | `StateBackendFactory` |
| `Builder` | 分步骤收集配置并构造对象 | `KafkaSourceBuilder` |
| `Function` | 用户自定义处理逻辑 | `MapFunction` |
| `ProcessFunction` | 可访问上下文能力的处理函数 | `KeyedProcessFunction` |
| `Operator` | 运行时执行算子 | `WindowOperator` |
| `Transformation` | 作业拓扑中的转换节点 | `OneInputTransformation` |
| `Source` | 数据输入组件 | `KafkaSource` |
| `Sink` | 数据输出组件 | `KafkaSink` |
| `Schema` | 数据格式或结构转换定义 | `DeserializationSchema` |
| `Serializer` | 将对象序列化 | `TypeSerializer` |
| `Deserializer` | 将字节反序列化为对象 | `KafkaRecordDeserializationSchema` |
| `State` | 状态对象 | `ValueState` |
| `Descriptor` | 状态或表等对象的声明信息 | `ValueStateDescriptor` |
| `Backend` | 某项能力的底层实现 | `HashMapStateBackend` |
| `Storage` | 数据或快照存储方式 | `CheckpointStorage` |
| `Context` | 当前函数或算子的运行上下文 | `RuntimeContext` |
| `Manager` | 管理一组对象或生命周期 | `WatermarkManager` |
| `Coordinator` | 协调多个任务或组件 | `CheckpointCoordinator` |
| `Trigger` | 判断何时触发窗口 | `EventTimeTrigger` |
| `Evictor` | 清除窗口中的部分元素 | `CountEvictor` |
| `Window` | 窗口范围对象 | `TimeWindow` |
| `Enumerator` | 发现并分配 Source Split | `SplitEnumerator` |
| `Reader` | 读取数据或 Split | `SourceReader` |
| `Writer` | 写出数据 | `SinkWriter` |
| `Committer` | 提交待确认的写出结果 | `Committer` |
| `Result` | 某个操作的结果对象 | `JobExecutionResult` |
| `Exception` | 异常类型 | `CheckpointException` |

## 6. 常见组合词拆解

| 名称 | 拆解 | 大致含义 |
|---|---|---|
| `WatermarkStrategy` | Watermark + Strategy | Watermark 策略 |
| `WatermarkGenerator` | Watermark + Generator | Watermark 生成器 |
| `TimestampAssigner` | Timestamp + Assigner | 时间戳分配器 |
| `WatermarkGeneratorSupplier` | Watermark + Generator + Supplier | Watermark 生成器提供者 |
| `CheckpointStorage` | Checkpoint + Storage | 检查点存储方式 |
| `StateBackend` | State + Backend | 状态后端 |
| `SourceReader` | Source + Reader | 数据源读取器 |
| `SplitEnumerator` | Split + Enumerator | 数据分片发现与分配组件 |
| `SerializationSchema` | Serialization + Schema | 序列化规则 |
| `CheckpointCoordinator` | Checkpoint + Coordinator | 检查点协调组件 |

## 7. 快速判断口诀

```text
set 是设置，get 是获取，is 是判断；
enable 是开启，disable 是关闭；
with 是追加配置，for 是创建适用策略；
create 是创建实际对象，assign 是分配属性；
register 是注册资源，process 是处理数据；
on 是事件回调，最后一个单词通常决定类的职责。
```

命名只能用于初步判断，准确用法仍以方法签名、返回类型和 `@Deprecated` 等注解为准。

