# Flink WatermarkStrategy 创建与使用

> 适用版本：Flink 1.17。本文讨论经典 DataStream API 的事件时间 Watermark。

## 1. 核心定义

`WatermarkStrategy<T>` 同时描述两件事：通过 `TimestampAssigner` 为每条记录分配事件时间；通过 `WatermarkGenerator` 根据已观察到的事件时间生成 Watermark。Watermark 是事件时间进度声明，不负责排序数据，也不等同于让每条数据固定等待一段时间。

```java
WatermarkStrategy<Event> strategy =
        WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(
                        (event, recordTimestamp) -> event.getTs()
                )
                .withIdleness(Duration.ofSeconds(30));

DataStream<Event> eventTimeStream =
        stream.assignTimestampsAndWatermarks(strategy);
```

方法链可以按三类理解：

```text
WatermarkStrategy
├── forXxx()/noWatermarks()：创建基础策略
├── withXxx()：在基础策略上追加配置
└── createXxx()：Flink 运行时创建实际组件，业务代码通常不直接调用
```

## 2. 创建基础策略

### 2.1 forBoundedOutOfOrderness

适合事件时间可能乱序，但乱序程度存在上限的数据流：

```java
WatermarkStrategy.<Event>forBoundedOutOfOrderness(
        Duration.ofSeconds(5)
);
```

周期发射时的核心计算为：

```text
Watermark = 当前观察到的最大事件时间 - 最大乱序时间 - 1ms
```

例如最大事件时间是 `10:00:10.000`，最大乱序时间是 5 秒，则 Watermark 约为 `10:00:04.999`。后续时间戳不大于当前 Watermark 的记录属于迟到数据，但是否被丢弃还取决于下游窗口和迟到数据配置。

### 2.2 forBoundedOutOfOrderness(Duration.ZERO)

```java
WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO);
```

表示最大乱序容忍度为零：

```text
Watermark = 当前最大事件时间 - 1ms
```

它只适合事件时间有序或基本有序的数据。`Duration.ZERO` 不代表每条事件到达后立刻发送 Watermark；内置生成器仍然按照自动发射周期调用 `onPeriodicEmit()`。

### 2.3 forMonotonousTimestamps

```java
WatermarkStrategy.<Event>forMonotonousTimestamps();
```

用于时间戳严格单调递增的流，例如 `1000 → 2000 → 3000`。它与零乱序策略的运行效果接近，但表达的业务假设更明确。如果可能出现 `1000 → 3000 → 2000`，应使用 `forBoundedOutOfOrderness()` 并设置合理的乱序范围。

### 2.4 forGenerator

```java
WatermarkStrategy.<Event>forGenerator(
        context -> new CustomWatermarkGenerator(5_000L)
);
```

用于注册自定义 `WatermarkGenerator`。只有内置的单调递增和有界乱序策略不能描述业务时间进度时，才需要自定义。

### 2.5 noWatermarks

```java
WatermarkStrategy.<Event>noWatermarks();
```

不生成渐进式 Watermark，适合纯处理时间任务。在无界流中，如果事件时间窗口依赖 Watermark 触发，使用该策略通常会导致窗口长期不输出。

## 3. 追加策略配置

### 3.1 withTimestampAssigner

```java
.withTimestampAssigner(
        (event, recordTimestamp) -> event.getTs()
)
```

它指定事件时间从哪里提取：`event` 是当前业务记录；`recordTimestamp` 是上游 `StreamRecord` 已有的时间戳；返回值是当前记录的事件时间，单位必须为毫秒。

完整类写法：

```java
public class EventTimestampAssigner
        implements SerializableTimestampAssigner<Event> {

    @Override
    public long extractTimestamp(Event event, long recordTimestamp) {
        return event.getTs();
    }
}
```

如果不调用 `withTimestampAssigner()`，默认分配器会使用记录已有的时间戳。这适合直接使用 Kafka Record 时间戳等情况；如果业务对象拥有独立的事件时间字段，应明确配置提取逻辑。

### 3.2 withIdleness

```java
.withIdleness(Duration.ofSeconds(30))
```

下游算子的 Watermark 通常取所有活跃输入通道 Watermark 的最小值。某个分区长时间没有数据时，它的 Watermark 无法前进，可能阻塞整个算子。超过 idle timeout 后，该输入被标记为 `IDLE`，暂时退出最小值计算；重新收到数据后恢复为 `ACTIVE`。

idle timeout 应大于正常数据间隔，过短会频繁切换状态，过长则不能及时解除阻塞。

### 3.3 withWatermarkAlignment

```java
.withWatermarkAlignment(
        "order-source-group",
        Duration.ofSeconds(20),
        Duration.ofSeconds(1)
)
```

三个参数依次是对齐组、允许的最大 Watermark 偏差、对齐信息更新周期。它用于限制过快 Source，防止下游 Join 或窗口因不同输入进度差距过大而缓存过多数据。

Flink 1.17 中，Watermark Alignment 应配置在支持 FLIP-27 的新 Source 上：

```java
env.fromSource(source, strategy, "source-name");
```

在普通流后调用 `assignTimestampsAndWatermarks(strategy)` 时，Watermark Alignment 不生效。

## 4. Flink 运行时方法

### 4.1 createTimestampAssigner

```java
TimestampAssigner<T> createTimestampAssigner(
        TimestampAssignerSupplier.Context context
);
```

它是创建实际 `TimestampAssigner` 的工厂方法。业务代码通常配置 `withTimestampAssigner()`，随后由 Flink 在算子启动时调用 `createTimestampAssigner(context)`；每条数据到达后，再调用返回对象的 `extractTimestamp()`。

### 4.2 createWatermarkGenerator

```java
WatermarkGenerator<T> createWatermarkGenerator(
        WatermarkGeneratorSupplier.Context context
);
```

它在算子启动时创建实际 `WatermarkGenerator`。随后每条数据调用 `onEvent()`，自动发射周期到达时调用 `onPeriodicEmit()`。

### 4.3 getAlignmentParameters

```java
strategy.getAlignmentParameters();
```

用于取得 Watermark Alignment 配置，主要由框架内部读取，普通业务代码很少直接使用。

运行时调用链：

```text
WatermarkStrategy
    ↓ 算子启动
createTimestampAssigner(context)
createWatermarkGenerator(context)
    ↓ 每条记录
extractTimestamp(event, previousTimestamp)
onEvent(event, eventTimestamp, output)
    ↓ 周期到达
onPeriodicEmit(output)
    ↓
Watermark 向下游传播
```

## 5. 自定义 WatermarkGenerator

```java
public class CustomWatermarkGenerator
        implements WatermarkGenerator<Event> {

    private final long outOfOrdernessMillis;
    private long maxTimestamp;

    public CustomWatermarkGenerator(long outOfOrdernessMillis) {
        this.outOfOrdernessMillis = outOfOrdernessMillis;
        this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
    }

    @Override
    public void onEvent(
            Event event,
            long eventTimestamp,
            WatermarkOutput output) {
        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(
                new Watermark(maxTimestamp - outOfOrdernessMillis - 1)
        );
    }
}
```

`onEvent()` 每条记录调用一次，通常只更新当前最大事件时间；`onPeriodicEmit()` 按配置周期调用，负责真正创建并发射 Watermark。没有特殊业务规则时，应优先使用 Flink 内置实现。

## 6. 发射周期

```java
env.getConfig().setAutoWatermarkInterval(1_000L);
```

该配置表示大约每 1 秒调用一次周期型生成器的 `onPeriodicEmit()`，不等同于允许乱序 1 秒：

```text
autoWatermarkInterval：多久尝试发射一次 Watermark
maxOutOfOrderness：Watermark 落后最大事件时间多少
```

## 7. 在事件时间窗口中使用

```java
WatermarkStrategy<Event> strategy =
        WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(
                        (event, recordTimestamp) -> event.getTs()
                )
                .withIdleness(Duration.ofSeconds(30));

DataStream<Event> eventTimeStream =
        stream.assignTimestampsAndWatermarks(strategy);

eventTimeStream
        .keyBy(Event::getUrl)
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .sum("count");
```

窗口 `[10:00:00, 10:00:10)` 的最大时间戳为 `10:00:09.999`。当算子的当前 Watermark 大于等于该时间戳时，窗口触发计算。Watermark 会先触发当前算子的时间逻辑和输出，再继续传递给下游。

## 8. 方法速查

| 方法 | 类别 | 用途 |
|---|---|---|
| `forMonotonousTimestamps()` | 创建策略 | 时间戳严格递增 |
| `forBoundedOutOfOrderness(Duration)` | 创建策略 | 有界乱序 |
| `forGenerator(...)` | 创建策略 | 自定义生成器 |
| `noWatermarks()` | 创建策略 | 不生成 Watermark |
| `withTimestampAssigner(...)` | 追加配置 | 提取事件时间 |
| `withIdleness(Duration)` | 追加配置 | 空闲输入检测 |
| `withWatermarkAlignment(...)` | 追加配置 | Source 进度对齐 |
| `createTimestampAssigner(context)` | 运行时方法 | 创建时间戳分配器 |
| `createWatermarkGenerator(context)` | 运行时方法 | 创建 Watermark 生成器 |
| `getAlignmentParameters()` | 运行时方法 | 读取对齐参数 |

## 9. 选择原则与常见误区

- 时间戳严格递增：使用 `forMonotonousTimestamps()`。
- 存在可估计乱序：使用 `forBoundedOutOfOrderness()`。
- 分区可能长时间无数据：追加 `withIdleness()`。
- Kafka 等新 Source：优先在 `env.fromSource()` 中传入策略，以保留 split/partition 级 Watermark 信息。
- `Duration.ZERO` 是零乱序容忍，不是零发射周期。
- Watermark 不负责排序，也不保证迟到数据绝对不会再出现。
- `withTimestampAssigner()` 是用户配置；`createTimestampAssigner()` 是运行时工厂调用，二者不能混为一谈。
- 无界流使用事件时间窗口时，不应使用 `noWatermarks()`。

推荐模板：

```java
env.getConfig().setAutoWatermarkInterval(1_000L);

WatermarkStrategy<Event> strategy =
        WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(
                        (event, previousTimestamp) -> event.getTs()
                )
                .withIdleness(Duration.ofSeconds(30));

DataStream<Event> eventTimeStream =
        stream.assignTimestampsAndWatermarks(strategy);
```

