# 水位线 Watermark

## 1. Watermark 的含义

Watermark 用来表示事件时间的推进进度。

可以这样理解：

```text
Watermark = T
表示 Flink 认为正常情况下，不会再收到事件时间 <= T 的数据
```

如果后续又来了事件时间小于等于当前 Watermark 的数据，这类数据就可能被认为是迟到数据。

Watermark 主要用于事件时间语义，例如：

```text
事件时间窗口什么时候触发
事件时间定时器什么时候执行
迟到数据如何判断
```

## 2. Watermark 通常在哪里生成

Watermark 通常在 Source 附近生成。

常见写法：

```java
stream.assignTimestampsAndWatermarks(...)
```

或者在新 Source API 中：

```java
env.fromSource(source, watermarkStrategy, "source")
```

后续普通算子一般不会重新生成 Watermark，而是接收、合并、转发上游传来的 Watermark。

如果中间再次调用：

```java
.assignTimestampsAndWatermarks(...)
```

那么中间也可以重新生成 Watermark。但实际开发中不建议随便多次生成，除非明确知道为什么要重置事件时间和 Watermark 逻辑。

## 3. 乱序时间如何影响 Watermark

例如设置最大乱序时间为 2 秒：

```java
WatermarkStrategy
        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
```

它的核心含义是：

```text
允许数据最多乱序 2 秒
```

源码中的核心计算逻辑在 `BoundedOutOfOrdernessWatermarks`：

```java
public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
    maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
}

public void onPeriodicEmit(WatermarkOutput output) {
    Watermark watermark = new Watermark(maxTimestamp - outOfOrdernessMillis - 1);
    output.emitWatermark(watermark);
}
```

所以严格来说：

```text
Watermark = 当前 subtask 见过的最大事件时间 - 最大乱序时间 - 1ms
```

课程或日常讨论中经常简化为：

```text
Watermark ≈ 最大事件时间 - 最大乱序时间
```

例如某个 Source subtask 当前见过的最大事件时间是 82 秒，最大乱序时间是 2 秒：

```text
Watermark ≈ 82 - 2 = 80 秒
```

## 4. Watermark 和并行实例的关系

Watermark 不是整个算子只有一个。

更准确的说法是：

```text
每个算子的每个并行实例 subtask，都有自己的 Watermark
```

例如 `map` 算子并行度为 3：

```text
map-0 有自己的 Watermark
map-1 有自己的 Watermark
map-2 有自己的 Watermark
```

所以一般不要简单说：

```text
整个 map 算子有一个唯一 Watermark
```

更严谨的说法是：

```text
map 算子的某个 subtask 当前 Watermark 是多少
```

## 5. 下游如何合并多个 Watermark

如果某个下游 subtask 有多个输入通道，它的当前 Watermark 取这些输入通道 Watermark 的最小值。

例如：

```text
channel-0 Watermark = 100
channel-1 Watermark = 80
channel-2 Watermark = 60
```

那么当前下游 subtask 的 Watermark 是：

```text
min(100, 80, 60) = 60
```

原因是 Flink 必须等待最慢的输入通道。如果直接把 Watermark 推进到 80，就可能导致另一路后续到来的事件时间 70 的数据被错误判断为迟到数据。

需要注意：

```text
新来一条事件时间为 82 的数据，不代表下游 Watermark 立即变成 80
```

`82 - 2 ≈ 80` 是这条数据所在的上游 subtask 生成 Watermark 的逻辑。

下游 subtask 仍然要看所有输入通道的最小 Watermark。

## 6. keyBy 和 Watermark

`keyBy` 会按照 key 重新分区数据，但不会给每个 key 单独维护 Watermark。

也就是说：

```text
Watermark 不是每个 key 一个
```

而是：

```text
每个 subtask / 输入通道维护 Watermark
```

如果多个 key 被分到同一个 subtask，它们共享这个 subtask 的 Watermark。

## 7. Window 和 Watermark

事件时间窗口是否触发，取决于当前 subtask 的 Watermark 是否到达窗口结束时间。

例如窗口范围：

```text
[0, 5)
```

当 Watermark 推进到窗口结束时间附近时，窗口才会触发计算。

如果 Watermark 一直卡住，事件时间窗口也会迟迟不触发。

常见窗口写法：

```java
stream
        .keyBy(Event::getUserId)
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .sum("count");
```

普通 `.window(...)` 通常在 `keyBy(...)` 之后使用。

如果不按 key 开窗，可以使用：

```java
stream.windowAll(...)
```

但 `windowAll` 是全局窗口，通常并行度为 1，容易成为性能瓶颈。

## 8. 空闲分区与 Watermark 卡住

如果某个 Source partition 或输入通道长期没有数据，它的 Watermark 可能不再推进。

由于下游会取所有输入通道 Watermark 的最小值，这个空闲通道可能拖住整体事件时间进度，导致窗口不触发。

解决方式是设置空闲检测：

```java
WatermarkStrategy
        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
        .withIdleness(Duration.ofSeconds(30))
```

含义是：

```text
某一路超过 30 秒没有数据，就标记为空闲
下游计算 Watermark 时暂时不等待这一路
```

## 9. 源码位置

最大乱序 Watermark 生成逻辑：

```text
org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks
```

核心方法：

```text
onEvent(...)
onPeriodicEmit(...)
```

多个输入通道取最小 Watermark 的逻辑：

```text
org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve
```

核心方法：

```text
inputWatermark(...)
findAndOutputNewMinWatermarkAcrossAlignedChannels(...)
```

源码中不是直接写 `Math.min(...)`，而是用优先队列维护最小值：

```java
alignedChannelStatuses.peek().watermark
```

因为队列按照 Watermark 从小到大排序，所以 `peek()` 取到的就是当前 active 输入通道中的最小 Watermark。

## 10. 小结

Watermark 的整体链路可以概括为：

```text
Source subtask 根据最大事件时间和乱序时间生成 Watermark
普通算子接收、合并、转发 Watermark
下游 subtask 对多个输入通道取最小 Watermark
Window 根据当前 subtask 的 Watermark 判断是否触发
空闲分区需要通过 withIdleness 避免拖住事件时间进度
```
