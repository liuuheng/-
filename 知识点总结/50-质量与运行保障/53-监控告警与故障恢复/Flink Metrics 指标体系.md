# Flink Metrics 指标体系：概念、类型与 Sink 实践

> 版本说明：本文按 Flink 当前通用 Metrics API 整理；`open(...)` 等生命周期方法的签名可能随 Flink 版本变化，使用时应以项目实际依赖版本为准。

## 1. 核心定义

Flink Metrics 是作业的可观测性机制，用数字描述 JobManager、TaskManager、Job、Operator 和 Subtask 当前的运行状态，例如吞吐量、反压、延迟、失败次数、缓冲区大小、Checkpoint 耗时以及 JVM 资源使用情况。Metrics 不参与业务计算，也不负责故障恢复；它可能在任务重启后归零，也可能因失败重放而重复计数，因此不能用作业务账本或 Exactly-once 的证明。

```text
Flink 算子注册并更新指标
        ↓
MetricGroup / Flink Metric System
        ↓
Web UI、REST API 或 Metric Reporter
        ↓
Prometheus、Grafana 等监控系统
```

## 2. Metrics、State 与日志的区别

| 能力 | 解决的问题 | 是否参与业务正确性 | 是否随 Checkpoint 恢复 |
|---|---|---:|---:|
| Metrics | 作业当前运行得怎么样 | 否 | 通常否 |
| State | 作业恢复后如何继续正确计算 | 是 | 是 |
| 日志 | 某次事件具体发生了什么 | 否 | 否 |
| 外部业务数据 | 最终写入结果和业务事实 | 是 | 由外部系统保证 |

判断原则：需要恢复并影响计算结果的数据应放进 Flink State；需要观察趋势、设置告警的数据应做成 Metric；需要保存异常堆栈、输入上下文等事件细节时使用日志。

## 3. 四种指标类型

四类指标都通过当前算子的 `MetricGroup` 注册，但创建方式并不完全相同：`counter(name)` 可以由 Flink 直接创建 Counter；Gauge、Meter 和 Histogram 需要传入一个具体实现或取值函数。

```java
MetricGroup group = getRuntimeContext()
        .getMetricGroup()
        .addGroup("customSink");
```

| 类型 | 常用创建入口 | 核心含义 | 主要更新方式 |
|---|---|---|---|
| Counter | `group.counter(name)` | 累计发生多少次 | `inc/dec` |
| Gauge | `group.gauge(name, gauge)` | 采集时刻的当前值 | `getValue` 被采集端调用 |
| Meter | `group.meter(name, meter)` | 一段时间内的事件速率 | `markEvent` 或关联 Counter |
| Histogram | `group.histogram(name, histogram)` | 一组数值的统计分布 | `update(value)` |

### 3.1 Counter：累计数量

Counter 用于统计事件累计发生次数。最简单的创建方式是只提供指标名称，由 Flink 创建并注册 Counter：

```java
Counter flushedRecords = getRuntimeContext()
        .getMetricGroup()
        .counter("flushedRecords");
```

也可以创建或提供自己的 Counter 实现，再交给 MetricGroup 注册：

```java
Counter flushedRecords = getRuntimeContext()
        .getMetricGroup()
        .counter("flushedRecords", new SimpleCounter());
```

第一种适合绝大多数场景；第二种适合需要明确控制 Counter 实现的场景。`SimpleCounter` 开销较低但不是线程安全实现，通常应只在算子主线程更新；异步线程直接更新指标时还需要结合实际 Flink API 和线程模型确认安全性。

Counter 默认通常从 `0` 开始，具体操作含义如下：

```java
flushedRecords.inc();       // 当前值加 1
flushedRecords.inc(100);    // 当前值加 100
flushedRecords.dec();       // 当前值减 1
flushedRecords.dec(10);     // 当前值减 10
long count = flushedRecords.getCount(); // 读取当前累计值
```

例如两次批量写出分别成功 100 条和 50 条：

```java
flushedRecords.inc(100);
flushedRecords.inc(50);

// 当前值为 150
long count = flushedRecords.getCount();
```

`getCount()` 返回的是当前 Subtask 内这个 Counter 的累计值，不是整个作业所有并行 Subtask 的自动汇总值。适合记录 `flushedRecords`、`failedRecords`、`retryCount`、`invalidRecords` 等；不适合表示当前队列或缓冲区里还有多少数据。

### 3.2 Gauge：当前瞬时值

Gauge 表示采集时刻的当前值。创建 Gauge 时需要提供“如何取得当前值”的函数：

```java
Gauge<Integer> bufferGauge = () -> buffer.size();

getRuntimeContext()
        .getMetricGroup()
        .gauge("bufferSize", bufferGauge);
```

也可以直接写成 Lambda：

```java
getRuntimeContext()
        .getMetricGroup()
        .gauge("bufferSize", () -> buffer.size());
```

这段 Lambda 等价于显式实现 `Gauge#getValue()`：

```java
Gauge<Integer> bufferGauge = new Gauge<Integer>() {
    @Override
    public Integer getValue() {
        return buffer.size();
    }
};

getRuntimeContext()
        .getMetricGroup()
        .gauge("bufferSize", bufferGauge);
```

Gauge 不需要像 Counter 一样调用 `inc()`。当 Web UI、REST API 或 Reporter 采集指标时，Flink 调用 `getValue()`，此时 `buffer.size()` 是多少，采集值就是多少：

```text
buffer: 0 → 20 → 100 → flush → 0
Gauge : 0 → 20 → 100 →         0
```

它适合记录 `bufferSize`、`pendingRequests`、`activeConnections`、`queueSize`、`lastSuccessTimestamp`。Gauge 回调应只读取本地已有变量，不应访问数据库、发起网络请求或执行耗时计算；如果变量会被异步线程修改，还要考虑内存可见性，可用 `AtomicInteger` 等对象维护一个供 Gauge 读取的值：

```java
AtomicInteger currentBufferSize = new AtomicInteger();
group.gauge("bufferSize", currentBufferSize::get);

currentBufferSize.incrementAndGet(); // 添加一条数据后更新
currentBufferSize.set(0);            // Flush 并清空后更新
```

### 3.3 Meter：事件速率

Meter 用于衡量一段时间内的平均吞吐。创建时必须提供 Meter 实现，常见写法是使用 `MeterView`：

```java
Meter recordsPerSecond = getRuntimeContext()
        .getMetricGroup()
        .meter("recordsPerSecond", new MeterView(60));
```

其中 `60` 表示统计时间跨度为 60 秒。处理一条事件时调用：

```java
recordsPerSecond.markEvent();
```

批量成功处理 `batch.size()` 条时调用：

```java
recordsPerSecond.markEvent(batch.size());
```

Meter 主要提供两个结果：

```java
long total = recordsPerSecond.getCount(); // 已标记的事件累计数
double rate = recordsPerSecond.getRate(); // 时间窗口内的平均每秒速率
```

假设最近 60 秒标记了 600 条记录，则平均速率约为 `10 records/s`。短窗口对吞吐变化更敏感但波动更大；长窗口曲线更平滑，但发现突发变化更慢。

如果已经有一个 Counter，也可以让 MeterView 基于该 Counter 计算速率：

```java
Counter outputRecords = group.counter("outputRecords");
Meter outputRate = group.meter(
        "outputRate",
        new MeterView(outputRecords, 60)
);

// 成功处理一批数据时只更新关联的 Counter
outputRecords.inc(batch.size());
```

这种写法同时暴露累计数量和速率。不要对同一批数据既调用 `outputRecords.inc(n)`，又调用 `outputRate.markEvent(n)`，否则可能产生重复计数。`MeterView` 会由后台机制定期更新统计历史，速率不是逐条事件实时重算；另外部分版本将它标记为内部 API，使用前应按项目实际 Flink 版本确认兼容性。也可以使用 Dropwizard 等库提供的 Meter Wrapper。

### 3.4 Histogram：数值分布

Histogram 统计一组 `long` 数值的分布，典型场景是请求耗时、Flush 耗时、批次大小和记录大小。它不是“按名称直接创建”的内置容器，注册时需要传入 Histogram 实现。常见方式是使用 Dropwizard Histogram：

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-metrics-dropwizard</artifactId>
    <version>${flink.version}</version>
</dependency>
```

创建一个保存最近 500 个样本的滑动窗口，并注册为 Flink Histogram：

```java
com.codahale.metrics.Histogram dropwizardHistogram =
        new com.codahale.metrics.Histogram(
                new com.codahale.metrics.SlidingWindowReservoir(500)
        );

org.apache.flink.metrics.Histogram flushLatency =
        getRuntimeContext()
                .getMetricGroup()
                .histogram(
                        "flushLatency",
                        new org.apache.flink.dropwizard.metrics
                                .DropwizardHistogramWrapper(dropwizardHistogram)
                );
```

这里的 `500` 表示最多保留最近 500 个样本，不是 500 秒。每次 Flush 完成后，将本次耗时写入 Histogram：

```java
long startNanos = System.nanoTime();

client.write(buffer);

long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(
        System.nanoTime() - startNanos
);
flushLatency.update(elapsedMillis);
```

主要方法含义如下：

```java
flushLatency.update(120);       // 记录一个值，例如本次耗时 120 ms
long count = flushLatency.getCount(); // 已记录的样本数量
HistogramStatistics statistics = flushLatency.getStatistics();
```

`getStatistics()` 可提供最小值、最大值、平均值、标准差和分位数等统计结果，具体能被外部系统展示到什么程度取决于 Reporter。P50 表示 50% 的样本不超过该值，P95 表示 95% 的样本不超过该值，P99 更适合观察长尾慢请求。相比平均值，分位数不容易被少量极快或极慢样本掩盖。

### 3.5 四类指标的选择示例

```text
累计成功写出了多少条           → Counter
采集这一刻 Buffer 中有多少条   → Gauge
最近 60 秒平均每秒写出多少条   → Meter
每次 Flush 耗时如何分布         → Histogram
```

这四类指标描述的是不同维度，通常需要组合使用。例如 Counter 显示累计成功量持续增加，但 Meter 明显下降，说明作业还在运行但吞吐正在恶化；Gauge 显示 Buffer 持续增大，且 Histogram 的 P99 同时上升，则更可能是外部系统写入变慢。

## 4. RuntimeContext 与 MetricGroup

RichFunction 在运行阶段可通过 `getRuntimeContext()` 获取当前并行函数实例的运行上下文，再通过 `getMetricGroup()` 获取指标注册入口：

```java
MetricGroup group = getRuntimeContext()
        .getMetricGroup()
        .addGroup("customSink");

Counter flushedRecords = group.counter("flushedRecords");
```

`MetricGroup` 是指标的命名空间。完整指标标识通常由系统 Scope、用户 Scope 和指标名组成，可能包含 host、TaskManager、Job、Operator、Subtask 等信息：

```text
<host>.<taskmanager>.<job>.<operator>.<subtask>.customSink.flushedRecords
```

也可以添加键值分组：

```java
MetricGroup group = getRuntimeContext()
        .getMetricGroup()
        .addGroup("target", "mysql");
```

支持标签的 Reporter 可能将其导出为 `target="mysql"`。不要使用 `userId`、`orderId`、`traceId`、时间戳等高基数值作为指标名或标签，否则可能产生海量时间序列并拖垮监控系统。

## 5. FunctionInitializationContext 与 RuntimeContext

两者虽然都叫 Context，但职责不同：

| Context | 获取方式 | 主要职责 |
|---|---|---|
| `FunctionInitializationContext` | Flink 调用 `initializeState(...)` 时注入 | 注册或恢复 Operator State、Keyed State，判断是否恢复 |
| `RuntimeContext` | RichFunction 调用 `getRuntimeContext()` | 获取 MetricGroup、并行度、Subtask 信息及其他运行时能力 |

因此，`context.getOperatorStateStore()` 是状态入口；`getRuntimeContext().getMetricGroup()` 是监控指标入口。两者不能相互替代。

## 6. 并行度与指标作用域

指标通常属于一个具体并行 Subtask。假设 Sink 并行度为 3，则存在三份独立的 `flushedRecords`：

```text
subtask 0: 1000
subtask 1: 1200
subtask 2: 800
```

全局数量需要在外部监控系统中求和；分 Subtask 观察则可以发现热点或数据倾斜。例如两个 Subtask 每秒处理 100 条，另一个处理 5000 条，通常意味着分区不均、热点 Key 或下游分片负载不均。

## 7. 内置指标与自定义指标

Flink 已提供 JVM CPU、内存、GC、网络 Buffer、输入输出记录数、吞吐、忙碌/空闲/反压时间、Watermark、Checkpoint、状态后端和 Connector 等内置指标。排查问题时应先检查内置指标；只有 Flink 无法理解的业务或组件语义，例如“成功刷出”“脏数据”“外部接口重试”，才需要自定义指标。

常用观察顺序：先看吞吐是否下降，再看反压和忙碌时间定位瓶颈方向，然后检查 Checkpoint、资源和外部系统指标，最后结合自定义失败率、重试次数和耗时分布确认原因。

## 8. 自定义批量 Sink 的指标设计

| 指标 | 类型 | 准确定义 |
|---|---|---|
| `receivedRecords` | Counter | Sink 收到的记录数 |
| `attemptedRecords` | Counter | 尝试写入外部系统的记录数 |
| `acknowledgedRecords` | Counter | 客户端收到成功响应的记录数 |
| `failedRecords` | Counter | 已确认写入失败的记录数 |
| `retryCount` | Counter | 重试次数 |
| `bufferSize` | Gauge | 当前内存缓冲区大小 |
| `pendingRequests` | Gauge | 当前未完成异步请求数 |
| `recordsPerSecond` | Meter | 写出速率 |
| `flushLatency` | Histogram | Flush 耗时分布 |

注册指标通常放在 `open(...)` 中，因为构造函数执行时 RuntimeContext 尚未准备好；字段通常声明为 `transient`，避免随用户函数序列化。

```java
private transient Counter receivedRecords;
private transient Counter flushedRecords;
private transient Counter failedRecords;

@Override
public void open(Configuration parameters) {
    MetricGroup group = getRuntimeContext()
            .getMetricGroup()
            .addGroup("customSink");

    receivedRecords = group.counter("receivedRecords");
    flushedRecords = group.counter("flushedRecords");
    failedRecords = group.counter("failedRecords");
    group.gauge("bufferSize", () -> buffer.size());
}
```

处理与 Flush：

```java
@Override
public void invoke(String value, Context context) throws Exception {
    receivedRecords.inc();
    buffer.add(value);

    if (buffer.size() >= batchSize) {
        flush();
    }
}

private void flush() throws Exception {
    if (buffer.isEmpty()) {
        return;
    }

    int size = buffer.size();

    try {
        client.write(buffer);
        flushedRecords.inc(size); // 确认成功后计数
        buffer.clear();
    } catch (Exception e) {
        failedRecords.inc(size);
        throw e;
    }
}
```

计数位置决定指标语义：发送前递增表示“尝试写入”，成功响应后递增表示“客户端确认成功”。遇到请求超时时，外部系统可能已经成功写入，但客户端没有收到响应，因此任何单一 Metric 都不等同于严格的端到端结果。

## 9. Metrics 与 Checkpoint、Exactly-once

Metrics 通常不会进入 Checkpoint。任务可能在外部写入成功、Metric 已递增但 Checkpoint 尚未完成时失败；恢复后相同数据会重新处理，导致监控数据归零、跳变或重复计数。即使指标命名为 `committedRecords`，它仍主要用于观察，不应代替事务协议、幂等键、Two-Phase Commit 或业务对账。

如果要区分不同阶段，应使用明确名称：`attemptedRecords` 表示尝试量，`acknowledgedRecords` 表示收到成功响应，`committedRecords` 表示事务提交确认；告警和看板必须按照相同口径解释。

## 10. 查看与输出方式

- Flink Web UI：适合临时查看 Job、Operator、Subtask 的实时指标。
- REST API：适合脚本、运维平台或自动化诊断。
- Metric Reporter：将运行时指标暴露或发送给 Prometheus、JMX、StatsD 等系统。
- Grafana：负责时间趋势、聚合看板和告警展示。

Prometheus Reporter 配置示例：

```yaml
metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
metrics.reporter.prom.port: 9250-9260
```

Reporter 的依赖和配置项应与项目实际 Flink 版本匹配。由于 Flink Counter 支持递减，而 Prometheus Counter 不允许递减，Flink Counter 导出到 Prometheus 时通常映射为 Gauge。

## 11. 常见误区

1. **把 Metric 当业务状态**：Metric 不保证恢复和精确一次，业务去重与恢复应依靠 State、事务或外部幂等机制。
2. **在构造函数注册 Metric**：应在 `open(...)` 等运行时初始化阶段注册。
3. **每条数据创建一个 Metric**：指标应注册一次、持续更新，不能按订单或用户动态创建。
4. **Gauge 执行耗时操作**：Gauge 应返回本地变量，避免网络和数据库访问。
5. **只看累计失败数**：失败总数必须结合时间窗口、处理总量和失败率判断。
6. **只看全局聚合**：全局值可能掩盖单个 Subtask 的热点、阻塞和数据倾斜。
7. **只看平均耗时**：平均值可能掩盖长尾，应结合 P95、P99 等分位数。

## 12. 实际判断原则

```text
累计发生多少次        → Counter
当前值是多少          → Gauge
每秒发生多少次        → Meter
一组数值如何分布      → Histogram
失败后必须恢复的数据  → State，而不是 Metric
需要具体事件上下文    → 日志，而不是 Metric
```

对于自定义 Sink，最低限度建议监控成功量、失败量、重试量、当前 Buffer 大小和 Flush 耗时；生产环境还应结合内置反压、吞吐、Checkpoint 和 JVM 指标，不能只依赖自定义指标判断作业健康状态。

## 参考资料

- [Apache Flink：Metrics](https://nightlies.apache.org/flink/flink-docs-stable/zh/docs/ops/metrics/)
- [Apache Flink：Metric Reporters](https://nightlies.apache.org/flink/flink-docs-stable/zh/docs/deployment/metric_reporters/)
- [Apache Flink：MetricGroup API](https://nightlies.apache.org/flink/flink-docs-stable/api/java/org/apache/flink/metrics/MetricGroup.html)
- [Apache Flink：Counter API](https://nightlies.apache.org/flink/flink-docs-stable/api/java/org/apache/flink/metrics/Counter.html)
- [Apache Flink：MeterView API](https://nightlies.apache.org/flink/flink-docs-stable/api/java/org/apache/flink/metrics/MeterView.html)
- [Apache Flink：Histogram API](https://nightlies.apache.org/flink/flink-docs-stable/api/java/org/apache/flink/metrics/Histogram.html)
