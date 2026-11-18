---
title: "Doris 高阶篇07：Metrics 体系与可观测性源码解析"
author: "外星人的知识杂谈"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484404&idx=1&sn=9b038657388c3fc89394ecadae047012&chksm=c1bf09e9f6c880ff0f92a473c7863776bebb6dd4c1ab3aac8d18c3127d2b20ce97e831c5184f#wechat_redirect"
published_at: "2026-09-19T07:12:45+08:00"
annotated_at: "2026-11-18"
cssclasses:
  - wechat-article
  - doris-intro-annotated
tags:
  - doris
  - 文章批注
---

# Doris 高阶篇07：Metrics 体系与可观测性源码解析

<div class="article-meta">外星人的知识杂谈 · 2026-09-19</div>

[原文链接](https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484404&idx=1&sn=9b038657388c3fc89394ecadae047012&chksm=c1bf09e9f6c880ff0f92a473c7863776bebb6dd4c1ab3aac8d18c3127d2b20ce97e831c5184f#wechat_redirect)

<!-- 正文按原文顺序保留；批注和核对说明为新增。装饰封面和独立的篇末预告已去除。 -->

## 定位与阅读前提

本文解析 Doris 的 Metrics 和 Query Profile 两大可观测性子系统的源码实现，涵盖 BE 端指标框架、FE 端指标体系、Prometheus 导出机制，以及 Query Profile 的采集与聚合流程。

**阅读前提：**

- 了解 Prometheus 指标模型（Counter/Gauge/Histogram）
- 熟悉 Doris FE/BE 架构分工
- 了解 Pipeline 执行引擎基本概念（参见本系列第 04 篇）

---

## 背景动机

分布式 OLAP 系统的运维和性能调优高度依赖可观测性：

1. **系统健康监控：**

   需要实时掌握 CPU/内存/磁盘/网络使用率
2. **查询性能诊断：**

   需要逐算子的执行时间、处理行数等细粒度 Profile
3. **Compaction 状态：**

   需要监控 compaction score、任务队列深度
4. **容量规划：**

   需要趋势数据支撑扩缩容决策

Doris 通过两个互补的子系统实现可观测性：

- **Metrics 体系：**

  面向系统级监控，以 Counter/Gauge/Histogram 形式暴露，支持 Prometheus 抓取
- **Query Profile：**

  面向单次查询诊断，以树形 RuntimeProfile 记录每个算子的执行细节

> [!note] 批注｜Metrics 找时间和节点，Profile 找查询和算子
>
> Metrics 按采集周期记录进程或 Tablet 的状态，适合确认哪段时间、哪些节点出现异常。Query Profile 绑定一次查询，适合核对哪个算子处理了多少行、在哪里等待。两者可按查询时间和节点关联，但进程 Counter 的增量不能直接归给某条 SQL。

---

## BE 端 Metrics 框架

### 核心架构


```text
MetricRegistry (全局注册中心)
  └── MetricEntity (指标实体，如 server/tablet)
        └── Metric (具体指标实例)
              ├── AtomicCounter<T>  (只增计数器)
              ├── AtomicGauge<T>    (可升可降度量)
              └── HistogramMetric   (分布直方图)

```


### MetricRegistry


```text
源码路径: be/src/common/metrics/metrics.h (line 308+)

```


`MetricRegistry` 是全局指标注册中心，管理所有`MetricEntity`：


```cpp
class MetricRegistry {
public:
    MetricRegistry(std::string name);
    // 注册一个指标实体（server 或 tablet 级别）
    std::shared_ptr<MetricEntity> register_entity(
        const std::string& name, const Labels& labels = {},
        MetricEntityType type = MetricEntityType::kServer);
    void deregister_entity(const std::shared_ptr<MetricEntity>& entity);
    std::shared_ptr<MetricEntity> get_entity(const std::string& name, ...);
};

```


### MetricEntity


```text
源码路径: be/src/common/metrics/metrics.h (line 245+)

```


`MetricEntity` 是一组相关指标的容器，支持两种级别：

- `kServer`

  ：BE 进程全局指标
- `kTablet`

  ：Tablet 级指标

核心机制：

- `register_metric<T>()`

  ：注册具体指标（模板方法）
- `register_hook()`

  ：注册 hook 回调，在指标被读取前自动刷新值
- `trigger_hook_unlocked()`

  ：触发所有 hook 刷新指标

> [!note] 批注｜标签决定时间序列数量
>
> 同名指标每增加一组标签值，就增加一条时间序列。Tablet、用户等维度有定位价值，但不能默认把高基数标签全量长期采集；应先确认目标版本实际启用了哪些实体、样本数量和抓取耗时。

### Metric 类型


```text
源码路径: be/src/common/metrics/metrics.h

```


| 类型 | C++ 类 | 说明 |
| --- | --- | --- |
| Counter |`IntCounter`  /`UIntCounter` /`DoubleCounter` | 单调递增，如请求总数 |
| Gauge |`IntGauge`  /`UIntGauge` /`DoubleGauge` | 可增可减，如当前内存使用 |
| Histogram |`HistogramMetric` | 分布统计，如延迟分位数 |

类继承：


```cpp
AtomicMetric<T> : public Metric    // 基于 std::atomic 的通用指标
AtomicCounter<T> : public AtomicMetric<T>  // Counter 特化
AtomicGauge<T> : public AtomicMetric<T>    // Gauge 特化

```

> [!note] 批注｜用指标类型选择计算方式
>
> 对累计 Counter 用 `rate` 或 `increase` 计算指定窗口内的变化，节点重启时计数可能归零。Gauge 代表采样时刻的水平，不应套用 Counter 的计算。即使名字含 `_total`，也要按 `/metrics` 输出的 `# TYPE`、单位和源码定义判断。


### MetricPrototype（指标原型）


```cpp
struct MetricPrototype {
    bool is_core_metric;   // 是否为核心指标（影响导出过滤）
    MetricType type;       // COUNTER/GAUGE/HISTOGRAM
    MetricUnit unit;       // BYTES/ROWS/MILLISECONDS/...
    std::string name;      // 指标名
    std::string description;
    std::string group_name;
    Labels labels;
};

```


注册宏：


```cpp
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(name, unit)
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(name, unit)
DEFINE_HISTOGRAM_METRIC_PROTOTYPE_2ARG(name, unit)
INT_COUNTER_METRIC_REGISTER(entity, metric)
INT_GAUGE_METRIC_REGISTER(entity, metric)

```


### DorisMetrics 单例


```text
源码路径: be/src/common/metrics/doris_metrics.h
源码路径: be/src/common/metrics/doris_metrics.cpp

```


`DorisMetrics` 是 BE 端所有业务指标的集中管理器（单例模式）：


```cpp
class DorisMetrics {
public:
    static DorisMetrics* instance();
    MetricEntity* server_entity() { return _server_metric_entity.get(); }

    // 查询相关
    IntCounter* fragment_requests_total;
    IntCounter* query_scan_bytes;
    IntCounter* query_scan_rows;

    // Compaction 相关
    IntCounter* base_compaction_request_total;
    IntCounter* cumulative_compaction_request_total;
    IntGauge* tablet_cumulative_max_compaction_score;
    IntGauge* tablet_base_max_compaction_score;
    IntGauge* compaction_used_permits;

    // IO 相关
    IntCounter* local_bytes_read_total;
    IntCounter* s3_bytes_read_total;

    // 内存相关
    IntGauge* memory_pool_bytes_total;
    IntGauge* process_thread_num;

    // Segment 相关
    IntCounter* segment_read_total;
    IntCounter* segment_row_total;
};

```


### Hook 机制

对于需要实时计算的指标（如进程线程数），通过 Hook 回调在采集时动态更新：


```cpp
REGISTER_HOOK_METRIC(metric, func)
// 展开为：
// DorisMetrics::instance()->metric = entity->register_metric<UIntGauge>(&METRIC_metric);
// entity->register_hook("metric", [&]() { metric->set_value(func()); });

```


---

## FE 端 Metrics 框架

### MetricRepo


```text
源码路径: fe/fe-core/src/main/java/org/apache/doris/metric/MetricRepo.java

```


`MetricRepo` 是 FE 端指标的全局仓库（static final），包含：

- `DORIS_METRIC_REGISTER`

  ：DorisMetricRegistry 实例
- `METRIC_REGISTER`

  ：Codahale MetricRegistry（用于 Histogram）
- `SYSTEM_METRICS`

  ：系统级指标

核心指标：


```java
public static LongCounterMetric COUNTER_REQUEST_ALL;
public static LongCounterMetric COUNTER_QUERY_ALL;
public static LongCounterMetric COUNTER_QUERY_ERR;
public static LongCounterMetric COUNTER_QUERY_SLOW;
public static Histogram HISTO_QUERY_LATENCY;

```


### FE Metric 类型


```text
源码路径: fe/fe-core/src/main/java/org/apache/doris/metric/

```


| 类 | 说明 |
| --- | --- |
|`Metric` | 抽象基类 |
|`CounterMetric`  /`LongCounterMetric` | Counter 类型 |
|`GaugeMetric`  /`GaugeMetricImpl` | Gauge 类型 |
|`HistogramMetric` | Histogram（基于 Codahale） |
|`AutoMappedMetric<T>` | 按 label 自动映射的指标（如按用户统计） |

### PrometheusMetricVisitor


```text
源码路径: fe/fe-core/src/main/java/org/apache/doris/metric/PrometheusMetricVisitor.java

```


FE 通过 Visitor 模式将内部 Metric 格式化为 Prometheus 文本格式：


```text
# HELP doris_fe_query_total doris_fe_query_total
# TYPE doris_fe_query_total counter
doris_fe_query_total 12345

```


导出内容包括：

- JVM 指标（heap/non-heap/GC/线程）
- 查询指标（QPS/错误数/延迟分布）
- 导入指标（routine load/stream load）
- 元数据指标（tablet 数/compaction score）
- 事务指标（txn 数量/状态）

> [!warning] 核对说明｜内部 Histogram 不一定导出为直方图
>
> [Doris 4.1 的 FE 导出代码](https://github.com/apache/doris/blob/branch-4.1/fe/fe-core/src/main/java/org/apache/doris/metric/PrometheusMetricVisitor.java)将这里的延迟 Histogram 以 Prometheus `summary` 输出，带有 `quantile` 标签。各 FE 的 P99 不能相加或平均得到全局 P99。只有实际导出了可合并的直方图桶，才可按 [Prometheus 的直方图方法](https://prometheus.io/docs/practices/histograms/)聚合后再算分位数。

---

## Prometheus 导出端点

### BE 端


```text
源码路径: be/src/service/http/action/metrics_action.h
源码路径: be/src/service/http/action/metrics_action.cpp

```


BE 通过 HTTP handler`MetricsAction` 暴露`/metrics` 端点：


```http
GET http://<be_host>:<be_http_port>/metrics

```


`MetricsAction::handle()` 调用`MetricRegistry` 遍历所有注册的 Entity 和 Metric，输出 Prometheus 格式文本。

支持参数：

- `?type=core`

  ：仅导出核心指标（减少采集压力）
- `?type=json`

  ：JSON 格式输出

### FE 端

FE 通过`/metrics` HTTP 端点暴露，由`PrometheusMetricVisitor` 格式化输出。

---

## 关键指标分类

### 查询指标（BE）

| 指标名 | 类型 | 说明 |
| --- | --- | --- |
|`fragment_requests_total` | Counter | Fragment 执行总请求数 |
|`query_scan_bytes` | Counter | 查询扫描字节数 |
|`query_scan_rows` | Counter | 查询扫描行数 |
|`query_scan_bytes_per_second` | Gauge | 查询扫描吞吐率 |

> [!note] 批注｜扫描字节要区分计数来源
>
> `query_scan_bytes` 是进程侧累计量，`query_scan_bytes_per_second` 是导出的吞吐 Gauge。不要用 Gauge 再求 `rate`，也不要把两者相加。要判断单次查询读了多少，要查看对应算子的 Profile 指标定义，并区分本地、远端、缓存和解压后的逻辑数据口径。

### Compaction 指标（BE）

| 指标名 | 类型 | 说明 |
| --- | --- | --- |
|`base_compaction_request_total` | Counter | Base Compaction 请求总数 |
|`cumulative_compaction_request_total` | Counter | Cumulative Compaction 请求总数 |
|`tablet_cumulative_max_compaction_score` | Gauge | 最大 Cumulative Score |
|`tablet_base_max_compaction_score` | Gauge | 最大 Base Score |
|`compaction_used_permits` | Gauge | 当前已使用的 Compaction Permit |
|`all_rowsets_num` | Gauge | 全局 Rowset 总数 |
|`all_segments_num` | Gauge | 全局 Segment 总数 |

> [!note] 批注｜最大 Score 可提示热点
>
> 最大 Score 持续增长说明至少有 Tablet 的合并压力累积，但不能只用这个 Gauge 判断原因。结合 Rowset 数、Compaction 成功或失败计数及磁盘读写延迟判断；具体 Tablet 应通过当前版本支持的诊断入口定位。导出前缀、分组名和标签以实际端点为准。

### IO/存储指标（BE）

| 指标名 | 类型 | 说明 |
| --- | --- | --- |
|`local_bytes_read_total` | Counter | 本地读取字节数 |
|`s3_bytes_read_total` | Counter | S3 读取字节数 |
|`segment_read_total` | Counter | Segment 读取总次数 |
|`segment_row_total` | Counter | Segment 原始行数 |

### 内存/进程指标（BE）

| 指标名 | 类型 | 说明 |
| --- | --- | --- |
|`memory_pool_bytes_total` | Gauge | 内存池总字节数 |
|`process_thread_num` | Gauge | 进程线程数 |
|`process_fd_num_used` | Gauge | 已用文件描述符数 |

---

## Query Profile 系统

### 概述

Query Profile 是 Doris 提供的单查询级别性能诊断工具，以树形结构记录查询执行过程中每个算子的时间、行数、内存等信息。

### BE 端 RuntimeProfile


```text
源码路径: be/src/runtime/runtime_profile.h
源码路径: be/src/runtime/runtime_profile.cpp

```


`RuntimeProfile` 是 Profile 树的基本节点，每个 Pipeline Operator 都持有一个`RuntimeProfile` 实例：


```cpp
class RuntimeProfile {
public:
    // 添加计数器
    Counter* add_counter(const std::string& name, TUnit::type type);
    Counter* add_counter_with_level(const std::string& name, TUnit::type type, int level);

    // 子 Profile（树形嵌套）
    void add_child(RuntimeProfile* child, ...);

    // 序列化为 Thrift
    void to_thrift(TRuntimeProfileTree* tree);
};

```


常用宏简化 Profile 收集：


```cpp
ADD_COUNTER(profile, name, type)    // 添加计数器
ADD_TIMER(profile, name)            // 添加时间计数器
SCOPED_TIMER(counter)               // 作用域内自动计时
ADD_CHILD_COUNTER(profile, name, type, parent)  // 子计数器

```


### Profile 树结构


```text
Query Profile
├── Summary (FE 端汇总)
│   ├── Query ID / Query Type / Start Time / End Time
│   ├── Total Time / Query State
│   └── 各阶段耗时 (Parse/Analyze/Rewrite/Optimize/...)
└── Execution Profile (BE 端详细)
    ├── Fragment 0
    │   ├── Pipeline 0
    │   │   ├── EXCHANGE_OPERATOR (id=1)
    │   │   │   ├── RowsReturned: 10000
    │   │   │   ├── ExecTime: 5.2ms
    │   │   │   └── WaitForDependencyTime: 1.1ms
    │   │   └── AGGREGATION_OPERATOR (id=2)
    │   │       ├── RowsReturned: 100
    │   │       ├── HashTableSize: 256KB
    │   │       └── ExecTime: 12.5ms
    │   └── Pipeline 1
    │       └── ...
    └── Fragment 1
        └── ...

```

> [!note] 批注｜并行算子耗时不能相加当作总时长
>
> Fragment、Pipeline 和实例可能并行，还会有依赖等待和队列时间。算子时间的求和可能超过 SQL 的墙钟耗时，树中两个计时项也可能包含重叠范围。先从最慢实例沿依赖链查找阻塞点，再分辨是执行慢、等输入，还是等下游消费。


### FE 端 Profile 管理


```text
源码路径: fe/fe-core/src/main/java/org/apache/doris/common/profile/ExecutionProfile.java
源码路径: fe/fe-core/src/main/java/org/apache/doris/common/profile/ProfileManager.java
源码路径: fe/fe-core/src/main/java/org/apache/doris/common/profile/SummaryProfile.java

```


**ExecutionProfile：** 收集一次完整查询的执行 Profile，按 Fragment → Pipeline → Operator 组织。


```java
public class ExecutionProfile {
    private final TUniqueId queryId;
    private RuntimeProfile root;
    // 按 Fragment ID 组织的子 Profile
    private Map<Integer, RuntimeProfile> fragmentProfiles;
}

```


**ProfileManager：** 全局 Profile 管理器（MasterDaemon），负责：

1. 收集各 BE 上报的 RuntimeProfile
2. 合并同一查询不同 Instance 的 Profile
3. 持久化到磁盘（`ProfileArchiveManager`）
4. 提供 HTTP/SQL 查询接口

**SummaryProfile：** 记录 FE 端各阶段耗时（Parse/Analyze/Plan/Schedule 等）。

### Profile 采集流程


```text
1. BE 执行 Fragment
   │ 各 Operator 通过 RuntimeProfile 记录指标
   │

2. Fragment 执行完成
   │ BE 序列化 RuntimeProfile 为 TRuntimeProfileTree
   │

3. BE → FE (RPC 上报)
   │ 通过 TDetailedReportParams 上报
   │

4. FE ExecutionProfile
   │ 反序列化并合并各 Instance 的 Profile
   │

5. ProfileManager 存储
   │ 内存 + 磁盘归档
   │

6. 用户查询
   └── SHOW QUERY PROFILE / HTTP API / Web UI

```

> [!note] 批注｜聚合视图和实例明细各有用处
>
> [Doris 4.x Query Profile 指南](https://doris.apache.org/docs/4.x/query-acceleration/query-profile/)区分 MergedProfile 和 DetailProfile。汇总有助于定位慢的算子阶段，判断数据倾斜则要回到各实例，比较相同算子 ID 的输入行数、内存和耗时。原图说明了上报路径，具体可见字段受 Profile 配置和版本影响。


---

## 自定义 Metric 扩展模式

### BE 端添加新指标

1. 在`doris_metrics.h` 中声明指标指针：


```cpp
IntCounter* my_new_metric = nullptr;

```


2. 在`doris_metrics.cpp` 中定义 Prototype 并注册：


```cpp
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(my_new_metric, MetricUnit::ROWS);
// 在 initialize() 中：
INT_COUNTER_METRIC_REGISTER(_server_metric_entity, my_new_metric);

```


3. 在业务代码中使用：


```cpp
DorisMetrics::instance()->my_new_metric->increment(delta);

```


### FE 端添加新指标

1. 在`MetricRepo` 中声明：


```java
public static LongCounterMetric COUNTER_MY_METRIC;

```


2. 在`init()` 中初始化并注册：


```java
COUNTER_MY_METRIC = new LongCounterMetric("my_metric", Metric.MetricUnit.NOUNIT, "desc");
DORIS_METRIC_REGISTER.addMetrics(COUNTER_MY_METRIC);

```


3. 在业务代码中使用：


```java
MetricRepo.COUNTER_MY_METRIC.increase(1L);

```


---

## 源码路径速查表

| 模块 | 路径 |
| --- | --- |
| BE Metrics 框架 |`be/src/common/metrics/metrics.h/.cpp` |
| BE DorisMetrics |`be/src/common/metrics/doris_metrics.h/.cpp` |
| BE System Metrics |`be/src/common/metrics/system_metrics.h/.cpp` |
| BE JVM Metrics |`be/src/common/metrics/jvm_metrics.h/.cpp` |
| BE Metrics HTTP |`be/src/service/http/action/metrics_action.h/.cpp` |
| BE RuntimeProfile |`be/src/runtime/runtime_profile.h/.cpp` |
| FE MetricRepo |`fe/.../metric/MetricRepo.java` |
| FE Metric 基类 |`fe/.../metric/Metric.java` |
| FE Counter/Gauge |`fe/.../metric/CounterMetric.java` ,`GaugeMetric.java` |
| FE Prometheus 导出 |`fe/.../metric/PrometheusMetricVisitor.java` |
| FE DorisMetricRegistry |`fe/.../metric/DorisMetricRegistry.java` |
| FE ExecutionProfile |`fe/.../common/profile/ExecutionProfile.java` |
| FE RuntimeProfile |`fe/.../common/profile/RuntimeProfile.java` |
| FE ProfileManager |`fe/.../common/profile/ProfileManager.java` |
| FE SummaryProfile |`fe/.../common/profile/SummaryProfile.java` |

---

## 核心数据结构/类索引

| 类名 | 位置 | 职责 |
| --- | --- | --- |
|`MetricRegistry` | BE metrics.h | 全局指标注册中心 |
|`MetricEntity` | BE metrics.h | 指标实体（server/tablet 级） |
|`Metric` | BE metrics.h | 指标抽象基类 |
|`AtomicCounter<T>` | BE metrics.h | 原子 Counter |
|`AtomicGauge<T>` | BE metrics.h | 原子 Gauge |
|`HistogramMetric` | BE metrics.h | 直方图指标 |
|`MetricPrototype` | BE metrics.h | 指标元数据原型 |
|`DorisMetrics` | BE doris\_metrics.h | 业务指标单例 |
|`MetricsAction` | BE metrics\_action.h | HTTP 导出 handler |
|`RuntimeProfile` | BE runtime\_profile.h | Profile 树节点 |
|`MetricRepo` | FE MetricRepo.java | FE 指标全局仓库 |
|`DorisMetricRegistry` | FE | FE 指标注册表 |
|`PrometheusMetricVisitor` | FE | Prometheus 格式化导出 |
|`ExecutionProfile` | FE | 单查询 Profile 收集 |
|`ProfileManager` | FE | Profile 全局管理器 |
|`SummaryProfile` | FE | FE 端阶段耗时汇总 |
|`RuntimeProfile`  (FE) | FE | FE 端 Profile 树节点 |

---

## 运维实践

### Prometheus 接入配置


```yaml
# prometheus.yml
scrape_configs:

  - job_name: 'doris_be'
    metrics_path: '/metrics'
    static_configs:

      - targets: ['be_host1:8040', 'be_host2:8040']

  - job_name: 'doris_fe'
    metrics_path: '/metrics'
    static_configs:

      - targets: ['fe_host:8030']

```


### 关键告警指标建议

| 场景 | 指标 | 阈值建议 |
| --- | --- | --- |
| Compaction 积压 |`tablet_cumulative_max_compaction_score` | > 1000 |
| 查询失败率 |`doris_fe_query_err / doris_fe_query_total` | > 1% |
| 内存使用 |`memory_pool_bytes_total` | > 80% 系统内存 |
| 文件描述符 |`process_fd_num_used / process_fd_num_limit_soft` | > 80% |
| Rowset 碎片化 |`all_rowsets_num / tablet_num` | > 50 per tablet |

> [!warning] 核对说明｜失败率按时间窗算，阈值按集群校准
>
> 失败率应在同一时间窗内比较失败数与总量，如用 `sum(rate(doris_fe_query_err[5m])) / sum(rate(doris_fe_query_total[5m]))`，并对低流量或分母为零单独处理。`memory_pool_bytes_total` 也不能直接当作 BE 进程全部内存。原表阈值可供试配，正式告警要核对业务基线、节点或容器限额及指标单位。

### Profile 查看方式


```sql
-- 开启 Profile 收集
SET enable_profile = true;

-- 执行查询后查看
SHOW QUERY PROFILE '/query_id';

-- 或通过 FE Web UI
-- http://fe_host:8030/QueryProfile

```

> [!note] 批注｜先用同一查询 ID 做对照
>
> 在当前会话启用 `enable_profile` 后执行待查 SQL，再拿该次查询的 ID 查明细。对照改写前后时，记录 SQL、输入分区、并发条件和 Query ID；同一 SQL 的不同运行可能有不同缓存与排队情况，不宜只按总耗时下结论。


---

## 小结

Doris 可观测性体系的设计要点：

1. **BE Metrics 框架：**

   基于 MetricRegistry → MetricEntity → Metric 三层结构，通过宏简化注册，Hook 机制支持惰性求值
2. **FE Metrics 框架：**

   使用 Visitor 模式支持多种输出格式（Prometheus/JSON），AutoMappedMetric 支持按标签自动分组
3. **Prometheus 兼容：**

   BE/FE 均通过`/metrics` 端点原生支持 Prometheus 抓取
4. **Query Profile：**

   从 BE Operator 级 RuntimeProfile 到 FE ExecutionProfile 的完整采集链路，支持 Fragment → Pipeline → Operator 多层次诊断
5. **扩展性：**

   通过标准宏（BE）和标准注册模式（FE）可快速添加新业务指标

---

## 系列总览

本篇是高阶篇的最后一篇，至此我们已完成了从存储引擎底层到查询优化器、执行引擎、Compaction 以及可观测性的全链路源码解析。回顾整个系列：

- 01-存储引擎源码解析
- 02-数据类型底层实现
- 03-向量化执行引擎源码
- 04-Pipeline 调度引擎源码
- 05-查询优化器源码解析
- 06-Compaction 与数据合并源码
- 07-Metrics 体系与可观测性 （本篇）
