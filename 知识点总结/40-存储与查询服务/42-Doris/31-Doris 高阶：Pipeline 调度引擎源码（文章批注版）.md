---
title: "Doris 高阶篇04：Pipeline 调度引擎源码"
author: "外星人的知识杂谈"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484401&idx=1&sn=10a54c7cbf5c67364b007e458604d277&chksm=c1bf09ecf6c880faff5f42034aae0dd400ca509bf1b82325c751241a8d28564418ab6f702bf4#wechat_redirect"
published_at: "2026-09-15T20:56:57+08:00"
annotated_at: "2026-11-18"
cssclasses:
  - wechat-article
  - doris-intro-annotated
tags:
  - doris
  - 文章批注
---

# Doris 高阶篇04：Pipeline 调度引擎源码

<div class="article-meta">外星人的知识杂谈 · 2026-09-15</div>

[原文链接](https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484401&idx=1&sn=10a54c7cbf5c67364b007e458604d277&chksm=c1bf09ecf6c880faff5f42034aae0dd400ca509bf1b82325c751241a8d28564418ab6f702bf4#wechat_redirect)

<!-- 正文按原文顺序保留；批注和核对说明为新增。装饰封面和独立的篇末预告已去除。 -->

## 定位与阅读前提

本文深入解析 Doris BE 端 Pipeline 执行框架的核心设计：PipelineTask 生命周期、TaskScheduler 调度实现、Operator 状态机、Dependency 机制以及反压控制。Pipeline 引擎是 Doris 2.0+ 的默认执行模式，理解它是分析查询性能问题的关键。

**阅读前提：**

- 理解传统 Volcano 模型（pull-based 逐行迭代）的局限性
- 了解协程/任务调度的基本概念
- 熟悉前一篇向量化执行引擎的 Block/Operator 概念
- 了解线程池、任务队列等并发编程模式

## 背景动机

传统 Volcano 模型中，每个查询片段（Fragment）独占一个线程，当查询并发量大时会导致：

- 线程数爆炸（context switch 开销大）
- I/O 阻塞时线程空等（CPU 利用率低）
- 无法精细控制资源分配

Pipeline 引擎的设计目标：

1. **解耦计算与线程：**

   任务可以被挂起和恢复
2. **统一调度：**

   共享线程池，支持优先级调度
3. **非阻塞：**

   通过 Dependency 机制避免线程阻塞

## 一、Pipeline 执行模型概述

### 1.1 核心概念关系


```text
Query
 └── PipelineFragmentContext (一个 Fragment 的执行上下文)
      └── Pipeline (一条算子链)
           ├── Source Operator (数据源)
           ├── 中间 Operator (filter/project/join_probe/...)
           └── Sink Operator (数据汇)
                |
                v
           PipelineTask (Pipeline 的一个并行实例)

```


一个 Pipeline 由一组 Operator 链组成，表示数据从 Source 流向 Sink 的一段计算逻辑。当遇到需要“阻断”的算子（如 HashJoin 的 build 端需要收集所有数据后才能进入 probe），Pipeline 会被切断为两段。

> [!note] 批注｜区分计划结构、任务实例和线程
>
> Fragment 表示分布式计划中的一个执行片段，Pipeline 是可以按批传递数据的一段算子链，PipelineTask 是这条链的执行实例。多个 Task 可以在同一线程池中轮流运行；一个 Task 等待 I/O 时让出线程，其算子状态仍然保留。

### 1.2 Pipeline 切分规则


```text
ScanNode → Filter → HashJoin(probe) → Agg → Sink
                        |
                  HashJoin(build) ← Exchange

切分为:
Pipeline 1: Exchange → HashJoin_Build_Sink
Pipeline 2: Scan → Filter → HashJoin_Probe → Agg → Result_Sink

```


Pipeline 1 的 Sink 产出数据后，通过 Dependency 通知 Pipeline 2 的 Source 可以开始工作。

> [!warning] 核对说明｜依赖约束的是 Probe 使用哈希表的时机
>
> Build Sink 完成并发布哈希表后，Probe 才能使用相应的哈希表。原图用两条 Pipeline 示意这层依赖；是否拆出聚合的读写两段、是否允许其他 Scan 工作提前进行，要以实际物理计划为准。定位等待时追到具体的 Join Build 或 Exchange 比仅看 Pipeline 编号更有用。

## 二、PipelineTask 生命周期

### 2.1 状态定义


```cpp
// be/src/exec/pipeline/pipeline_task.h
class PipelineTask : public std::enable_shared_from_this<PipelineTask> {
public:
    Status prepare(...);
    virtual Status execute(bool* done);
    virtual Status close(Status exec_status, bool close_sink = true);
    virtual Status finalize();
    virtual bool is_blockable() const;
};

```


PipelineTask 的状态转换：


```text
                    prepare()
    PENDING ─────────────────> RUNNABLE
       |                          |
       |                    submit to scheduler
       |                          |
       |                          v
       |                      RUNNING  ──────> execute() 返回
       |                      /      \
       |              (需等待)        (完成)
       |               /                \
       |              v                  v
       |          BLOCKED            FINISHED
       |              |
       |         (依赖就绪)
       |              |
       |              v
       +──────── RUNNABLE (重新入队)

```


### 2.2 execute() 核心循环


```cpp
Status PipelineTask::execute(bool* done) {
    // 核心执行逻辑（简化）
    while (!*done) {
        if (_sink->is_pending_finish()) {
            // sink 还不能结束，task 需要等待
            break;
        }

        Block block;
        bool eos = false;

        if (_source->need_more_input_data(state)) {
            // Source 需要更多数据
            // → 从上游拉取或等待
        }

        // 从 source 拉取数据
        RETURN_IF_ERROR(_source->pull(state, &block, &eos));

        if (block.rows() > 0) {
            // 将数据推入 sink
            RETURN_IF_ERROR(_sink->sink(state, &block, eos));
        }

        if (eos) {
            *done = true;
        }
    }
    return Status::OK();
}

```

> [!warning] 核对说明｜空 Block 不等于无需传递结束信号
>
> `eos`（输入结束）与 `block.rows()` 是独立信号；最后一次拉取可以既没有行又已结束。Sink 仍可能需要处理这个结束信号来发布聚合或 Join 状态。原循环用于示意数据流，不可照搬 `rows() > 0` 后才通知 Sink 的写法。


## 三、TaskScheduler 调度实现

### 3.1 类结构


```cpp
// be/src/exec/pipeline/task_scheduler.h
class TaskScheduler {
public:
    virtual Status submit(PipelineTaskSPtr task);
    virtual Status start();
    virtual void stop();

private:
    std::string _name;
    const int _num_threads;
    std::unique_ptr<ThreadPool> _fix_thread_pool;
    MultiCoreTaskQueue _task_queue;
    std::weak_ptr<CgroupCpuCtl> _cgroup_cpu_ctl;
};

```


### 3.2 MultiCoreTaskQueue


```cpp
// be/src/exec/pipeline/task_queue.h
class MultiCoreTaskQueue;

```


任务队列的设计要点：

- **多级队列：**

  按优先级分级（支持 Workload Group 资源隔离）
- **Per-Core 队列：**

  减少线程间竞争
- **Work Stealing：**

  空闲线程可以从其他队列偷取任务

### 3.3 调度循环


```text
Worker Thread 工作循环:
    loop {
        task = task_queue.take()  // 从队列取任务
        if (task == nullptr) break  // 停止信号

        task.set_thread_id(current_thread_id)

        bool done = false
        status = task.execute(&done)

        if (done || status.is_error()) {
            task.finalize()
            task.close()
        } else if (task.is_blockable()) {
            // task 进入 BLOCKED 状态
            // 当 dependency 就绪时，会被重新 submit
        } else {
            // task 还有工作要做，重新入队
            task_queue.push(task)
        }
    }

```

> [!note] 批注｜主动让出时间片和依赖阻塞要分开看
>
> 在 [Doris 4.1 的 PipelineTask](https://github.com/apache/doris/blob/branch-4.1/be/src/exec/pipeline/pipeline_task.cpp) 中，任务可以因时间片用完让出线程，也可以因读写依赖尚未就绪而等待唤醒。前者可以再次竞争执行，后者要等生产者或下游状态变化。Profile 中的任务排队、依赖等待和实际执行耗时不能直接当作同一类 CPU 消耗。


### 3.4 HybridTaskScheduler


```cpp
class HybridTaskScheduler;

```


HybridTaskScheduler 整合了 TaskScheduler，支持 Workload Group 级别的 CPU 资源隔离（通过 CGroup CPU 控制器）。

> [!note] 批注｜调度队列和 CPU 配额是两层控制
>
> 任务队列决定哪个就绪任务获得执行机会；Workload Group 中的 CPU 上限则约束一组任务能消耗多少 CPU 带宽。另有 FE 并发准入和排队。检查配置时按[目标版本的 Workload Group 文档](https://doris.apache.org/docs/4.x/admin-manual/workload-management/workload-group/)分清组级排队与 BE 执行线程的争用。

## 四、Operator 状态机

### 4.1 Source Operator 关键方法


```cpp
class OperatorXBase {
    // Source 端
    virtual bool need_more_input_data(RuntimeState* state) const = 0;
    virtual Status pull(RuntimeState* state, Block* block, bool* eos) = 0;
};

```


- `need_more_input_data()`

  : 返回 true 表示需要上游继续喂数据
- `pull()`

  : 拉取一批输出数据

### 4.2 Sink Operator 关键方法


```cpp
class DataSinkOperatorXBase {
    virtual Status sink(RuntimeState* state, Block* input_block, bool eos) = 0;
    virtual bool is_pending_finish() const;
};

```


- `sink()`

  : 接收一批输入数据
- `is_pending_finish()`

  : 是否还有异步工作未完成

### 4.3 典型算子状态转换示例

以 HashJoin Build Sink 为例：


```text
初始状态: 接收数据
    |
    | sink(block, eos=false)  // 持续接收 build 侧数据
    v
接收完毕: eos=true
    |
    | 构建 HashTable
    v
通知下游 Dependency 就绪 (probe 侧可以开始)
    |
    v
完成

```


## 五、Dependency 机制

### 5.1 核心类


```cpp
// be/src/exec/pipeline/dependency.h
class Dependency;

struct BasicSharedState {
    std::vector<DependencySPtr> source_deps;
    std::vector<DependencySPtr> sink_deps;
    int id = 0;
    std::set<int> related_op_ids;

    void create_source_dependencies(int num_sources, int operator_id, 
                                     int node_id, const std::string& name);
    Dependency* create_source_dependency(int operator_id, int node_id, 
                                          const std::string& name);
    Dependency* create_sink_dependency(int dest_id, int node_id, 
                                        const std::string& name);
};

```


### 5.2 Dependency 工作原理

Dependency 是 Pipeline 间通信的核心机制：


```text
Pipeline A (Build)                    Pipeline B (Probe)
    |                                      |
    | sink()...sink(eos=true)              | [BLOCKED, 等待依赖]
    |                                      |
    v                                      |
Dependency.set_ready()  ─────────────────> |
                                           v
                                    [RUNNABLE, 重新入队]
                                           |
                                           v
                                    pull() 开始工作

```

> [!note] 批注｜真正要等的是状态就绪
>
> Build 和 Probe 之间共享的是哈希表等数据，Dependency 负责通知是否可用。[Doris 4.1 的 `set_ready`](https://github.com/apache/doris/blob/branch-4.1/be/src/exec/pipeline/dependency.cpp)会唤醒等待任务，唤醒后仍需等线程调度。若 Probe 等待很久，要沿同一查询的依赖向上检查 Build，而不能仅凭等待时间归因到 Probe 自身。


### 5.3 SharedState 跨 Pipeline 数据共享


```cpp
struct BasicSharedState {
    // 被多个 PipelineTask 共享
    // 例如: HashJoin 的 HashTable 在 Build 和 Probe 之间共享
};

```


PipelineTask 通过`inject_shared_state()` 建立跨 Pipeline 的数据连接：


```cpp
bool PipelineTask::inject_shared_state(std::shared_ptr<BasicSharedState> shared_state);
BasicSharedState* PipelineTask::get_op_shared_state(int id);

```


### 5.4 慢依赖检测


```cpp
static constexpr auto SLOW_DEPENDENCY_THRESHOLD = 60 * 1000L * 1000L * 1000L;  // 60s
static constexpr auto TIME_UNIT_DEPENDENCY_LOG = 30 * 1000L * 1000L * 1000L;    // 30s

```


当一个 Dependency 等待超过 60 秒未就绪时，会触发慢依赖告警日志，帮助定位查询阻塞问题。

> [!note] 批注｜慢依赖阈值是诊断线索
>
> 源码常量和日志行为要用运行版本核对。读到慢依赖时，先确认被等待的是 Build、Exchange、扫描、写缓冲还是结束阶段，再与 Query Profile 的算子耗时、输入量及实例差异结合。等待长只说明依赖迟迟未就绪，不能单独说明是 I/O 或 CPU 瓶颈。

## 六、反压与内存控制

### 6.1 Sink 端反压

当下游处理不过来时（如网络发送缓冲区满），Sink 的`is_pending_finish()` 返回 true，PipelineTask 进入 BLOCKED 状态，停止从上游拉取数据，实现端到端反压。

> [!warning] 核对说明｜写反压和结束等待是不同依赖
>
> [PipelineTask](https://github.com/apache/doris/blob/branch-4.1/be/src/exec/pipeline/pipeline_task.cpp)分别检查读取、写入和结束依赖。发送缓冲已满属于写入暂未就绪；输入耗尽后仍要等异步工作结束则属于结束依赖。两者都可能让 Task 暂停，但应检查的生产者、消费者和 Profile 指标不同。

### 6.2 内存预算


```cpp
// be/src/util/block_budget.h
class BlockBudget;

```


每个 Pipeline 有内存预算限制，超出时触发 Spill（溢写到磁盘）：


```cpp
// be/src/exec/operator/spill_utils.h
// Spill 相关工具

```


### 6.3 Spill 机制

当内存压力大时，算子可以将中间数据溢写到磁盘：


```text
内存中的 HashTable/Sort Buffer
    |
    | (内存超限)
    v
序列化为 Block → 写入本地磁盘 SpillFile
    |
    | (需要数据时)
    v
从 SpillFile 反序列化恢复

```


关键类：

- `be/src/exec/spill/spill_file.h`
- `be/src/exec/operator/spill_utils.h`

> [!warning] 核对说明｜触发落盘有算子和配置前提
>
> [Doris 4.x Spill 文档](https://doris.apache.org/docs/4.x/key-features/spill-to-disk/)说明，支持 Spill 的算子在内存预留受限时，可以落盘可回收的中间状态。需要启用相应功能，且实际计划要有可落盘算子；不会因为任意 Pipeline 达到某个数字就自动落盘。验证时看 Query Profile 的溢写字节、读写耗时与磁盘可用空间。

## 七、查询片段执行上下文

### 7.1 PipelineFragmentContext


```cpp
// be/src/exec/pipeline/pipeline_fragment_context.h
class PipelineFragmentContext;

```


管理一个 Fragment 内所有 Pipeline 和 PipelineTask 的生命周期：

- 创建所有 Pipeline 和对应的 PipelineTask
- 协调 Task 的提交和完成
- 处理取消和错误传播

### 7.2 Pipeline 构建


```cpp
// be/src/exec/pipeline/pipeline.h
class Pipeline;

using PipelinePtr = std::shared_ptr<Pipeline>;

```


## 源码路径速查表

| 模块 | 路径 |
| --- | --- |
| PipelineTask |`be/src/exec/pipeline/pipeline_task.h/.cpp` |
| TaskScheduler |`be/src/exec/pipeline/task_scheduler.h/.cpp` |
| TaskQueue |`be/src/exec/pipeline/task_queue.h/.cpp` |
| Dependency |`be/src/exec/pipeline/dependency.h/.cpp` |
| Pipeline |`be/src/exec/pipeline/pipeline.h/.cpp` |
| PipelineFragmentContext |`be/src/exec/pipeline/pipeline_fragment_context.h/.cpp` |
| Operator 基类 |`be/src/exec/operator/operator.h` |
| Spill 文件 |`be/src/exec/spill/spill_file.h` |
| Spill 工具 |`be/src/exec/operator/spill_utils.h` |
| BlockBudget |`be/src/util/block_budget.h` |
| RevokableTask |`be/src/exec/pipeline/revokable_task.h` |

## 核心数据结构/类索引

| 类名 | 作用 |
| --- | --- |
|`PipelineTask` | Pipeline 的一个并行执行实例 |
|`TaskScheduler` | 任务调度器（线程池+任务队列） |
|`HybridTaskScheduler` | 支持 Workload Group 的混合调度器 |
|`MultiCoreTaskQueue` | 多核任务队列（Per-Core + Work Stealing） |
|`Dependency` | Pipeline 间的依赖/通知机制 |
|`BasicSharedState` | 跨 Pipeline 共享的状态 |
|`OperatorXBase` | Source/中间算子基类 |
|`DataSinkOperatorXBase` | Sink 算子基类 |
|`PipelineFragmentContext` | Fragment 级执行上下文 |
|`Pipeline` | 一条算子链的抽象 |

## 小结

Doris Pipeline 引擎通过以下设计实现了高效的查询执行：

1. **任务粒度调度：**

   将 Fragment 拆分为细粒度的 PipelineTask，共享线程池
2. **非阻塞设计：**

   通过 Dependency 机制避免线程等待，最大化 CPU 利用率
3. **弹性资源控制：**

   Workload Group + 内存预算 + Spill，保障多租户场景下的稳定性
4. **端到端反压：**

   从 Sink 到 Source 的反压传播，防止内存溢出

这套框架使得 Doris 能够在高并发混合负载下保持稳定的查询性能。

