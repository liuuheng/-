---
tags:
  - flink
  - checkpoint
  - source-api
status: active
---

# Flink 运行时：Checkpoint、部署与 Source API

返回：[[README|企业微信知识地图]]

## 1. Checkpoint 的完整流程

1. Coordinator 触发 checkpoint，并让 Source 在当前位置记录读取进度。
2. Source 向每个下游 channel 注入同一 checkpoint barrier。
3. Barrier 随数据流传播，把逻辑流划分为 checkpoint 前后。
4. 单输入算子到达 barrier 后快照自己的 operator/keyed state，再向下游转发。
5. 多输入算子根据 aligned/unaligned 策略处理不同 channel 的 barrier。
6. Sink 快照缓冲或事务状态。
7. 所有 Task ACK 后，checkpoint 完成；两阶段提交 sink 才能提交相应事务。

Checkpoint 默认需要显式启用。它的 exactly-once 指状态在失败恢复后每个事件逻辑上恰好影响一次，不等于记录物理上只执行一次。端到端 exactly-once 还要求 Source 可重放、Sink 事务性或幂等。

## 2. Aligned 与 Unaligned Checkpoint

Aligned 模式下，多输入算子收到某 channel 的 barrier 后暂停该 channel，继续处理其他 channel 的 barrier 前数据，直到所有 barrier 对齐，再拍状态快照。背压严重时，barrier 可能长时间追不上。

Unaligned 模式不等待所有在途数据处理完成，把 input/output channel buffer 一并保存为 channel state：

```text
Aligned：处理到边界再拍照
Unaligned：先拍照，水管中未处理的数据也入镜
```

它能降低背压下 checkpoint 时长，但会增加 checkpoint 数据量和恢复时 I/O，不是无条件更好。每个 Task 只保存自己可见 channel 的在途数据，不是某个下游 Task 保存全链路所有未到数据。

## 3. Checkpoint、外部化 Checkpoint 与 Savepoint

- Checkpoint 由系统周期管理，用于自动故障恢复，生命周期可能随作业删除。
- Externalized Checkpoint 在作业取消后仍保留，但本质仍是 checkpoint。
- Savepoint 由用户触发和管理，用于计划内升级、迁移和调整并行度。

不要写成 savepoint 永远与 state backend 无关。现代 Flink 也支持 native savepoint，速度更快但可移植性有限。升级前要验证算子 UID、状态 schema、serializer 版本和 TTL 兼容性。

## 4. State TTL 的迁移风险

DataStream State TTL 通常基于 processing time。TTL 配置未必作为状态内容完整保存在 checkpoint 中，旧版本对启用/禁用 TTL 或修改 serializer 的兼容性可能抛 `StateMigrationException`。生产变更前应在相同 Flink 版本和 backend 上用真实 savepoint 演练。

TTL 过短会删除仍可能被迟到事件引用的状态，产生业务错误；窗口状态通常由 watermark 和窗口生命周期清理，TTL 可做兜底，但应大于窗口长度加允许迟到时间。

## 5. Client、JobManager 与 TaskManager

- Client 读取 jar、参数和配置，准备或提交 JobGraph。
- JobManager 负责协调、调度、checkpoint 和故障恢复。
- TaskManager 提供 slot，真正持续执行算子和处理数据。

不同部署模式 main() 的位置不同：

- Session/Per-Job 常由提交客户端执行 main，生成 JobGraph 后提交。
- Application Mode 把用户 main 和 JobGraph 生成移到集群侧，减少大依赖和复杂计划对客户端的压力，并让集群生命周期与应用绑定。

main() 只负责描述作业和提交，不是持续处理每条数据的线程。

## 6. 生命周期 Hook

Hook 是框架在生命周期节点预留的扩展点。Flink 算子的 `open()` 在处理数据前执行，可初始化连接、反序列化器或缓存；`close()` 用于结束清理，但故障时不保证一定调用，因此关键一致性不能依赖 close。

在 UDF/UDTF 中：

- process 内局部变量每次调用独享，最安全。
- 普通实例字段通常属于某个 Task/算子实例，但要考虑对象复用和生命周期。
- `static` 可变变量在同一个 TaskManager JVM 内被多个 Task 线程共享，容易产生并发和跨任务污染，应避免。

## 7. 新 Source API 的组件边界

```text
Source
├── 创建 SplitEnumerator
├── 创建 SourceReader
├── Split Serializer
└── Enumerator State Serializer

SplitEnumerator：发现、切分、分配任务，保存全局发现进度
SourceSplit：可分配、可恢复的一段读取任务
SourceReader：真正读取 Split 并输出记录
```

Source 是总入口，不负责持续扫描外部系统。Enumerator 是控制面，Reader 是数据面。

## 8. SplitEnumerator 的职责

以按时间窗口读取 ES 为例：

- `start()` 注册周期发现任务，计算安全上界 `now - upperBoundDelay`。
- 发现器把新时间范围切成多个 slice split，放入 `pendingSplits`。
- Reader 启动或读完后通过 `handleSplitRequest` 拉取新 split。
- `assignPendingSplits` 把待分配 split 交给等待 Reader。
- Reader 失败时 `addSplitsBack` 接收未完成 split 并重新分配。
- `snapshotState` 保存已发现时间上界和尚未分配的 split。

Reader checkpoint 保存的是已经分给自己但尚未读完的 split 及进度，两者缺一不可。

## 9. snapshotState 与 Serializer

```text
snapshotState 决定保存什么
→ EnumeratorState
→ EnumeratorStateSerializer 决定状态如何编码
→ SplitSerializer 编码其中每个 Split
→ checkpoint/savepoint
```

`SimpleVersionedSerializer#getVersion()` 使代码升级时能按版本读取旧状态。字符串序列化必须读写配对，推荐使用 Flink 提供的 `StringValue.writeString/readString` 或自定义长度前缀：

```java
byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
out.writeInt(bytes.length);
out.write(bytes);
```

反序列化时先读长度再 `readFully`。版本升级新增字段时，应在 `deserialize(version, bytes)` 中保留旧版本分支，而不是直接拒绝所有旧 checkpoint。

## 10. SourceReader 的拉取循环

`pollNext()` 返回：

- `MORE_AVAILABLE`：还有数据，可以立即再次调用。
- `NOTHING_AVAILABLE`：当前没有，但未来可能有；Flink 获取 `isAvailable()` Future 等待通知。
- `END_OF_INPUT`：有界输入彻底结束。

Reader 内部可以有 `bufferedHits`。Flink 不直接访问缓存，只反复调用 `pollNext`：缓存有数据就输出；缓存空且有 split 就拉下一页；没有 split 就返回 NOTHING_AVAILABLE。Future 完成后，运行时再调度 pollNext，避免空转轮询。

## 11. ES Source 的可恢复读取语义

每个 split 可包含：

```text
时间下界（inclusive）
时间上界（exclusive）
sliceId / sliceCount
search_after 游标
```

`upper.bound.delay` 避免读取太新的、尚未稳定可见的数据；`lookback` 让下一轮回看一小段历史，覆盖迟到写入和可见性延迟。Reader 失败恢复后 PIT 可能已过期，无法保证回到完全相同快照，因此整体语义通常是“尽量不漏、允许重复”。下游必须按业务 ID 与版本/更新时间幂等覆盖。

稳定排序至少包含更新时间和唯一 ID。只按更新时间 search_after，多个文档时间相同会漏读或重复。

## 12. HTTP Client 共享的权衡

每个 Reader 独立创建客户端实现简单，但大量并行度会建立过多连接池。按 TaskManager 共享客户端可以减少连接，但需要线程安全、引用计数、配置隔离和生命周期管理。不能让不同 endpoint、认证或超时配置的 Source 误用同一静态客户端。

## 13. Builder 模式

```java
KafkaSource.<String>builder()
    .setBootstrapServers(...)
    .setTopics(...)
    .build();
```

`KafkaSource` 是最终对象，Builder 收集参数、提供链式 API、统一校验并在 build 时创建对象。从目标类型调用静态 `builder()`，可以让使用者不必记住 Builder 类名，也避免暴露参数很多且容易误用的构造函数。

## 参考依据

- [Flink Fault Tolerance 与 exactly-once](https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/fault_tolerance/)
- [Checkpoint 与 Savepoint](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpoints_vs_savepoints/)
- [Flink State Migration](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state_migration/)
