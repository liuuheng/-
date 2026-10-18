# 算子链 Operator Chain

## 1. 数据分区规则

Flink 中，上游算子把数据发送给下游算子时，并不是随便发送的，而是由数据分区规则决定：

```text
上游的一条数据 -> 应该发送给下游哪个并行子任务
```

在 Flink 底层，这类数据分发规则可以理解为由 `ChannelSelector` 负责选择下游通道。不同的分区方式，对应不同的 `Partitioner`。

常见分区规则如下：

| 分区方式 | 底层 Partitioner | 说明 |
|---|---|---|
| `rebalance()` | `RebalancePartitioner` | 绝对负载均衡，通过轮询方式把数据发送给下游各个并行子任务。上下游并行度不一致时，默认常见规则就是 `REBALANCE` |
| `rescale()` | `RescalePartitioner` | 相对负载均衡，也是轮询发送，但只会发送给当前上游任务对应的一组下游任务 |
| `keyBy()` | `KeyGroupStreamPartitioner` | 按 key 分组，相同 key 的数据会进入同一个下游并行子任务，常用于聚合、状态计算 |
| `shuffle()` | `ShufflePartitioner` | 随机发送给下游并行子任务 |
| `broadcast()` | `BroadcastPartitioner` | 广播发送，每条数据都会发送给下游所有并行子任务 |
| `global()` | `GlobalPartitioner` | 全局发送，所有数据都发送给下游第一个并行子任务，容易造成单点压力 |
| `forward()` | `ForwardPartitioner` | 直连发送，要求上下游并行度一致，上游第几个并行子任务就发送给下游第几个并行子任务 |

需要重点区分的是 `rebalance()`、`rescale()` 和 `forward()`。

`rebalance()` 是全局轮询，它会把数据尽量均匀地分发给所有下游并行子任务：

```text
上游任务 -> 下游所有任务轮询分发
```

`rescale()` 是局部轮询，它不会面向所有下游任务，而是只在当前上游任务对应的一组下游任务中分发：

```text
上游任务 -> 对应的一部分下游任务轮询分发
```

`forward()` 是一对一直连，它不做重新分发：

```text
上游 subtask 0 -> 下游 subtask 0
上游 subtask 1 -> 下游 subtask 1
```

所以 `forward()` 要求上下游并行度必须一致。

默认情况下可以这样理解：

```text
上下游并行度一致时，默认可能使用 forward
上下游并行度不一致时，通常需要重新分区，常见就是 rebalance
```

这也和算子链有关：如果上下游之间是 `forward`，并且并行度相同，就有机会合并成算子链。

## 2. 算子链的含义

算子链是指：Flink 会把满足条件的上下游算子合并到同一个 Task 中执行。

形成算子链通常需要满足两个条件：

```text
上下游算子的并行度相同
数据分发规则是 forward
```

这样做的主要作用是：

```text
减少线程切换
减少网络和缓冲区开销
降低数据处理延迟
提升整体吞吐量
```

## 3. 禁止算子链的方式

全局禁用算子链：

```java
env.disableOperatorChaining();
```

让某个算子不和上游算子合并：

```java
.startNewChain()
```

让某个算子不和上游、下游算子合并：

```java
.disableChaining()
```

## 4. 小结

`Flink02_OperatorChain` 主要用于理解 Flink 中数据在算子之间如何分发，以及满足条件的算子为什么会被合并成算子链。

简单来说：

```text
分区规则决定数据发给谁，算子链决定多个算子能不能合并在一起执行。
```
