# 09 并发、线程安全与 Flink 状态意识

## 1. 为什么学习 Flink 前要理解并发

Flink 是分布式流处理框架。即使你没有手动创建线程，同一个作业也可能在多个进程、多个节点、多个并行子任务中执行。

需要先建立几个概念：

- 进程：运行中的程序实例，有独立内存空间。
- 线程：进程中的执行单元。
- 并发：多个任务在时间上交替推进。
- 并行：多个任务在同一时刻同时执行。
- 分布式：任务运行在多个 JVM 或多个机器上。

## 2. 共享可变状态为何危险

```java
public class Counter {
    private int count = 0;

    public void increment() {
        count++;
    }
}
```

`count++` 不是不可分割的操作。它大致包含：

1. 读取旧值。
2. 加一。
3. 写回新值。

多个线程同时执行时，可能丢失更新。

## 3. synchronized

```java
public synchronized void increment() {
    count++;
}
```

`synchronized` 可以让同一时刻只有一个线程进入临界区。

也可以锁定代码块：

```java
synchronized (lock) {
    count++;
}
```

## 4. volatile

```java
private volatile boolean running = true;
```

`volatile` 主要保证变量修改对其他线程可见，并限制某些指令重排。它不保证复合操作的原子性。

```java
private volatile int count = 0;
count++; // 仍然不是线程安全
```

## 5. 原子类

```java
AtomicInteger count = new AtomicInteger();
count.incrementAndGet();
```

对于简单计数，原子类比手动加锁更直接。

## 6. Flink 并行度

```java
env.setParallelism(4);
```

并行度为 4，表示某个算子可以有 4 个并行子任务。它们可能运行在不同线程、不同 JVM，甚至不同机器上。

如果你写：

```java
private static final Map<String, Double> totals =
    new HashMap<>();
```

这不是可靠的分布式状态：

1. 每个 JVM 可能有自己的副本。
2. 发生故障后内容会丢失。
3. 调整并行度后无法自动重新分配。
4. 静态可变对象可能引入线程安全问题。
5. 它不会自动进入 Checkpoint。

## 7. Flink 托管状态

正确方向是使用 Flink State API。以后会学习：

- `ValueState<T>`
- `ListState<T>`
- `MapState<K, V>`
- `ReducingState<T>`
- `AggregatingState<IN, OUT>`

概念示例：

```java
private transient ValueState<Double> totalState;
```

Flink 托管状态带来的价值：

1. 可以随 Checkpoint 持久化。
2. 失败后可以恢复。
3. 在重新扩缩容时能够按 key 重新分配。
4. 与 `keyBy()` 后的 key 分区机制配合。

## 8. keyBy 与状态的关系

```java
orders
    .keyBy(Order::getUserId)
    .process(...);
```

`keyBy()` 会按 key 对数据重新分区。相同 key 的数据进入同一逻辑分组。

之后使用 keyed state 时，每个 key 都像拥有自己的状态：

```text
u-1 -> total = 100
u-2 -> total = 250
u-3 -> total = 30
```

你不需要手动维护一个大 `HashMap<String, Double>`。

## 9. 不要随意创建线程

在 Flink 算子中自行创建线程或线程池需要非常谨慎：

- 生命周期不容易管理。
- 异常可能绕开 Flink 的失败处理。
- Checkpoint 一致性可能受到影响。
- 阻塞操作可能拖慢数据处理。

访问外部系统时，后续优先学习 Flink Async I/O。

## 10. 不可变对象的价值

并发问题通常来自共享和修改。不可变对象创建后不再变化，更容易安全共享。

```java
public final class Threshold {
    private final double value;

    public Threshold(double value) {
        this.value = value;
    }

    public double getValue() {
        return value;
    }
}
```

## 11. 自测

1. 并发、并行和分布式分别是什么？
2. `volatile int count; count++;` 是否线程安全？
3. 为什么 `static HashMap` 不能替代 Flink State API？
4. `keyBy()` 与 keyed state 有什么关系？

## 12. 阅读时常见问题

### 问题 1：Flink 已经管理并行执行了，我还需要学习 Java 并发吗？

需要理解基本概念，但入门阶段不要求手写复杂并发代码。

你需要知道为什么共享可变变量有风险、为什么不能随意创建线程、为什么普通内存变量无法自动容错。这样学习状态 API、并行度和 Checkpoint 时才不会只记住表面用法。

### 问题 2：每个算子实例通常由一个线程调用，那普通成员变量是不是就能当状态使用？

某些简单场景下，它看起来可能暂时能工作，但仍不适合作为需要可靠保存的业务状态：

```java
private double total;
```

发生故障、重新部署或调整并行度后，普通成员变量无法自动恢复和重新分配。需要容错的业务状态应使用 Flink State API。

### 问题 3：为什么 `static HashMap` 更危险？

`static` 只表示它在当前 JVM 中属于类，不表示全局唯一，也不表示分布式共享。

如果作业运行在三个 TaskManager JVM 中，可能存在三个互不一致的 `static HashMap`。它也不会自动写入 Checkpoint。

### 问题 4：`keyBy()` 后，相同 key 是否一定在同一台机器？

在某一次作业运行期间，相同 key 会被路由到同一个下游并行子任务，从而能够使用 keyed state。

但发生扩缩容或重新部署后，这个 key 可能被分配到不同实例。Flink 负责迁移和恢复托管状态，普通成员变量做不到这一点。

### 问题 5：`volatile` 和 `synchronized` 应该怎么区分？

- `volatile` 主要解决可见性问题：一个线程修改后，其他线程能看到新值。
- `synchronized` 还可以保护临界区，避免多个线程同时修改共享数据。

下面仍然不是线程安全的：

```java
volatile int count = 0;
count++;
```

因为 `count++` 包含读取、加一和写回多个步骤。

### 问题 6：Flink 状态是不是一个隐藏起来的全局 Map？

不能简单这样理解。状态由 Flink 管理，并与 key、算子实例、状态后端和 Checkpoint 机制配合。它不仅保存值，还要支持恢复、重新分配和一致性语义。
