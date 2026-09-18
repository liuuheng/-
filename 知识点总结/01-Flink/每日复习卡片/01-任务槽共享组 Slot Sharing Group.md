---
tags:
  - Flink
  - 每日复习
  - Slot
---

# Slot Sharing Group 每日复习 Q&A

## Q1：Slot Sharing Group 中文是什么？

**A：** 任务槽共享组。

它决定同一组内不同算子的 subtask 是否可以共享 Slot。

---

## Q2：同一个 Slot Sharing Group 需要多少个 Slot？

**A：** 通常取组内各算子并行度的最大值。

```text
Source：2
Map：4
Sink：3

Slot 数量 = max(2, 4, 3) = 4
```

---

## Q3：存在多个 Slot Sharing Group 时，怎样计算 Slot 数量？

**A：** 每个共享组分别取最大并行度，然后将各组结果相加。

```text
g1：Source(2)、Map(4) -> 4
g2：Sink(3)          -> 3

总数 = 4 + 3 = 7
```

---

## Q4：算子不能形成算子链，还能共享 Slot 吗？

**A：** 可以。

- 算子链决定多个算子是否在同一个 Task、同一线程执行。
- Slot Sharing 决定不同算子的 subtask 是否可以共享同一个 Slot。

即使 `keyBy()`、`rebalance()` 打断了算子链，只要上下游算子属于同一个 Slot Sharing Group，它们仍然可以共享 Slot。

---

## Q5：Slot Sharing 是故障恢复机制吗？

**A：** 不是。

Slot Sharing 只负责资源共享和任务放置。

故障恢复由以下机制负责：

- Failover Strategy：决定重启哪些 Tasks。
- Restart Strategy：决定是否重启以及怎样重启。
- Checkpoint：提供需要恢复的状态和数据读取位置。

## 一句话记忆

> 算子链决定是否在同一线程执行；Slot Sharing Group 决定是否可以共享同一个 Slot。
