---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 14
source_sections: "主教材第9.1节；优化第2.2、7.7、7.8节"
---

# Flink 2.3 Checkpoint 屏障、对齐与恢复

检查点把 Source 位置、算子状态和必要的在途数据组合成一致的恢复边界。它保证的是恢复后的逻辑状态，不保证每条记录物理上只执行一次。

## 屏障如何切分逻辑进度

协调器触发检查点，Source 记录读取位置并插入 barrier。数据与屏障沿图传播；各任务在相应边界保存状态并确认，全部必要参与者完成后，协调器宣布检查点完成。事务 Sink 的外部提交与完成通知衔接。

多输入的 aligned exactly-once 检查点需要对齐屏障：某输入收到屏障后暂缓处理其后记录，继续处理其他输入屏障前的数据，形成一致切面。它可能因慢通道或反压等待较久。

## Unaligned 保存在途数据

Unaligned 检查点把必要的网络缓冲作为 channel state 一起保存，让屏障绕过排队数据。代价是快照与恢复 I/O 可能增加。它仍不能中断一条正在执行、长时间阻塞的用户函数。

```yaml
execution.checkpointing.interval: 30 s
execution.checkpointing.timeout: 5 min
execution.checkpointing.min-pause: 10 s
execution.checkpointing.max-concurrent-checkpoints: 1
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.unaligned.enabled: true
execution.checkpointing.aligned-checkpoint-timeout: 20 s
```

这是用于比较的配置，不是普适模板。启用 unaligned 后，设置正的 aligned timeout 可先尝试对齐，再在超时条件下切换；并发检查点需遵守当前限制。不要混淆 aligned timeout 与整个检查点 timeout。

## 从检查点恢复

失败后，相关任务恢复最近可用的已完成检查点，Source 回到对应读取位置，算子恢复状态，必要时重放在途数据。最近一次尚未完成的检查点不能因为“部分文件已经上传”就当作成功快照使用。

本地恢复可复用仍可用的本地文件以减少下载，但机器磁盘损坏时不能依赖它；远端持久化仍是恢复基础。重启策略决定是否、何时重试，不会替代检查点数据。

## Finished 输入和最终提交

含有界阶段的流式流水线可能有部分任务先结束，后续仍需按当前检查点协议完成快照与最终提交。作业代码执行结束或 Sink flush 不等于全部外部事务已可见；应核对最终检查点和提交状态。

## 故障演练应验证什么

准备可重放输入，等待一次检查点成功，在新记录已进入但下一次检查点未完成时终止一个任务，再验证恢复后的业务最终值、输入进度和外部输出。日志出现“restored”只证明开始恢复；还需对账证明没有遗漏或重复计入。

故障定位保留 JobID、检查点 ID、子任务和最早异常；取消通知经常是上游失败的后果，不能只根据最后一条 `checkpoint cancelled` 日志判定根因。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：主教材第9.1节；优化第2.2、7.7、7.8节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [checkpointing](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/checkpointing/)
- [checkpointing under backpressure](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/checkpointing_under_backpressure/)
- [task failure recovery](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/task_failure_recovery/)

前篇：[[07-Flink 2.3 状态后端与异步状态访问]]

后篇：[[09-Flink 2.3 端到端一致性与Sink提交]]
