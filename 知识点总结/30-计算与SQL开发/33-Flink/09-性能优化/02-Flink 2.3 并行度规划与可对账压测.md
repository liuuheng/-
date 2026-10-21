---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 26
source_sections: "优化第1.3、5.1节"
---

# Flink 2.3 并行度规划与可对账压测

并行度应由瓶颈算子的处理能力、输入高峰和下游容量共同决定。把整条链统一扩大，可能只是让更多线程争抢外部连接或同一热 key。

## 容量估算先给前提

若某算子单子任务在目标延迟下稳定处理 q 条/秒，高峰输入为 Q，则可用 `ceil(Q/q/目标利用率)` 得到初始估算。例如 Q=120000、q=8000、目标利用率 0.7，初始并行度约为 22。

该估算要求 key 分布相对均衡、输入记录大小和业务逻辑一致、外部系统容量未先耗尽。不能把一次短时间最大吞吐直接当作稳定单实例处理能力。

## 分别规划 Source、计算和 Sink

消息分区数约束实际同时读取的 split 数，Source 并行度高于可读分区时会有空闲实例；一个读取实例可以处理多个分区。计算算子可在 shuffle 后采用不同并行度。Sink 并发还受目标系统连接数、批量效率和事务成本限制。

keyed 并行度不强制为 2 的幂；应关注最大并行度、key group、扩缩容和负载分布。无论把并行度加到多少，同一个未拆分热 key 仍由一个实例处理。

## 使用有限且可复现的数据

```sql
CREATE TEMPORARY TABLE load_input (
  id BIGINT,
  user_id BIGINT,
  amount BIGINT
) WITH (
  'connector'='datagen',
  'rows-per-second'='10000',
  'number-of-rows'='1000000',
  'fields.id.kind'='sequence',
  'fields.id.start'='1', 'fields.id.end'='1000000',
  'fields.user_id.min'='1', 'fields.user_id.max'='1000',
  'fields.amount.min'='1', 'fields.amount.max'='1'
);
```

金额固定 1，完成后总金额应为 1000000，便于对账。随机用户不保证每个用户分布相同，若要验证热 key 行为，应使用确定的热点生成规则和固定随机种子。

## 压测分三个阶段

先用低速验证数据契约和结果；再逐级提高输入直至目标延迟或资源边界；最后验证峰值积压后能否恢复。测试记录平均与 P99 延迟、lag 增长/下降速度、检查点成功率、GC、磁盘和外部错误率。

存在积压时，读入速率暂时高于生产速率可能只是追赶历史，不是业务输入高峰。积压清空后观察稳定阶段，才能判断长期资源配置。

## 结束条件

输入总数和业务总额符合基准、没有未解释的侧输出、检查点和恢复稳定，且峰值后能在约定时间内追平积压，才能接受配置。只观察“CPU 高、吞吐高”不代表满足延迟和恢复目标。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：优化第1.3、5.1节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [parallel](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/execution/parallel/)
- [datagen](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/datagen/)
- [generating watermarks](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/event-time/generating_watermarks/)

前篇：[[01-Flink 2.3 内存模型、Slot与CPU预算]]

后篇：[[03-Flink 2.3 RocksDB缓存、写放大与增量快照]]
