---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 16
source_sections: "主教材第9.1.5节；优化第5.2节"
---

# Flink 2.3 Savepoint、UID 与状态演进

Checkpoint 服务于故障自动恢复，Savepoint 服务于人为控制的升级、迁移和扩缩容。两者都是状态快照，但生命周期、格式、所有权和恢复选项需要分别判断。

## 稳定标识先于升级

```java
input.keyBy(e -> e.eventId)
     .process(new Dedup())
     .name("event-dedup")
     .uid("event-dedup-v1")
     .setMaxParallelism(128);
```

`name` 用于展示，`uid` 用于稳定关联算子状态。发布时随机生成 UID 会让新作业无法找到旧状态；把 UID 保持不变也不能自动修复不兼容的状态 schema 或已经改变含义的 key。

## 计划内停止与恢复

```bash
bin/flink stop --savepointPath s3://example-bucket/flink/savepoints JOB_ID
bin/flink run -s s3://example-bucket/flink/savepoints/savepoint-xxx \
  -c notes.flink.MyJob my-job.jar
```

以上为操作模板，不是本次已执行命令。停止时是否 drain 要依据业务判断：drain 会推进事件时间，可能触发窗口和定时器；无意 drain 后再继续接同一无界业务，可能改变窗口生命周期。不要在需要原样续跑时机械追加该选项。

## 格式和所有权

Canonical savepoint 强调可移植的规范格式；native savepoint 更依赖后端，可能更快，但迁移范围不同。后端切换前需确认格式与目标后端支持，不能把“任意 savepoint 都可跨后端”当作通则。

Restore mode 涉及快照文件所有权和后续清理。共享同一快照做多次验证时，应避免让某个实验作业取得会影响其他使用者的文件清理权。

## 序列化演进是恢复协议

状态名、数据类型、序列化器和 key 结构共同决定可恢复性。POJO 增删字段与修改字段类型不是同一种变化；通用 Kryo 类型不能随意承诺 schema 演进。修改 key 可能把原本不同实体合并为一个，运行时无法自动决定如何合并其历史状态。

扩缩容还要求新并行度处于最大并行度允许范围内，并保持算子身份及 key group 兼容。扩容后输入分布更均匀并不意味着历史热 key 自动被拆成多个独立 key。

## 不能把跳过状态当作修复

允许未恢复状态的选项会跳过部分旧状态，只适合业务已确认可以放弃的算子。对于去重、账务累计和待匹配订单，它可能直接造成重复处理或丢失历史。

发布前用真实输入副本和真实 savepoint 演练：恢复成功、输出对账、延迟稳定、再触发新快照并可二次恢复。仅通过编译或看到 RUNNING，不足以证明升级正确。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：主教材第9.1.5节；优化第5.2节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [savepoints](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/savepoints/)
- [schema evolution](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/serialization/schema_evolution/)
- [upgrading](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/upgrading/)

前篇：[[09-Flink 2.3 端到端一致性与Sink提交]]

后篇：[[10-Flink 2.3 两阶段Top-N与排名修正]]
