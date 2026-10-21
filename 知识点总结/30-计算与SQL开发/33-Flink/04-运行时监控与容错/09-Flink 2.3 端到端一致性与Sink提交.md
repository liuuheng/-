---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 15
source_sections: "主教材第5.4、9.2、9.3节"
---

# Flink 2.3 端到端一致性与 Sink 提交

端到端精确一次需要同时满足：输入能恢复到正确位置、Flink 内部状态与该位置一致、外部写入能处理失败重放。只开启检查点，最多说明具备内部状态一致性的基础。

## 三种“重复”需要区分

任务重启后，同一事件可能重新执行；检查点保证它不会在恢复后的内部状态里重复累加。外部已经写出的结果如果不能撤销或识别重试，仍可能重复。源系统本身发送两条内容相同但业务标识不同的消息，则还需要业务去重，检查点无法猜测它们是否同一事件。

## Sink V2 的职责

Writer 接收记录和维护待提交内容；支持提交协议的 Sink 在检查点边界产生 committable，Committer 执行外部提交及恢复后的重试。具体保证依赖连接器实现和外部系统能力。

文件输出使用 `FileSink` 与 `sinkTo`，核心示例：

```java
FileSink<String> files = FileSink
    .forRowFormat(new Path(outputUri), new SimpleStringEncoder<String>("UTF-8"))
    .withRollingPolicy(DefaultRollingPolicy.builder()
        .withRolloverInterval(Duration.ofMinutes(15))
        .withInactivityInterval(Duration.ofMinutes(5))
        .withMaxPartSize(MemorySize.ofMebiBytes(128)).build())
    .build();
lines.sinkTo(files).uid("result-files-v1");
```

流式文件输出的可见性与检查点及文件提交协议有关，应区分 in-progress、pending 和 finished。批执行结束时的最终提交不能直接拿来解释持续流式写入。

## 幂等写入必须对重放封闭

写“订单主键=1001，总额=300”可以设计成同主键覆盖；写“订单总额再加 300”重试会重复增加。前者仍要防旧版本结果晚到覆盖新值，所以常需要业务版本、源 offset 或其他确定的比较规则。

全量累计结果应按结果主键覆盖，局部增量才适合相加。仅在数据库声明唯一键不能自动修复一个错误的增量协议。

## 事务提交与可见延迟

Kafka 事务 Sink 需要开启检查点、选择 exactly-once 保证、给同时运行的作业使用不冲突的事务标识，并让下游消费者采用已提交隔离。事务超时必须覆盖检查点与可能的故障恢复等待，并受 broker 最大值约束。

这也解释了“输入已经到了，下游暂时看不到”：记录可能已写入但事务尚未提交。不能直接按 Source 读到的时间判断输出丢失。

## 当前连接器限制

2026-10-21 核验的官网 2.3 Kafka/JDBC 页面仍提示尚无对应连接器发布。此处保留协议机制，未给出冒充可直接运行的 2.3 Kafka/JDBC 依赖。JDBC 的普通批量重试也不能自动升级为事务精确一次，具体接入需查匹配版本的 Sink V2 实现。

验证必须覆盖“外部写入成功但检查点未完成”“检查点完成但提交确认丢失”两个故障窗口，再对账业务主键、版本和值。吞吐成功和重复写入不报错都不是一致性证明。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：主教材第5.4、9.2、9.3节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [sinks](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/internals/sinks/)
- [filesystem](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/filesystem/)
- [kafka](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/kafka/)
- [jdbc](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/jdbc/)

前篇：[[08-Flink 2.3 Checkpoint屏障、对齐与恢复]]

后篇：[[10-Flink 2.3 Savepoint、UID与状态演进]]
