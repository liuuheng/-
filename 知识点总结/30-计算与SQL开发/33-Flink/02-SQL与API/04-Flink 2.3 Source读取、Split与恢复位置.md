---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 4
source_sections: "主教材第5.2节；优化第5.1、7.9节"
---

# Flink 2.3 Source 读取、Split 与恢复位置

Source 不只是“循环读数据”。可恢复的 Source 必须知道有哪些读取任务、每个任务分配给谁、已经读到哪里；缺少其中任何一项，重启后都可能漏读或重读。

## 新 Source 的职责划分

`SplitEnumerator` 在协调侧发现并分配 split；`SourceReader` 在 TaskManager 读取已分配 split；split 是一个可恢复的工作单元。文件 split 可以表示文件片段，消息 split 常对应分区及读取进度。

Enumerator 快照保存待分配任务和发现进度；Reader 快照保存已分配任务及当前位置。读取 offset 必须和下游状态来自同一个已完成检查点，才能恢复到一致的逻辑边界。

## 文件源和 DataGen 的核心写法

```java
FileSource<String> files = FileSource
    .forRecordStreamFormat(new TextLineInputFormat(), new Path(inputUri))
    .build();
DataStream<String> lines = env.fromSource(
    files, WatermarkStrategy.noWatermarks(), "input-files");

DataGeneratorSource<Long> generated = new DataGeneratorSource<>(
    index -> index, 1000L, Types.LONG);
DataStream<Long> ids = env.fromSource(
    generated, WatermarkStrategy.noWatermarks(), "finite-datagen");
```

依赖分别为 `flink-connector-files` 和 `flink-connector-datagen`，本系列使用 2.3.0。文件 URI 必须对实际读取它的 TaskManager 可见；客户端本地路径不会自动变成整个集群共享路径。

有限 DataGen 适合结果校验；吞吐压测还需要显式限速、合理 key 分布和足够运行时间。无限随机数且没有对账基准，只能验证程序在跑，不能证明统计正确。

## KafkaSource 需要区分初始配置和恢复状态

Kafka 读取使用 `KafkaSource` 和 `env.fromSource(...)`，核心配置包括 broker、topic、group、反序列化器和初始 offset。新作业的初始 offset 配置决定第一次从哪里开始；从检查点恢复时，通常以已恢复的 split offset 为准，不能认为修改为 earliest 就必然重新读全量。

分区动态发现由 `partition.discovery.interval.ms` 控制。扩分区后还要确认 Enumerator 发现、Reader 分配和新分区 lag，不能只看订阅表达式写了 topic。

**连接器边界（2026-10-21 核验）：** 官方 2.3 Kafka 文档提示尚无对应发布连接器；下载页列出的 Kafka Connector 5.0.0 兼容范围为 2.1.x、2.2.x。本系列不编造 2.3 连接器版本，也不把跨版本 jar 混入核心实验。该机制保留为接入设计，兼容包发布后再验证完整链路。

## 常见的恢复误判

Kafka group offset 便于观察消费进度，但它不是 Flink 算子状态。删除 group offset 不能替代正确清理作业状态；只改 groupId 也不能解释 savepoint 恢复后的全部行为。

自定义 Source 的 `pollNext()` 应能快速返回，阻塞 I/O 应交给受控读取线程和可用性通知。永远占用任务线程会拖住检查点与取消操作。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：主教材第5.2节；优化第5.1、7.9节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [sources](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/internals/sources/)
- [datagen](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/datagen/)
- [kafka](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/kafka/)
- [downloads](https://flink.apache.org/downloads/)

前篇：[[05-Flink 2.3 执行图、算子链与Slot]]

后篇：[[05-Flink 2.3 转换、分区与数据质量]]
