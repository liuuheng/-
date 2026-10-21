---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 13
source_sections: "主教材第8.4节；优化第2.1节"
---

# Flink 2.3 状态后端与异步状态访问

状态后端决定运行中如何保存和访问状态，检查点存储决定快照持久化到哪里。本地 RocksDB 目录和远端检查点目录承担不同职责，不能互相替代。

## 三种后端的选择依据

HashMap 后端把状态对象放在 JVM 堆上，访问直接，容量受堆与 GC 制约；Embedded RocksDB 把状态序列化后存入本地 LSM 数据库，支持更大状态，但增加序列化和磁盘访问成本；ForSt 将 SST 放到远端文件系统并使用本地缓存，适合探索存算分离。

官方 2.3 文档仍将 ForSt 标为实验阶段，并列出 canonical savepoint 等能力限制。因此本系列可运行基础实验采用成熟的同步状态接口，ForSt/异步状态作为明确标注的进阶机制，不把它写成默认生产选型。

```yaml
# conf/config.yaml；示例数值需按负载实测
state.backend.type: rocksdb
execution.checkpointing.storage: filesystem
execution.checkpointing.dir: s3://example-bucket/flink/checkpoints
execution.checkpointing.incremental: true
taskmanager.memory.managed.fraction: 0.4
state.backend.rocksdb.memory.managed: true
```

对象存储 URI 还要求正确安装文件系统插件、网络权限与凭据。仅写入目录字符串不能证明远端快照已成功保存。

## 同步快照与同步状态访问不是一回事

RocksDB 日常 `get/put` 会在处理记录路径上发生；其检查点异步上传不代表业务访问天然异步。诊断时要区分“单条状态读取慢”和“快照文件上传慢”。

增量检查点复用已有的 SST 文件，只上传新的共享文件及必要元数据。它不是逐条输出业务增量，也不意味着恢复只读最近一次新增文件；恢复仍需完整状态依赖。

## State V2 改变访问方式

异步状态接口需要在 keyed 流上启用 `enableAsyncState()`，并使用 `org.apache.flink.api.common.state.v2` 下的状态。读取返回 `StateFuture`，后续逻辑通过回调组合；不要在任务线程阻塞等结果，也不要在同一函数中随意混合同步与异步状态访问。

其目标是让远端状态 I/O 与其他记录处理重叠。不同 key 的记录处理可能交错，因此不能把“当前事件”存在一个普通实例成员里，再在异步回调中读取该成员。每条记录需要的上下文应由回调正确捕获。

## 怎样做后端对比

保持相同输入、key 基数和状态结构，比较吞吐、P99 状态延迟、GC、磁盘 IOPS、检查点耗时与恢复时间。小数据内存实验不能证明 RocksDB 慢到不适用，单次峰值吞吐也不能证明远端状态恢复更好。

状态 schema 是长期协议。POJO 字段新增删除、字段类型变化、key 变化的兼容性不同；上线应从真实快照演练恢复，不能只凭两边 Java 类名相同判断兼容。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：主教材第8.4节；优化第2.1节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [state backends](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/state_backends/)
- [state v2](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/state_v2/)
- [schema evolution](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/serialization/schema_evolution/)

前篇：[[06-Flink 2.3 算子状态与广播规则一致性]]

后篇：[[08-Flink 2.3 Checkpoint屏障、对齐与恢复]]
