---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 33
source_sections: "优化第7章；主教材第3、9章"
---

# Flink 2.3 故障排查与证据保留

排障先保留失败时的身份和最早异常，再确定数据正确性风险。最后一行错误常是后果，不一定是根因。

## 需要保留的最小证据

记录 Job/Application ID、Flink 与连接器版本、部署参数、最近成功检查点、失败时间、目标 subtask 和对应 TaskManager/容器。保留最早 `Caused by`，避免只有一张 Web UI 红色截图。

| 症状 | 先核验 | 后续措施 |
|---|---|---|
| 非法内存配置 | 总量和分量冲突、fraction 与上下限 | 按当前内存模型重新推导 |
| Heap OOM | 全量窗口、巨大集合、无限 key 状态 | 限定状态生命周期，分析堆 |
| Direct/native 超限 | 网络、用户直接内存、RocksDB、容器 RSS | 对应区域调预算并查泄漏 |
| Metaspace OOM | 类加载、反复部署、依赖冲突 | 类加载诊断后调整预算 |
| 网络缓冲不足 | 通道数、并行度、网络内存 | 合理预算或减少连接放大 |
| Checkpoint 失败 | 首个失败实例与四阶段耗时 | 按检查点诊断篇处理 |
| Watermark 停滞 | 空闲输入、无时间戳、慢分区 | 修正时间策略与输入瓶颈 |
| NoSuchMethod/ClassNotFound | 依赖树、重复核心包、连接器兼容 | 对齐版本和打包范围 |
| 文件描述符耗尽 | 打开文件/连接、资源关闭、RocksDB 文件量 | 修复泄漏，再评估系统上限 |
| 通信超时/心跳丢失 | GC、CPU、容器限流、网络、线程阻塞 | 修复根因后再评估超时 |

## 依赖冲突的检查路径

```bash
mvn dependency:tree -Dincludes=org.apache.flink
jar tf target/my-job.jar
```

核对实际运行 JVM 与编译目标、核心依赖是否由集群提供、连接器是否支持当前 Flink。不能为“找到类”就把多版本 Flink 核心 jar 同时塞进 lib。Shade 重定位应只针对确有冲突且允许隔离的第三方依赖。

本系列选 Java 17，避免用机器上碰巧安装的更高 JDK 自动替换。涉及 JVM 模块开放参数时，在发行包默认参数基础上补充，不应删掉默认开放列表。

## 无输出不等于没处理

先确认输入是否实际增长，再查过滤、脏数据侧输出、watermark、窗口触发、Sink pending/事务提交和消费者隔离。累计 SQL 可能需要 changelog Sink，结果不兼容时应修正协议，不是换一个打印函数了事。

## 恢复后从哪里读

确认恢复路径和已恢复状态，区分新启动 offset 与检查点 offset。动态分区未读时，查发现周期、权限与 split 分配。不要在不清楚状态归属的情况下反复删 offset、改 group 或跳过未恢复状态。

## 修复验收

在相同输入/快照条件下复现问题，验证修改后原故障消失，并做业务数量、金额和重复率对账。还要验证新的检查点可恢复，避免只把错误从处理阶段挪到恢复阶段。

本系列没有把容器启动、配置被接受或某次检查点成功写成完整生产验收；它们分别只是恢复链路中的局部证据。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：优化第7章；主教材第3、9章。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [mem trouble](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/memory/mem_trouble/)
- [checkpoint monitoring](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/monitoring/checkpoint_monitoring/)
- [metrics](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/metrics/)
- [java compatibility](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/java_compatibility/)

前篇：[[08-Flink 2.3 SQL MiniBatch、Local-Global与Distinct优化]]

后篇：[[01-Flink 2.3 核心代码运行与验证边界]]
