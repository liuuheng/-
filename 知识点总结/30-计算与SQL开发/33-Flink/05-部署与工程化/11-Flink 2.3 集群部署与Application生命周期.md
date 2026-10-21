---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 24
source_sections: "主教材第3章；优化第1章"
---

# Flink 2.3 集群部署与 Application 生命周期

部署时要分别确定资源提供者、集群生命周期和应用入口。Standalone、YARN、Kubernetes 是资源环境；Session 与 Application 描述资源共享和应用生命周期，两组概念不在同一层。

## 当前两条主要部署路径

Session 先有集群，再提交多个应用；资源共享便于开发和轻任务，但依赖、负载和故障隔离需要治理。Application 为应用创建对应集群，适合希望独立管理依赖、资源和生命周期的生产作业。

```bash
# YARN Application，示例路径需替换为实际可访问文件
bin/flink run -t yarn-application \
  -Djobmanager.memory.process.size=2g \
  -Dtaskmanager.memory.process.size=4g \
  -Dtaskmanager.numberOfTaskSlots=2 \
  -p 4 -c com.example.OrderJob order-job.jar
```

YARN 配置、Hadoop classpath、远端文件系统和队列资源均要准备好。示例未在本机启动 YARN 集群；不要把它与已通过的本地核心实验混为一谈。

## main 在哪里执行

Application 模式把用户 main 放到集群侧；Session 的常见 CLI 提交流程由客户端执行 main 并提交 job。main 负责组织作业，不承担持续处理每条消息的任务线程工作。

JobManager 协调应用与作业、调度和检查点；TaskManager 提供任务运行资源。客户端命令退出不必然意味着作业停止，具体取决于部署模式和 attached/detached 行为。

## Flink 2.3 的应用层级

当前使用 Cluster—Application—Job 层级，一个 Application 可以关联零到多个 Job。诊断“应用结束但某个提交调用还在等待”时，要核对 main 执行状态与其关联作业，而不能只拿单个 JobID 代替整个应用生命周期。

Application 的失败、取消和结束会影响其关联作业；Web UI 和 REST 也提供相应应用信息。这是新版新增的观察层，旧教材只有 cluster/job 的图不够解释完整生命周期。

## 配置文件使用 config.yaml

```yaml
jobmanager.rpc.address: jobmanager
rest.port: 8081
taskmanager.numberOfTaskSlots: 2
parallelism.default: 2
jobmanager.memory.process.size: 2g
taskmanager.memory.process.size: 4g
restart-strategy.type: exponential-delay
```

该文件遵循标准 YAML，重复键等问题会在加载时暴露。容器场景同时检查宿主机端口和容器内部地址；TaskManager 用容器内可达的 JobManager 地址，浏览器则使用发布到宿主的 REST 地址。

Kubernetes 模式还需准备镜像、ServiceAccount/RBAC、持久化检查点、资源请求和限制。资源被容器平台杀死时先看容器事件及内存限制，不能只等 Java 抛出堆 OOM。

## 历史服务与高可用分工

History Server 从归档信息展示已结束作业，不负责继续执行旧任务，也不替代 HA 和持久化状态。生产恢复还需要可靠检查点、作业恢复元数据以及资源环境的重建能力。

部署验收顺序是：任务和 slot 就绪、固定输入作业结果正确、检查点成功、故障可恢复、重启后服务与持久数据仍可用。Web UI 能打开只完成了第一层可见性验证。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：主教材第3章；优化第1章。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [yarn](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/resource-providers/yarn/)
- [native kubernetes](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/resource-providers/native_kubernetes/)
- [config](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/config/)
- [application lifecycle](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/internals/application_lifecycle/)

前篇：[[16-Flink 2.3 PTF与物化表的职责边界]]

后篇：[[01-Flink 2.3 内存模型、Slot与CPU预算]]
