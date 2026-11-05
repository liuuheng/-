# Flink 导航

[[知识点总结/00-知识库导航|知识库导航]]

按学习阶段进入阅读路线，遇到问题时从排查入口查阅；完整笔记按现有十个主题列在下方。

`06-Java基础` 是面向 Flink 1.17 的开发前置路线，`07-Java生态常用类` 也保留了 1.17 项目的上下文。标题带 Flink 2.3 的笔记属于另一套系列。其他笔记以及 CDC Pipeline 内容，按各页标注核对 Flink、JDK、连接器和示例项目版本。

## 学习路线

1. Java 前置从 [[知识点总结/30-计算与SQL开发/33-Flink/06-Java基础/01-Flink 1.17 学习前的 Java 基础|Flink 开发前置 Java]] 开始，按其中的阅读顺序完成语言、类型信息、序列化与依赖管理练习。
2. 学习 Flink 2.3 时，先读 [[知识点总结/30-计算与SQL开发/33-Flink/01-核心概念/04-Flink 2.3 流批执行与有状态计算|流批执行与有状态计算]]、[[知识点总结/30-计算与SQL开发/33-Flink/01-核心概念/05-Flink 2.3 执行图、算子链与Slot|执行图与 Slot]] 和 [[知识点总结/30-计算与SQL开发/33-Flink/01-核心概念/06-Flink 2.3 事件时间与Watermark推进|事件时间与 Watermark]]。
3. SQL 开发从 [[知识点总结/30-计算与SQL开发/33-Flink/02-SQL与API/11-Flink 2.3 动态表、Changelog与流表转换|动态表与 Changelog]]、[[知识点总结/30-计算与SQL开发/33-Flink/02-SQL与API/12-Flink 2.3 SQL建表、时间属性与时区|SQL 建表与时间属性]] 进入，再按聚合、Join、UDF 等任务选择下方条目。
4. 需要保存和恢复状态时，结合运行时监控与容错主题阅读；部署代码从 [[知识点总结/30-计算与SQL开发/33-Flink/05-部署与工程化/10-Flink 2.3 Java项目与最小作业|Java 项目与最小作业]] 进入，实验范围见 [[知识点总结/30-计算与SQL开发/33-Flink/10-代码实验/01-Flink 2.3 核心代码运行与验证边界|核心代码运行与验证边界]]。

## 排查入口

- 提交失败、依赖缺类或本地环境异常：[[知识点总结/30-计算与SQL开发/33-Flink/05-部署与工程化/04-Flink 常见错误排查手册|常见错误排查手册]]、[[知识点总结/30-计算与SQL开发/33-Flink/05-部署与工程化/08-Maven依赖scope与classpath|Maven 依赖与 classpath 排查]]。
- 任务变慢或反压：[[知识点总结/30-计算与SQL开发/33-Flink/09-性能优化/05-Flink 2.3 反压、火焰图与外部IO瓶颈|反压与外部 IO 瓶颈]]、[[知识点总结/30-计算与SQL开发/33-Flink/09-性能优化/09-Flink 2.3 故障排查与证据保留|故障排查与证据保留]]。
- Checkpoint 慢或恢复异常：[[知识点总结/30-计算与SQL开发/33-Flink/09-性能优化/04-Flink 2.3 Checkpoint慢的分阶段诊断|Checkpoint 分阶段诊断]]、[[知识点总结/30-计算与SQL开发/33-Flink/04-运行时监控与容错/08-Flink 2.3 Checkpoint屏障、对齐与恢复|Checkpoint 屏障与恢复]]、[[知识点总结/30-计算与SQL开发/33-Flink/04-运行时监控与容错/10-Flink 2.3 Savepoint、UID与状态演进|Savepoint 与状态演进]]。
- CDC 结构变更、任务恢复和历史回补：先看 [[知识点总结/20-数据集成与调度/22-CDC与Schema演进/02-数据同步中的 Schema 变更、任务恢复与历史回补|同步流程与回补边界]]，再查下方 Flink CDC 实现笔记。
- 复习与面试：[[知识点总结/80-资料与复习/81-面试知识库/00-面试导航|面试导航]]、[[知识点总结/80-资料与复习/83-复习卡片/Flink/01-任务槽共享组 Slot Sharing Group|Slot Sharing Group 复习卡片]]。

## 按主题查阅

### 01 核心概念

执行模型、算子链、Slot 和事件时间。

- [[知识点总结/30-计算与SQL开发/33-Flink/01-核心概念/01-执行算子和分区策略|执行算子和分区策略]]
- [[知识点总结/30-计算与SQL开发/33-Flink/01-核心概念/02-水位线 Watermark|水位线 Watermark]]
- [[知识点总结/30-计算与SQL开发/33-Flink/01-核心概念/03-算子链 Operator Chain|算子链 Operator Chain]]
- [[知识点总结/30-计算与SQL开发/33-Flink/01-核心概念/04-Flink 2.3 流批执行与有状态计算|Flink 2.3 流批执行与有状态计算]]
- [[知识点总结/30-计算与SQL开发/33-Flink/01-核心概念/05-Flink 2.3 执行图、算子链与Slot|Flink 2.3 执行图、算子链与Slot]]
- [[知识点总结/30-计算与SQL开发/33-Flink/01-核心概念/06-Flink 2.3 事件时间与Watermark推进|Flink 2.3 事件时间与Watermark推进]]

### 02 SQL与API

SQL 与 DataStream 的使用入口，包含窗口、Join、Changelog 和作业组织。

- [[知识点总结/30-计算与SQL开发/33-Flink/02-SQL与API/01-Flink SQL：时间、窗口、Join与动态表|Flink SQL：时间、窗口、Join与动态表]]
- [[知识点总结/30-计算与SQL开发/33-Flink/02-SQL与API/02-Flink API 命名前缀与类名后缀|Flink API 命名前缀与类名后缀]]
- [[知识点总结/30-计算与SQL开发/33-Flink/02-SQL与API/03-Flink WatermarkStrategy 创建与使用|Flink WatermarkStrategy 创建与使用]]
- [[知识点总结/30-计算与SQL开发/33-Flink/02-SQL与API/04-Flink 2.3 Source读取、Split与恢复位置|Flink 2.3 Source读取、Split与恢复位置]]
- [[知识点总结/30-计算与SQL开发/33-Flink/02-SQL与API/05-Flink 2.3 转换、分区与数据质量|Flink 2.3 转换、分区与数据质量]]
- [[知识点总结/30-计算与SQL开发/33-Flink/02-SQL与API/06-Flink 2.3 窗口分配与增量聚合|Flink 2.3 窗口分配与增量聚合]]
- [[知识点总结/30-计算与SQL开发/33-Flink/02-SQL与API/07-Flink 2.3 迟到处理与窗口结果修正|Flink 2.3 迟到处理与窗口结果修正]]
- [[知识点总结/30-计算与SQL开发/33-Flink/02-SQL与API/08-Flink 2.3 ProcessFunction与可恢复定时器|Flink 2.3 ProcessFunction与可恢复定时器]]
- [[知识点总结/30-计算与SQL开发/33-Flink/02-SQL与API/09-Flink 2.3 双流Join的时间与状态边界|Flink 2.3 双流Join的时间与状态边界]]
- [[知识点总结/30-计算与SQL开发/33-Flink/02-SQL与API/10-Flink 2.3 两阶段Top-N与排名修正|Flink 2.3 两阶段Top-N与排名修正]]
- [[知识点总结/30-计算与SQL开发/33-Flink/02-SQL与API/11-Flink 2.3 动态表、Changelog与流表转换|Flink 2.3 动态表、Changelog与流表转换]]
- [[知识点总结/30-计算与SQL开发/33-Flink/02-SQL与API/12-Flink 2.3 SQL建表、时间属性与时区|Flink 2.3 SQL建表、时间属性与时区]]
- [[知识点总结/30-计算与SQL开发/33-Flink/02-SQL与API/13-Flink 2.3 SQL聚合、窗口TVF与去重排名|Flink 2.3 SQL聚合、窗口TVF与去重排名]]
- [[知识点总结/30-计算与SQL开发/33-Flink/02-SQL与API/14-Flink 2.3 SQL Join选型与历史维度一致性|Flink 2.3 SQL Join选型与历史维度一致性]]
- [[知识点总结/30-计算与SQL开发/33-Flink/02-SQL与API/15-Flink 2.3 UDF、Catalog与SQL作业组织|Flink 2.3 UDF、Catalog与SQL作业组织]]
- [[知识点总结/30-计算与SQL开发/33-Flink/02-SQL与API/16-Flink 2.3 PTF与物化表的职责边界|Flink 2.3 PTF与物化表的职责边界]]

### 03 CDC与Schema演进

Flink CDC 的流程与配置实现；跨组件同步决策通过上方的回补入口查阅。

- [[知识点总结/30-计算与SQL开发/33-Flink/03-CDC与Schema演进/01-Flink CDC Pipeline 3.6 完整流程|Flink CDC Pipeline 3.6 完整流程]]
- [[知识点总结/30-计算与SQL开发/33-Flink/03-CDC与Schema演进/02-Flink CDC Pipeline 动态多表 Hive Parquet Sink|Flink CDC Pipeline 动态多表 Hive Parquet Sink]]
- [[知识点总结/30-计算与SQL开发/33-Flink/03-CDC与Schema演进/03-Flink CDC Pipeline YAML 完整指南：配置、路由、分库分表、条件过滤与 Schema 演进|Flink CDC Pipeline YAML 完整指南：配置、路由、分库分表、条件过滤与 Schema 演进]]
- [[知识点总结/30-计算与SQL开发/33-Flink/03-CDC与Schema演进/04-Flink CDC Pipeline YAML：结构、路由、分库分表与边界条件|Flink CDC Pipeline YAML：结构、路由、分库分表与边界条件]]
- [[知识点总结/30-计算与SQL开发/33-Flink/03-CDC与Schema演进/05-Flink CDC 到 Hive：变更日志与每日快照合并|Flink CDC 到 Hive：变更日志与每日快照合并]]

### 04 运行时监控与容错

指标、状态、Checkpoint、恢复和端到端一致性。

- [[知识点总结/30-计算与SQL开发/33-Flink/04-运行时监控与容错/01-Flink运行时：Checkpoint、部署与Source API|Flink运行时：Checkpoint、部署与Source API]]
- [[知识点总结/30-计算与SQL开发/33-Flink/04-运行时监控与容错/02-Flink CheckpointConfig 参数速查|Flink CheckpointConfig 参数速查]]
- [[知识点总结/30-计算与SQL开发/33-Flink/04-运行时监控与容错/03-Flink Metrics 指标体系|Flink Metrics 指标体系]]
- [[知识点总结/30-计算与SQL开发/33-Flink/04-运行时监控与容错/04-Flink P0 任务故障恢复 SOP 与适用边界|Flink P0 任务故障恢复 SOP 与适用边界]]
- [[知识点总结/30-计算与SQL开发/33-Flink/04-运行时监控与容错/05-Flink 2.3 Keyed State、去重与TTL|Flink 2.3 Keyed State、去重与TTL]]
- [[知识点总结/30-计算与SQL开发/33-Flink/04-运行时监控与容错/06-Flink 2.3 算子状态与广播规则一致性|Flink 2.3 算子状态与广播规则一致性]]
- [[知识点总结/30-计算与SQL开发/33-Flink/04-运行时监控与容错/07-Flink 2.3 状态后端与异步状态访问|Flink 2.3 状态后端与异步状态访问]]
- [[知识点总结/30-计算与SQL开发/33-Flink/04-运行时监控与容错/08-Flink 2.3 Checkpoint屏障、对齐与恢复|Flink 2.3 Checkpoint屏障、对齐与恢复]]
- [[知识点总结/30-计算与SQL开发/33-Flink/04-运行时监控与容错/09-Flink 2.3 端到端一致性与Sink提交|Flink 2.3 端到端一致性与Sink提交]]
- [[知识点总结/30-计算与SQL开发/33-Flink/04-运行时监控与容错/10-Flink 2.3 Savepoint、UID与状态演进|Flink 2.3 Savepoint、UID与状态演进]]

### 05 部署与工程化

本地开发、集群提交、依赖打包与项目排查。

- [[知识点总结/30-计算与SQL开发/33-Flink/05-部署与工程化/01-Flink 本地开发与 Docker 集群部署|Flink 本地开发与 Docker 集群部署]]
- [[知识点总结/30-计算与SQL开发/33-Flink/05-部署与工程化/02-Flink作业提交与集群角色|Flink作业提交与集群角色]]
- [[知识点总结/30-计算与SQL开发/33-Flink/05-部署与工程化/03-Flink01-Parallelism本地与Docker运行实战|Flink01-Parallelism本地与Docker运行实战]]
- [[知识点总结/30-计算与SQL开发/33-Flink/05-部署与工程化/04-Flink 常见错误排查手册|Flink 常见错误排查手册]]
- [[知识点总结/30-计算与SQL开发/33-Flink/05-部署与工程化/05-Flink 与 Docker 命令速查表|Flink 与 Docker 命令速查表]]
- [[知识点总结/30-计算与SQL开发/33-Flink/05-部署与工程化/06-Flink 本地数仓集群：服务地址、启停命令与数据链路|Flink 本地数仓集群：服务地址、启停命令与数据链路]]
- [[知识点总结/30-计算与SQL开发/33-Flink/05-部署与工程化/07-Java源码到JAR再到JVM运行|Java源码到JAR再到JVM运行]]
- [[知识点总结/30-计算与SQL开发/33-Flink/05-部署与工程化/08-Maven依赖scope与classpath|Maven依赖scope与classpath]]
- [[知识点总结/30-计算与SQL开发/33-Flink/05-部署与工程化/09-thin-JAR-fat-JAR与作业拆分策略|thin-JAR-fat-JAR与作业拆分策略]]
- [[知识点总结/30-计算与SQL开发/33-Flink/05-部署与工程化/10-Flink 2.3 Java项目与最小作业|Flink 2.3 Java项目与最小作业]]
- [[知识点总结/30-计算与SQL开发/33-Flink/05-部署与工程化/11-Flink 2.3 集群部署与Application生命周期|Flink 2.3 集群部署与Application生命周期]]

### 06 Java基础

面向 Flink 1.17 的 Java 前置课程，保留原有教学顺序和自测。

- [[知识点总结/30-计算与SQL开发/33-Flink/06-Java基础/01-Flink 1.17 学习前的 Java 基础|Flink 1.17 学习前的 Java 基础]]
- [[知识点总结/30-计算与SQL开发/33-Flink/06-Java基础/02-类、对象、封装与不可变性|类、对象、封装与不可变性]]
- [[知识点总结/30-计算与SQL开发/33-Flink/06-Java基础/03-接口、抽象类、继承与多态|接口、抽象类、继承与多态]]
- [[知识点总结/30-计算与SQL开发/33-Flink/06-Java基础/04-Lambda、函数式接口与方法引用|Lambda、函数式接口与方法引用]]
- [[知识点总结/30-计算与SQL开发/33-Flink/06-Java基础/05-泛型、类型擦除与Flink类型信息|泛型、类型擦除与Flink类型信息]]
- [[知识点总结/30-计算与SQL开发/33-Flink/06-Java基础/06-集合、Stream-API与DataStream的区别|集合、Stream-API与DataStream的区别]]
- [[知识点总结/30-计算与SQL开发/33-Flink/06-Java基础/07-POJO、JavaBean、Tuple与序列化|POJO、JavaBean、Tuple与序列化]]
- [[知识点总结/30-计算与SQL开发/33-Flink/06-Java基础/08-异常处理、脏数据与资源管理|异常处理、脏数据与资源管理]]
- [[知识点总结/30-计算与SQL开发/33-Flink/06-Java基础/09-内部类、static、final与闭包捕获|内部类、static、final与闭包捕获]]
- [[知识点总结/30-计算与SQL开发/33-Flink/06-Java基础/10-并发、线程安全与Flink状态意识|并发、线程安全与Flink状态意识]]
- [[知识点总结/30-计算与SQL开发/33-Flink/06-Java基础/11-Maven、JAR、classpath与依赖作用域|Maven、JAR、classpath与依赖作用域]]
- [[知识点总结/30-计算与SQL开发/33-Flink/06-Java基础/12-综合练习-订单实时统计|综合练习-订单实时统计]]
- [[知识点总结/30-计算与SQL开发/33-Flink/06-Java基础/13-复习检查表|复习检查表]]
- [[知识点总结/30-计算与SQL开发/33-Flink/06-Java基础/14-容易漏掉但高频的Java细节|容易漏掉但高频的Java细节]]
- [[知识点总结/30-计算与SQL开发/33-Flink/06-Java基础/15-掌握度测试题与详细解析|掌握度测试题与详细解析]]

### 07 Java生态常用类

结合 Flink 项目学习配置、日志、JSON、外部连接和测试。

- [[知识点总结/30-计算与SQL开发/33-Flink/07-Java生态常用类/01-Flink日常开发中的Java生态类总览|Flink日常开发中的Java生态类总览]]
- [[知识点总结/30-计算与SQL开发/33-Flink/07-Java生态常用类/02-参数配置与日志生产实践|参数配置与日志生产实践]]
- [[知识点总结/30-计算与SQL开发/33-Flink/07-Java生态常用类/03-时间金额JSON与数据质量|时间金额JSON与数据质量]]
- [[知识点总结/30-计算与SQL开发/33-Flink/07-Java生态常用类/04-异步IO外部连接与并发安全|异步IO外部连接与并发安全]]
- [[知识点总结/30-计算与SQL开发/33-Flink/07-Java生态常用类/05-依赖管理序列化测试与上线检查|依赖管理序列化测试与上线检查]]

### 08 参考手册

查阅常用术语。

- [[知识点总结/30-计算与SQL开发/33-Flink/08-参考手册/01-Flink 常用英文术语表|Flink 常用英文术语表]]

### 09 性能优化

Flink 2.3 的资源规划、反压、Checkpoint、倾斜与 SQL 优化。

- [[知识点总结/30-计算与SQL开发/33-Flink/09-性能优化/01-Flink 2.3 内存模型、Slot与CPU预算|Flink 2.3 内存模型、Slot与CPU预算]]
- [[知识点总结/30-计算与SQL开发/33-Flink/09-性能优化/02-Flink 2.3 并行度规划与可对账压测|Flink 2.3 并行度规划与可对账压测]]
- [[知识点总结/30-计算与SQL开发/33-Flink/09-性能优化/03-Flink 2.3 RocksDB缓存、写放大与增量快照|Flink 2.3 RocksDB缓存、写放大与增量快照]]
- [[知识点总结/30-计算与SQL开发/33-Flink/09-性能优化/04-Flink 2.3 Checkpoint慢的分阶段诊断|Flink 2.3 Checkpoint慢的分阶段诊断]]
- [[知识点总结/30-计算与SQL开发/33-Flink/09-性能优化/05-Flink 2.3 反压、火焰图与外部IO瓶颈|Flink 2.3 反压、火焰图与外部IO瓶颈]]
- [[知识点总结/30-计算与SQL开发/33-Flink/09-性能优化/06-Flink 2.3 数据倾斜与两阶段聚合正确性|Flink 2.3 数据倾斜与两阶段聚合正确性]]
- [[知识点总结/30-计算与SQL开发/33-Flink/09-性能优化/07-Flink 2.3 对象复用、滑动窗口与延迟测量|Flink 2.3 对象复用、滑动窗口与延迟测量]]
- [[知识点总结/30-计算与SQL开发/33-Flink/09-性能优化/08-Flink 2.3 SQL MiniBatch、Local-Global与Distinct优化|Flink 2.3 SQL MiniBatch、Local-Global与Distinct优化]]
- [[知识点总结/30-计算与SQL开发/33-Flink/09-性能优化/09-Flink 2.3 故障排查与证据保留|Flink 2.3 故障排查与证据保留]]

### 10 代码实验

代码位置、运行方式和已记录的验证范围。

- [[知识点总结/30-计算与SQL开发/33-Flink/10-代码实验/01-Flink 2.3 核心代码运行与验证边界|Flink 2.3 核心代码运行与验证边界]]
