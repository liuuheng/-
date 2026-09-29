---
aliases:
  - Flink MOC
tags:
  - moc
  - flink
status: active
---

# Flink 知识地图

返回：[[知识库首页]]

## 推荐学习顺序

1. [[60-工程基础/61-Java与依赖管理/Flink开发Java基础/README|Flink 学习前的 Java 基础]]
2. [[30-计算与SQL开发/33-Flink/核心概念/执行算子和分区策略|执行算子和分区策略]]
3. [[30-计算与SQL开发/33-Flink/核心概念/算子链 Operator Chain|算子链 Operator Chain]]
4. [[30-计算与SQL开发/33-Flink/核心概念/水位线 Watermark|水位线 Watermark]]
5. [[50-质量与运行保障/54-集群部署与版本升级/Flink部署/README|Flink 本地开发与 Docker 集群部署]]
6. [[20-数据集成与调度/22-CDC与Schema演进/Flink CDC/具体的流程|Flink CDC 到 Hive 的处理流程]]

## Java 基础

完整课程和顺序见 [[60-工程基础/61-Java与依赖管理/Flink开发Java基础/README|Java 基础知识地图]]。其中与 Flink 联系最紧密的是：

- [[60-工程基础/61-Java与依赖管理/Flink开发Java基础/03-Lambda、函数式接口与方法引用|Lambda、函数式接口与方法引用]]
- [[60-工程基础/61-Java与依赖管理/Flink开发Java基础/04-泛型、类型擦除与Flink类型信息|泛型、类型擦除与 Flink 类型信息]]
- [[60-工程基础/61-Java与依赖管理/Flink开发Java基础/06-POJO、JavaBean、Tuple与序列化|数据类型与序列化]]
- [[60-工程基础/61-Java与依赖管理/Flink开发Java基础/09-并发、线程安全与Flink状态意识|并发、线程安全与状态意识]]
- [[60-工程基础/61-Java与依赖管理/Flink开发Java基础/11-综合练习-订单实时统计|订单实时统计综合练习]]

## 运行与部署

- [[50-质量与运行保障/54-集群部署与版本升级/Flink本地数仓集群|Flink 本地数仓集群]]
- [[50-质量与运行保障/54-集群部署与版本升级/Flink部署/03-Flink作业提交与集群角色|作业提交与集群角色]]
- [[50-质量与运行保障/54-集群部署与版本升级/Flink部署/04-Docker网络与地址选择|Docker 网络与地址选择]]
- [[60-工程基础/61-Java与依赖管理/Flink工程化/05-thin-JAR-fat-JAR与作业拆分策略|thin JAR、fat JAR 与作业拆分]]
- [[50-质量与运行保障/54-集群部署与版本升级/Flink部署/06-Flink01-Parallelism本地与Docker运行实战|并行度运行实战]]
- [[50-质量与运行保障/54-集群部署与版本升级/Flink部署/07-常见错误排查手册|常见错误排查]]
- [[50-质量与运行保障/54-集群部署与版本升级/Flink部署/08-命令速查表|命令速查表]]

## 架构延伸

- [[10-业务与数仓设计/14-需求沟通与方案设计/架构方案/flink-fluss-paimon-doris-架构补齐方案|Flink、Fluss、Paimon、Doris 架构]]
- [[50-质量与运行保障/54-集群部署与版本升级/00-集群基建知识地图|集群基建知识地图]]
- [[40-存储与查询服务/42-Doris/00-Doris知识地图|Doris 知识地图]]
