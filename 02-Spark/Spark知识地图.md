---
aliases:
  - Spark MOC
tags:
  - moc
  - spark
status: active
---

# Spark 知识地图

返回：[[知识库首页]]

> 本目录保留 Spark 通用执行原理；来自实际公司场景的参数、升级和 SQL 调优记录集中在 [[05-生产实践/生产实践知识地图#Spark 生产调优|生产实践区]]。

## 先理解执行过程

1. [[02-Spark/spark 运行相关的理解 （stage、task、job）|Job、Stage、Task 与 DAG]]
2. [[02-Spark/spark U I 界面讲解|Spark UI]]（待补充）
3. [[05-生产实践/Spark调优/spark 读取文件-集群资源弹性伸缩-shuffle配置|读取文件、资源伸缩与 Shuffle 配置]]

## SQL 与执行计划优化

- [[05-生产实践/Spark调优/纯SQL写法优化总结|SQL 优化总结]]
- [[05-生产实践/Spark调优/spark AQE 优化|AQE 优化]]
- [[05-生产实践/Spark调优/HashJoin和SortMergeJoin对比|Hash Join 与 Sort Merge Join]]
- [[05-生产实践/Spark调优/Spark 2.4 vs 3.2 Broadcast 行为差异|Spark 2.4 与 3.2 Broadcast 行为差异]]
- [[05-生产实践/Spark调优/spark中view的理解|Spark 中 View 的理解]]

## 文件与分区参数

- [[05-生产实践/Spark调优/参数maxPartitionBytes详解|maxPartitionBytes]]
- [[05-生产实践/Spark调优/参数openCostInBytes详解|openCostInBytes]]
- [[05-生产实践/Spark调优/Spark内存分布图|Spark 内存分布图]]（待补充）

## 版本升级

- [[05-生产实践/Spark调优/Spark2.4升级Spark3.2注意事项|Spark 2.4 升级 3.2 注意事项]]
- [[05-生产实践/Spark调优/Spark 2.4 vs 3.2 Broadcast 行为差异|Broadcast 行为差异]]

## 对比阅读

- [[03-Doris/doris的优化（和sprak的完全不同）|Doris 与 Spark 优化思路差异]]
- [[05-生产实践/SQL与数据质量/Hash + Sum 双重校验 (数据校验)|Hash + Sum 数据校验]]
