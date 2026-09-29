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

> 本目录保留 Spark 通用执行原理；来自实际公司场景的参数、升级和 SQL 调优记录集中在 [[00-首页/导航-生产实践#Spark 生产调优|生产实践区]]。

## 先理解执行过程

1. [[30-计算与SQL开发/32-Spark/spark 运行相关的理解 （stage、task、job）|Job、Stage、Task 与 DAG]]
2. [[30-计算与SQL开发/32-Spark/spark U I 界面讲解|Spark UI]]（待补充）
3. [[30-计算与SQL开发/32-Spark/性能优化/spark 读取文件-集群资源弹性伸缩-shuffle配置|读取文件、资源伸缩与 Shuffle 配置]]

## SQL 与执行计划优化

- [[30-计算与SQL开发/32-Spark/性能优化/纯SQL写法优化总结|SQL 优化总结]]
- [[30-计算与SQL开发/32-Spark/性能优化/spark AQE 优化|AQE 优化]]
- [[30-计算与SQL开发/32-Spark/性能优化/HashJoin和SortMergeJoin对比|Hash Join 与 Sort Merge Join]]
- [[30-计算与SQL开发/32-Spark/性能优化/Spark 2.4 vs 3.2 Broadcast 行为差异|Spark 2.4 与 3.2 Broadcast 行为差异]]
- [[30-计算与SQL开发/32-Spark/性能优化/spark中view的理解|Spark 中 View 的理解]]

## 文件与分区参数

- [[30-计算与SQL开发/32-Spark/性能优化/参数maxPartitionBytes详解|maxPartitionBytes]]
- [[30-计算与SQL开发/32-Spark/性能优化/参数openCostInBytes详解|openCostInBytes]]
- [[30-计算与SQL开发/32-Spark/性能优化/Spark内存分布图|Spark 内存分布图]]（待补充）

## 版本升级

- [[30-计算与SQL开发/32-Spark/性能优化/Spark2.4升级Spark3.2注意事项|Spark 2.4 升级 3.2 注意事项]]
- [[30-计算与SQL开发/32-Spark/性能优化/Spark 2.4 vs 3.2 Broadcast 行为差异|Broadcast 行为差异]]

## 对比阅读

- [[40-存储与查询服务/42-Doris/doris的优化（和sprak的完全不同）|Doris 与 Spark 优化思路差异]]
- [[50-质量与运行保障/51-数据质量与对账/Hash + Sum 双重校验 (数据校验)|Hash + Sum 数据校验]]
