---
aliases:
  - DataX MOC
tags:
  - moc
  - datax
  - data-integration
status: active
---

# DataX 知识地图

返回：[[知识库首页]]

## 学习路径

1. [[04-DataX/datax的特性|DataX 的特性]]
2. [[04-DataX/channel是如何提高同步速度的|Channel 如何提高同步速度]]
3. [[04-DataX/datax同步mysql中240G数据如何提速|MySQL 240G 数据同步提速]]

## 关联主题

- [[05-生产实践/业务分析/同步任务的思考：后端如何建表对于数仓的重要性|后端建表对同步任务与数仓的影响]]
- [[05-生产实践/SQL与数据质量/Hash + Sum 双重校验 (数据校验)|Hash + Sum 双重数据校验]]
- [[05-生产实践/架构与探索/数据迁移的流程|数据迁移流程]]（待补充）
- [[01-Flink/Flink CDC/具体的流程|Flink CDC 实时同步流程]]

## 核心问题

- 并发度增大何时有效，何时会把瓶颈转移到源库或目标库？
- Channel、切分键、批次大小和限速之间是什么关系？
- 全量迁移结束后，如何做完整性校验和增量衔接？
