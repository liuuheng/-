---
tags:
  - sql
  - tvf
status: seed
---

# TVF 函数的理解

TVF（Table-Valued Function）本质上是“可传参数的逻辑视图或子查询模板”。

- 不一定存储数据。
- 通常不会缓存。
- 每次调用时动态执行。
- 更像 SQL 层的函数化子查询。
- 适合复用复杂查询逻辑。

相关：[[05-生产实践/生产实践知识地图|业务与数仓知识地图]] · [[05-生产实践/Spark调优/spark中view的理解|Spark 中 View 的理解]]
