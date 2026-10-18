---
tags:
  - sql
  - tvf
status: seed
---

# SQL 表值函数 TVF：作用、执行方式与适用场景

TVF（Table-Valued Function）本质上是“可传参数的逻辑视图或子查询模板”。

- 不一定存储数据。
- 通常不会缓存。
- 每次调用时动态执行。
- 更像 SQL 层的函数化子查询。
- 适合复用复杂查询逻辑。

相关：[[30-计算与SQL开发/32-Spark/02-SQL与缓存/02-Spark SQL 临时视图、全局临时视图与永久视图|Spark 中 View 的理解]]
