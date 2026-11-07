# Spark安全数值转换：有限值校验与空值契约

对应代码：[SparkSafeToDouble.java](SparkSafeToDouble.java)。适用版本：Spark 3.5.7 / Scala 2.12 / Java 17。本篇解释同目录这一份实现，保留与 Hive 案例一致的业务规则，并明确引擎接口上的变化。

## 业务规则与返回契约

输入为字符串，输出为可空的 `DOUBLE`。`"12.5"` 返回 `12.5`；空值、空串、无法解析的文本返回 `NULL`。`NaN`、`Infinity` 以及 `1e309` 产生的无穷值也返回 `NULL`。

## 先解析，再判断是否为有限数

`Double.parseDouble` 负责 Java 浮点文本解析；`Double.isFinite` 负责排除非有限结果。两步不能合并成“没有抛异常就成功”：`NaN` 和无穷大是 Java 可解析的浮点值，却不符合这里的业务契约。

只捕获 `NumberFormatException`，将输入格式问题转换为 `NULL`。不捕获所有异常，避免把实现缺陷也伪装成普通坏数据。算法对一条文本只解析一次，时间随文本长度增长，没有跨行可变状态。

## 设计边界

返回 `Double` 包装类型，才能表达 SQL 空值；返回基本类型 `double` 无法表达这项契约。本函数保留 Java 的解析规则，不额外限制十进制格式，也不承诺与引擎的 `CAST` 或 `TRY_CAST` 完全等价。浮点数存在舍入，不能拿它代替金额所需的定点小数。非常小的非零值可能下溢为零，有限值检查不会拒绝这种情况。

## Spark 的函数接口

类实现 `UDF1<String,Double>`，入口为 `call(String value)`。注册时需要声明 `DataTypes.DoubleType`；Java 泛型描述方法参数，不应替代实际注册的 SQL 返回类型。

输入使用 Java 字符串，返回可空包装数值。它不需要 Hive 检查器或 Writable。对于原本是其他 SQL 类型的输入，调用前显式转换成字符串更容易保持迁移语义，不能假设任意对象都会自动按预期转换。

## 对照与接口依据

[Spark 3.5.7 UDFRegistration API](https://spark.apache.org/docs/3.5.7/api/java/org/apache/spark/sql/UDFRegistration.html)用于核对引擎接口；本篇业务规则以配套代码为准。

[Hive 同例讲解](<../../../../40-存储与查询服务/43-Hive/02-UDF开发与迁移/01-安全数值转换/01-安全数值转换UDF：空值、非法数字与有限值校验.md>)；[Flink 同例讲解](<../../../33-Flink/11-UDF原理与实现/01-安全数值转换/01-Flink安全数值转换：有限值校验与空值契约.md>)；[三引擎类型与状态对比](<../../../31-SQL开发/04-Hive、Flink与Spark UDF：类型、展开与聚合状态对比.md>)。
