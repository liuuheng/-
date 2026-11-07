# Flink安全数值转换：有限值校验与空值契约

对应代码：[FlinkSafeToDouble.java](FlinkSafeToDouble.java)。适用版本：Flink 2.3.0 / Java 17。本篇解释同目录这一份实现，保留与 Hive 案例一致的业务规则，并明确引擎接口上的变化。

## 业务规则与返回契约

输入为字符串，输出为可空的 `DOUBLE`。`"12.5"` 返回 `12.5`；空值、空串、无法解析的文本返回 `NULL`。`NaN`、`Infinity` 以及 `1e309` 产生的无穷值也返回 `NULL`。

## 先解析，再判断是否为有限数

`Double.parseDouble` 负责 Java 浮点文本解析；`Double.isFinite` 负责排除非有限结果。两步不能合并成“没有抛异常就成功”：`NaN` 和无穷大是 Java 可解析的浮点值，却不符合这里的业务契约。

只捕获 `NumberFormatException`，将输入格式问题转换为 `NULL`。不捕获所有异常，避免把实现缺陷也伪装成普通坏数据。算法对一条文本只解析一次，时间随文本长度增长，没有跨行可变状态。

## 设计边界

返回 `Double` 包装类型，才能表达 SQL 空值；返回基本类型 `double` 无法表达这项契约。本函数保留 Java 的解析规则，不额外限制十进制格式，也不承诺与引擎的 `CAST` 或 `TRY_CAST` 完全等价。浮点数存在舍入，不能拿它代替金额所需的定点小数。非常小的非零值可能下溢为零，有限值检查不会拒绝这种情况。

## Flink 的函数接口

类继承 `ScalarFunction`，公开方法为 `Double eval(String value)`。输入与输出由方法签名推断，代码直接处理 Java 字符串，不再操作 Hive 的 `ObjectInspector`、`DeferredObject` 或 `Text`。

参数使用 `String`，输出使用 `Double`，让空值能到达业务判断并返回可空结果。注册时使用本类，调用前应把非字符串输入显式转换为字符串，避免把引擎的隐式类型转换当成业务规则。业务计算可用于有界或无界表的逐行转换，本类本身不维护键控状态。

## 对照与接口依据

[Flink 2.3 用户自定义函数文档](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/functions/udfs/)用于核对引擎接口；本篇业务规则以配套代码为准。

[Hive 同例讲解](<../../../../40-存储与查询服务/43-Hive/02-UDF开发与迁移/01-安全数值转换/01-安全数值转换UDF：空值、非法数字与有限值校验.md>)；[Spark 同例讲解](<../../../32-Spark/04-UDF原理与实现/01-安全数值转换/01-Spark安全数值转换：有限值校验与空值契约.md>)；[三引擎类型与状态对比](<../../../31-SQL开发/04-Hive、Flink与Spark UDF：类型、展开与聚合状态对比.md>)。
