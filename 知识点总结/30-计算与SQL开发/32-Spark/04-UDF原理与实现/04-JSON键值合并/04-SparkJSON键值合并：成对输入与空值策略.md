# SparkJSON键值合并：成对输入与空值策略

对应代码：[SparkMergeJson.java](SparkMergeJson.java)。适用版本：Spark 3.5.7 / Scala 2.12 / Java 17。本篇解释同目录这一份实现，保留与 Hive 案例一致的业务规则，并明确引擎接口上的变化。

## 业务规则与返回契约

按相邻两个字符串组成一个键值对，输出 JSON 对象文本。输入 `a,1,a,2,b,NULL` 得到 `{"a":"2"}`：重复键由后一个**非空键值对**覆盖；键或值为空的整对跳过，不会删除已有值。

键和值都按字符串保存，`"1"` 不会自动变成 JSON 数字。空字符串是有效值，与 SQL `NULL` 不同。参数数量必须为正偶数，数量错误抛异常；所有键值对都被跳过时返回 `{}`。

## 顺序处理与覆盖机制

`JSONObject(true)` 使用有序映射，循环步长固定为 2，先检查成对输入，再逐对写入。重复键的值会更新，但更新已有键通常不会把键移到末尾。输出对象的字段顺序只用于可读性，不应成为业务关联条件。

统一调用 JSON 库完成转义，不手工拼接双引号、反斜线和逗号。只在当前行维护对象，不缓存调用者的输入，耗时与参数数量和文本大小有关。

## 设计取舍

把“缺失值”处理为省略字段，适合稀疏属性合并；它与输出 JSON `null` 是不同契约。如果业务需要保留空字段，应显式调整判断规则。代码沿用 Hive 案例的 `com.alibaba.fastjson` 1.x API；两个 JSON 案例需要这项依赖，其余函数不依赖 JSON 库。

## Spark 把可变参数改成 ARRAY

类实现 `UDF1<scala.collection.Seq<String>,String>`，调用形状为 `merge_json_note(array('a','1','b','2'))`。Spark SQL 的 `ARRAY<STRING>` 在此 Java UDF 入口由 Scala `Seq` 承接；不能把签名简单写成 Java 的 `String...`，也不能未经验证就改成 `List<String>`。

函数用 `size()` 校验正偶数，再用 `apply(index)` 读取相邻键值。数组整体为空值时返回 `NULL`，数组内的空元素按“跳过所在键值对”处理。注册返回 `DataTypes.StringType`。本例的数组访问适用于 SQL 传入的序列表示，不承诺任意用户构造的链表式 `Seq` 都有常数时间随机访问。

## 对照与接口依据

[Spark 3.5.7 UDFRegistration API](https://spark.apache.org/docs/3.5.7/api/java/org/apache/spark/sql/UDFRegistration.html)用于核对引擎接口；本篇业务规则以配套代码为准。

[Hive 同例讲解](<../../../../40-存储与查询服务/43-Hive/02-UDF开发与迁移/04-JSON键值合并/04-JSON键值合并UDF：参数成对校验与空值策略.md>)；[Flink 同例讲解](<../../../33-Flink/11-UDF原理与实现/04-JSON键值合并/04-FlinkJSON键值合并：成对输入与空值策略.md>)；[三引擎类型与状态对比](<../../../31-SQL开发/04-Hive、Flink与Spark UDF：类型、展开与聚合状态对比.md>)。
