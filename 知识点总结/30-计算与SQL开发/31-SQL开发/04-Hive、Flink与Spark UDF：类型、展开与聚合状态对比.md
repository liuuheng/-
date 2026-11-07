# Hive、Flink 与 Spark UDF：类型、展开与聚合状态对比

同一套业务算法可以迁移，但函数入口、SQL 类型和状态生命周期不能直接照搬。这里以已经整理的八个案例为依据，对比 **Hive 1.2.1、Flink 2.3.0、Spark 3.5.7** 的 Java 实现。新增 Flink 与 Spark 代码均以 Java 17 验证；Spark 使用 Scala 2.12 对应的依赖。它们按组件分别存放，每份 Java 文件都有一篇同目录的原理文章。

## 先分清三种函数

标量函数处理一行并返回一个值；展开函数将一行变成多行；聚合函数将一组输入汇成一个结果。前五例是标量，第六例是展开，最后两例是聚合。三类函数面对的数据量、类型声明和状态管理问题不同，不应只笼统地称为 UDF。

| 维度 | Hive 原生实现 | Flink 2.3 原生实现 | Spark 3.5 原生实现 |
| --- | --- | --- | --- |
| 标量入口 | `GenericUDF.initialize/evaluate` | `ScalarFunction.eval` | Java `UDF1/UDF3/UDF4.call` |
| 类型约定 | ObjectInspector 校验和转换，输出声明与对象匹配 | 方法签名推断，必要时加类型提示 | Java 接口描述参数，注册时显式指定 SQL 返回类型 |
| 一行变多行 | `GenericUDTF.process/forward` | `TableFunction.eval/collect` | 本例返回 `ARRAY<STRUCT>`，再由内置 `explode` 展开 |
| 聚合接口 | resolver 与 evaluator、四种执行模式 | `AggregateFunction<结果,状态>` | `Aggregator<输入,状态,结果>` |
| 聚合状态载体 | AggregationBuffer，另定义中间输出类型 | Accumulator，可增加撤回与合并方法 | buffer，加上输入、状态、输出的 Encoder |
| 本例字符串对象 | 输入经转换器读取，输出通常为 Text | Java String | Java String |

这张表描述的是本组代码采用的接口，不表示某引擎只支持其中一种写法。Flink 的方法提取和类型提示以[官方 UDF 文档](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/functions/udfs/)为依据；Spark Java 标量注册以[UDFRegistration API](https://spark.apache.org/docs/3.5.7/api/java/org/apache/spark/sql/UDFRegistration.html)为依据。

## 八个案例保持哪些规则，改了哪些参数

| 案例 | 共同业务规则 | Hive / Flink 调用形状 | Spark 3.5 调用形状 |
| --- | --- | --- | --- |
| 安全数值转换 | 非法文本与非有限浮点值返回 NULL | 一个字符串参数 | 一个字符串参数 |
| 字符串分段 | 字面分隔、1 起始闭区间、保留尾空段 | 三参数取单段，四参数取区间 | 固定四参数，单段传相同起止位置 |
| 正则全匹配 | 完整匹配拼接字符串，每项带尾分隔符 | 两参数默认逗号，或显式第三参数 | 固定三参数，显式传分隔符 |
| JSON 键值合并 | 空键值对跳过，重复键最后非空值覆盖 | 多个参数按相邻两项配对 | 一个 ARRAY 参数，Java 用 Scala Seq 接收 |
| JSON 数组排序 | 数字排序、空字段总在最后、相等键保持顺序 | 文本、字段名、布尔方向 | 相同三个逻辑参数 |
| 多列展开 | 各列按位置对齐，短列补 NULL | Hive 动态键列与数据列；Flink 固定字符串键加数据列数组 | 固定字符串键加数组，返回行数组后 explode |
| 条件计数 | 只算 TRUE，BIGINT，溢出报错 | 一列布尔值 | 一列布尔值，经 Aggregator 包装注册 |
| 连续日期 | 严格日期、忽略坏值、去重后的最长连续段 | 一列日期文本 | 一列日期文本，经 Aggregator 包装注册 |

业务相同不代表 SQL 文本可以不改。尤其是三参数分段、正则默认分隔符、JSON 可变参数和多列展开：迁移时要核对实际参数形状。Java 的 SQL `NULL` 使用可空包装类型，不应为减少几个字符改成基本类型并丢掉空值语义。

数值转换、正则拼接和 JSON 排序都遵循本组代码的规则，不能仅因内置函数名字相近就直接替换。Spark 3.5 的普通条件计数已有内置 `count_if`，可优先使用；保留自定义实现是为了学习状态如何累加、合并及编码。[Spark 3.5.7 functions API](https://spark.apache.org/docs/3.5.7/api/java/org/apache/spark/sql/functions.html)

## 多列展开：类型固定以后，执行方式仍不同

Flink 和 Spark 新版本统一输出 `row_key STRING, parts ARRAY<STRING>`。输入键为 `u`、数据为 `['a,b','10']`，产生 `['a','10']` 与 `['b',NULL]` 两条片段。全为空值的输入列仍产生一条记录，避免无声丢失键。

Hive 原实现根据初始化参数确定输出的多个列，适配能力更动态。Flink 的 `Row` 用类型提示明确字段，并在每个位置创建新数组、新行，通过 `collect` 逐条发送。Spark Java 标量 UDF 则用显式 ArrayType/StructType 声明结果，先构造 `List<Row>`，再交给 `explode`。

因此两份新实现的关系结果相同，但内存行为不同：Spark 当前类要额外持有整批输出行，Flink 当前类只逐条发出行；两者仍然都会先保存各输入列的拆分结果。不能据此给整个引擎下性能结论，实际成本还受单行膨胀率、分区、序列化和下游操作影响。

投影片段时也要改写下标：Flink SQL 数组下标从 1 开始；Spark 的 `parts[0]` 表示第一个元素。输入从多列改成数组后，数组元素的顺序就是业务列顺序，应由调用方稳定提供。

## 聚合：中间状态必须包含可合并的信息

| 生命周期 | Hive evaluator | Flink AggregateFunction | Spark Aggregator |
| --- | --- | --- | --- |
| 空状态 | 创建 buffer 并 reset | createAccumulator | zero |
| 输入一条数据 | iterate | accumulate | reduce |
| 合并局部状态 | merge | merge | merge |
| 读取结果 | terminate；局部结果另用 terminatePartial | getValue | finish |
| 删除已有输入的影响 | 本组实现未提供 | retract | 本接口没有对应方法 |
| 状态跨执行边界 | 中间值的 ObjectInspector | 框架提取的状态类型 | bufferEncoder；输入与输出分别编码 |

Hive 的 `PARTIAL1`、`PARTIAL2`、`FINAL`、`COMPLETE` 决定输入是原始记录还是中间状态，不能把所有阶段都当成 `iterate`。Flink 与 Spark 把状态类型放到不同的泛型位置，抄类声明时必须核对顺序。

Spark 3.5 本组使用 `Aggregator`，通过 `functions.udaf(aggregator,inputEncoder)` 注册到 SQL；没有延续已弃用的 `UserDefinedAggregateFunction`。`zero/reduce/merge/finish` 负责算法，buffer/output encoder 负责边界表示。[Aggregator API](https://spark.apache.org/docs/3.5.7/api/java/org/apache/spark/sql/expressions/Aggregator.html)、[udaf 的 Java 重载](https://spark.apache.org/docs/3.5.7/api/java/org/apache/spark/sql/functions.html#udaf-org.apache.spark.sql.expressions.Aggregator-org.apache.spark.sql.Encoder-)

### 条件计数为什么容易迁移

每条记录只贡献 0 或 1，中间状态只是一个 long。局部状态相加即可完成合并，不需要保留原始记录；三引擎都采用溢出检查。Flink 另有 `retract(TRUE)`，可以减去一次已有贡献，但实际调用仍取决于执行计划。

### 连续日期为什么不能只保存一个最大值

若两个分区分别包含日期 `01,02` 和 `03,04`，各自最大长度是 2，最终却是 4。只合并最大值必然丢信息。本例保留日期状态，再合并、排序和扫描。

Hive 以 TreeSet 保留去重日期；Spark 以 Bean 中的 `Map<Long,Boolean>` 表示同样的集合，并用结构化 encoder 保存，值恒为真。追加聚合只需知道某天是否存在，所以两者合并都是日期并集。

Flink 还要考虑撤回，使用 `Map<Long,Long>` 保存出现次数。某天出现两次、撤回一次之后仍应存在，因此合并时要**相加次数**，不能只做键并集。次数降为零才删除日期。这是状态语义的改变，不能用统一工具类把差异隐藏掉。

设分组中有 `d` 个不同日期，本组三种精确算法的状态都随 `d` 增长；Flink、Spark 取结果还要排序。实现 `retract` 不会自动提供无限历史下的内存上限，Spark 新 Aggregator 也不自动获得流式删除语义。窗口、保留周期、迟到修正和状态恢复必须结合真实查询另外设计。

## 对照阅读代码时关注什么

[Flink 数值转换](<../33-Flink/11-UDF原理与实现/01-安全数值转换/01-Flink安全数值转换：有限值校验与空值契约.md>)与[Spark 数值转换](<../32-Spark/04-UDF原理与实现/01-安全数值转换/01-Spark安全数值转换：有限值校验与空值契约.md>)可以先用来理解逐行入口和类型边界；[Flink 多列展开](<../33-Flink/11-UDF原理与实现/06-多列展开/06-Flink多列展开：固定行结构与对齐补齐.md>)与[Spark 多列展开](<../32-Spark/04-UDF原理与实现/06-多列展开/06-Spark多列展开：固定行结构与对齐补齐.md>)用于观察一行变多行的执行方式；[Flink 连续日期](<../33-Flink/11-UDF原理与实现/08-连续日期/08-Flink最长连续日期：去重、合并与状态边界.md>)与[Spark 连续日期](<../32-Spark/04-UDF原理与实现/08-连续日期/08-Spark最长连续日期：去重、合并与状态边界.md>)用于理解状态为什么随撤回需求改变。每篇还链接对应的 Hive 原理文章。

## 本次实际核验范围

16 份新增 Java 源码均在笔记目录外单独编译。Flink 2.3.0 已通过本地 TableEnvironment 的标量注册、LATERAL TABLE 展开和有界聚合执行，并直接核对撤回、重复日期、合并和重置逻辑。Spark 3.5.7 已通过 local[2] 的标量注册、explode 展开，以及 repartition(2) 后的两个聚合执行。

这些结果确认了所用版本的 API、类型编码和示例结果；不把本地有界验证表述为生产集群、全量流式窗口或故障恢复验证。笔记目录只保留优化实现与讲解，没有构建、启动或测试工程。
