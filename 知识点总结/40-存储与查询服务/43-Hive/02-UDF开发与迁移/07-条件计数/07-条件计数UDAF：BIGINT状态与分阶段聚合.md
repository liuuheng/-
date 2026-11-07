# MCCountIfUDAF：可合并计数状态与 BIGINT 类型契约

对应代码：[MCCountIfUDAF.java](MCCountIfUDAF.java)。该类实现条件计数：一个分组内每出现一行 `TRUE` 就加一，`FALSE` 和 SQL `NULL` 不计入，最终返回 `BIGINT`。

## 为什么 UDAF 需要状态

标量函数可以独立处理每一行，而条件计数需要汇总同一分组的多行。因此将计算拆成两部分：解析器 `MCCountIfUDAF` 决定是否接受输入类型，求值器 `CountIfEvaluator` 管理聚合阶段和分组状态。

`getEvaluator(TypeInfo[])` 限制只能输入一个 BOOLEAN 参数。另一个接收 `GenericUDAFParameterInfo` 的入口转交给同一检查方法，避免两套参数规则出现分歧。

`CountIfAggregationBuffer` 只保存一个 `long count`。缓冲区属于分组，不能简单把计数器放到求值器的普通字段中，因为一个求值器可能处理多个分组。

## iterate 与 merge 处理的是两种输入

`iterate` 接收原始布尔值；输入非空且为真时调用 `add(..., 1)`。`merge` 接收上游已经算好的部分计数，调用同一个 `add` 方法累加。

例如原始输入为 `TRUE、FALSE、NULL、TRUE`，直接计算得到 `2`。分成两批后分别得到 `1` 与 `1`，合并仍为 `2`。

计数可这样分解，是因为部分结果的合并操作为整数加法。在没有溢出的有效状态范围内，加法满足结合律和交换律；输入分到哪个分区、部分结果以什么顺序到达，都不应改变结果。

## init 为什么要区分四种模式

| 模式 | 输入内容 | 使用方法 | 输出内容 |
| --- | --- | --- | --- |
| `COMPLETE` | 原始 BOOLEAN | `iterate` | 最终 BIGINT |
| `PARTIAL1` | 原始 BOOLEAN | `iterate` | 部分 BIGINT |
| `PARTIAL2` | 部分 BIGINT | `merge` | 部分 BIGINT |
| `FINAL` | 部分 BIGINT | `merge` | 最终 BIGINT |

`init` 根据模式选择 `BooleanObjectInspector` 或 `LongObjectInspector` 读取输入。中间结果和最终结果的类型碰巧都是 BIGINT，因此所有模式都可以返回同一个 `writableLongObjectInspector`；这不意味着所有 UDAF 的中间与最终类型都相同。

`terminatePartial` 与 `terminate` 都创建 `LongWritable`。类型声明、状态类型和返回对象一致，才能让下一阶段正确读取计数。

## 新建状态与重置状态不是一回事

`getNewAggregationBuffer` 创建初始为零的新状态。`reset` 将已有状态清零，供框架复用；不能只依赖构造时的默认值，因为复用时构造函数不会再次执行。

全部输入为假或空值时，状态仍为零，最终输出 `0`。但如果查询根本没有产生某个分组，函数也不会凭空产生该分组的一行零值。

## 为什么使用 long 和显式溢出检查

条件满足的记录数可能超过 INT 范围，因此从状态、部分输入检查器到最终结果都统一使用 BIGINT 对应的 `long`。

```java
state.count = Math.addExact(state.count, delta);
```

即使 `long` 也有上限，普通加法在溢出时会回绕为错误结果。`Math.addExact` 将这种情况变成明确异常，外层包装为 `HiveException`，不会返回一个看似有效的负计数。

## 复杂度与适用范围

每行处理和每次部分计数合并都是 `O(1)`；每个分组的状态空间也是 `O(1)`，实际总状态空间还取决于同时活跃的分组数。

该实现按 Hive 1.2.1 的聚合接口和 Java 8 编写，内含求值器与缓冲区，不需要自定义公共类。一般 SQL 中也可用“条件映射成 1 或 0 再求和”表达同样的统计意图；本实现更适合用于理解聚合函数的状态、类型及合并设计。

## 跨引擎实现对照

[Flink 2.3 实现讲解](<../../../../30-计算与SQL开发/33-Flink/11-UDF原理与实现/07-条件计数/07-Flink条件计数：可合并的BIGINT状态.md>)；[Spark 3.5 实现讲解](<../../../../30-计算与SQL开发/32-Spark/04-UDF原理与实现/07-条件计数/07-Spark条件计数：可合并的BIGINT状态.md>)；[三引擎类型与状态对比](<../../../../30-计算与SQL开发/31-SQL开发/04-Hive、Flink与Spark UDF：类型、展开与聚合状态对比.md>)。业务规则尽量一致；参数形状、展开结构和撤回能力的差异见对应文章。
