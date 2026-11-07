# TransArrayUDTFV4：多列位置对齐与输出行快照

对应代码：[TransArrayUDTFV4.java](TransArrayUDTFV4.java)。该类直接继承 `GenericUDTF`，把一行中的多个分隔字符串按位置展开为多行，保留指定的键列，并给较短的数据列补 SQL `NULL`。

## 输入结构如何决定输出结构

参数依次为 `num_keys、separator、key1……、data1、data2……`。前两个参数必须是常量：`num_keys` 为非空 INT，指定后面多少列保持不拆分；`separator` 为非空字符串。键数必须非负，而且至少留下一列数据用于展开。

例如键为 `'u1'`，两个数据列为 `'a,b'` 和 `'10'`，使用逗号分隔，输出为：

| 键列 | 数据列一 | 数据列二 |
| --- | --- | --- |
| u1 | a | 10 |
| u1 | b | NULL |

它按位置对齐，不生成两个数据列片段之间的笛卡尔积。全部数据列为 `NULL` 时仍输出一行，保留键，数据列全部为 `NULL`。

## initialize：先确定列数和类型

UDTF 的返回值不是单个标量，而是一行的结构。`initialize` 必须在处理数据前声明 `StructObjectInspector`，确定输出列数、顺序和各列类型。

输出列数为 `arguments.length - 2`，即键列数加数据列数。常量键数决定了参数中哪部分是键，因此不能等到每一行才改变它。分隔符也在初始化时编译，避免逐行重复创建模式。

键列要求基本类型，使用 `getStandardObjectInspector(..., WRITABLE)` 得到相同 SQL 类型的标准输出表示；数据列要求字符串，输出检查器为 `writableStringObjectInspector`。因此整数键展开后仍是整数，不会为了方便输出而变成字符串。

## 字面拆分与预计算片段

`Pattern.quote(separator)` 把分隔符作为字面文本，`separatorPattern.split(value, -1)` 保留尾部空段。输入 `'a|b|'` 用 `'|'` 拆分会得到三项，其中最后一项是空字符串。

`process` 先提取各键，再把每个数据列拆分一次，保存为 `String[][] parts`，同时记录最长列的片段数 `maximumRows`。数据列为 `NULL` 时不拆分，对应数组保持 `null`。

外层循环按输出行号推进，内层按数据列取片段。当前列没有数组，或当前行号超出该列长度时，输出位置保持默认 `null`。这把“空输入列”和“较短列”统一为补空逻辑，同时区分了空字符串片段 `''` 与缺失值 `NULL`。

## 为什么输出数量至少为一

循环上界使用 `Math.max(1, maximumRows)`。这样所有数据列为空时，键不会随展开操作一起消失。这是函数自身的行数契约，调用方需要根据该规则理解后续计数和连接结果。

每列只拆分一次后，字符串解析成本与各列文本总长度有关；生成结果仍需遍历每个输出行和输出列，不能因为缓存片段就省掉实际输出的工作。

## 行数组与 Writable 都需要考虑可变性

每个输出行创建新的 `Object[]`，键可能仍引用输入的 Writable 对象。仅复制数组并不等于复制字段对象；如果输入对象被后续复用，旧行中的键仍可能改变。

```java
List<?> snapshot = (List<?>) ObjectInspectorUtils.copyToStandardObject(
        row, outputInspector, ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE);
forward(snapshot.toArray());
```

这一步按照已声明的结构复制整行，包含可变字段，再交给 `forward`。快照增加了分配开销，但让输出对象的所有权更清晰。逻辑直接包含在本文件中，不依赖另外的输出工具类。

## 状态和使用边界

输入检查器、输出检查器和分隔模式属于初始化状态，跨行复用；片段数组、结果行和快照都是当前输入行的局部数据。`close` 不需要刷新跨行缓存，也没有网络或文件资源需要释放。

实现按 Hive 1.2.1 API 编写，仅依赖 Java 与 Hive/Hadoop 类型。分隔符不接受正则语义；列内若有 CSV 引号或转义规则，需要换用专门解析方式。单行被展开成大量结果时，行内片段与快照也会带来明显的内存和输出开销。

## 跨引擎实现对照

[Flink 2.3 实现讲解](<../../../../30-计算与SQL开发/33-Flink/11-UDF原理与实现/06-多列展开/06-Flink多列展开：固定行结构与对齐补齐.md>)；[Spark 3.5 实现讲解](<../../../../30-计算与SQL开发/32-Spark/04-UDF原理与实现/06-多列展开/06-Spark多列展开：固定行结构与对齐补齐.md>)；[三引擎类型与状态对比](<../../../../30-计算与SQL开发/31-SQL开发/04-Hive、Flink与Spark UDF：类型、展开与聚合状态对比.md>)。业务规则尽量一致；参数形状、展开结构和撤回能力的差异见对应文章。
