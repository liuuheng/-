# SafeToDouble：字符串数值清洗与有限值判断

对应代码：[SafeToDouble.java](SafeToDouble.java)。该类直接继承 `GenericUDF`，实现“将字符串转成有限 DOUBLE”的行级函数，不依赖本目录中的其他 Java 文件。

## 要解决的问题与输出约定

日志或接口中的数值经常以字符串形式保存，其中可能混入空值、空字符串或无法解析的内容。本函数将这些情况统一表达为 SQL `NULL`，让调用方决定是过滤、补默认值，还是单独统计异常数据。

| 输入 | 返回结果 |
| --- | --- |
| `'12.5'`、`' 12.5 '` | `12.5` |
| `'-1.2e-2'` | `-0.012` |
| `NULL`、`''`、`'abc'` | `NULL` |
| `'NaN'`、`'Infinity'`、`'1e9999'` | `NULL` |

不直接返回 `0`，是因为零值本身可能是有效数据。把转换失败混进零值，会改变平均值、比例和异常计数的含义。

## initialize：把类型处理移出逐行计算

`initialize` 接收的 `ObjectInspector[]` 描述输入类型，不是某一行的数据。它先用 `checkArgsSize` 限制只能传入一个参数，再通过 `obtainStringConverter` 准备字符串转换器。

Hive 中的字符串可能采用 Java 或 Writable 等对象表示。转换器把取值方式预先确定下来，`evaluate` 便不需要逐行判断对象究竟是哪种表示。

返回的 `writableDoubleObjectInspector` 声明了输出类型为 `DOUBLE`，以及输出对象采用 `DoubleWritable`。类型描述和实际返回对象必须匹配，否则计算值正确也可能无法被后续算子读取。

## evaluate：缺失判断、解析、有限值检查

逐行处理依次完成三件事：先用 `getStringValue` 读取当前值，缺失或空字符串直接返回 `null`；再调用 `Double.parseDouble` 解析；最后用 `Double.isFinite` 拒绝非有限结果。

```java
double number = Double.parseDouble(value);
return Double.isFinite(number) ? new DoubleWritable(number) : null;
```

单纯捕获 `NumberFormatException` 不够。`NaN`、`Infinity` 都是 Java 能识别的浮点形式，超大指数也可能产生无穷大。这些值不一定抛异常，却通常不符合业务清洗对有效数值的要求。

`NumberFormatException` 在函数内部转换成 `NULL`，因为这里把非法数字定义为数据质量问题。代码没有捕获所有异常，避免将编程错误一并伪装成正常的缺失值。

## 为什么这样管理对象和状态

只有类型转换器跨行保存，数值结果每行创建一个新的 `DoubleWritable`。这样转换规则可以复用，不会把上一行的数值带到下一行，也不让调用方持有随后会被改写的结果对象。

转换器字段为 `transient`，表达它属于依赖 Hive 类型环境的初始化信息。该类按 Hive 的调用约定先执行 `initialize`，再执行 `evaluate`，没有另建共享基类来封装这两个方法。

## 适用边界

该实现面向 Hive 1.2.1 的 `GenericUDF` 接口，使用 Java 8 的 `Double.isFinite`，依赖 Hive/Hadoop 的检查器与 Writable 类型。

有效输入沿用 Java 的浮点解析语法，并不是“只允许十进制价格字符串”的严格校验器。`DOUBLE` 使用二进制浮点表示，小数精度、大整数精度和极小数下溢也不会由本函数修复。金额或精确计量需要另行定义 DECIMAL 契约。

## 跨引擎实现对照

[Flink 2.3 实现讲解](<../../../../30-计算与SQL开发/33-Flink/11-UDF原理与实现/01-安全数值转换/01-Flink安全数值转换：有限值校验与空值契约.md>)；[Spark 3.5 实现讲解](<../../../../30-计算与SQL开发/32-Spark/04-UDF原理与实现/01-安全数值转换/01-Spark安全数值转换：有限值校验与空值契约.md>)；[三引擎类型与状态对比](<../../../../30-计算与SQL开发/31-SQL开发/04-Hive、Flink与Spark UDF：类型、展开与聚合状态对比.md>)。业务规则尽量一致；参数形状、展开结构和撤回能力的差异见对应文章。
