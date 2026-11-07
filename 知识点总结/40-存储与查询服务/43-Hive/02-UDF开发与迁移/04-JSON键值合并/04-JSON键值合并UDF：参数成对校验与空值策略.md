# MergeJsonUDF：成对参数解析与 JSON 对象构造

对应代码：[MergeJsonUDF.java](MergeJsonUDF.java)。该类直接继承 `GenericUDF`，把同一行的多个字符串键值对组成 JSON 对象，适合封装扩展属性和统一输出结构。

## 函数契约决定如何组织参数

参数按 `key1、value1、key2、value2……` 排列，至少一对，返回类型为 `STRING`。它构造一个新 JSON 对象，不递归合并已经存在的 JSON 文档。

| 参数 | 返回结果 |
| --- | --- |
| `'name', 'alice', 'city', 'hangzhou'` | `{"name":"alice","city":"hangzhou"}` |
| `'a', '1', 'a', '2'` | `{"a":"2"}` |
| `'a', NULL, 'b', '2'` | `{"b":"2"}` |
| `'a', NULL` | `{}` |

键或值为 SQL `NULL` 时跳过这一对。重复键由后面的有效值覆盖；后面的空值对被忽略，不会删除之前已经写入的键。空字符串仍是有效键或值。

## initialize：把结构错误尽早拒绝

参数数量必须大于零且为偶数。奇数意味着某个键缺少对应值，不能静默丢弃尾部参数。

转换器数组按实际参数数量分配，没有固定的 200 项上限。初始化逐项检查 `STRING_GROUP` 与 `VOID_GROUP`：前者表示字符串类型，后者允许 SQL 中没有显式类型的 `NULL` 字面量。

这里只要求值具有字符串契约，不根据内容猜测数值、布尔或对象类型。字符串 `'12'` 会成为 JSON 字符串 `"12"`，不是 JSON 数字 `12`。

## evaluate：以两步为一组读取

处理从下标 `0` 开始，每次增加 `2`。当前下标读取键，下一个下标读取值，两者非空时写入对象：

```java
for (int index = 0; index < arguments.length; index += 2) {
    String key = getStringValue(arguments, index, converters);
    String value = getStringValue(arguments, index + 1, converters);
    if (key != null && value != null) {
        result.put(key, value);
    }
}
```

这种布局让“参数必须成对”的约束直接体现在循环结构中，减少索引表达式混淆。空值在每一对内部处理，不能在函数入口发现任一空值就让整行返回 `NULL`。

## 为什么每行创建独立 JSONObject

`JSONObject(true)` 使用有序映射保存当前行的属性，输出更便于阅读。重复键覆盖是映射的自然行为；覆盖已有键不保证把该键移动到末尾。

对象在 `evaluate` 内创建，转换器才保存在实例字段里。若把结果对象长期保存且忘记清空，上一行存在、当前行没有的键就可能残留。每行创建新对象可以直接避免这种跨行污染。

## 序列化与类型边界

使用 `toJSONString()` 负责引号、反斜杠等字符的转义，再包装为 `Text`，与字符串检查器保持一致。手工拼接 JSON 容易遗漏转义规则，使合法属性值破坏输出结构。

值如果是 `'{}'`，仍会作为普通字符串序列化，不会自动变为嵌套对象。需要嵌套结构时，应设计明确的复杂类型输入，不能未经约定就按值的外观解析。

设有 `p` 对参数，读取与写入为 `O(p)`；输出序列化还与属性文本的总长度有关。实现按 Hive 1.2.1 API 编写，除 Hive/Hadoop 类型外使用 `com.alibaba.fastjson.JSONObject`；本组源码采用 Fastjson 1.x 的接口，不包含依赖管理文件。

## 跨引擎实现对照

[Flink 2.3 实现讲解](<../../../../30-计算与SQL开发/33-Flink/11-UDF原理与实现/04-JSON键值合并/04-FlinkJSON键值合并：成对输入与空值策略.md>)；[Spark 3.5 实现讲解](<../../../../30-计算与SQL开发/32-Spark/04-UDF原理与实现/04-JSON键值合并/04-SparkJSON键值合并：成对输入与空值策略.md>)；[三引擎类型与状态对比](<../../../../30-计算与SQL开发/31-SQL开发/04-Hive、Flink与Spark UDF：类型、展开与聚合状态对比.md>)。业务规则尽量一致；参数形状、展开结构和撤回能力的差异见对应文章。
