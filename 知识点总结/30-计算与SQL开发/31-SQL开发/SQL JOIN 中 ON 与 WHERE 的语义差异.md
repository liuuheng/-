---
aliases:
  - ON 与 WHERE 的区别
  - 外连接条件位置
  - LEFT JOIN 条件写法
tags:
  - sql
  - join
  - sql-semantics
  - interview
status: complete
---

# SQL JOIN 中 ON 与 WHERE 的语义差异

`ON` 决定两侧哪些行能够匹配，`WHERE` 过滤连接完成后的行。对 `INNER JOIN`，把同一个确定性过滤条件在 `ON` 和 `WHERE` 之间移动，通常不改变结果；对 `LEFT`、`RIGHT`、`FULL OUTER JOIN`，条件位置会影响未匹配行是否保留。

判断时不要只背执行顺序，应沿着三步语义推演：

1. 根据 `ON` 判断左右记录是否匹配。
2. 外连接为未匹配的一侧补 `NULL`。
3. `WHERE` 只保留条件结果为 `TRUE` 的行；结果为 `FALSE` 或 `UNKNOWN` 的行都会被删除。

SQL 中 `NULL = 1`、`NULL <> 1` 的结果都是 `UNKNOWN`。因此，外连接完成后再用 `WHERE` 限制非保留侧字段，补出来的 `NULL` 行通常会被删除。这才是外连接“看起来退化成内连接”的直接原因。

## LEFT JOIN：条件位置会改变结果

假设有两张表：

```sql
-- 左表 a
id
1
2
3

-- 右表 b
id  status
1   active
2   inactive
```

把右表条件写进 `ON`：

```sql
SELECT
    a.id,
    b.status
FROM a
LEFT JOIN b
    ON  a.id = b.id
    AND b.status = 'active';
```

结果是：

| `a.id` | `b.status` |
|---:|---|
| 1 | active |
| 2 | NULL |
| 3 | NULL |

`a.id = 2` 能按 ID 找到右表记录，但该记录不满足 `b.status = 'active'`，所以本次连接视为未匹配；LEFT JOIN 仍保留左表行，并把右表列补成 `NULL`。`a.id = 3` 没有右表记录，同样补 `NULL`。

把相同条件写进 `WHERE`：

```sql
SELECT
    a.id,
    b.status
FROM a
LEFT JOIN b
    ON a.id = b.id
WHERE b.status = 'active';
```

连接阶段先得到 `(1, active)`、`(2, inactive)`、`(3, NULL)`。`WHERE` 随后删除后两行，最终只剩 `a.id = 1`。对这个条件而言，查询结果与 INNER JOIN 相同。

更准确的编码规则是：

- 右表记录是否有资格与左表匹配，写在 `ON`。
- 最终结果是否需要保留，写在 `WHERE`。
- 要过滤左表输入，写在 `WHERE` 或先在左表子查询中筛选；不要以为把左表条件写进 `ON` 就能删掉左表行。

例如：

```sql
SELECT
    a.id,
    b.status
FROM a
LEFT JOIN b
    ON  a.id = b.id
    AND a.category = 'A';
```

如果 `a.category <> 'A'`，该左表行仍会保留，只是右表列为 `NULL`。LEFT JOIN 的 `ON` 条件不能删除保留侧的行。

## 查找“左表有、右表没有”

常见写法是 LEFT JOIN 后检查右表中“匹配时必不为 `NULL`”的列：

```sql
SELECT
    a.*
FROM a
LEFT JOIN b
    ON a.id = b.id
WHERE b.id IS NULL;
```

这里隐含一个条件：`b.id` 在真实匹配记录中不能为 `NULL`，通常应选择右表主键或受 `NOT NULL` 约束的关联键。如果检查的是右表本来就允许为 `NULL` 的普通字段，就无法区分“没有匹配”和“匹配到了，但该字段本身为 NULL”。

Hive 和 Spark SQL 支持 `LEFT ANTI JOIN` 时，直接表达反连接更清楚：

```sql
SELECT
    a.*
FROM a
LEFT ANTI JOIN b
    ON a.id = b.id;
```

不能笼统地说“把 `b.id IS NULL` 写在 `ON` 里一定连不上任何数据”。准确结论是：

- 如果 `b.id` 同时参与 `a.id = b.id` 的普通等值连接，那么 `b.id IS NULL` 与该等值条件无法同时为 `TRUE`，因此不会匹配。
- 如果检查的是右表其他可空字段，`ON b.other_col IS NULL` 仍可能匹配该字段确实为 `NULL` 的右表记录，但语义已经不是反连接。

## INNER JOIN：结果通常相同，职责仍应分开

对下面这类确定性合取条件，两种写法结果等价：

```sql
-- 写法一
SELECT *
FROM a
JOIN b
    ON  a.id = b.id
    AND b.status = 'active';

-- 写法二
SELECT *
FROM a
JOIN b
    ON a.id = b.id
WHERE b.status = 'active';
```

INNER JOIN 不产生需要保留的未匹配行，因此不论 `b.status = 'active'` 在匹配阶段还是连接后过滤，留下的都是同时满足两个条件的记录。

工程上仍建议把条件按职责摆放：表之间的关联关系写 `ON`，最终结果过滤写 `WHERE`。这不是为了强迫优化器按文本顺序执行，而是为了让读者快速区分“表为何能关联”和“业务要保留哪些行”。现代 Hive、Spark SQL 通常会重写逻辑计划并下推安全谓词，文本位置不等于物理执行位置。

“INNER JOIN 中完全没有区别”也有边界。多层外连接、相关子查询、非确定性函数或某个 `ON` 位置尚不可见的表，会限制条件能否安全移动。判断等价性时，应确认移动条件没有跨过外连接边界，表达式引用的表在该位置可见，并且表达式不会因重复求值或求值时机改变结果。

## RIGHT JOIN 与 FULL OUTER JOIN

RIGHT JOIN 与 LEFT JOIN 对称。RIGHT JOIN 保留右表全部行：

- 左表条件写在 `ON`，控制哪些左表记录能与右表匹配，不删除未匹配的右表行。
- 连接后在 `WHERE` 中使用拒绝 `NULL` 的左表条件，会删除左表补 `NULL` 的行，使结果呈现 INNER JOIN 效果。
- 要过滤右表输入，应写 `WHERE` 或先过滤右表；把右表条件写在 `ON` 里不会删除被保留的右表行。

FULL OUTER JOIN 会保留左右两侧的未匹配行。`WHERE` 条件的退化方向取决于过滤哪一侧：

| `WHERE` 条件 | 被删除的未匹配行 | 结果形态 |
|---|---|---|
| 只要求左表字段满足非空/比较条件 | 右表独有行，因为左表列为 `NULL` | LEFT JOIN 类似效果 |
| 只要求右表字段满足非空/比较条件 | 左表独有行，因为右表列为 `NULL` | RIGHT JOIN 类似效果 |
| 左右两侧都要求非空/比较条件 | 两侧独有行都被删除 | INNER JOIN 类似效果 |

“类似效果”不代表任何条件下都能直接替换 JOIN 类型。`WHERE` 还可能过滤原本已经匹配的行，必须结合具体谓词判断。

## Hive、Spark SQL 中怎样兼顾语义和性能

### 先保证外连接语义

以下写法能明确表示“保留所有有效左表记录，只关联右表 active 数据”：

```sql
SELECT
    a.id,
    b.status
FROM (
    SELECT *
    FROM a
    WHERE ds = '${bizdate}'
) a
LEFT JOIN (
    SELECT *
    FROM b
    WHERE ds = '${bizdate}'
      AND status = 'active'
) b
    ON a.id = b.id;
```

右表过滤也可以写在 `ON` 中。使用子查询的价值是把“输入范围”和“关联关系”分开表达；是否真正下推到文件扫描层，还取决于数据源、分区字段、表达式类型和引擎版本，应通过 `EXPLAIN` 或 Spark UI 检查。

不要使用“驱动表条件写 WHERE、被驱动表条件写 ON”作为通用规则。逻辑 SQL 中更准确的称呼是“保留侧”和“非保留侧”；Spark 最终采用广播哈希连接、排序合并连接还是其他物理计划，由优化器根据统计信息、表大小和配置决定，并不完全服从 SQL 左右位置。

### 谓词下推不是只看写在 ON 还是 WHERE

对 LEFT JOIN，右表过滤写在 `ON` 中通常可以安全地下推到右表输入，因为它只限制匹配资格。右表过滤写在 `WHERE` 中会改变未匹配行的保留语义；优化器如果证明它是“拒绝 NULL”的条件，可能先把 LEFT JOIN 改写成 INNER JOIN，再执行下推。因此，“OUTER JOIN 的 WHERE 条件一定不能下推”并不准确。

需要验证的是物理计划中的事实：

- 分区条件是否出现在 `PartitionFilters`。
- 数据源过滤是否出现在 `PushedFilters` 或等价扫描信息中。
- Join 前是否已经出现 `Filter`。
- 实际参与 Shuffle 的行数和字节数是否减少。

### 倾斜处理要区分 NULL 与热点值

普通等值连接使用 `=` 时，`NULL` 不会与另一个 `NULL` 匹配。INNER JOIN 的空关联键通常可以提前过滤；LEFT JOIN 保留侧的空键不能随意删除，否则会改变结果。真正形成大分区的常见原因是某个非空热点 Key（包括被当作正常值的空字符串、`0`、`unknown`）集中出现。

处理前先确认 Key 分布和 JOIN 类型，再选择：过滤业务无效键、把热点键拆为单独分支、广播小表、启用并验证 Spark AQE Skew Join，或对确需匹配的热点键做可逆加盐。直接用 `CASE WHEN` 随机打散但不对另一侧做对应扩展，会造成漏匹配。

## 面试回答模板

可以直接回答：

> `ON` 决定两张表哪些记录能够匹配，`WHERE` 过滤连接后的结果。INNER JOIN 中，同一个普通确定性条件放在 `ON` 或 `WHERE`，结果通常等价；外连接中会不同，因为 `ON` 判断完匹配后还要为未匹配行补 `NULL`，而 `WHERE` 会删除条件结果为 `FALSE` 或 `UNKNOWN` 的行。例如 LEFT JOIN 后在 WHERE 中限制右表字段，通常会删掉右表补 NULL 的记录，使结果呈现 INNER JOIN 效果。写 SQL 时，先确定哪一侧必须保留，再决定条件是在限制输入、限制匹配，还是过滤最终结果。

如果面试官追问反连接：

> 查左表有、右表没有的数据，可以用 `LEFT JOIN ... WHERE b.id IS NULL`，但检查的右表字段必须保证匹配时非空；在 Hive 或 Spark SQL 中也可以直接使用 `LEFT ANTI JOIN`，语义更明确。

如果面试官追问性能：

> 我会先保证外连接语义，再看优化。右表匹配条件可以写在 ON 或右表子查询中，给优化器下推机会；左表输入过滤写 WHERE 或左表子查询。最终通过 EXPLAIN、扫描过滤信息和 Shuffle 指标确认是否真的提前过滤，不根据 SQL 文本位置猜物理执行顺序。

## 快速自检

写完外连接后，逐项检查：

- [ ] 哪一侧是必须保留的？
- [ ] 每个条件是在过滤输入、限制匹配，还是过滤最终结果？
- [ ] `WHERE` 引用补 `NULL` 一侧时，是否会误删未匹配行？
- [ ] 反连接检查的右表字段是否在真实匹配时必不为 `NULL`？
- [ ] 是否把保留侧过滤误写进 `ON`，导致该侧行没有被删除？
- [ ] 优化结论是否经过 `EXPLAIN`、扫描量和 Shuffle 指标验证？

## 关联笔记

- [[30-计算与SQL开发/31-SQL开发/03-SQL模式、数据质量与易错语义|SQL 模式、数据质量与易错语义]]
- [[30-计算与SQL开发/32-Spark/04-Spark执行模型、Join与AQE|Spark 执行模型、Join 与 AQE]]
- [[30-计算与SQL开发/32-Spark/性能优化/HashJoin和SortMergeJoin对比|HashJoin 和 SortMergeJoin 对比]]

