---
tags:
  - sql
  - data-quality
  - patterns
status: active
---

# SQL 模式、数据质量与易错语义

返回：[[README|企业微信知识地图]]

## 1. SQL 的逻辑处理顺序

理解逻辑顺序能解释很多“为什么别名不能用”的问题：

```text
FROM/JOIN → WHERE → GROUP BY → HAVING → 窗口函数 → SELECT → DISTINCT → ORDER BY → LIMIT
```

优化器可以重排物理执行，但不能改变结果语义。由于 WHERE 逻辑上早于 SELECT，同层 WHERE 通常不能引用 SELECT 中刚定义的别名，应使用子查询或 CTE。别名能否用于 GROUP BY/HAVING 属于方言差异，不应依赖跨引擎不一致行为。

## 2. LEFT JOIN 中 ON 与 WHERE 的差异

右表过滤放在 ON 中，左表未匹配行仍会保留；放在 WHERE 中会把右表为空的扩展行过滤掉，LEFT JOIN 实际退化为 INNER JOIN。

```sql
-- 保留没有有效订单的用户
SELECT u.user_id, o.order_id
FROM users u
LEFT JOIN orders o
  ON u.user_id = o.user_id
 AND o.status = 'paid';

-- 只保留有已支付订单的用户
SELECT u.user_id, o.order_id
FROM users u
LEFT JOIN orders o ON u.user_id = o.user_id
WHERE o.status = 'paid';
```

## 3. NULL 不是空字符串或零

普通 `=` 与 NULL 比较得到 UNKNOWN，而不是 true/false。Spark SQL 可用 `<=>` 做 null-safe equality。`COUNT(*)` 统计行数，`COUNT(col)` 忽略 NULL；多数聚合在空输入上返回 NULL，因此金额汇总可能需要 `COALESCE(SUM(amount),0)`。

`NOT IN` 尤其危险：只要子查询结果包含 NULL，比较可能整体变成 UNKNOWN。需要反连接时优先使用 `NOT EXISTS` 并明确关联条件：

```sql
SELECT a.*
FROM a
WHERE NOT EXISTS (
  SELECT 1 FROM b WHERE b.id = a.id
);
```

## 4. 时间边界使用半开区间

增量抽取推荐：

```sql
WHERE lastmodified >= TIMESTAMP '2026-07-03 00:00:00'
  AND lastmodified <  TIMESTAMP '2026-07-04 00:00:00'
```

不要写到 `23:59:59.999`，因为源字段可能是微秒或更高精度，还会遇到时区和夏令时。半开区间能首尾无缝拼接，不重不漏。

时间解析必须明确输入格式。`unix_timestamp` 在部分 Spark 版本中只有秒精度，处理毫秒可能截断；高精度排序应保留 timestamp 或使用明确的毫秒/微秒数值。

## 5. 补全缺失日期并计算累计值

思路是先求每个用户的最早和最晚日期，生成日期序列，再左连接原数据，最后用 ROWS 窗口累计：

```sql
WITH bounds AS (
  SELECT user_id, MIN(dt) AS start_dt, MAX(dt) AS end_dt
  FROM daily_pay
  GROUP BY user_id
), calendar AS (
  SELECT user_id, EXPLODE(SEQUENCE(start_dt, end_dt, INTERVAL 1 DAY)) AS dt
  FROM bounds
)
SELECT
  c.user_id,
  c.dt,
  COALESCE(p.pay_amount, 0) AS pay_amount,
  SUM(COALESCE(p.pay_amount, 0)) OVER (
    PARTITION BY c.user_id
    ORDER BY c.dt
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_pay_amount
FROM calendar c
LEFT JOIN daily_pay p
  ON c.user_id = p.user_id AND c.dt = p.dt;
```

显式写 `ROWS` 是为了避免默认 RANGE 在重复排序键下把同一日期的 peer 行一起纳入，导致累计结果难以解释。

## 6. 连续登录：日期减行号

连续日期按天增长 1，`row_number` 也增长 1，两者相减得到相同分组锚点：

```sql
WITH dedup AS (
  SELECT DISTINCT user_id, login_date FROM login_log
), marked AS (
  SELECT
    user_id,
    login_date,
    DATE_SUB(login_date, ROW_NUMBER() OVER (
      PARTITION BY user_id ORDER BY login_date
    )) AS grp
  FROM dedup
)
SELECT user_id, MIN(login_date), MAX(login_date), COUNT(*) AS consecutive_days
FROM marked
GROUP BY user_id, grp;
```

第一步必须按天去重，否则一天多次登录会让行号多加，破坏连续分组。

## 7. 合并重叠区间

只比较 `LAG(end_date)` 在嵌套区间中可能出错。例如 `[1,10]`、`[2,3]`、`[9,12]`，第三段与前面整体重叠，但只看上一行的结束 3 会误判为新区间。更稳妥的是比较“此前最大结束时间”：

```sql
WITH x AS (
  SELECT *,
    MAX(end_date) OVER (
      PARTITION BY product_id
      ORDER BY start_date, end_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS prev_max_end
  FROM product_promotions
), y AS (
  SELECT *,
    CASE WHEN prev_max_end IS NULL OR start_date > prev_max_end THEN 1 ELSE 0 END AS new_grp
  FROM x
), z AS (
  SELECT *,
    SUM(new_grp) OVER (
      PARTITION BY product_id ORDER BY start_date, end_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS grp
  FROM y
)
SELECT product_id, grp, MIN(start_date), MAX(end_date), COUNT(*) AS merged_count
FROM z
GROUP BY product_id, grp;
```

若相邻日期也算连续，应把判断改为 `start_date > DATE_ADD(prev_max_end,1)`。

## 8. 递归 CTE 与组织架构

递归 CTE 包含锚点和递归成员。每轮用上一轮结果寻找下一层下属，直到没有新行：

```sql
WITH RECURSIVE hierarchy AS (
  SELECT employee_id, name, manager_id, 0 AS level,
         CAST(employee_id AS STRING) AS path
  FROM employees
  WHERE employee_id = 101

  UNION ALL

  SELECT e.employee_id, e.name, e.manager_id, h.level + 1,
         CONCAT(h.path, ',', e.employee_id)
  FROM employees e
  JOIN hierarchy h ON e.manager_id = h.employee_id
  WHERE FIND_IN_SET(CAST(e.employee_id AS STRING), h.path) = 0
)
SELECT * FROM hierarchy;
```

路径判断用于防止脏数据形成环。生产中还应限制最大深度，并单独监控孤儿节点和循环引用。

已有 `node_path` 时无需递归，可先 `posexplode(split(node_path,','))`，再通过条件聚合把第 0、1、2 层转成列。路径字段应规定分隔符、是否包含自身和根节点，避免不同来源解释不一致。

## 9. 数组高阶函数与炸裂

Spark SQL 常用：

```sql
SELECT
  transform(arr, x -> x * x),
  filter(arr, x -> x % 3 = 1),
  aggregate(arr, 0, (acc, x) -> acc + x),
  zip_with(arr1, arr2, (x, y) -> x * y)
FROM t;
```

`explode`/`posexplode` 会把一行扩成多行，之后 join 或聚合前要重新确认粒度。Flink SQL 通常使用 `LATERAL TABLE(UNNEST(tags))`；想保留空数组对应的左表行，应使用 `LEFT JOIN LATERAL ... ON TRUE`。

## 10. 按权重进行整数金额分摊

先按比例向下取整，再把余数逐个分给排序靠前的项目，可以保证分摊和等于订单金额：

```text
raw_i   = order_amount * weight_i / sum(weight)
base_i  = floor(raw_i)
remain  = order_amount - sum(base_i)
final_i = base_i + (row_number <= remain ? 1 : 0)
```

如果要求“权重大者分得不能更少”，余数排序应按权重降序，并增加稳定的 `item_id` 作为并列 tie-breaker。更符合公平分配的方式是按小数余数从大到小分配，这就是最大余数法。

## 11. 去重必须有确定规则

“取最新一条”至少需要业务键、事件时间、入仓时间和稳定序列：

```sql
ROW_NUMBER() OVER (
  PARTITION BY business_key
  ORDER BY event_time DESC, ingest_time DESC, source_seq DESC
) AS rn
```

只按秒级 `lastmodified` 排序可能并列，结果不确定。CDC 场景优先使用源端单调版本或 binlog 位点；若拿不到，必须承认只能做近似最后写入覆盖，并依赖下游幂等。

## 12. UNION、INTERSECT 与 EXCEPT

- `UNION ALL` 保留全部行，通常更快；`UNION` 需要全局去重。
- `INTERSECT ALL` 保留两侧重复次数的最小值。
- `EXCEPT ALL` 保留左侧计数减右侧计数后的正数部分。

不能为了“保险”默认使用 UNION，因为无必要的去重会引入 Shuffle/Sort；也不能默认 UNION ALL，因为重复行可能改变业务口径。

## 13. 新旧表逐行校验

逐行方案把核心字段规范化后计算行哈希，再按主键比较。原聊天 SQL 中第二个分支误写成了同一张表，实际必须分别读取 `table1` 与 `table2`。

```sql
WITH all_rows AS (
  SELECT pk1, pk2,
         MD5(CONCAT_WS('|',
           COALESCE(CAST(col1 AS STRING), '<NULL>'),
           COALESCE(CAST(col2 AS STRING), '<NULL>')
         )) AS row_hash,
         't1' AS src
  FROM table1
  UNION ALL
  SELECT pk1, pk2,
         MD5(CONCAT_WS('|',
           COALESCE(CAST(col1 AS STRING), '<NULL>'),
           COALESCE(CAST(col2 AS STRING), '<NULL>')
         )) AS row_hash,
         't2' AS src
  FROM table2
)
SELECT pk1, pk2,
       COUNT(DISTINCT src) AS source_count,
       COUNT(DISTINCT row_hash) AS hash_count
FROM all_rows
GROUP BY pk1, pk2
HAVING source_count <> 2 OR hash_count <> 1;
```

使用 `<NULL>` 而不是空字符串，是为了区分 NULL 与真实空串；分隔符可能出现在字段中时，应使用长度前缀、结构化序列化或逐列比较，避免拼接碰撞。

## 14. 分区级 Hash + Sum 校验的边界

把每列 hash 求和可以快速发现大分区差异，适合数亿行初筛，但不能定位具体行，也不是数学上绝对无碰撞。稳妥流程是：

1. 比较行数、主键去重数、空值数、最值和金额总和。
2. 比较分区级多种 hash 摘要。
3. 只对异常分区运行逐行校验。

随机数、当前时间、浮点格式、数组顺序、字符串拼接和非确定 UDF 会让两次计算不稳定，应先规范化或排除。

## 15. SQL 性能不能只背复杂度

“GROUP BY 一定比窗口快”或“Hash Join 一定是 O(n)”都过于绝对。真实代价还取决于 Shuffle、排序是否可复用、数据倾斜、内存溢写和扫描次数。多个窗口共享同一分区和排序时，一次排序可能比 GROUP BY 后再 join 更划算。结论必须通过执行计划和运行指标验证。

## 参考依据

- [Spark SQL NULL 语义](https://spark.apache.org/docs/latest/sql-ref-null-semantics.html)
- [Spark SQL 窗口函数](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html)
- [PostgreSQL 表表达式与 JOIN/WHERE 语义](https://www.postgresql.org/docs/current/queries-table-expressions.html)
