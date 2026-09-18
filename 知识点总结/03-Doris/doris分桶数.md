BUCKETS ≈ BE 节点数 × 2 ~ ×4   分桶数

补充一个概念：
> “高基数”（High Cardinality）通俗来讲就是：这一列里的数据“重复率极低，绝大多数都是独一无二的”。


分桶键：
```text
string 字符

DECIMAL(p, s)​  浮点

BIGINT / LARGEINT  整数

DATETIMEV2  2026-05-13 15:30:45

DATETIMEV2(3)  2026-05-13 15:30:45.123

DATETIMEV2(6)  2026-05-13 15:30:45.123456

DATE  2026-05-13
分桶键（Bucket Key）必须是 BIGINT​ 或 LARGEINT​ 或 STRING
```