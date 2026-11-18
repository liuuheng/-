---
aliases:
  - "09-Doris 分区变更：Bucket、分布键与副本数"
  - "Doris 分区变更：Bucket、分布键与副本数"
tags:
  - doris
  - doris-精讲
---

# Doris 精讲（四）：已有分区、新分区与副本的变更边界

订单表原来每天 32 桶，后续数据量增加，于是想把 5 月 15 日调成 64 桶，5 月 16 日调成 128 桶。Doris 4.x 可以为新分区选择新的 Bucket 数。已有分区的物理布局、未来分区的默认设置和副本数分别有独立的变更范围。

## 一、给后续日期增加分区时，边界如何生效

假设数据库中具备三副本的 BE 放置条件，示例表没有 Colocation 约束或动态分区规则。先创建 5 月 14 日分区，再追加两天。

```sql
CREATE TABLE t_order (
    order_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    dt DATE NOT NULL,
    amount DECIMAL(18, 2) NULL
)
DUPLICATE KEY(order_id)
PARTITION BY RANGE(dt) (
    PARTITION p20260514 VALUES [("2026-05-14"), ("2026-05-15"))
)
DISTRIBUTED BY HASH(user_id) BUCKETS 32
PROPERTIES (
    "replication_num" = "3"
);

ALTER TABLE t_order
ADD PARTITION p20260515 VALUES LESS THAN ('2026-05-16')
DISTRIBUTED BY HASH(user_id) BUCKETS 64;

ALTER TABLE t_order
ADD PARTITION p20260516 VALUES LESS THAN ('2026-05-17')
("replication_num" = "3")
DISTRIBUTED BY HASH(user_id) BUCKETS 128;
```

| 执行后 | 分区范围（左闭右开） | 桶数 | 路由列 |
| --- | --- | --- | --- |
| 建表 | 5 月 14 日至 5 月 15 日 | 32 | `user_id` |
| 第一次 ADD | 5 月 15 日至 5 月 16 日 | 64 | `user_id` |
| 第二次 ADD | 5 月 16 日至 5 月 17 日 | 128 | `user_id` |

`VALUES LESS THAN` 只写上界，本例根据已有连续分区确定左边界。若名为 `p20260515` 的分区写入了 2025 年边界，系统以边界判断范围，分区名不能纠正年份。实际追加时，应先确认是否有相交区间、MAXVALUE 分区或自动管理规则。建表的 `PROPERTIES (...)` 与 `ADD PARTITION` 中边界后的属性括号是不同语法。

新分区增加 Bucket 后，三天的数据仍各自在自己的分区里。若要把分桶列从 `user_id` 换成 `order_id`，不能靠 `ADD PARTITION` 更换同表的分布方式或列，需要评估新表和数据重写。

## 二、改默认桶数和改副本数，各影响谁

要将未来分区的默认值设为 64，可用 Doris 4.x 的分布配置语法。

```sql
ALTER TABLE t_order
MODIFY DISTRIBUTION DISTRIBUTED BY HASH(user_id) BUCKETS 64;
```

上一步已有的 32、64、128 桶保持各自配置。Dynamic Partition 创建的新分区还可由 `dynamic_partition.buckets` 控制，它同样不追溯修改已有分区。Colocation 表需同时检查组的 Bucket 数约束。

副本数控制同一 Tablet 的冗余份数。下例单独说明已有的 `p20260714` 如何从较少副本调整到三份。该分区须先存在，也可将目标换成示例中已创建的 `p20260514`。

```sql
ALTER TABLE t_order
MODIFY PARTITION p20260714
SET ("replication_num" = "3");
```

增加副本后，Tablet 的路由和数据范围保持不变。命令先改变预期副本配置，系统再进行后台读取、复制和放置。满足约束要看可用 BE、标签和存储介质，不能只数集群总 BE。对大表要评估网络和磁盘读写；需要调整多个已有分区时可列举分区，支持的版本可使用 `MODIFY PARTITION (*)`，并另外核对未来分区的默认值和动态分区属性。

## 三、在配置与实际分布之间做一次对照

用 `SHOW CREATE TABLE t_order;` 看表定义和默认分布，用 `SHOW PARTITIONS FROM t_order;` 看每个分区的实际边界、桶数和副本配置。继续用 `SHOW TABLETS FROM t_order;` 查看实际副本放置与版本健康。如果分区已配置三副本而 Tablet 仍只有两份健康副本，说明后台补齐或修复还需要时间。Colocation 表还要确认组稳定并复查 Join 计划，见 [[09-Doris 精讲（九）：从两张表的桶布局推导 Colocation Join|从两张表的桶布局推导 Colocation Join]]。
