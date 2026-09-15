# Paimon Snapshot、Manifest、Parquet 与 Compaction 的关系

## 核心结论

Paimon 的业务数据最终保存在 Parquet、ORC、Avro 等数据文件中。`dt` 等分区字段决定数据属于哪个 Partition，Snapshot 定义某一已提交表版本，Manifest 负责记录该版本增加或移除了哪些文件，Compaction 则读取多个旧数据文件并生成新的整理后文件，再通过新的 Snapshot 原子发布文件替换。

最重要的关系是：

```text
dt / Partition：数据放在哪个业务分区
Bucket：分区内部如何分布
Parquet：真正保存业务记录
Manifest：记录文件级 ADD/DELETE
Snapshot：发布一个完整、可查询的表版本
Compaction：重写数据文件，并用新 Snapshot 发布结果
```

Snapshot 不会转换成 Parquet。正确顺序是先生成 Parquet，再生成 Manifest，最后提交 Snapshot。

## 一、底层文件的分层关系

一个 Paimon 表的典型读取链路是：

```text
Snapshot
  → Manifest List
    → Manifest
      → Data File / Changelog File / Index File
```

各层职责如下：

| 对象 | 主要职责 | 是否保存业务记录 |
| --- | --- | --- |
| Schema | 保存表结构版本 | 否 |
| Snapshot | 描述某次提交后的表状态 | 否 |
| Manifest List | 保存 Manifest 文件列表 | 否 |
| Manifest | 记录数据文件的新增、删除及统计信息 | 否 |
| Data File | 保存实际业务数据 | 是 |
| Changelog File | 保存供流式下游消费的变更 | 是，保存变更记录 |
| Index File | 保存动态桶、Deletion Vector 等索引信息 | 否 |

因此，不能把 Paimon 简单理解成“一堆 Parquet 文件”。Parquet 只保存数据，Snapshot 和 Manifest 才赋予这些文件版本、一致性、更新和 Time Travel 语义。

## 二、`dt` 与 Parquet 文件的关系

假设表定义为：

```sql
CREATE TABLE orders (
    order_id BIGINT,
    status STRING,
    amount DECIMAL(18, 2),
    dt STRING,
    PRIMARY KEY (dt, order_id) NOT ENFORCED
) PARTITIONED BY (dt);
```

写入一条记录时，Paimon 先根据 `dt` 确定 Partition，再根据 Bucket 规则确定 Bucket，最后由 Writer 在对应位置生成数据文件：

```text
order_id=1001, status=CREATED, dt=2026-09-15
    ↓
Partition：dt=2026-09-15
    ↓
Bucket：bucket-0
    ↓
Data File：file-A.parquet
```

概念上的目录结构可能是：

```text
orders/
├── snapshot/
├── manifest/
├── schema/
└── dt=2026-09-15/
    └── bucket-0/
        └── file-A.parquet
```

但查询不能直接把该目录中的所有 Parquet 都当成有效数据，因为目录里可能同时存在：

- 当前 Snapshot 正在使用的文件；
- 已被 Compaction 替换、但仍被历史 Snapshot 引用的旧文件；
- 未成功提交的文件；
- 等待清理的孤儿文件；
- 同一主键的多个物理版本。

哪些文件真正属于目标表版本，必须由 Snapshot 和 Manifest 决定。

## 三、第一次写入如何形成可查询数据

第一次写入可以分为五步：

```text
1. Writer 接收记录
2. 根据 Partition 和 Bucket 缓冲、排序数据
3. 生成 file-A.parquet
4. Manifest 记录 ADD file-A.parquet
5. 提交 Snapshot 1，正式发布 file-A.parquet
```

刚生成 Parquet 不等于数据已经对查询可见。只有成功提交的 Snapshot 引用了这个文件，它才成为正式表数据。

关系可以表示为：

```text
Snapshot 1
  → Manifest
    → ADD file-A.parquet
```

## 四、普通查询如何读取

普通批查询写的是表，而不是 Snapshot 文件：

```sql
SELECT *
FROM orders
WHERE dt = '2026-09-15';
```

Paimon 内部执行过程是：

```text
1. 选择最新 Snapshot
2. 读取 Snapshot 引用的 Manifest List
3. 根据 Manifest 还原当前有效文件集合
4. 根据 dt 进行分区裁剪
5. 根据 Bucket、主键范围和文件统计继续裁剪
6. 读取目标 Parquet 文件
7. 主键表应用 sequence.field 和 merge-engine
8. 返回最终逻辑结果
```

如果明确指定历史 Snapshot：

```sql
SELECT *
FROM orders /*+ OPTIONS('scan.snapshot-id' = '2') */
WHERE dt = '2026-09-15';
```

返回的是 Snapshot 2 对应的完整表状态，不是仅返回 Snapshot 2 本次增加的记录。

如果只想查看两个版本之间的增量，应使用 `incremental-between`：

```sql
SELECT *
FROM orders /*+ OPTIONS('incremental-between' = '2,5') */;
```

## 五、更新为什么会产生多个 Parquet

假设 `file-A.parquet` 已保存：

```text
order_id=1001, status=CREATED
order_id=1002, status=CREATED
```

随后收到：

```text
order_id=1001, status=PAID
```

Paimon 通常不会打开旧 Parquet 原地修改，而是生成新的不可变文件：

```text
file-A.parquet：
  order_id=1001, status=CREATED
  order_id=1002, status=CREATED

file-B.parquet：
  order_id=1001, status=PAID
```

提交 Snapshot 2 后，当前有效文件可能同时包含 `file-A` 和 `file-B`。主键表读取时根据主键、`sequence.field` 和 `merge-engine` 得到：

```text
order_id=1001, status=PAID
order_id=1002, status=CREATED
```

此时物理层有多个版本，逻辑查询结果只有最终版本。

## 六、Compaction 如何生成最终 Parquet

Compaction 合并的是数据文件和其中的记录版本，不是合并 Snapshot。

```text
输入：
  file-A.parquet
  file-B.parquet

Compaction：
  读取旧文件
  → 按主键排序和合并版本
  → 生成 file-C.parquet
```

新的 `file-C.parquet` 可能保存：

```text
order_id=1001, status=PAID
order_id=1002, status=CREATED
```

文件成功写完后，再提交新的 COMPACT Snapshot：

```text
Snapshot 3
  DELETE file-A.parquet
  DELETE file-B.parquet
  ADD    file-C.parquet
```

所以正确流程是：

```text
旧 Parquet
  → Compaction 读取和合并
  → 新 Parquet
  → Manifest 记录旧文件 DELETE、新文件 ADD
  → 新 Snapshot 原子发布
```

Snapshot 始终是元数据，不会“变成最终 Parquet”。所谓最终 Parquet，是 Compaction Worker 直接根据旧数据文件写出的新文件。

## 七、Compaction 前后 Snapshot 的关系

假设表经历三个版本：

```text
Snapshot 1：
  有效文件：file-A

Snapshot 2：
  有效文件：file-A、file-B

Snapshot 3：
  有效文件：file-C
```

其中 Snapshot 3 是 Compaction 的结果。它没有把 Snapshot 1 和 Snapshot 2 合并，而是记录“最新版本应该使用 `file-C`”。

不同版本的查询行为是：

```text
查询 Snapshot 1 → 读取 file-A
查询 Snapshot 2 → 读取 file-A、file-B，并在读取时合并版本
查询 Snapshot 3 → 读取 file-C
```

Compaction 前后逻辑查询结果通常一致，变化的是物理读取成本：

```text
Compaction 前：读取多个文件并合并多个版本
Compaction 后：读取更少、已经整理好的文件
```

## 八、为什么旧 Parquet 不立即删除

Snapshot 3 发布后，`file-A`、`file-B` 已经不属于最新表状态，但历史 Snapshot 1、2 仍可能引用它们。

因此：

```text
Manifest DELETE
  = 从新 Snapshot 的逻辑状态中移除
  ≠ 立即从存储系统物理删除
```

只有当旧 Snapshot 过期，且没有 Tag 等对象继续引用旧文件时，Paimon 才能清理这些 Parquet。

完整生命周期是：

```text
Compaction 生成新 Parquet
  → 新 Snapshot 发布文件替换
  → 旧 Snapshot 继续保留一段时间
  → 旧 Snapshot 过期
  → 不再被引用的旧 Parquet 被物理清理
```

## 九、Checkpoint、Snapshot 与 Compaction

流式写入中的关系可以概括为：

```text
上游持续输入
  → Writer 生成增量 Parquet
  → Checkpoint 成功
  → 提交 APPEND Snapshot
  → 新数据对查询可见

旧文件和记录版本增加
  → Compaction 生成整理后的 Parquet
  → 提交 COMPACT Snapshot
  → 新查询切换到整理后的文件集合
```

Checkpoint 是 Flink 作业的一致性边界；Snapshot 是 Paimon 表的版本发布边界；Compaction 是数据文件的物理重写过程。

## 十、最终记忆模型

```text
dt 决定数据属于哪个 Partition
        ↓
Bucket 决定分区内部如何分布
        ↓
Parquet 保存实际业务数据
        ↓
Manifest 记录文件级 ADD/DELETE
        ↓
Snapshot 原子发布一个完整表版本
        ↓
查询从 Snapshot 出发找到有效 Parquet
        ↓
Compaction 重写 Parquet，并生成新 Snapshot 发布结果
```

一句话总结：

> `dt` 决定 Parquet 写到哪个分区；Parquet 保存实际业务数据；Snapshot 通过 Manifest 决定某个表版本下哪些 Parquet 有效；Compaction 直接读取旧 Parquet 并生成新 Parquet，再用新的 Snapshot 原子发布这次文件替换。

## 相关笔记

- [[Apache Paimon 表模型、存储组织与读取语义]]
- [[paimon-create-table-research]]

## 官方资料

- [Apache Paimon 2.0 Basic Concepts](https://paimon.apache.org/docs/2.0/concepts/basic-concepts/)
- [Apache Paimon 2.0 Flink SQL Query](https://paimon.apache.org/docs/2.0/flink/sql-query/)
- [Apache Paimon 2.0 Manage Snapshots](https://paimon.apache.org/docs/2.0/maintenance/manage-snapshots/)
