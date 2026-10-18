# Paimon Snapshot 历史分区查询

## 一、核心结论

查询 Paimon 的历史分区需要分成两步：

1. 通过 `snapshot_id`、提交时间或 Tag 选择一个历史表版本。
2. 通过 `WHERE dt = '...'` 从该版本中选择业务分区。

两者不是同一个概念：

- Paimon Snapshot 是整张表的一次原子提交版本。
- `dt` 是表内的业务分区字段。
- 一个 Snapshot 可以同时包含或修改多个 `dt` 分区。
- 同一个 `dt` 分区在不同 Snapshot 中可能呈现不同状态。

因此应记住：**先选版本，再选分区。**

## 二、先查询当前保留的 Snapshot

在 Flink SQL 中先使用批模式：

```sql
SET 'execution.runtime-mode' = 'batch';

SELECT
    snapshot_id,
    schema_id,
    commit_kind,
    commit_time,
    total_record_count,
    delta_record_count
FROM `orders$snapshots`
ORDER BY snapshot_id DESC;
```

如果需要使用完整表名：

```sql
SELECT *
FROM my_catalog.my_database.`orders$snapshots`
ORDER BY snapshot_id DESC;
```

`snapshot_id` 是 Paimon 表版本号，不是 Flink Checkpoint ID。常见 `commit_kind` 包括 `APPEND`、`COMPACT` 和 `OVERWRITE`。其中 `COMPACT` 通常只改变文件布局，不改变逻辑业务结果。

## 三、查询某个 Snapshot 中的指定分区

假设需要查询 Snapshot 103 中 `dt = '2026-09-15'` 的数据：

```sql
SELECT *
FROM orders /*+ OPTIONS(
    'scan.snapshot-id' = '103'
) */
WHERE dt = '2026-09-15';
```

执行逻辑为：

```text
选择 Snapshot 103
    → 读取该 Snapshot 引用的 Manifest
    → 得到该版本的有效文件集合
    → 按 dt=2026-09-15 做分区裁剪
    → 读取目标分区中的 Bucket 和数据文件
```

该 SQL 返回的是：**Snapshot 103 时，目标分区的完整逻辑状态。**

它不是只返回 Snapshot 103 这一次提交新增的数据。

如果目标分区在 Snapshot 103 时尚未创建，查询结果为空；即使该分区存在于最新 Snapshot 中，也不会被历史查询读到。

## 四、按时间查询历史分区

可以按照提交时间选择历史表版本，再过滤分区：

```sql
SELECT *
FROM orders /*+ OPTIONS(
    'scan.timestamp' = '2026-09-15 10:59:30'
) */
WHERE dt = '2026-09-15';
```

也可以使用 Unix 毫秒时间戳：

```sql
SELECT *
FROM orders /*+ OPTIONS(
    'scan.timestamp-millis' = '1789441170000'
) */
WHERE dt = '2026-09-15';
```

Flink 1.18 及以上还支持标准 Time Travel 语法：

```sql
SELECT *
FROM orders
FOR SYSTEM_TIME AS OF TIMESTAMP '2026-09-15 10:59:30'
WHERE dt = '2026-09-15';
```

使用 `scan.timestamp` 时应确认 Flink SQL Client、集群和业务使用的时区一致。需要精确、可复现地回查时，优先记录并使用明确的 `snapshot_id`。

## 五、比较同一分区在两个 Snapshot 中的状态

分别查询两个版本：

```sql
SELECT *
FROM orders /*+ OPTIONS(
    'scan.snapshot-id' = '100'
) */
WHERE dt = '2026-09-15';
```

```sql
SELECT *
FROM orders /*+ OPTIONS(
    'scan.snapshot-id' = '105'
) */
WHERE dt = '2026-09-15';
```

这两次查询得到的是同一个业务分区在两个表版本上的完整状态。可以将结果写入临时表，再按主键比较新增、删除和字段变化。

## 六、查询两个 Snapshot 之间的增量

如果目标不是查看历史完整状态，而是查看 Snapshot 100 到 105 之间发生的变化，可以使用：

```sql
SELECT *
FROM orders /*+ OPTIONS(
    'incremental-between' = '100,105'
) */
WHERE dt = '2026-09-15';
```

区间语义是 `(100, 105]`：不包含起始 Snapshot 100，包含结束 Snapshot 105。

必须区分：

| 目标 | 配置 |
| --- | --- |
| Snapshot 103 时分区的完整状态 | `scan.snapshot-id = 103` 加 `WHERE dt = ...` |
| Snapshot 100 到 105 之间的变化 | `incremental-between = 100,105` 加 `WHERE dt = ...` |

批式增量查询不一定返回完整的删除和更新前镜像。需要观察 `+I`、`-U`、`+U`、`-D` 时，可以查询 `audit_log`：

```sql
SELECT *
FROM `orders$audit_log` /*+ OPTIONS(
    'incremental-between' = '100,105'
) */
WHERE dt = '2026-09-15';
```

能否获得完整 Before/After，还取决于表的 `changelog-producer` 和实际写入语义。

## 七、历史查询依赖 Snapshot 尚未过期

如果目标 Snapshot 已不在 `$snapshots` 系统表中，普通 Time Travel 就无法继续查询该版本。Snapshot 保留由以下参数共同控制：

```sql
WITH (
    'snapshot.time-retained' = '24 h',
    'snapshot.num-retained.min' = '10',
    'snapshot.num-retained.max' = '4000'
)
```

- `snapshot.time-retained`：控制时间保留窗口。
- `snapshot.num-retained.min`：控制最少保留的 Snapshot 数。
- `snapshot.num-retained.max`：控制最多保留的 Snapshot 数。

不能只看 `snapshot.time-retained`。如果 Checkpoint 每 30 秒提交一次，一天可能产生约 2880 个 Snapshot；此时如果 `snapshot.num-retained.max = 1000`，实际覆盖时间可能不足 24 小时。

## 八、长期保存关键历史版本应使用 Tag

Snapshot 适合滚动版本管理；需要长期保留的月末、关账或上线前版本，应创建 Tag。查询时可以使用：

```sql
SELECT *
FROM orders /*+ OPTIONS(
    'scan.tag-name' = 'month-end-2026-08'
) */
WHERE dt = '2026-08-31';
```

可以将二者理解为：

- Snapshot：短期滚动的表版本。
- Tag：对重要版本建立长期、稳定的名称引用。

## 九、常见误区

1. `snapshot_id` 不是 `dt`，它表示整张表的版本。
2. `scan.snapshot-id` 返回该 Snapshot 的完整表状态，不是该次提交的增量。
3. `WHERE dt = ...` 只负责分区过滤，不负责选择历史版本。
4. 最新表中存在的分区，不代表它在某个历史 Snapshot 中已经存在。
5. 已过期的 Snapshot 无法仅凭原 ID 恢复查询。
6. Compaction 可能生成新 Snapshot，但新旧 Snapshot 的逻辑数据可能完全相同，只是底层文件布局发生变化。
7. 长期历史应使用业务 `dt` 分区或 Tag，不能完全依赖短期滚动的 Snapshot。

## 十、查询模板

```sql
-- 1. 查询当前保留的 Snapshot
SELECT snapshot_id, commit_kind, commit_time
FROM `table_name$snapshots`
ORDER BY snapshot_id DESC;

-- 2. 查询指定 Snapshot 中的指定分区
SELECT *
FROM table_name /*+ OPTIONS(
    'scan.snapshot-id' = '<snapshot-id>'
) */
WHERE dt = '<partition-value>';

-- 3. 查询指定时间点的指定分区
SELECT *
FROM table_name
FOR SYSTEM_TIME AS OF TIMESTAMP '<yyyy-MM-dd HH:mm:ss>'
WHERE dt = '<partition-value>';

-- 4. 查询两个 Snapshot 之间指定分区的增量
SELECT *
FROM table_name /*+ OPTIONS(
    'incremental-between' = '<start-snapshot>,<end-snapshot>'
) */
WHERE dt = '<partition-value>';
```

## 十一、官方资料

- [Apache Paimon 2.0：Flink SQL Query](https://paimon.apache.org/docs/2.0/flink/sql-query/)
- [Apache Paimon 2.0：System Tables](https://paimon.apache.org/docs/2.0/concepts/system-tables/)
- [Apache Paimon 2.0：Manage Snapshots](https://paimon.apache.org/docs/2.0/maintenance/manage-snapshots/)

最终记忆：**Snapshot 选择历史表版本，`dt` 选择该版本中的业务分区；查看完整状态用 `scan.snapshot-id`，查看版本间变化用 `incremental-between`，长期版本用 Tag。**
