---
aliases:
  - "07-Doris Unique Key 表的 MOW 与部分列更新参数"
  - "Doris Unique Key 表的 MOW 与部分列更新参数"
tags:
  - doris
  - doris-精讲
---

# Doris 精讲（五）：Unique Key 如何覆盖旧行与更新部分列

用户 1 的画像原来是“上海，80 分”。现在只收到一条“城市改为深圳”的消息，分数应该还在。Doris Unique Key 表能否得到这个结果，取决于写入用的是整行 UPSERT 还是部分列更新。MOW 决定旧版本如何失效，skip bitmap 决定灵活更新如何表达每一行的缺列，两者作用在不同层次。

## 一、同一个 Key 怎样从两条物理记录变成一个可见状态

以下示例表配置一个桶和一份副本，假设使用支持灵活部分列更新的版本。先写入两个用户的画像。

```sql
CREATE TABLE user_profile (
    user_id BIGINT NOT NULL,
    city VARCHAR(32) NULL,
    score INT NULL
)
UNIQUE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true",
    "enable_unique_key_skip_bitmap_column" = "true"
);

INSERT INTO user_profile (user_id, city, score)
VALUES (1, '上海', 80), (2, '南京', 70);
```

如果新的整行 UPSERT 传入用户 1 的完整画像 `(1, 深圳, 80)`，MOW 写入路径会查找同 Key 的旧行，记录应失效的旧行位置，并在新版本可见时让查询看到深圳这条。旧 Rowset 里可能仍有上海那条，查询通过 delete bitmap 排除它。后续 Compaction 才能在新写出的 Rowset 中清理该旧行。见 [[06-Doris 精讲（六）：从导入版本到 Rowset 与 Compaction|从导入版本到 Rowset 与 Compaction]]。

`enable_unique_key_merge_on_write` 在建表时确定实现。MOR 将同 Key 的版本合并更多留在查询和 Compaction 路径，与 Aggregate Key 的 `SUM` 业务语义无关。已有表应以 `SHOW CREATE TABLE` 为准。若有乱序事件，另需核对导入版本、Sequence 列规则以及业务期望的顺序。

## 二、只交一列时，什么时候保留旧分数

保持初始数据不变。普通整行 UPSERT 只提供 `user_id` 和 `city` 时，Doris 可能为未提供的 `score` 填 NULL 或默认值。固定部分列更新显式改变这次写入的语义，并要求同一批记录提供相同的一组更新列及全部 Key 列。

```sql
SET enable_unique_key_partial_update = true;

INSERT INTO user_profile (user_id, city)
VALUES (1, '深圳');
```

此时预计用户 1 变成 `(1, 深圳, 80)`，用户 2 仍为 `(2, 南京, 70)`。Stream Load 的固定部分更新可通过 `partial_columns:true` 配合 `columns` 指定输入列。写入侧为新行保留未更新字段，可能要读取旧值并补齐数据，因此导入输入变小不等于写入 I/O 按相同比例减少。

## 三、同一批里有人改城市，有人改分数

接着在上一状态上导入两条 JSON Lines。这个批次各行提供的更新列不同，适用 Doris 3.1.0 起支持的灵活部分列更新。表启用 `enable_unique_key_skip_bitmap_column` 后，Stream Load 还需指定 `format:json`、`read_json_by_line:true` 和 `unique_key_update_mode:UPDATE_FLEXIBLE_COLUMNS`。

```jsonl
{"user_id": 1, "city": "杭州"}
{"user_id": 2, "score": 90}
```

| 用户 | 导入前 | 该行提供的非 Key 列 | 预期可见结果 |
| --- | --- | --- | --- |
| 1 | 深圳，80 | `city` | 杭州，80 |
| 2 | 南京，70 | `score` | 南京，90 |

隐藏列 `__DORIS_SKIP_BITMAP_COL__` 表达每行未提供哪些字段。用户 1 没传 `score` 时保留 80。若显式传 `"score": null`，这次更新就将允许为空的分数写成 NULL。delete bitmap 则标记旧版本中哪些**行**不可见。skip bitmap 记录的是输入缺**列**，存储仍需补齐数据，读旧值与写出成本取决于实际路径。

### 已有表怎样开启，哪些条件要分开检查

对已经存在但尚未启用灵活更新的 MOW 表，先检查目标版本与轻量 Schema Change 前提，再检查变更后的表属性。以下语句用于这种已有表。

```sql
ALTER TABLE user_profile ENABLE FEATURE "UPDATE_FLEXIBLE_COLUMNS";

SHOW CREATE TABLE user_profile;
```

确认 DDL 包含 `"enable_unique_key_skip_bitmap_column" = "true"`。Stream Load 及使用它的工具可用 JSON 进行灵活更新。Routine Load 对 `"unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"` 的支持受版本约束，4.x 的用法不能直接套给任意 3.1.x。还需核对连接器版本、同步物化视图、VARIANT 列与 Schema Change 状态。

每一行仍需携带全部 Key 列。缺 Key 的记录是否被过滤或导致失败，取决于导入容错配置。对于此前不存在的 Key，还要确认是否允许插入、缺失的非空列怎样补值，以及 Sequence 的顺序规则。
