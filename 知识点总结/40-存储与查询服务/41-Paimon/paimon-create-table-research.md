# Apache Paimon Flink SQL 建表研究笔记

> 核对日期：2026-09-14。本文以 **Apache Paimon 2.0.0 最新稳定版**为基线；官网 `master` 当前是 `2.2-SNAPSHOT`，不将其新能力默认套用到稳定版。版本依据：[Paimon 2.0 下载页](https://paimon.apache.org/docs/2.0/project/download/)、[Apache Paimon 官方 Releases](https://github.com/apache/paimon/releases)。

## 核心结论

Paimon 建表的关键不是记住一堆参数，而是按下列顺序决策：

1. Catalog 决定表存在哪里、由谁管理。
2. 是否定义主键，决定它是 append table 还是 primary-key table。
3. `PARTITIONED BY` 和 `bucket` 决定数据物理布局。
4. `merge-engine` 和 `sequence.field` 决定同一主键的多个版本如何合并、谁胜出。
5. `changelog-producer` 决定流式下游看到的是 upsert 变化还是完整 changelog。

## 1. 建表前：先选中 Paimon Catalog

```sql
CREATE CATALOG lakehouse WITH (
    'type' = 'paimon',
    'warehouse' = 'hdfs:///warehouse/paimon'
);

USE CATALOG lakehouse;
```

在 Paimon Catalog 中建表时，表默认就是 Paimon 表，不需要再写 `'connector' = 'paimon'`。这类表是 managed table，`DROP TABLE` 会同时删除表文件。如果使用 `paimon-generic` Catalog，才需要在表的 `WITH` 中声明 `'connector' = 'paimon'`。

官方依据：[Flink SQL DDL](https://paimon.apache.org/docs/2.0/flink/sql-ddl/)。

## 2. 普通表：无主键即 Append Table

```sql
CREATE TABLE event_log (
    event_id BIGINT,
    user_id BIGINT,
    event_type STRING,
    event_time TIMESTAMP(3),
    dt STRING
) PARTITIONED BY (dt) WITH (
    'bucket' = '-1',
    'file.format' = 'parquet',
    'target-file-size' = '256 MB'
);
```

未定义主键的 Paimon 表就是 append table。每次插入都保存为新记录，即使整行重复也不会按键去重，普通流式写入也不会将 changelog 当作 upsert 来更新旧行。它适合日志、事件流、不可变明细。某些引擎支持显式行级操作，但这不会改变其默认的追加写入模型。

官方依据：[Append Table](https://paimon.apache.org/docs/2.0/append-table/)。

## 3. 主键表：按键 Upsert

```sql
CREATE TABLE orders (
    order_id BIGINT,
    user_id BIGINT,
    status STRING,
    amount DECIMAL(12, 2),
    update_time TIMESTAMP(3),
    dt STRING,
    PRIMARY KEY (dt, order_id) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
    'merge-engine' = 'deduplicate',
    'sequence.field' = 'update_time',
    'bucket' = '-1'
);
```

定义 `PRIMARY KEY (... ) NOT ENFORCED` 后，它是 primary-key table。同一主键可能在文件中有多个物理版本，Paimon 通过 LSM 和 merge engine 在读取或 compaction 时合并成一个逻辑行。`NOT ENFORCED` 是 Flink SQL 的声明方式；它不代表建表或写入时逐行检查“重复键并报错”，而是按 Paimon 的合并规则处理同键记录。

官方依据：[Primary-Key Table](https://paimon.apache.org/docs/2.0/primary-key-table/)、[Merge Engine](https://paimon.apache.org/docs/2.0/primary-key-table/merge-engine/)。

## 4. 分区与跨分区更新

稳妥的建模是让主键包含全部分区字段，例如 `PRIMARY KEY (dt, order_id)` 搭配 `PARTITIONED BY (dt)`。这样记录的逻辑键自带分区边界，不会因分区值变化而在两个分区留下两份数据。

主键不包含所有分区列时，属于 cross-partition upsert：

- 官方推荐使用动态桶 `bucket = -1`，通过本地索引维护 key 到 partition/bucket 的映射。
- 启动流式 writer 时要读取已有 key 初始化索引，大表会有明显的启动时间、本地磁盘和内存成本。
- 动态桶的同一分区只支持一个写入 job；多 job 并发写同一分区可能造成重复数据，即使开启 `write-only` 也不能规避。
- 固定桶 `bucket > 0` 和 postpone 桶 `bucket = -2` 没有这个全局映射，跨分区更新必须依赖输入提供完整 changelog，包括旧分区的撤回记录。

官方依据：[Data Distribution / Cross Partitions Upsert](https://paimon.apache.org/docs/2.0/primary-key-table/data-distribution/)。

## 5. 常用 `WITH` 参数

| 参数 | 2.0 默认值/可选值 | 作用与注意点 |
| --- | --- | --- |
| `bucket` | 默认 `-1`；`N > 0` 固定桶；`-2` postpone | 动态桶自动扩容但有单 writer 限制；固定桶过多易产生小文件，过少则可能影响写入与读取。 |
| `bucket-key` | 未设置 | 指定 hash 分桶字段；未指定时，主键表使用主键，无主键表使用整行。 |
| `merge-engine` | `deduplicate` | 仅主键表有效。可选 `deduplicate`、`partial-update`、`aggregation`、`first-row`。 |
| `sequence.field` | 未设置 | 指定判断同键数据新旧的字段。适用于分布式乱序；建议用真正单调的版本号或高精度更新序列。 |
| `changelog-producer` | `none` | 可选 `none`、`input`、`lookup`、`full-compaction`；决定流式下游可见的变更完整度。 |
| `file.format` | `parquet` | 数据文件格式，2.0 支持 `parquet`、`orc`、`avro`。 |
| `target-file-size` | PK 表 `128 MB`；append 表 `256 MB` | 目标数据文件大小，需结合吞吐、小文件数和对象存储特性调整。 |
| `file.compression` | 依文件格式 | 控制数据文件压缩。 |
| `snapshot.time-retained` | `1 h` | 已完成 snapshot 的最大时间保留窗口。 |
| `snapshot.num-retained.min` | `10` | 至少保留的已完成 snapshot 数。 |
| `write-only` | `false` | 设为 `true` 会跳过 compaction 和 snapshot expiration，必须配套独立 compact 任务与运维策略。 |

官方依据：[Configurations](https://paimon.apache.org/docs/2.0/maintenance/configurations/)、[Sequence & Rowkind](https://paimon.apache.org/docs/2.0/primary-key-table/sequence-rowkind/)。

## 6. Append 表与主键/Changelog 表的根本区别

| 维度 | Append table | Primary-key table |
| --- | --- | --- |
| 建表标志 | 没有 `PRIMARY KEY` | 定义 `PRIMARY KEY (...) NOT ENFORCED` |
| 普通插入 | 每行追加，重复值也保留 | 同键记录按 merge engine 合并 |
| 主要场景 | 日志、事件、不可变明细 | CDC、upsert、实时宽表、聚合状态 |
| 乱序处理 | 通常不做同键胜负判定 | 可用 `sequence.field` 判断最新版本 |
| 流式变更 | 默认是追加流 | 通过 `changelog-producer` 决定 upsert/完整 changelog |

`changelog-producer` 的含义：

- `none`（默认）：不额外写 changelog 文件。流式读取看到 snapshot 间合并后的 upsert 变化，没有完整 old value；Flink 若需要 before image，可能引入有状态 normalize，状态成本可能很高。
- `input`：原样保存上游输入 changelog。只适合上游本来就提供完整 before/after 记录的 CDC 或 Flink 有状态计算；它不会自动补造缺失的 before image。
- `lookup`：在 compaction 期间查找旧值来生成较完整的 changelog，会使用内存和本地磁盘 cache，并增加 compaction/提交成本。
- `full-compaction`：比较相邻全量 compaction 结果产生 changelog，延迟取决于全量 compaction 频率，成本和时延通常高于 `input`。

官方明确提醒 `changelog-producer` 可能显著降低 compaction 性能，无完整 changelog 消费需求时不要盲目开启。短 checkpoint 周期叠加大量 bucket 可能产生大量小 changelog 文件，可评估 `precommit-compact = true`，但它会在 writer 后增加 compact operator。

官方依据：[Changelog Producer](https://paimon.apache.org/docs/2.0/primary-key-table/changelog-producer/)。

## 7. 典型限制与坑

1. **流式写入要开 checkpoint。** 否则无法持续产生可见 commit。官方入门示例使用 `SET 'execution.checkpointing.interval' = '10 s'`。依据：[Flink Quick Start](https://paimon.apache.org/docs/2.0/flink/quick-start/)。
2. **不设 `sequence.field` 时依赖输入顺序。** 分布式乱序可能让旧值覆盖新值；相同 sequence 值仍需依赖输入顺序决胜。依据：[Sequence & Rowkind](https://paimon.apache.org/docs/2.0/primary-key-table/sequence-rowkind/)。
3. **Flink 的 sink upsert materialize 可干扰 Paimon 语义。** 官方 merge-engine 文档要求将 `table.exec.sink.upsert-materialize` 设为 `NONE`。依据：[Merge Engine](https://paimon.apache.org/docs/2.0/primary-key-table/merge-engine/)。
4. **不要把 `bucket` 当成普通并行度参数。** 固定桶修改需要离线 rescale；桶数过多小文件会爆炸，过少则可能形成写入和合并瓶颈。依据：[Data Distribution](https://paimon.apache.org/docs/2.0/primary-key-table/data-distribution/)。
5. **`write-only = true` 不是免运维开关。** 它会跳过 compaction 和 snapshot expiration，必须有独立 compact 作业及 snapshot 清理策略。依据：[Configurations](https://paimon.apache.org/docs/2.0/maintenance/configurations/)。
6. **Hive Catalog 还有命名和分区同步约束。** 库名、表名、字段名应使用小写；新 Paimon 分区默认不同步到 Hive Metastore，需要 HMS 可见分区时设置 `metastore.partitioned-table = true`。依据：[Flink SQL DDL / Hive Catalog](https://paimon.apache.org/docs/2.0/flink/sql-ddl/)。
7. **不要混用不同版本文档。** `master` 是开发版，0.x、1.x、2.0 在默认值、CTAS 语法和能力上可能不同；实际集群不是 2.0.0 时，应切换到对应版本文档重新核对。依据：[Paimon 2.0 下载页](https://paimon.apache.org/docs/2.0/project/download/)。

## 总结

日志、流水类数据优先建 append table；需要 CDC、按键更新或聚合状态时建 primary-key table。主键表优先让主键包含分区键，除非确实需要且能承担 cross-partition upsert 的索引成本。`merge-engine` 解决“同键如何合并”，`sequence.field` 解决“乱序时谁更新”，`changelog-producer` 解决“下游看到什么变更”；三者不能混为一谈。
