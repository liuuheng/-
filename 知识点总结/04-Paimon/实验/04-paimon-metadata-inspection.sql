-- 实验 04：$ 后缀系统表与物理组织巡检
-- 前置：先运行 00-paimon-table-models.sql 和 03-paimon-versions.sql。
-- 问题：Schema、Snapshot、Manifest、Data File、Partition、Bucket、Level 分别描述哪一层？
-- 本脚本只读，不创建、修改或删除表。

SET 'execution.runtime-mode' = 'batch';
-- CHANGELOG 像持续打印的变更日志；TABLEAU 像不断刷新的表格；
-- TABLE 只打印最终静态结果。本脚本是批查询，使用 TABLEAU 便于人工查看。
-- 白话理解：CHANGELOG 看“每一步怎么变”，TABLEAU 看“屏幕上现在长什么样”，TABLE 看“最终打印件”。
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG paimon_lab_catalog WITH (
  'type' = 'paimon',
  'paimon.catalog.type' = 'filesystem',
  'warehouse' = 's3://paimon/warehouse'
);

USE CATALOG paimon_lab_catalog;
USE paimon_lab;

-- 04.1 业务表结构和 $options
-- Paimon 将 `业务表名$后缀` 解析成依附于业务表的系统表。
-- 例如 `pk_orders$snapshots` 不是用户创建的物理表，而是 pk_orders 的提交历史视图。
-- `$` 是表名的一部分，Flink SQL 中应使用反引号包住完整名称。
-- 系统表字段会随 Paimon 版本和表特性变化；本脚本先 DESCRIBE，再按 Paimon 1.3.x 字段查询。
-- key：参数名，例如 bucket、merge-engine、sequence.field。
-- value：参数值，统一以字符串形式展示。
-- 查询不到某个参数不表示该能力未启用；它可能正在使用 Paimon 当前版本的默认值。
-- 这里查到的就是建表 DDL 的 WITH 参数；未显式设置的默认参数通常不在结果中。
-- 白话理解：$options 是“显式配置单”，没写在单子上的参数可能仍在使用系统默认值。
DESCRIBE pk_orders;
SHOW CREATE TABLE pk_orders;
DESCRIBE `pk_orders$options`;
SELECT `key`, `value` FROM `pk_orders$options` ORDER BY `key`;

-- 04.2 $schemas：一行是一份完整 Schema，不是一条字段变更事件；查询的是字段和表结构版本信息。
-- schema_id：Schema 版本号；建表通常从 0 开始，每次 Schema Evolution 产生新版本。
-- fields：该版本的字段定义，包含稳定字段 ID、字段名、数据类型等信息。
-- partition_keys：该版本的分区字段列表。
-- primary_keys：该版本的主键字段列表；Append Table 为空。
-- options：该 Schema 版本保存的表参数，可能与当前 $options 不同；comment：表注释。
-- update_time：Schema 创建或更新时间，不是数据事件时间。
-- schema_events 执行过 ADD COLUMN，因此至少应看到建表和加字段两个 Schema 版本。
-- $schemas 返回的是每次 Schema 变更后的一份“完整快照”，不是“仅变更的参数”。
-- 白话理解：$schemas 是历代表结构档案，每一行都是一整版字段说明书，不是只记本次改了哪一列。
DESCRIBE `schema_events$schemas`;
SELECT * FROM `schema_events$schemas` ORDER BY schema_id;

-- 04.3 $snapshots：成功发布的提交历史。每行代表一个 Snapshot，不代表一个业务分区。
-- snapshot_id：递增表版本，过期后可能有缺口；不是 Checkpoint ID。
-- schema_id：读取该 Snapshot 时使用的 Schema，可与 $schemas 关联。
-- commit_user：提交者唯一标识；commit_identifier：该提交者内部的提交标识。
-- commit_kind：APPEND、COMPACT、OVERWRITE、ANALYZE 等提交类型。
-- commit_time：提交成功时间，不是业务事件时间。
-- base_manifest_list：承载既有文件状态的 Manifest List 文件名。
-- delta_manifest_list：承载本次文件 ADD/DELETE 的 Manifest List 文件名。
-- changelog_manifest_list：本次 Changelog File 的 Manifest List；未生产时可为空。
-- total_record_count：当前 Snapshot 引用的数据文件物理记录总数。
-- delta_record_count：本次提交引起的物理记录净变化。
-- changelog_record_count：本次 Changelog 文件中的记录数。
-- watermark：提交携带的事件时间 Watermark；没有上游 Watermark 时可为空。
-- 白话理解：$snapshots 是版本发布记录；每行说明某次提交发布了哪套结构和文件清单。
DESCRIBE `pk_orders$snapshots`;
SELECT * FROM `pk_orders$snapshots` ORDER BY snapshot_id;

-- 04.4 $partitions：按分区汇总当前 Snapshot 引用的数据文件。
-- partition：分区值组成的 Row，例如 {2026-09-27}；它不是目录字符串。
-- record_count：该分区有效数据文件中的物理记录数，主键表中可能尚未合并。
-- file_size_in_bytes：该分区有效数据文件的总字节数。
-- file_count：该分区当前有效的数据文件数量，可用于发现小文件积压。
-- last_update_time：分区元数据最近更新时间，不等于分区内最大的业务时间。
-- 白话理解：$partitions 先按分区把文件账目汇总，看的是存储规模，不是业务最终行数。
DESCRIBE `append_events$partitions`;
SELECT * FROM `append_events$partitions` ORDER BY `partition`;

-- 04.5 $buckets：在 $partitions 基础上继续按 Bucket 拆分。
-- partition：分区值；bucket：逻辑 Bucket 编号。
-- record_count/file_size_in_bytes/file_count：该分区 Bucket 的物理文件统计。
-- last_update_time：该分区 Bucket 最近一次元数据更新时间。
-- 白话理解：$buckets 是把分区账目再按桶拆开，适合找哪个桶文件特别多或特别大。
DESCRIBE `pk_orders$buckets`;
SELECT * FROM `pk_orders$buckets` ORDER BY `partition`, bucket;

-- 04.6 $files：一行对应目标 Snapshot 当前引用的一个 Data File。
-- 默认读取最新 Snapshot；不会列出已被新 Snapshot 替换但仍被旧 Snapshot 引用的旧文件。
-- partition：文件所属分区；bucket：逻辑 Bucket；file_path：数据文件路径或文件名。
-- file_format：ORC/Parquet 等格式；schema_id：写文件时使用的 Schema。
-- level：LSM 层级，L0 文件键范围可重叠；高层文件通常来自 Compaction。
-- record_count/file_size_in_bytes：文件物理记录数和字节数。
-- min_key/max_key：完整主键元组排序边界，不是各主键列独立 Min/Max。
-- null_value_counts：各 Value 字段 NULL 数量统计。
-- min_value_stats/max_value_stats：各 Value 字段独立的文件级 Min/Max。
-- min_sequence_number/max_sequence_number：文件内 Sequence 边界。
-- creation_time：数据文件创建时间，不是业务事件时间。
-- 白话理解：$files 是当前版本真正会去读的数据文件清单；一行就是一个 Data File 的“体检报告”。
DESCRIBE `pk_orders$files`;
SELECT * FROM `pk_orders$files`
ORDER BY `partition`, bucket, level, file_path;

-- 聚合 $files，用于识别小文件、Bucket 倾斜和 L0 积压。
SELECT
  `partition`, bucket, level,
  COUNT(*) AS file_count,
  SUM(record_count) AS physical_record_count,
  SUM(file_size_in_bytes) AS total_bytes,
  AVG(file_size_in_bytes) AS avg_file_bytes
FROM `pk_orders$files`
GROUP BY `partition`, bucket, level
ORDER BY `partition`, bucket, level;

-- 04.7 $manifests：选定 Snapshot 引用的 Manifest 文件，默认读取最新 Snapshot。
-- file_name/file_size：Manifest 元数据文件名和自身字节数，不是 Data File。
-- num_added_files/num_deleted_files：ADD/DELETE 文件条目数，不是业务行数。
-- schema_id：写这些 Manifest 条目时关联的 Schema。
-- min_partition_stats/max_partition_stats：该 Manifest 覆盖的分区统计边界。
-- 一个 Manifest 可以包含多个分区和 Bucket 的文件条目。
-- 不能把它理解成“一个分区对应一个 Manifest”。
-- 白话理解：Manifest 像文件目录，记录哪些 Data File 加入或退出当前版本，本身不保存业务行。
DESCRIBE `pk_orders$manifests`;
SELECT * FROM `pk_orders$manifests` ORDER BY file_name;

-- 04.8 $consumers：命名流式消费者的读取进度；未配置 consumer-id 时通常为空。
-- consumer_id：消费者名称；next_snapshot_id：下一次应读取的 Snapshot。
-- 它表示读取位置，不表示该 Snapshot 的业务行数。
-- 白话理解：$consumers 像阅读书签，记录下次从哪个 Snapshot 接着读，不记录读到了多少业务行。
DESCRIBE `pk_orders$consumers`;
SELECT * FROM `pk_orders$consumers` ORDER BY consumer_id;

-- 04.9 其他 $ 后缀：知识点保留在对应 SQL 旁，但默认不执行。
-- 原因不是这些系统表不重要，而是当前表没有启用相应 Feature，或本地 1.3.1 未确认支持。

-- $binlog = rowkind + 原业务字段；UPDATE 会把 Before/After 打包在同一字段值中。
-- 需要目标表能产生相应 Binlog；Flink 计算列目前还有展示限制。
-- DESCRIBE `pk_orders$binlog`;
-- SELECT * FROM `pk_orders$binlog` LIMIT 20;

-- $ro 与原业务表字段相同，只读无需 Merge 的最高层文件。
-- 它代表最近一次 Full Compaction 的读优化结果；不同 Bucket 可能对应不同提交时点。
-- DESCRIBE `pk_orders$ro`;
-- SELECT * FROM `pk_orders$ro` ORDER BY dt, order_id;

-- $branches 字段：branch_name 是 Branch 名；create_time 是创建时间。
-- 当前实验没有创建 Branch；Paimon 1.3.1 是否暴露此表先以 DESCRIBE 为准。
-- DESCRIBE `versioned_products$branches`;
-- SELECT * FROM `versioned_products$branches` ORDER BY create_time;

-- $aggregation_fields 字段：
-- field_name 字段名；field_type 数据类型；function 聚合函数；
-- function_options 对应函数配置；comment 是字段注释。
-- 先运行 01-paimon-merge-engines.sql 创建 service_metrics，再取消下面注释。
-- DESCRIBE `service_metrics$aggregation_fields`;
-- SELECT * FROM `service_metrics$aggregation_fields` ORDER BY field_name;

-- $statistics 字段：snapshot_id/schema_id 标识统计版本；
-- mergedRecordCount/mergedRecordSize 描述合并后的记录规模；colstat 保存列统计。
-- 只有执行过对应统计收集后，非空结果才适合解释。
-- DESCRIBE `pk_orders$statistics`;
-- SELECT * FROM `pk_orders$statistics` ORDER BY snapshot_id;

-- $table_indexes 用于动态 Bucket Hash Index 和 Deletion Vector：
-- partition/bucket 定位范围；index_type 区分 HASH、DELETION_VECTORS；
-- file_name/file_size/row_count 描述索引文件；dv_ranges 描述 DV 覆盖的数据文件范围。
-- 当前 fixed bucket 表通常返回空结果，不能据此判断索引功能损坏。
-- DESCRIBE `pk_orders$table_indexes`;
-- SELECT * FROM `pk_orders$table_indexes` ORDER BY `partition`, bucket, index_type;

-- $file_indexes / $file_key_ranges 用于观察 Data File Index 覆盖和文件主键范围。
-- 它们未列入 Paimon 1.3 的稳定通用系统表清单，字段随版本变化；
-- 本地 1.3.1 必须先确认 SHOW TABLES / DESCRIBE 成功，不能直接复制新版字段查询。
-- DESCRIBE `pk_orders$file_indexes`;
-- DESCRIBE `pk_orders$file_key_ranges`;

-- 04.10 对比 Merge-Read 后逻辑行数与最新 Snapshot 的物理记录数。
-- 第一条 SELECT 查逻辑行数；第二条 SELECT 查最新 Snapshot 的物理记录数。
-- 白话理解：物理记录数像仓库里实际放了几张版本纸，逻辑行数像把同一订单的版本合并后还剩几张订单卡。
-- 普通表查询会按主键、merge-engine、sequence.field 执行 Merge-Read，得到最终逻辑行。
-- $snapshots.total_record_count 来自 Snapshot 引用的数据文件统计，未执行主键合并。
-- 同一 order_id 的多个版本可能分别占一条物理记录，却只产生一条逻辑结果。
-- Compaction 后物理记录数可能减少，但这不等于业务行被删除。
SELECT COUNT(*) AS logical_row_count FROM pk_orders;

SELECT snapshot_id, total_record_count AS unmerged_physical_record_count
FROM `pk_orders$snapshots`
ORDER BY snapshot_id DESC
LIMIT 1;
