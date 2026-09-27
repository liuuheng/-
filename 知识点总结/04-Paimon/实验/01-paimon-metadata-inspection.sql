-- Paimon 元数据与存储组织巡检
-- 先运行 00-paimon-batch-core.sql 创建实验数据。

SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG paimon_lab_catalog WITH (
  'type' = 'paimon',
  'paimon.catalog.type' = 'filesystem',
  'warehouse' = 's3://paimon/warehouse'
);

USE CATALOG paimon_lab_catalog;
USE paimon_lab;

-- 1. 当前结构与完整 DDL
DESCRIBE pk_orders;
SHOW CREATE TABLE pk_orders;

-- 2. 只显示 DDL 中显式设置的选项；未显示的选项使用默认值。
SELECT *
FROM `pk_orders$options`
ORDER BY `key`;

-- 3. Schema 历史；append_events 至少应有建表和 ADD COLUMN 两个版本。
SELECT schema_id, fields, partition_keys, primary_keys, options
FROM `append_events$schemas`
ORDER BY schema_id;

-- 4. Snapshot 是提交历史，不等同于业务日期分区。
SELECT
  snapshot_id,
  schema_id,
  commit_kind,
  commit_time,
  total_record_count,
  delta_record_count
FROM `pk_orders$snapshots`
ORDER BY snapshot_id;

-- 5. 分区是数据组织边界。
SELECT *
FROM `append_events$partitions`;

-- 6. 文件、Bucket、Level 用于识别小文件与 Compaction 状态。
SELECT
  `partition`,
  bucket,
  level,
  COUNT(*) AS file_count,
  SUM(record_count) AS physical_record_count,
  SUM(file_size_in_bytes) AS total_bytes
FROM `pk_orders$files`
GROUP BY `partition`, bucket, level
ORDER BY `partition`, bucket, level;

-- 7. Manifest 是 Snapshot 到数据文件之间的元数据索引。
SELECT
  file_name,
  file_size,
  num_added_files,
  num_deleted_files,
  schema_id
FROM `pk_orders$manifests`;

-- 8. 对比物理记录数与主键合并后的逻辑行数。
SELECT COUNT(*) AS logical_row_count
FROM pk_orders;

SELECT
  snapshot_id,
  total_record_count AS unmerged_physical_record_count
FROM `pk_orders$snapshots`
ORDER BY snapshot_id DESC
LIMIT 1;
