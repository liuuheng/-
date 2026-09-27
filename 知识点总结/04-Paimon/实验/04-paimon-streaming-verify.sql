-- 在 02-paimon-streaming-checkpoint.sql 提交作业后，从另一个 SQL Client 执行。

SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG paimon_lab_catalog WITH (
  'type' = 'paimon',
  'paimon.catalog.type' = 'filesystem',
  'warehouse' = 's3://paimon/warehouse'
);

USE CATALOG paimon_lab_catalog;
USE paimon_lab;

SELECT COUNT(*) AS visible_logical_rows
FROM streaming_events;

SELECT
  snapshot_id,
  commit_kind,
  commit_time,
  total_record_count,
  delta_record_count
FROM `streaming_events$snapshots`
ORDER BY snapshot_id;
