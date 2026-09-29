-- 实验 06：从独立 SQL Client 验证流式提交
-- 前置：05-paimon-streaming-writer.sql 对应的作业正在运行且至少完成一次 Checkpoint。
-- 本脚本只读，可间隔 10 秒重复执行，对比可见行数与 Snapshot 列表。

SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG paimon_lab_catalog WITH (
  'type' = 'paimon',
  'paimon.catalog.type' = 'filesystem',
  'warehouse' = 's3://paimon/warehouse'
);

USE CATALOG paimon_lab_catalog;
USE paimon_lab;

-- 普通表查询经过 Merge-Read，得到当前已提交 Snapshot 的逻辑行数。
-- Writer 已收到但尚未完成 Checkpoint 的记录不会出现在这里。
SELECT COUNT(*) AS visible_logical_rows
FROM streaming_events;

-- total_record_count / delta_record_count 是文件级物理记录统计，不是业务逻辑行数。
-- commit_kind=COMPACT 表示文件重写，不表示产生了新的业务事件。
-- 白话理解：文件统计在数“仓库里写了多少版本记录”，普通 COUNT(*) 在数“合并后还剩多少业务行”。
SELECT
  snapshot_id,
  commit_kind,
  commit_time,
  total_record_count,
  delta_record_count
FROM `streaming_events$snapshots`
ORDER BY snapshot_id;

-- 预期：两次执行之间若有成功 Checkpoint，最大 snapshot_id 和可见行数通常增长。
-- random event_id 可能重复，因此逻辑行数不要求等于 total_record_count。
-- 两条 SELECT 也不是同一原子观测；持续写入时可能分别取到不同提交时刻。
