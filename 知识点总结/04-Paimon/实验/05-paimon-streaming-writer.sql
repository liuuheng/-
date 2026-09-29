-- 实验 05：流式 Writer、Checkpoint 与 Snapshot 提交
-- 这是持续运行的作业。提交前先确认旧 streaming_events Writer 已取消。
-- 问题：流入算子的记录何时成为其他读者可见的 Paimon 数据？

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.storage' = 'filesystem';
SET 'state.checkpoints.dir' = 's3://paimon/flink-checkpoints';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

CREATE CATALOG paimon_lab_catalog WITH (
  'type' = 'paimon',
  'paimon.catalog.type' = 'filesystem',
  'warehouse' = 's3://paimon/warehouse'
);

USE CATALOG paimon_lab_catalog;
CREATE DATABASE IF NOT EXISTS paimon_lab;
USE paimon_lab;

-- 只有在确认不存在旧 Writer 时才能重建此表。
-- 旧作业若仍在运行，DROP/重建后它可能继续向同一路径写入，形成两个 Writer 竞争。
DROP TABLE IF EXISTS streaming_events;
CREATE TABLE streaming_events (
  event_id BIGINT,
  user_id BIGINT,
  amount INT,
  event_time TIMESTAMP_LTZ(3),
  dt STRING,
  PRIMARY KEY (dt, event_id) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
  'bucket' = '2',
  'bucket-key' = 'event_id',
  'merge-engine' = 'deduplicate',
  'snapshot.num-retained.min' = '10',
  'snapshot.num-retained.max' = '30'
);

USE CATALOG default_catalog;
USE default_database;

-- DataGen 是临时输入源，放在 SQL Client 的默认 Catalog；Paimon 表才是持久化结果。

DROP TABLE IF EXISTS paimon_lab_datagen;
CREATE TABLE paimon_lab_datagen (
  event_id BIGINT,
  user_id BIGINT,
  amount INT
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '2',
  -- random 避免 Flink 1.18 SequenceGenerator 为超大范围维护过多状态。
  'fields.event_id.kind' = 'random',
  'fields.event_id.min' = '1',
  'fields.event_id.max' = '100000',
  'fields.user_id.kind' = 'random',
  'fields.user_id.min' = '1',
  'fields.user_id.max' = '100',
  'fields.amount.kind' = 'random',
  'fields.amount.min' = '1',
  'fields.amount.max' = '1000'
);

-- 该语句持续运行。每次成功 Checkpoint 才发布一个原子可见的 Snapshot。
-- Flink 流式写入 Paimon 时，数据不是到一条就立刻成为表的可见数据，而是以 Checkpoint 为提交边界；
-- Checkpoint 成功后，Paimon 才提交一个新的 Snapshot，使这一批数据原子可见。
-- Checkpoint ID 与 Snapshot ID 分属 Flink 运行时和 Paimon 表版本，不能按编号直接等同。
-- random event_id 会重复，因此 deduplicate 逻辑行数可能小于文件物理记录数。
-- 白话理解：记录先进入暂存区，Checkpoint 成功像“盖章发布”；没盖章前，另一个 SQL Client 看不到。
INSERT INTO paimon_lab_catalog.paimon_lab.streaming_events
SELECT
  event_id,
  user_id,
  amount,
  CURRENT_TIMESTAMP,
  DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
FROM paimon_lab_datagen;
