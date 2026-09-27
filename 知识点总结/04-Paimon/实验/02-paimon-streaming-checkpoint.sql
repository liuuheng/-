-- Paimon 流式写入与 Checkpoint / Snapshot 实验
-- 这是持续运行的实验。提交后请到 Flink Web UI 观察，再手动取消作业。
-- 重跑前必须先确认并取消旧 streaming_events Writer，
-- 不要让旧作业与新作业同时写入同一表路径。

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

DROP TABLE IF EXISTS paimon_lab_datagen;

CREATE TABLE paimon_lab_datagen (
  event_id BIGINT,
  user_id BIGINT,
  amount INT
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '2',
  -- 使用 random 避免 Flink 1.18 DataGen SequenceGenerator 为超大范围
  -- 初始化内部队列而耗尽当前 TaskManager 堆内存。
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

-- 该语句会提交一个持续运行的 Flink Job。
-- 每次成功 Checkpoint 后，Paimon 才会发布新的可见 Snapshot。
INSERT INTO paimon_lab_catalog.paimon_lab.streaming_events
SELECT
  event_id,
  user_id,
  amount,
  CURRENT_TIMESTAMP,
  DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
FROM paimon_lab_datagen;
