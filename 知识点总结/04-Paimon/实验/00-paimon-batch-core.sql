-- Paimon 1.3.1 + Flink 1.18.1 核心知识验证
-- 可重复执行：只会重建 paimon_lab 数据库中的实验表。

SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'table.dynamic-table-options.enabled' = 'true';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

CREATE CATALOG paimon_lab_catalog WITH (
  'type' = 'paimon',
  'paimon.catalog.type' = 'filesystem',
  'warehouse' = 's3://paimon/warehouse'
);

USE CATALOG paimon_lab_catalog;
CREATE DATABASE IF NOT EXISTS paimon_lab;
USE paimon_lab;

-- ============================================================
-- 实验 1：Append Table 不按业务主键去重
-- ============================================================
DROP TABLE IF EXISTS append_events;

CREATE TABLE append_events (
  event_id BIGINT,
  user_id BIGINT,
  event_type STRING,
  event_time TIMESTAMP(3),
  dt STRING
) PARTITIONED BY (dt) WITH (
  'bucket' = '2',
  'bucket-key' = 'event_id',
  'snapshot.num-retained.min' = '10',
  'snapshot.num-retained.max' = '20'
);

INSERT INTO append_events VALUES
  (1, 101, 'view',     TIMESTAMP '2026-09-27 09:00:00', '2026-09-27'),
  (1, 101, 'view',     TIMESTAMP '2026-09-27 09:00:00', '2026-09-27'),
  (2, 101, 'click',    TIMESTAMP '2026-09-27 09:01:00', '2026-09-27'),
  (3, 102, 'purchase', TIMESTAMP '2026-09-28 10:00:00', '2026-09-28');

SELECT
  '01_append_keeps_duplicates' AS check_name,
  CASE
    WHEN COUNT(*) = 4
      AND SUM(CASE WHEN event_id = 1 THEN 1 ELSE 0 END) = 2
    THEN 'PASS' ELSE 'FAIL'
  END AS check_result,
  COUNT(*) AS actual_rows,
  CAST(4 AS BIGINT) AS expected_rows
FROM append_events;

-- ============================================================
-- 实验 2：主键表 + sequence.field 处理乱序更新
-- 第一个 INSERT 形成 Snapshot 1；第二个 INSERT 形成 Snapshot 2。
-- ============================================================
DROP TABLE IF EXISTS pk_orders;

CREATE TABLE pk_orders (
  order_id BIGINT,
  user_id BIGINT,
  status STRING,
  amount DECIMAL(10, 2),
  update_version BIGINT,
  dt STRING,
  PRIMARY KEY (dt, order_id) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
  'bucket' = '2',
  'bucket-key' = 'order_id',
  'merge-engine' = 'deduplicate',
  'sequence.field' = 'update_version',
  'snapshot.num-retained.min' = '10',
  'snapshot.num-retained.max' = '20'
);

INSERT INTO pk_orders VALUES
  (1001, 101, 'CREATED', CAST(100.00 AS DECIMAL(10, 2)), 1, '2026-09-27'),
  (1002, 102, 'CREATED', CAST( 80.00 AS DECIMAL(10, 2)), 1, '2026-09-27');

-- version=2 比 version=3 晚出现在输入中，也不能覆盖 version=3。
INSERT INTO pk_orders VALUES
  (1001, 101, 'PAID',    CAST(120.00 AS DECIMAL(10, 2)), 3, '2026-09-27'),
  (1001, 101, 'PENDING', CAST(110.00 AS DECIMAL(10, 2)), 2, '2026-09-27');

SELECT
  '02_pk_sequence_deduplicate' AS check_name,
  CASE
    WHEN COUNT(*) = 2
      AND MAX(CASE
        WHEN order_id = 1001
         AND status = 'PAID'
         AND update_version = 3
        THEN 1 ELSE 0 END) = 1
    THEN 'PASS' ELSE 'FAIL'
  END AS check_result,
  COUNT(*) AS logical_rows,
  CAST(2 AS BIGINT) AS expected_rows
FROM pk_orders;

-- Snapshot 1 中 order 1001 仍是 CREATED；最新 Snapshot 中是 PAID。
SELECT
  '03_snapshot_1_time_travel' AS check_name,
  CASE
    WHEN MAX(CASE
      WHEN order_id = 1001
       AND status = 'CREATED'
       AND update_version = 1
      THEN 1 ELSE 0 END) = 1
    THEN 'PASS' ELSE 'FAIL'
  END AS check_result
FROM pk_orders /*+ OPTIONS('scan.snapshot-id' = '1') */;

-- ============================================================
-- 实验 3：partial-update 将不同来源的非空字段合并
-- ============================================================
DROP TABLE IF EXISTS partial_profiles;

CREATE TABLE partial_profiles (
  user_id BIGINT,
  user_name STRING,
  city STRING,
  score INT,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'bucket' = '2',
  'merge-engine' = 'partial-update'
);

INSERT INTO partial_profiles VALUES
  (101, 'Alice', CAST(NULL AS STRING), CAST(NULL AS INT));

INSERT INTO partial_profiles VALUES
  (101, CAST(NULL AS STRING), 'Shanghai', 90);

SELECT
  '04_partial_update' AS check_name,
  CASE
    WHEN COUNT(*) = 1
      AND MAX(CASE
        WHEN user_name = 'Alice'
         AND city = 'Shanghai'
         AND score = 90
        THEN 1 ELSE 0 END) = 1
    THEN 'PASS' ELSE 'FAIL'
  END AS check_result
FROM partial_profiles;

-- ============================================================
-- 实验 4：aggregation 对同一主键的贡献值执行 sum / max
-- ============================================================
DROP TABLE IF EXISTS service_metrics;

CREATE TABLE service_metrics (
  service_name STRING,
  request_count BIGINT,
  max_latency_ms BIGINT,
  PRIMARY KEY (service_name) NOT ENFORCED
) WITH (
  'bucket' = '2',
  'merge-engine' = 'aggregation',
  'fields.request_count.aggregate-function' = 'sum',
  'fields.max_latency_ms.aggregate-function' = 'max'
);

INSERT INTO service_metrics VALUES
  ('order-api', 10, 120),
  ('order-api', 15, 180),
  ('order-api',  5, 150),
  ('user-api',   8,  90);

SELECT
  '05_aggregation_sum_max' AS check_name,
  CASE
    WHEN MAX(CASE
      WHEN service_name = 'order-api'
       AND request_count = 30
       AND max_latency_ms = 180
      THEN 1 ELSE 0 END) = 1
    THEN 'PASS' ELSE 'FAIL'
  END AS check_result
FROM service_metrics;

-- ============================================================
-- 实验 5：Schema Evolution
-- 旧数据的新字段为 NULL，新数据可以写入新增字段。
-- ============================================================
ALTER TABLE append_events ADD source_system STRING;

INSERT INTO append_events VALUES
  (4, 103, 'view', TIMESTAMP '2026-09-28 11:00:00', '2026-09-28', 'app');

SELECT
  '06_schema_evolution' AS check_name,
  CASE
    WHEN COUNT(*) = 5
      AND SUM(CASE WHEN source_system IS NULL THEN 1 ELSE 0 END) = 4
      AND SUM(CASE WHEN source_system = 'app' THEN 1 ELSE 0 END) = 1
    THEN 'PASS' ELSE 'FAIL'
  END AS check_result,
  COUNT(*) AS actual_rows
FROM append_events;

-- 最终逻辑结果，便于人工核对。
SELECT * FROM append_events ORDER BY dt, event_id;
SELECT * FROM pk_orders ORDER BY dt, order_id;
SELECT * FROM partial_profiles ORDER BY user_id;
SELECT * FROM service_metrics ORDER BY service_name;
