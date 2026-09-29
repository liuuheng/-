-- 实验 00：Append Table 与主键表
-- 环境：Flink 1.18.1 + Paimon 1.3.1
-- 问题：没有主键时是否保留重复事件？有主键时如何处理乱序版本？
-- 重跑行为：仅重建 paimon_lab.append_events 和 paimon_lab.pk_orders。

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

-- 00.1 Append Table：event_id 只是业务字段，不触发去重。
-- 不定义 PRIMARY KEY 时，Paimon 默认是 Append Table。
-- fixed bucket 只决定数据分布和文件组织，不会把相同 event_id 自动去重。
-- 白话理解：Append Table 像流水账，来一条记一条；两张内容相同的小票也会保留两张。
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
  '00_01_append_keeps_duplicates' AS check_name,
  CASE
    WHEN COUNT(*) = 4
      AND SUM(CASE WHEN event_id = 1 THEN 1 ELSE 0 END) = 2
    THEN 'PASS' ELSE 'FAIL'
  END AS check_result,
  COUNT(*) AS actual_rows,
  CAST(4 AS BIGINT) AS expected_rows
FROM append_events;

-- 00.2 主键表：同一主键只产生一行逻辑结果，sequence.field 决定版本顺序。
-- merge-engine 是 Paimon 主键表的概念。
-- 不定义主键时，Paimon 默认是追加表。
-- 分区表的 PRIMARY KEY 必须包含全部分区字段，所以这里是 (dt, order_id)。
-- 白话理解：主键先确定“哪些记录属于同一件事”，Merge Engine 再决定它们怎样合成一行。
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

-- 第一个 INSERT 形成 Snapshot 1；第二个 INSERT 形成 Snapshot 2。
INSERT INTO pk_orders VALUES
  (1001, 101, 'CREATED', CAST(100.00 AS DECIMAL(10, 2)), 1, '2026-09-27'),
  (1002, 102, 'CREATED', CAST( 80.00 AS DECIMAL(10, 2)), 1, '2026-09-27');

-- version=2 后到，但不能覆盖 version=3。
-- sequence.field 的作用是：同主键出现多条记录时，sequence 字段值更大的那条会被当作“最新状态”保留下来。
-- 白话理解：同一主键打架时，版本号更大的记录胜出，不是最后到达 SQL Client 的记录一定胜出。
INSERT INTO pk_orders VALUES
  (1001, 101, 'PAID',    CAST(120.00 AS DECIMAL(10, 2)), 3, '2026-09-27'),
  (1001, 101, 'PENDING', CAST(110.00 AS DECIMAL(10, 2)), 2, '2026-09-27');

SELECT
  '00_02_sequence_resolves_disorder' AS check_name,
  CASE
    WHEN COUNT(*) = 2
      AND MAX(CASE WHEN order_id = 1001
                    AND status = 'PAID'
                    AND update_version = 3 THEN 1 ELSE 0 END) = 1
    THEN 'PASS' ELSE 'FAIL'
  END AS check_result,
  COUNT(*) AS logical_rows
FROM pk_orders;

-- Snapshot 1 中 order 1001 仍是 CREATED；最新 Snapshot 中是 PAID。

-- Snapshot 1 是当时的完整表状态，不是“第一次提交的增量记录”。
-- 白话理解：Snapshot 像某次发布后的整表照片，不是这次快门只拍到的新增记录。
SELECT
  '00_03_snapshot_time_travel' AS check_name,
  CASE
    WHEN COUNT(*) = 2
      AND MAX(CASE WHEN order_id = 1001
                    AND status = 'CREATED'
                    AND update_version = 1 THEN 1 ELSE 0 END) = 1
    THEN 'PASS' ELSE 'FAIL'
  END AS check_result
FROM pk_orders /*+ OPTIONS('scan.snapshot-id' = '1') */;

SELECT * FROM append_events ORDER BY dt, event_id;
SELECT * FROM pk_orders ORDER BY dt, order_id;
