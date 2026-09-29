-- 实验 01：主键表 Merge Engine
-- 环境：Flink 1.18.1 + Paimon 1.3.1
-- 问题：同一主键的多条记录应覆盖、补列、聚合，还是只保留首次到达？
-- 重跑行为：仅重建本脚本定义的四张表。

SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG paimon_lab_catalog WITH (
  'type' = 'paimon',
  'paimon.catalog.type' = 'filesystem',
  'warehouse' = 's3://paimon/warehouse'
);

USE CATALOG paimon_lab_catalog;
CREATE DATABASE IF NOT EXISTS paimon_lab;
USE paimon_lab;

-- 01.1 partial-update：NULL 不覆盖旧值，不同输入补齐同一逻辑行。
-- 它适合“姓名流只写姓名、积分流只写积分”这类稀疏更新。
-- 默认 NULL 的意思是“不更新这个字段”，不是“把旧值清空为 NULL”。
-- 白话理解：像拼资料卡，这次只交城市和积分，就只补这两格，其他格子沿用旧值。
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
  '01_01_partial_update' AS check_name,
  CASE WHEN COUNT(*) = 1
    AND MAX(CASE WHEN user_name = 'Alice' AND city = 'Shanghai' AND score = 90
                 THEN 1 ELSE 0 END) = 1
    THEN 'PASS' ELSE 'FAIL' END AS check_result
FROM partial_profiles;

-- 01.2 aggregation：输入是贡献值；sum 不具备按事件自动去重的幂等性。
-- 同一条贡献重复写两次会累加两次，必须由上游保证事件不会重复计算。
-- 白话理解：它像记账，每来一笔就累计；同一张账单误投两次，也会被算两次。
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
  '01_02_aggregation_sum_max' AS check_name,
  CASE WHEN MAX(CASE WHEN service_name = 'order-api'
                      AND request_count = 30
                      AND max_latency_ms = 180 THEN 1 ELSE 0 END) = 1
    THEN 'PASS' ELSE 'FAIL' END AS check_result
FROM service_metrics;

-- 01.3 first-row：保留最先到达的记录，不能同时设置 sequence.field。
-- 与 deduplicate 的“后到覆盖先到”相反，适合日志首次出现去重。
-- “first”指最先写入 Paimon，不是 first_seen_at 最小；后到的更早业务时间也不会替换。
-- first-row 不接受 DELETE / UPDATE_BEFORE；如需忽略要显式评估 ignore-delete。
-- 白话理解：它执行“先到先得”，认的是到达顺序，不会事后比较谁的业务时间更早。
DROP TABLE IF EXISTS first_seen_users;
CREATE TABLE first_seen_users (
  user_id BIGINT,
  register_channel STRING,
  first_seen_at TIMESTAMP(3),
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'bucket' = '2',
  'merge-engine' = 'first-row'
);

INSERT INTO first_seen_users VALUES
  (101, 'app', TIMESTAMP '2026-09-27 09:00:00');
INSERT INTO first_seen_users VALUES
  (101, 'web', TIMESTAMP '2026-09-27 09:05:00'),
  (102, 'web', TIMESTAMP '2026-09-27 09:06:00');

-- 同一主键后到的 web 记录不会替换第一条 app 记录。
SELECT
  '01_03_first_row' AS check_name,
  CASE WHEN COUNT(*) = 2
    AND MAX(CASE WHEN user_id = 101 AND register_channel = 'app'
                 THEN 1 ELSE 0 END) = 1
    THEN 'PASS' ELSE 'FAIL' END AS check_result
FROM first_seen_users;

-- 01.4 sequence-group：画像版本与积分版本分别保护自己的字段组。
-- profile_version 只决定 user_name、city；score_version 只决定 score。
-- 两组版本互不阻塞，所以旧画像和新积分可以在同一条输入里分别作出不同判断。
-- 白话理解：画像和积分各用自己的版本尺子，不能拿画像版本去否决一条更新更晚的积分。
DROP TABLE IF EXISTS profile_sequence_groups;
CREATE TABLE profile_sequence_groups (
  user_id BIGINT,
  user_name STRING,
  city STRING,
  profile_version BIGINT,
  score INT,
  score_version BIGINT,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'bucket' = '2',
  'merge-engine' = 'partial-update',
  'fields.profile_version.sequence-group' = 'user_name,city',
  'fields.score_version.sequence-group' = 'score'
);

INSERT INTO profile_sequence_groups VALUES
  (101, 'Alice', 'Shanghai', 10, 80, 100);
-- 画像版本 9 被拒绝；积分版本 101 被接受。
INSERT INTO profile_sequence_groups VALUES
  (101, 'Old Name', 'Beijing', 9, 90, 101);
-- profile_version=11 更新画像；score_version=NULL 跳过积分字段组。
INSERT INTO profile_sequence_groups VALUES
  (101, 'Alice Chen', 'Hangzhou', 11, CAST(NULL AS INT), CAST(NULL AS BIGINT));

-- 结果应同时保留最新画像版本 11 和最新积分版本 101。
SELECT
  '01_04_partial_update_sequence_groups' AS check_name,
  CASE WHEN COUNT(*) = 1
    AND MAX(CASE WHEN user_name = 'Alice Chen' AND city = 'Hangzhou'
                  AND profile_version = 11 AND score = 90 AND score_version = 101
                 THEN 1 ELSE 0 END) = 1
    THEN 'PASS' ELSE 'FAIL' END AS check_result
FROM profile_sequence_groups;

SELECT * FROM partial_profiles ORDER BY user_id;
SELECT * FROM service_metrics ORDER BY service_name;
SELECT * FROM first_seen_users ORDER BY user_id;
SELECT * FROM profile_sequence_groups ORDER BY user_id;
