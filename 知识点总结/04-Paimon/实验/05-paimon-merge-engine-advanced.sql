-- Paimon 1.3.1 + Flink 1.18.1 高级 Merge Engine 实验
-- 验证 first-row 与 partial-update sequence-group。

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

-- ============================================================
-- 实验 1：first-row 保留同一主键最先到达的记录
-- 与 deduplicate 的“后到覆盖先到”相反，适合日志首次出现去重。
-- first-row 不能再配置 sequence.field，也不接受 DELETE / UPDATE_BEFORE。
-- ============================================================
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

-- 同一主键后到的 web 记录不会替换第一条 app 记录。
INSERT INTO first_seen_users VALUES
  (101, 'web', TIMESTAMP '2026-09-27 09:05:00'),
  (102, 'web', TIMESTAMP '2026-09-27 09:06:00');

SELECT
  '07_first_row_keeps_earliest_arrival' AS check_name,
  CASE
    WHEN COUNT(*) = 2
      AND MAX(CASE
        WHEN user_id = 101
         AND register_channel = 'app'
         AND first_seen_at = TIMESTAMP '2026-09-27 09:00:00'
        THEN 1 ELSE 0 END) = 1
    THEN 'PASS' ELSE 'FAIL'
  END AS check_result
FROM first_seen_users;

-- ============================================================
-- 实验 2：partial-update 的 sequence-group 分组处理乱序
-- profile_version 只决定 user_name、city 是否更新；
-- score_version 只决定 score 是否更新，两个来源互不阻塞。
-- ============================================================
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

-- 画像版本 9 是旧数据，应被忽略；积分版本 101 更新，应该生效。
INSERT INTO profile_sequence_groups VALUES
  (101, 'Old Name', 'Beijing', 9, 90, 101);

-- 画像版本 11 更新，应该生效；积分字段为空，不影响已有积分。
INSERT INTO profile_sequence_groups VALUES
  (101, 'Alice Chen', 'Hangzhou', 11, CAST(NULL AS INT), CAST(NULL AS BIGINT));

SELECT
  '08_partial_update_sequence_groups' AS check_name,
  CASE
    WHEN COUNT(*) = 1
      AND MAX(CASE
        WHEN user_name = 'Alice Chen'
         AND city = 'Hangzhou'
         AND profile_version = 11
         AND score = 90
         AND score_version = 101
        THEN 1 ELSE 0 END) = 1
    THEN 'PASS' ELSE 'FAIL'
  END AS check_result
FROM profile_sequence_groups;

SELECT * FROM first_seen_users ORDER BY user_id;
SELECT * FROM profile_sequence_groups ORDER BY user_id;
