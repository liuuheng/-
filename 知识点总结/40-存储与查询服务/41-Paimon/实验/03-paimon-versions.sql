-- 实验 03：Schema、Snapshot、Tag 与增量区间
-- 环境：Flink 1.18.1 + Paimon 1.3.1
-- 问题：结构版本、完整历史状态和两个版本间的变化分别如何读取？
-- 重跑行为：DROP 后 Snapshot ID 从 1 重新开始，脚本中的固定 ID 才成立。

SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'table.dynamic-table-options.enabled' = 'true';

CREATE CATALOG paimon_lab_catalog WITH (
  'type' = 'paimon',
  'paimon.catalog.type' = 'filesystem',
  'warehouse' = 's3://paimon/warehouse'
);

USE CATALOG paimon_lab_catalog;
CREATE DATABASE IF NOT EXISTS paimon_lab;
USE paimon_lab;

-- 03.1 Schema Evolution：ADD COLUMN 只创建新 Schema，不改写旧数据文件。
-- 旧文件没有 source_system，读取时通过 Schema 映射补成 NULL；新文件可以直接写 app。
-- 白话理解：只是换了新版表头，没有把仓库里的旧文件全部拆开重写；旧行缺少的新列读取为 NULL。
DROP TABLE IF EXISTS schema_events;
CREATE TABLE schema_events (
  event_id BIGINT,
  event_type STRING
) WITH (
  'bucket' = '2',
  'bucket-key' = 'event_id'
);

INSERT INTO schema_events VALUES (1, 'view'), (2, 'click');
ALTER TABLE schema_events ADD source_system STRING;
INSERT INTO schema_events VALUES (3, 'purchase', 'app');

SELECT
  '03_01_schema_evolution' AS check_name,
  CASE WHEN COUNT(*) = 3
    AND SUM(CASE WHEN source_system IS NULL THEN 1 ELSE 0 END) = 2
    AND SUM(CASE WHEN source_system = 'app' THEN 1 ELSE 0 END) = 1
    THEN 'PASS' ELSE 'FAIL' END AS check_result
FROM schema_events;

-- 03.2 Tag 固定某个 Snapshot；最新状态继续向前演进。
-- Snapshot 是一次提交后的完整表状态；Tag 是给某个 Snapshot 起一个长期可读的名字。
-- 白话理解：Snapshot 是版本号，Tag 是给某个重要版本贴一张不容易忘的书签。
DROP TABLE IF EXISTS versioned_products;
CREATE TABLE versioned_products (
  product_id BIGINT,
  product_name STRING,
  price DECIMAL(10, 2),
  version_no BIGINT,
  PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
  'bucket' = '2',
  'merge-engine' = 'deduplicate',
  'sequence.field' = 'version_no',
  'snapshot.num-retained.min' = '10',
  'snapshot.num-retained.max' = '20'
);

INSERT INTO versioned_products VALUES
  (1, 'Keyboard', CAST(100.00 AS DECIMAL(10, 2)), 1),
  (2, 'Mouse',    CAST( 50.00 AS DECIMAL(10, 2)), 1);

-- Snapshot 1：形成基线，并用 Tag 长期固定这一版数据。
CALL sys.create_tag(
  `table` => 'paimon_lab.versioned_products',
  tag => 'baseline'
);

INSERT INTO versioned_products VALUES
  (1, 'Keyboard', CAST(120.00 AS DECIMAL(10, 2)), 2),
  (3, 'Monitor',  CAST(800.00 AS DECIMAL(10, 2)), 1);

-- Snapshot 2：商品 1 改价，新增商品 3。
CALL sys.create_tag(
  `table` => 'paimon_lab.versioned_products',
  tag => 'after_price_change'
);

SELECT
  '03_02_latest_snapshot' AS check_name,
  CASE WHEN COUNT(*) = 3
    AND MAX(CASE WHEN product_id = 1 AND price = CAST(120.00 AS DECIMAL(10, 2))
                 THEN 1 ELSE 0 END) = 1
    THEN 'PASS' ELSE 'FAIL' END AS check_result
FROM versioned_products;

-- 最新 Snapshot 已是 120；baseline Tag 仍固定在价格 100 的历史状态。

-- scan.tag-name 和 scan.snapshot-id 读取历史完整状态。
SELECT
  '03_03_read_historical_state' AS check_name,
  CASE WHEN COUNT(*) = 2
    AND MAX(CASE WHEN product_id = 1 AND price = CAST(100.00 AS DECIMAL(10, 2))
                 THEN 1 ELSE 0 END) = 1
    AND SUM(CASE WHEN product_id = 3 THEN 1 ELSE 0 END) = 0
    THEN 'PASS' ELSE 'FAIL' END AS check_result
FROM versioned_products /*+ OPTIONS('scan.tag-name' = 'baseline') */;

-- Tag 系统表说明每个 Tag 固定了哪个 Snapshot。
-- $tags 字段：
-- tag_name：Tag 名；snapshot_id：Tag 固定的 Snapshot。
-- schema_id：该 Snapshot 使用的 Schema；commit_time：该 Snapshot 的提交时间。
-- record_count：Tag 对应版本的数据文件记录统计，不应替代主键表逻辑 COUNT(*)。
-- branches：Paimon 1.3.2 文档还展示关联 Branch 列表；本地 1.3.1 是否存在以 DESCRIBE 为准。
DESCRIBE `versioned_products$tags`;
-- 为兼容本地 1.3.1，执行查询只选择已确认存在的五个字段。
SELECT tag_name, snapshot_id, schema_id, commit_time, record_count
FROM `versioned_products$tags`
ORDER BY snapshot_id;

-- incremental-between 读取 (baseline, after_price_change] 的变化，不是末端完整状态。
-- 通俗理解：Tag 查询是在看“当时整张表”，增量查询是在看“两个时点之间改了什么”。
SELECT product_id, product_name, price, version_no
FROM versioned_products
     /*+ OPTIONS('incremental-between' = 'baseline,after_price_change') */
ORDER BY product_id;

-- 普通批增量查询丢弃 DELETE；要观察 RowKind 时使用 $audit_log。
-- rowkind：+I/-U/+U/-D；其余字段完全沿用 versioned_products 的业务字段。
SELECT rowkind, product_id, product_name, price, version_no
FROM `versioned_products$audit_log`
     /*+ OPTIONS('incremental-between' = 'baseline,after_price_change') */
ORDER BY product_id, rowkind;

SELECT *
FROM versioned_products /*+ OPTIONS('scan.snapshot-id' = '1') */
ORDER BY product_id;

SELECT * FROM schema_events ORDER BY event_id;
