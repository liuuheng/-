-- Paimon 1.3.1 + Flink 1.18.1 Tag、时间旅行与增量读取实验
-- 每次 DROP TABLE 后 Snapshot ID 从 1 重新开始，本脚本可独立重跑。

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

-- Snapshot 1：形成基线，并用 Tag 长期固定这一版数据。
INSERT INTO versioned_products VALUES
  (1, 'Keyboard', CAST(100.00 AS DECIMAL(10, 2)), 1),
  (2, 'Mouse',    CAST( 50.00 AS DECIMAL(10, 2)), 1);

CALL sys.create_tag(
  `table` => 'paimon_lab.versioned_products',
  tag => 'baseline'
);

-- Snapshot 2：商品 1 改价，新增商品 3。
INSERT INTO versioned_products VALUES
  (1, 'Keyboard', CAST(120.00 AS DECIMAL(10, 2)), 2),
  (3, 'Monitor',  CAST(800.00 AS DECIMAL(10, 2)), 1);

CALL sys.create_tag(
  `table` => 'paimon_lab.versioned_products',
  tag => 'after_price_change'
);

-- 最新 Snapshot 已是 120；baseline Tag 仍固定在价格 100 的历史状态。
SELECT
  '12_latest_snapshot' AS check_name,
  CASE
    WHEN COUNT(*) = 3
      AND MAX(CASE
        WHEN product_id = 1 AND price = CAST(120.00 AS DECIMAL(10, 2))
        THEN 1 ELSE 0 END) = 1
    THEN 'PASS' ELSE 'FAIL'
  END AS check_result
FROM versioned_products;

SELECT
  '13_read_by_tag' AS check_name,
  CASE
    WHEN COUNT(*) = 2
      AND MAX(CASE
        WHEN product_id = 1 AND price = CAST(100.00 AS DECIMAL(10, 2))
        THEN 1 ELSE 0 END) = 1
      AND SUM(CASE WHEN product_id = 3 THEN 1 ELSE 0 END) = 0
    THEN 'PASS' ELSE 'FAIL'
  END AS check_result
FROM versioned_products /*+ OPTIONS('scan.tag-name' = 'baseline') */;

-- Tag 系统表说明每个 Tag 固定了哪个 Snapshot。
SELECT tag_name, snapshot_id, schema_id, commit_time, record_count
FROM `versioned_products$tags`
ORDER BY snapshot_id;

-- 增量区间语义是 (start, end]，不是 end Tag 的完整表状态。
SELECT product_id, product_name, price, version_no
FROM versioned_products
     /*+ OPTIONS('incremental-between' = 'baseline,after_price_change') */
ORDER BY product_id;

-- 普通增量查询会丢弃 DELETE；需要审计 RowKind 时查询 $audit_log。
SELECT rowkind, product_id, product_name, price, version_no
FROM `versioned_products$audit_log`
     /*+ OPTIONS('incremental-between' = 'baseline,after_price_change') */
ORDER BY product_id, rowkind;

-- 同一历史版本也可以直接按 Snapshot ID 读取。
SELECT *
FROM versioned_products /*+ OPTIONS('scan.snapshot-id' = '1') */
ORDER BY product_id;
