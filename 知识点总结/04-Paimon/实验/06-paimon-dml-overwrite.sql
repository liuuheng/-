-- Paimon 1.3.1 + Flink 1.18.1 批量 DML 与分区覆盖实验
-- UPDATE / DELETE 仅在 batch 模式执行；UPDATE 不能修改主键字段。

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

-- ============================================================
-- 实验 1：主键表支持批量 UPDATE / DELETE
-- deduplicate 支持 UPDATE 和 DELETE；aggregation 不支持这组 DML。
-- ============================================================
DROP TABLE IF EXISTS mutable_orders;

CREATE TABLE mutable_orders (
  order_id BIGINT,
  status STRING,
  amount DECIMAL(10, 2),
  dt STRING,
  PRIMARY KEY (dt, order_id) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
  'bucket' = '2',
  'bucket-key' = 'order_id',
  'merge-engine' = 'deduplicate',
  'snapshot.num-retained.min' = '10',
  'snapshot.num-retained.max' = '20'
);

INSERT INTO mutable_orders VALUES
  (2001, 'CREATED', CAST(100.00 AS DECIMAL(10, 2)), '2026-09-27'),
  (2002, 'CREATED', CAST( 80.00 AS DECIMAL(10, 2)), '2026-09-27'),
  (2003, 'CREATED', CAST( 60.00 AS DECIMAL(10, 2)), '2026-09-28');

UPDATE mutable_orders
SET status = 'PAID', amount = CAST(120.00 AS DECIMAL(10, 2))
WHERE dt = '2026-09-27' AND order_id = 2001;

DELETE FROM mutable_orders
WHERE dt = '2026-09-27' AND order_id = 2002;

SELECT
  '09_batch_update_delete' AS check_name,
  CASE
    WHEN COUNT(*) = 2
      AND SUM(CASE WHEN order_id = 2002 THEN 1 ELSE 0 END) = 0
      AND MAX(CASE
        WHEN order_id = 2001
         AND status = 'PAID'
         AND amount = CAST(120.00 AS DECIMAL(10, 2))
        THEN 1 ELSE 0 END) = 1
    THEN 'PASS' ELSE 'FAIL'
  END AS check_result
FROM mutable_orders;

-- Audit Log 比普通表多出 rowkind；none 模式不保证提供 UPDATE 的旧值 -U。
-- 若下游必须获得完整的 -U/+U，应另行评估 lookup changelog-producer 的成本。
-- 限定 (1, 3]，跳过初始 INSERT，只看 UPDATE 和 DELETE 两次提交。
SELECT rowkind, order_id, status, amount, dt
FROM `mutable_orders$audit_log`
     /*+ OPTIONS('incremental-between' = '1,3') */
ORDER BY order_id, rowkind;

-- ============================================================
-- 实验 2：动态分区覆盖只替换输入数据实际出现的分区
-- Flink/Paimon 默认是 dynamic partition overwrite。
-- ============================================================
DROP TABLE IF EXISTS daily_sales;

CREATE TABLE daily_sales (
  sale_id BIGINT,
  amount DECIMAL(10, 2),
  dt STRING
) PARTITIONED BY (dt) WITH (
  'bucket' = '2',
  'bucket-key' = 'sale_id'
);

INSERT INTO daily_sales VALUES
  (1, CAST(10.00 AS DECIMAL(10, 2)), '2026-09-27'),
  (2, CAST(20.00 AS DECIMAL(10, 2)), '2026-09-27'),
  (3, CAST(30.00 AS DECIMAL(10, 2)), '2026-09-28');

-- 输入只包含 2026-09-27，所以只替换该分区；2026-09-28 保留。
INSERT OVERWRITE daily_sales VALUES
  (4, CAST(40.00 AS DECIMAL(10, 2)), '2026-09-27');

SELECT
  '10_dynamic_partition_overwrite' AS check_name,
  CASE
    WHEN COUNT(*) = 2
      AND SUM(CASE WHEN sale_id IN (1, 2) THEN 1 ELSE 0 END) = 0
      AND SUM(CASE WHEN sale_id = 3 THEN 1 ELSE 0 END) = 1
      AND SUM(CASE WHEN sale_id = 4 THEN 1 ELSE 0 END) = 1
    THEN 'PASS' ELSE 'FAIL'
  END AS check_result
FROM daily_sales;

-- Paimon 的动态表参数 Hint：只影响这一条语句，不修改 Catalog 中的表参数。
-- 关闭动态覆盖并给出静态分区，可用空输入清空指定分区。
INSERT OVERWRITE daily_sales
  /*+ OPTIONS('dynamic-partition-overwrite' = 'false') */
  PARTITION (dt = '2026-09-27')
SELECT sale_id, amount
FROM daily_sales
WHERE FALSE;

SELECT
  '11_static_partition_purge' AS check_name,
  CASE
    WHEN COUNT(*) = 1
      AND MAX(CASE WHEN sale_id = 3 AND dt = '2026-09-28' THEN 1 ELSE 0 END) = 1
    THEN 'PASS' ELSE 'FAIL'
  END AS check_result
FROM daily_sales;

SELECT * FROM mutable_orders ORDER BY dt, order_id;
SELECT * FROM daily_sales ORDER BY dt, sale_id;
