-- 实验 02：批量 UPDATE / DELETE 与 INSERT OVERWRITE
-- 环境：Flink 1.18.1 + Paimon 1.3.1；本脚本必须使用 batch 模式。
-- 问题：行级 DML 如何改变主键表？动态覆盖与静态覆盖的删除范围有何不同？

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

-- 02.1 deduplicate 主键表支持批量 UPDATE / DELETE；UPDATE 不能修改主键。
-- UPDATE / DELETE 仅在 batch 模式执行。
-- deduplicate 和 partial-update 支持相应 UPDATE；aggregation 不能直接套用本实验的 DML 语义。
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
  '02_01_batch_update_delete' AS check_name,
  CASE WHEN COUNT(*) = 2
    AND SUM(CASE WHEN order_id = 2002 THEN 1 ELSE 0 END) = 0
    AND MAX(CASE WHEN order_id = 2001 AND status = 'PAID'
                  AND amount = CAST(120.00 AS DECIMAL(10, 2)) THEN 1 ELSE 0 END) = 1
    THEN 'PASS' ELSE 'FAIL' END AS check_result
FROM mutable_orders;

-- (1,3] 对应 UPDATE 与 DELETE 两次提交。rowkind 形状取决于 changelog-producer。
-- Audit Log 比普通表多一个 rowkind；它展示“发生了什么变化”，不是“现在表里有什么”。
-- none 模式不保证提供 UPDATE 的旧值 -U；下游需要完整 -U/+U 时应评估 lookup 的成本。
-- 白话理解：普通表像当前照片，Audit Log 像操作录像；录像是否拍到更新前画面取决于 Changelog 能力。
-- rowkind：+I 插入，-U 更新前，+U 更新后，-D 删除。
-- order_id/status/amount/dt：沿用业务表原字段及其含义；系统表只额外增加 rowkind。
SELECT rowkind, order_id, status, amount, dt
FROM `mutable_orders$audit_log`
     /*+ OPTIONS('incremental-between' = '1,3') */
ORDER BY order_id, rowkind;

-- 02.2 动态覆盖：只替换输入数据实际出现的分区。
-- Flink/Paimon 默认是 dynamic partition overwrite。
-- 输入只有 2026-09-27，所以该分区被替换；2026-09-28 原数据保留。
-- 白话理解：把每天的数据看成一个抽屉，输入里出现哪天，就只换哪天的抽屉。
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

INSERT OVERWRITE daily_sales VALUES
  (4, CAST(40.00 AS DECIMAL(10, 2)), '2026-09-27');

SELECT
  '02_02_dynamic_partition_overwrite' AS check_name,
  CASE WHEN COUNT(*) = 2
    AND SUM(CASE WHEN sale_id IN (1, 2) THEN 1 ELSE 0 END) = 0
    AND SUM(CASE WHEN sale_id = 3 THEN 1 ELSE 0 END) = 1
    AND SUM(CASE WHEN sale_id = 4 THEN 1 ELSE 0 END) = 1
    THEN 'PASS' ELSE 'FAIL' END AS check_result
FROM daily_sales;

-- 02.3 静态覆盖 + 空输入：明确清空 dt='2026-09-27'，其他分区保留。
-- OPTIONS Hint 只影响这一条 INSERT OVERWRITE，不修改 Catalog 中保存的表参数。
-- 白话理解：静态 PARTITION 是先点名抽屉；即使新数据为空，也会把被点名的抽屉清空。
INSERT OVERWRITE daily_sales
  /*+ OPTIONS('dynamic-partition-overwrite' = 'false') */
  PARTITION (dt = '2026-09-27')
SELECT sale_id, amount
FROM daily_sales
WHERE FALSE;

SELECT
  '02_03_static_partition_purge' AS check_name,
  CASE WHEN COUNT(*) = 1
    AND MAX(CASE WHEN sale_id = 3 AND dt = '2026-09-28' THEN 1 ELSE 0 END) = 1
    THEN 'PASS' ELSE 'FAIL' END AS check_result
FROM daily_sales;

SELECT * FROM mutable_orders ORDER BY dt, order_id;
SELECT * FROM daily_sales ORDER BY dt, sale_id;
