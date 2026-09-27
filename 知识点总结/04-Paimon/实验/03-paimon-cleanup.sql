-- 清理本套实验创建的 Paimon 对象。
-- 逐表删除，不使用 CASCADE，并保留 paimon_lab 数据库本身。

SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG paimon_lab_catalog WITH (
  'type' = 'paimon',
  'paimon.catalog.type' = 'filesystem',
  'warehouse' = 's3://paimon/warehouse'
);

USE CATALOG paimon_lab_catalog;
USE paimon_lab;

DROP TABLE IF EXISTS append_events;
DROP TABLE IF EXISTS pk_orders;
DROP TABLE IF EXISTS partial_profiles;
DROP TABLE IF EXISTS service_metrics;
DROP TABLE IF EXISTS streaming_events;
