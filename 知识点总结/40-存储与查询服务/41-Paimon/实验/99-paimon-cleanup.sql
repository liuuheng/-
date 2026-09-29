-- 实验 99：清理本套实验创建的对象
-- 前置：必须先取消 05-paimon-streaming-writer.sql 提交的持续作业。
-- 范围：逐表删除，不使用 CASCADE；保留 paimon_lab 数据库和 warehouse。

SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG paimon_lab_catalog WITH (
  'type' = 'paimon',
  'paimon.catalog.type' = 'filesystem',
  'warehouse' = 's3://paimon/warehouse'
);

USE CATALOG paimon_lab_catalog;
USE paimon_lab;

-- 停止 Writer、删除 Catalog 表对象、清理对象存储旧文件是三件事。
-- DROP TABLE 只针对下列实验表；不要据此推断旧 Snapshot 文件已立即物理删除。
DROP TABLE IF EXISTS append_events;
DROP TABLE IF EXISTS pk_orders;
DROP TABLE IF EXISTS partial_profiles;
DROP TABLE IF EXISTS service_metrics;
DROP TABLE IF EXISTS first_seen_users;
DROP TABLE IF EXISTS profile_sequence_groups;
DROP TABLE IF EXISTS mutable_orders;
DROP TABLE IF EXISTS daily_sales;
DROP TABLE IF EXISTS schema_events;
DROP TABLE IF EXISTS versioned_products;
DROP TABLE IF EXISTS streaming_events;

-- DataGen 源表位于 SQL Client 的默认内存 Catalog，不在 Paimon warehouse 中。
USE CATALOG default_catalog;
USE default_database;
DROP TABLE IF EXISTS paimon_lab_datagen;
