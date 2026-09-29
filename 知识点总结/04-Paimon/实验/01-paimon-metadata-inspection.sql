-- Paimon 元数据与存储组织巡检
-- 先运行 00-paimon-batch-core.sql 创建实验数据。
--
-- Paimon 将 `业务表名$后缀` 解析成依附于业务表的系统表。
-- 例如 `pk_orders$snapshots` 不是用户创建的物理表，而是 pk_orders 的提交历史视图。
-- `$` 是表名的一部分，Flink SQL 中应使用反引号包住完整名称。
-- 系统表字段会随 Paimon 版本和表特性变化；本脚本按 Paimon 1.3.x 选取字段。


-- CHANGELOG 是“流式日志”，一条一条往下走；TABLEAU 是“动态表格”，在原地刷新同一个格子；TABLE：静态表格，只打印当前结果表的“快照
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG paimon_lab_catalog WITH (
  'type' = 'paimon',
  'paimon.catalog.type' = 'filesystem',
  'warehouse' = 's3://paimon/warehouse'
);

USE CATALOG paimon_lab_catalog;
USE paimon_lab;

-- 1. 当前结构与完整 DDL
DESCRIBE pk_orders;
SHOW CREATE TABLE pk_orders;

-- 2. `$options`：当前 Schema 中显式保存的表参数。
-- key：参数名，例如 bucket、merge-engine、sequence.field。
-- value：参数值，统一以字符串形式展示。
-- 查询不到某个参数不表示该能力未启用；它可能正在使用 Paimon 当前版本的默认值。
-- 建表 DDL 里的 WITH 参数
SELECT `key`, `value`
FROM `pk_orders$options`
ORDER BY `key`;

-- 3. `$schemas`：表的 Schema 版本历史，不是 Snapshot 历史。   查询的是字段的信息
-- schema_id：Schema 版本号；建表通常从 0 开始，每次 Schema Evolution 产生新版本。
-- fields：该版本的字段定义，包含稳定字段 ID、字段名、数据类型等信息。
-- partition_keys：该版本的分区字段列表。
-- primary_keys：该版本的主键字段列表；Append Table 为空。
-- options：该 Schema 版本保存的表参数，可能与当前 `$options` 不同。
-- append_events 执行过 ADD COLUMN，因此至少应看到建表和加字段两个 Schema 版本。
-- $schemas 返回的是每次 Schema 变更后的一份“完整快照”，不是“仅变更的参数”
SELECT schema_id, fields, partition_keys, primary_keys, options
FROM `append_events$schemas`
ORDER BY schema_id;

-- 4. `$snapshots`：成功发布的提交历史。每行代表一个 Snapshot，不代表一个业务分区。
-- snapshot_id：Snapshot 序号，用于 scan.snapshot-id 时间旅行；它不是 Flink Checkpoint ID。
-- schema_id：读取该 Snapshot 时采用的 Schema 版本，可与 `$schemas.schema_id` 关联。
-- commit_kind：物理提交类型，常见值为 APPEND、COMPACT、OVERWRITE、ANALYZE。
-- commit_time：Snapshot 成功提交到 Paimon 的时间，不是业务事件时间。
-- total_record_count：该 Snapshot 引用的有效数据文件中的未合并物理记录数。
-- delta_record_count：本次提交因新增、删除数据文件带来的物理记录数净变化。
-- 主键表需要 Merge-Read；因此 total_record_count 不一定等于 SELECT COUNT(*)。
SELECT
  snapshot_id,
  schema_id,
  commit_kind,
  commit_time,
  total_record_count,
  delta_record_count
FROM `pk_orders$snapshots`
ORDER BY snapshot_id;

-- 5. `$partitions`：按分区汇总当前 Snapshot 引用的数据文件。
-- partition：分区值组成的 Row，例如 {2026-09-27}；它不是目录字符串。
-- record_count：该分区有效数据文件中的物理记录数，主键表中可能尚未合并。
-- file_size_in_bytes：该分区有效数据文件的总字节数。
-- file_count：该分区当前有效的数据文件数量，可用于发现小文件积压。
-- last_update_time：分区元数据最近更新时间，不等于分区内最大的业务时间。
SELECT
  `partition`,
  record_count,
  file_size_in_bytes,
  file_count,
  last_update_time
FROM `append_events$partitions`;

-- 6. `$files`：选定 Snapshot 当前引用的有效数据文件，默认读取最新 Snapshot。
-- 它不会列出已被新 Snapshot 替换但尚未从对象存储删除的旧文件。
-- partition：文件所属分区；同一分区可以包含多个 Bucket。
-- bucket：逻辑 Bucket 编号，不是文件数量；一个 Bucket 可以包含多个数据文件。
-- level：LSM 层级。Level 0 通常是新写入且键范围可能重叠的文件，高层文件由 Compaction 产生。
-- record_count：单个文件内的物理记录数；SUM 后仍不是主键合并后的逻辑行数。
-- file_size_in_bytes：单个文件大小；SUM 后可观察分区、Bucket、Level 的存储规模。
-- file_count：当前 GROUP BY 范围内的有效文件数，是本查询计算出的别名，不是系统表原字段。
SELECT
  `partition`,
  bucket,
  level,
  COUNT(*) AS file_count,
  SUM(record_count) AS physical_record_count,
  SUM(file_size_in_bytes) AS total_bytes
FROM `pk_orders$files`
GROUP BY `partition`, bucket, level
ORDER BY `partition`, bucket, level;

-- 7. `$manifests`：选定 Snapshot 引用的 Manifest 文件，默认读取最新 Snapshot。
-- file_name：Manifest 元数据文件名，不是存放业务记录的 Data File 名称。
-- file_size：Manifest 文件自身大小，单位为字节。
-- num_added_files：该 Manifest 中状态为 ADD 的数据文件条目数量，不是新增业务行数。
-- num_deleted_files：该 Manifest 中状态为 DELETE 的数据文件条目数量；这里只是从新表状态移除引用，
--                    物理文件仍可能被旧 Snapshot 或 Tag 引用。
-- schema_id：写入这些 Manifest 条目时关联的 Schema 版本。
-- 一个 Manifest 可以记录多个分区和 Bucket，不能把它理解成“一个分区对应一个 Manifest”。
SELECT
  file_name,
  file_size,
  num_added_files,
  num_deleted_files,
  schema_id
FROM `pk_orders$manifests`;

-- 8. 对比逻辑行数与物理记录数。
-- 逻辑行数和物理记录数，用来判断表内数据版本堆积情况

-- 第一条：逻辑行数
-- 普通表查询会按主键、merge-engine、sequence.field 执行 Merge-Read，得到最终逻辑行。
SELECT COUNT(*) AS logical_row_count
FROM pk_orders;

-- 第二条：物理记录数
-- `$snapshots.total_record_count` 来自 Snapshot 引用的数据文件统计，未执行主键合并。
-- 同一 order_id 的多个版本可能分别占一条物理记录，却只产生一条逻辑结果。
-- 合并（compaction）之后，物理记录数会减少。
SELECT
  snapshot_id,
  total_record_count AS unmerged_physical_record_count
FROM `pk_orders$snapshots`
ORDER BY snapshot_id DESC
LIMIT 1;
