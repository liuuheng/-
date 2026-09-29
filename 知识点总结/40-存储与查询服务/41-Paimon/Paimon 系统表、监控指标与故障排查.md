# Paimon 系统表、监控指标与故障排查

## 结论

排查 Paimon 要把三类证据对齐：Flink checkpoint 说明运行时是否完成一致性提交，Paimon 系统表说明表当前引用了哪些元数据和文件，Metrics 说明写入、提交、Compaction 或 Lookup 的哪个阶段正在积压。

只看文件系统目录容易误判。目录中可能同时存在当前文件、旧 Snapshot 仍引用的文件和 Orphan File；只有当前 Snapshot 的 Manifest 才定义当前表状态。

## 1. 常用系统表

数据表后加 `$表名` 可以查询表级元数据。包含 `$` 的标识符在 Flink SQL 中通常要使用反引号：

```sql
SELECT *
FROM my_catalog.my_db.`orders$snapshots`;
```

| 问题 | 优先查询的系统表 |
| --- | --- |
| 最近是否提交、谁提交、提交类型 | `$snapshots` |
| Schema 如何变化 | `$schemas` |
| 当前表配置是什么 | `$options` |
| 文件数量、大小、Level、Bucket | `$files` |
| Manifest 数量和规模 | `$manifests` |
| 分区/Bucket 分布 | `$partitions`、`$buckets` |
| Tag、Branch 保留了什么 | `$tags`、`$branches` |
| 流式 Consumer 读到哪里 | `$consumers` |
| 索引覆盖和主键范围 | `$file_indexes`、`$table_indexes`、`$file_key_ranges` |
| 变更明细或审计 | `$audit_log`、`$binlog` |

不同版本、表类型和 Feature 暴露的系统表不同。启用 `query-auth.enabled=true` 时，部分含原始元数据的系统表可能不可用。

## 2. `$snapshots`：判断数据是否真正提交

```sql
SELECT
  snapshot_id,
  schema_id,
  commit_kind,
  commit_time,
  total_record_count,
  delta_record_count
FROM `orders$snapshots`
ORDER BY snapshot_id DESC;
```

Writer 运行但没有新 Snapshot，先查 Flink checkpoint 是否成功。流式写只有完成 checkpoint 后才提交；在 SQL Client 中修改 checkpoint 配置，也不会改变已经提交运行的作业。

有新 Snapshot 但查询看不到新数据，再检查 Catalog、Database、分区过滤、Time Travel Hint、Read-optimized 视图和 MOW Level-0 可见性。

`total_record_count`、`delta_record_count` 是数据文件中的未合并记录数，不是主键表最终逻辑行数。更新前后版本、专用 BLOB 文件等都可能增加物理记录数。需要逻辑行数时执行普通 `COUNT(*)`，不要直接把 Snapshot 计数当作业务行数。

## 3. `$files`：判断小文件与 Compaction 积压

```sql
SELECT
  `partition`,
  bucket,
  level,
  COUNT(*) AS file_count,
  SUM(file_size_in_bytes) AS total_bytes,
  AVG(file_size_in_bytes) AS avg_file_bytes
FROM `orders$files`
GROUP BY `partition`, bucket, level
ORDER BY file_count DESC;
```

观察时要同时看分区、Bucket 和 Level：

- Level 0 文件持续增加：写入速度超过 Compaction 处理速度，或 Compactor 没有覆盖该分区。
- 文件数高且平均文件很小：checkpoint/commit 太频繁、并行度过高、单 Bucket 数据量太小，或 `target-file-size` 与流量不匹配。
- 少数 Bucket 文件和字节远高于其他 Bucket：主键或 Bucket Key 倾斜。
- 历史分区文件长期不收敛：Streaming Compactor 只处理活跃范围，可能需要指定分区的 Batch Compaction。

对象存储 `LIST` 得到的文件数不能替代 `$files`。旧 Snapshot、Tag 和 Orphan File 都可能仍在目录中，却不属于当前 Snapshot。

## 4. `$manifests` 与元数据压力

Manifest 记录数据文件的新增和删除。Manifest 数量过多或过小会增加 Scan Planning 与提交元数据开销。

当查询慢在“真正读数据之前”，同时检查 `$manifests`、Scan Metrics 和 JobManager 日志。数据文件 Compaction 与 Manifest Compaction 是两件事：前者合并业务数据文件，后者整理文件清单元数据。

`write-only=true` 默认不一定跳过 Manifest Merge。只有明确配置相应选项后才会跳过；不要根据 write-only 推断所有维护都停止。

## 5. Commit Metrics

| 指标 | 含义 | 异常时优先检查 |
| --- | --- | --- |
| `lastCommitDuration` | 最近一次提交耗时 | Catalog、对象存储、锁、Manifest 规模 |
| `commitDuration` | 多次提交耗时分布 | 是否持续变慢还是偶发尖刺 |
| `lastCommitAttempts` | 最近提交尝试次数 | 多 Writer Snapshot 竞争 |
| `lastTableFilesAdded` | 本次加入表状态的文件 | 写入并行度、小文件 |
| `lastTableFilesDeleted` | 本次从新 Snapshot 移除的文件 | Compaction/Overwrite 范围 |
| `lastGeneratedSnapshots` | 本次生成的 Snapshot 数量 | 是否同时产生额外维护提交 |

提交尝试次数上升但最终成功，说明乐观重试正在吸收竞争；如果同时出现作业重启，要检查是否已经升级为 File Conflict。只增加重试次数会延长症状，不能消除冲突来源。

## 6. Write、Write Buffer 与 Compaction Metrics

Writer 指标按表、分区和 Bucket 暴露。排查热点时不要只看 Job 总吞吐，要下钻到具体 Bucket。

Write Buffer 长期接近上限并频繁 Spill，说明内存不足、单个 Writer 负责的活跃 Bucket 太多，或存在主键倾斜。Spill 能防止 OOM，但会把压力转移到本地磁盘 I/O。

Compaction 重点观察 Level-0 文件数、参与/产生文件数量、Compaction 耗时和失败。`maxLevel0FileCount` 持续上升通常表示 Compaction 追不上；调高停止阈值只会允许更多积压，不会增加处理能力。

平均文件过小不能只靠增大 `target-file-size` 修复。若每个 checkpoint 每个 Bucket 只有少量数据，根因可能是 Bucket/并行度过多或 checkpoint 太频繁。

## 7. Lookup Metrics

`lookup` Changelog、MOW、first-row 等路径会使用本地内存和磁盘 Cache。重点指标：

- `partialLookupCount`：Lookup 调用次数。
- `partialLookupRemoteAccessCount`：至少需要从表存储创建一个本地 Lookup 文件的调用次数。

后者按“调用”计数，不是对象存储请求次数。两者比例持续较高，说明本地 Cache 命中差，可能是 Cache 容量不足、保留时间太短、Task 经常迁移/重启，或活跃数据范围超过本地盘能力。

结合 `lookup.cache-file-retention`、`lookup.cache-max-disk-size`、`lookup.cache-max-memory-size` 和 TaskManager 本地盘使用量判断，不能只调大内存 Cache。

## 8. Flink 中去哪里看 Metrics

Paimon 把指标桥接到 Flink 指标系统，不同指标挂在不同 Operator：

- Commit：Global Committer/Committer。
- Write 与 Compaction：Writer 的具体分区、Bucket Metric Group。
- Write Buffer：Writer 的 `writeBuffer` Group。
- Lookup：Lookup Operator 或相关 Writer Group。
- Scan：Source Enumerator/Coordinator；Scan Metrics 需要受支持的 Flink 版本。

完整指标名受 Flink Metric Scope 配置影响。排查时先在 Web UI 定位 Operator，再搜索短指标名，不要把官方示例中的完整 Host/Job 前缀硬编码到告警规则。

## 9. 症状到证据的排查路径

### Writer 在跑，但新数据不可见

1. 查看 Flink 是否完成 checkpoint。
2. 查询 `$snapshots` 是否产生新提交。
3. 没有 Snapshot：查 Committer 日志、Catalog 和提交指标。
4. 有 Snapshot：查查询端 Catalog、分区条件、Time Travel/Read-optimized 设置。
5. MOW/first-row：检查 Lookup Compaction 和 Level-0 可见性。

### Checkpoint 越来越慢

1. 对齐 checkpoint duration 与 `lastCommitDuration`。
2. `lastCommitAttempts` 高：查并发 Writer。
3. Level-0 与 Sorted Run 高：查 Compaction 是否阻塞提交。
4. Lookup Remote Access 高：查本地 Cache 和 Task 重启。
5. Manifest 多：查元数据规划与 Manifest Merge。

### 查询越来越慢

1. Scan 是否慢在 Planning 还是读取数据。
2. `$files` 是否出现大量小文件或重叠 Level-0。
3. Bucket/分区是否倾斜。
4. 查询是否使用普通表、Read-optimized 表或 Time Travel。
5. 查询引擎是否正确支持 Merge Engine 和 DV。

### 历史查询失败

1. `$snapshots` 中目标 Snapshot 是否仍保留。
2. `$tags` 是否保存长期版本。
3. `$consumers` 是否阻止或影响 Snapshot 清理边界。
4. 查询启动模式是否在不存在的 Snapshot 之前/之后。

### 作业反复重启

1. 区分 OOM、本地盘满、Checkpoint Timeout 和 Commit Conflict。
2. Commit Conflict 时列出所有 Writer/Compactor 的分区范围。
3. Cross-partition 或动态桶恢复慢时，检查索引 Bootstrap 和状态规模。
4. 不要先通过无限重启掩盖持续存在的文件冲突或资源缺口。

## 10. 建议告警

- 一段时间没有成功 checkpoint，也没有新 Snapshot。
- `lastCommitDuration` 接近 checkpoint timeout。
- `lastCommitAttempts` 连续大于 1。
- Level-0 文件数持续上升而非短时波动。
- 平均文件大小长期远低于目标文件大小。
- Write Buffer Spill 与本地盘占用同步上升。
- Lookup Remote Access 比例持续升高。
- Compaction 作业没有进展或同分区出现多个 Compactor。
- Snapshot、Manifest、Tag、Consumer 数量超过已定义的保留预算。

阈值应按表流量、checkpoint 间隔和 SLA 建基线。单个瞬时数值通常不足以判断故障，趋势和跨指标关联更有意义。

## 11. 日常巡检 SQL

```sql
-- 最近提交
SELECT snapshot_id, commit_kind, commit_time, schema_id
FROM `orders$snapshots`
ORDER BY snapshot_id DESC
LIMIT 20;

-- 分区、Bucket、Level 文件分布
SELECT `partition`, bucket, level,
       COUNT(*) AS file_count,
       SUM(file_size_in_bytes) AS total_bytes,
       AVG(file_size_in_bytes) AS avg_bytes
FROM `orders$files`
GROUP BY `partition`, bucket, level;

-- 保留的 Tag
SELECT * FROM `orders$tags`;

-- Consumer 进度
SELECT * FROM `orders$consumers`;
```

字段名会随版本和系统表变化，首次上线应先 `DESCRIBE` 对应系统表，再把巡检 SQL 固化到实际版本。

## 相关笔记

- [[Paimon 流式读写中的 Snapshot 与 Changelog]]
- [[Paimon Snapshot、Manifest、Parquet 与 Compaction 的关系]]
- [[Paimon 多 Writer、Catalog 与原子提交]]
- [[Paimon MOR、COW、MOW 与 Deletion Vector]]

## 官方资料

- [System Tables](https://paimon.apache.org/docs/master/concepts/system-tables/)
- [Metrics](https://paimon.apache.org/docs/master/maintenance/metrics/)
- [Flink Troubleshooting](https://paimon.apache.org/docs/master/flink/troubleshooting/)
- [Streaming Writes and Small Files](https://paimon.apache.org/docs/master/learn-paimon/small-files/)

> 核对日期：2026-09-16。指标名称、系统表字段和引擎支持范围应以实际部署版本为准。
