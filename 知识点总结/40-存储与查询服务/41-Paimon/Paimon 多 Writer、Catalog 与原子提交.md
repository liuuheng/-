# Paimon 多 Writer、Catalog 与原子提交

## 结论

Paimon 允许并发 Writer，但“能同时提交 Snapshot”不等于“任意多个作业都能安全修改同一批文件”。并发正确性依赖三层：Catalog/文件系统能原子发布 Snapshot，乐观并发控制能发现冲突，表的分桶和维护方式允许这些 Writer 同时工作。

生产设计应尽量让 Writer 修改不同分区。多个 Writer 写同一分区时，固定桶仍需协调 Compaction；动态桶则要求同一分区只能由一个作业写入，独立 Compaction 不能解除这个限制。

> [!info] 本次补充的来源与覆盖范围
> 来源：[微信文章](https://mp.weixin.qq.com/s/po4y9l0ZNtowdHKgVrfvvA)。微信页面触发环境验证，未能取得原文全文、图片和全部源码截图；以下内容依据用户提供的文章总结整理，并用 Apache Paimon Core `master` 与 `paimon-flink-common 1.4.2` 源码及官方文档核对。它覆盖总结中的路由、并发冲突和 `write-only` 结论，不代表已逐节覆盖微信原文。

## 1. 固定桶在单个 Flink Job 中如何路由

讨论 Sink 并发前，需要把三个对象分开：

- `bucketId=0` 只是一个分区内部的桶编号。
- `partition=P1, bucketId=0` 才是实际写入和 Compaction 的 Partition-Bucket，对应一棵独立 LSM Tree。
- Writer 可能指单个 Job 内的 Sink Subtask，也可能指多个独立写入 Job。Job 内路由不能协调 Job 之间的写入。

固定桶记录先根据主键或 `bucket-key` 算出 Bucket，再由 `ChannelComputer` 选择 Sink Subtask。Paimon Core 当前实现是：

```java
static int select(BinaryRow partition, int bucket, int numChannels) {
    return (startChannel(partition, numChannels) + bucket) % numChannels;
}

static int startChannel(BinaryRow partition, int numChannels) {
    int hashCode = partition.hashCode();
    // 省略 Integer.MIN_VALUE 的特殊处理
    return Math.abs(hashCode) % numChannels;
}
```

可记为：

```text
startChannel = abs(partition.hashCode()) % numChannels
channel      = (startChannel + bucketId) % numChannels
```

`RowDataChannelComputer` 从记录中提取 Partition 和 Bucket，然后调用这个公式。相同的 Partition-Bucket 在同一个算子拓扑和相同并行度下始终得到相同 Channel，因此只进入一个 Sink Subtask。

### 无分区表：可用 Channel 数受 Bucket 数限制

无分区表的 Partition 值固定，所以 `startChannel` 也是常量。假设固定桶数为 `512`、Writer 并行度为 `800`，合法 `bucketId` 为 `0~511`。这里的 `511` 来自“该表配置了 512 个 Bucket”，不是 Paimon 所有固定桶表的全局 Bucket ID 上限。

```text
同一个固定 startChannel
+ bucketId 0~511
→ 最多只能命中 512 个 Channel
```

因此，若 Writer 并行度确实是 `800`，至少有 `288` 个 Subtask 收不到该表的数据。具体空闲的是哪几个 Subtask 取决于固定的 `startChannel`；“只有 0~511 收到数据”只在 `startChannel=0` 时成立。

Paimon Flink 1.4.2 的 `FlinkSinkBuilder.buildForFixedBucket()` 确实有自动降并发逻辑，但同时要求以下三个条件：

1. 没有显式设置 Sink 并行度，即内部 `parallelism == null`；
2. Bucket 数小于上游并行度；
3. 表没有分区字段。

满足时，Writer 并行度被设置为 Bucket 数。如果显式配置了 `sink.parallelism=800`，这段逻辑不会把它自动改成 `512`。因此不能无条件写成“引擎一定会自动降为 Bucket 数”。

### 分区表：相同 bucketId 可以落到不同 Subtask

假设仍有 `512` 个 Bucket 和 `800` 个 Writer：

```text
Partition P1：startChannel=30
P1/bucket-0   → channel 30
P1/bucket-511 → channel 541

Partition P2：startChannel=500
P2/bucket-0   → channel 500
P2/bucket-511 → channel 211  // 对 800 取模
```

不同 Partition 的起点不同，它们命中的 Channel 区间可以错开。活跃分区数量足够、Partition Hash 分布较均匀时，即使单分区 Bucket 数小于 Sink 并行度，多个分区合起来也可能覆盖全部 `800` 个 Subtask。

这里仍不能保证“一定跑满全部 Subtask”。如果只有一个活跃分区、分区严重倾斜，或者多个分区的 `startChannel` 相近，仍可能存在空闲和负载不均。应以 Flink 的 `numRecordsIn`、Busy Time、Backpressure 和各 Subtask 吞吐验证实际分布。

更重要的是：

```text
P1/bucket-0 ≠ P2/bucket-0
```

两者只是 Bucket 编号相同，实际位于不同分区目录，具有不同 Manifest 身份和不同 LSM Tree。把它描述为“多个 Writer 写同一个 Bucket”容易让人误以为多个 Subtask 同时修改同一棵 LSM Tree。准确说法是：**不同 Writer 可以处理不同分区中编号相同的 Bucket。**

### 为什么这种路由不会把数据写乱

单个 Flink Job 内的直接保证来自确定性路由：同一个 Partition-Bucket 只交给一个 Sink Subtask。Flush 产生 UUID 命名的新文件、L0 允许主键范围重叠以及 Snapshot 乐观提交，是文件生成和提交阶段的后续保证，并不是用来允许同一个 Job 内多个 Subtask 随机拆写同一个 Partition-Bucket。

需要分两种情况判断：

| 场景 | 是否可能同时写同一 Partition-Bucket | 主要保证与风险 |
| --- | --- | --- |
| 一个 Flink Job 内的多个 Sink Subtask | 正常固定桶路由下不会 | `Partition + Bucket → Channel` 确定性路由 |
| 多个独立 Job 写固定桶表 | 会，各 Job 有自己的路由拓扑 | 需要原子 Snapshot 提交；Writer Compaction 可能发生 File Conflict |
| 多个独立 Job 且全部 `write-only=true` | 会，但摄入 Job 只追加文件 | 减少 Compaction DELETE 冲突；仍有 Snapshot 竞争和顺序语义问题 |
| 多个独立 Job 写动态桶同一分区 | 不支持 | 独立 Key-Bucket 索引可能把同一 Key 分到不同 Bucket |

### ADD、L0 与 File Conflict 的准确关系

普通 Flush 会生成新的 L0 Data File，并在 Manifest 中提交 `ADD`。文件名中的 UUID 降低名称碰撞风险；同一 Partition-Bucket 的不同 L0 文件允许主键范围重叠。Paimon 对标准 LSM 布局的同层 Key Range 重叠检查针对 Level 0 以上文件。

这只能说明“彼此独立的新增 L0 文件通常可以一起进入表状态”，不能推出所有 Writer 提交都只有 `ADD`。默认 Writer 会执行 Compaction：读取旧文件、生成新文件，并在新 Snapshot 中用 `DELETE` 让旧文件退出当前表状态。如果两个 Writer 或 Compactor 选择了相同旧文件，第一个提交后，第二个提交仍要删除已经失效的文件，就会触发 File Conflict。

Snapshot Conflict 与 File Conflict 也要分开：

- 两个提交争用同一个 Snapshot ID 时，后提交者可以读取最新 Snapshot 后重试，但受重试次数、超时和原子发布条件限制。
- 待删除文件已经被其他提交替换时，单纯换一个 Snapshot ID 不能让旧 Compaction 计划重新有效，流作业可能 failover 后重新计算。

### `write-only=true` 改变了什么

摄入 Writer 配置 `write-only=true` 后，会跳过 Writer 内 Compaction 和 Snapshot Expiration。摄入提交因此主要是新增文件，不再由多个写入 Job 争抢同一批 Compaction 输入文件。正确的拓扑是：

```text
摄入 Job A（write-only=true）──┐
摄入 Job B（write-only=true）──┼→ 固定桶表 ← 唯一独立 Compactor
摄入 Job C（write-only=true）──┘
```

它没有消除以下问题：

- Snapshot ID 发布竞争；
- 多 Job 对同一主键的先后顺序，需要 `sequence.field` 或符合业务的顺序规则；
- Overwrite、Rescale 和普通 Append 同时修改相同范围的兼容性；
- 动态 Bucket 的同分区单写限制；
- Snapshot Expiration 等维护职责。

### 小文件结论也要绑定正确原因

`write-only=true` 会让 L0 和 Sorted Run 持续积累；没有独立 Compactor 时，读时需要打开并合并更多文件，读放大和元数据开销会上升。这部分结论成立。

但“Sink 并行度越高，同一个 Partition-Bucket 就被更多 Subtask 拆得越碎”不适用于单个固定桶 Flink Job。确定性路由保证一个 Partition-Bucket 只归一个 Subtask。小文件数量更直接地取决于：

- Checkpoint 和提交频率；
- MemTable Flush、Write Buffer 与目标文件大小；
- 活跃 Partition-Bucket 数量；
- 同时写入同一 Partition-Bucket 的独立 Job 数量；
- Compaction 是否启用、是否积压以及 Compactor 吞吐。

提高 Sink 并行度可能增加同时活跃的 Writer、内存需求和整张表的总文件生成速率，但不能直接等同于“单个 Bucket 被更多 Subtask 拆写”。

### 可执行的配置原则

1. 单个热点分区持续写入时，让固定 Bucket 数不少于目标 Sink 并行度；希望分配均匀时，可让 Bucket 数是 Sink 并行度的整数倍。
2. 多分区表可以通过不同 `startChannel` 利用更多 Subtask，但不能仅凭公式假设已经跑满，要看各 Subtask 指标。
3. 开启 `write-only=true` 时，必须指定唯一的独立 Compactor，并安排 Snapshot Expiration；监控 `$files` 中的 L0 文件数、文件大小和 Sorted Run 积压。
4. 多 Job 写同一固定桶分区时，还要验证主键顺序、Overwrite/Rescale 边界、Catalog 和原子提交配置。
5. 上述结论仅用于固定 Bucket；动态 Bucket 同一分区仍只允许一个写入 Job。

相关源码和文档：

- [ChannelComputer：Partition-Bucket 到 Channel 的公式](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/sink/ChannelComputer.java)
- [FixedBucketWriteSelector：固定桶记录调用 ChannelComputer](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/sink/FixedBucketWriteSelector.java)
- [Paimon Flink 1.4.2 Source Jar](https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-common/1.4.2/paimon-flink-common-1.4.2-sources.jar)
- [Write Performance：官方建议 Sink 并行度不高于 Bucket 数](https://paimon.apache.org/docs/master/maintenance/write-performance/)
- [Concurrency Control](https://paimon.apache.org/docs/master/concepts/concurrency-control/)
- [Dedicated Compaction](https://paimon.apache.org/docs/master/maintenance/dedicated-compaction/)

## 2. 一次提交如何发生

Writer 的提交过程可以拆成四步：

1. 写出新的数据文件，准备本次要新增和逻辑删除的文件列表。
2. 读取最新 Snapshot，验证这些文件变更仍适用于当前表状态。
3. 生成新 Snapshot，通过 Catalog 或文件系统原子发布。
4. 如果其他 Writer 抢先提交，重新读取最新状态；变化仍兼容时重试，否则拒绝提交。

数据文件已经出现在存储中，不代表它对读者可见。只有被已提交 Snapshot 引用后，它才进入表状态。Compaction 标记旧文件为删除，也只是让新 Snapshot 不再引用；旧 Snapshot 和 Tag 仍可能引用这些文件，物理清理由 Snapshot Expiration 等维护操作决定。

## 3. Snapshot Conflict 与 File Conflict

Snapshot Conflict 是两个 Writer 都想发布 `N+1`。Writer A 先成功后，Writer B 重新读取最新 Snapshot；如果 B 的文件变更仍有效，可以改为提交 `N+2`。这是可重试的发布竞争。

File Conflict 是两个操作修改了同一批输入文件。例如两个 Compactor 都计划用新文件替换 A、B。第一个提交后，A、B 已不属于最新表状态；第二个提交仍要删除 A、B，因此验证失败。仅换一个 Snapshot ID 重试不能让过期的文件计划重新有效，作业需要重新计算或从 checkpoint 恢复。

流作业提交失败可能触发重启。若多个 Writer 持续对同一批文件做 Compaction，就可能反复冲突、恢复、再次冲突，表现为“表没坏，但作业一直重启”。

## 4. 为什么独立 Compaction 能减少冲突

默认每个 Writer 都会在写入过程中做 Compaction。多个 Writer 写同一分区时，它们可能同时选择相同旧文件。

可以让所有摄入 Writer 设置：

```sql
'write-only' = 'true'
```

再运行一个独立 Compaction 作业。这样只有一个 Compactor 负责替换旧文件，摄入 Writer 只追加新文件，能减少文件级冲突，并把 Compaction 的 CPU、内存和 I/O 从写入作业中隔离出来。

`write-only=true` 还会跳过 Writer 内的 Snapshot Expiration。启用后必须安排独立 Compaction 和历史清理，否则文件、Snapshot 与元数据会持续增长。若还希望跳过自动 Manifest Merge，可按版本核对 `manifest.merge.skip-on-write-only`。

Flink SQL 可以运行指定表的 Compaction：

```sql
CALL sys.compact(
  `table` => 'default.orders',
  partitions => 'dt=2026-09-16',
  options => 'sink.parallelism=8'
);
```

Batch 模式通常一次性处理当前文件；Streaming 模式持续发现新变化。不要让两个独立 Compaction 作业覆盖同一分区。

## 5. 独立 Compaction 不能解决什么

它不能消除 Snapshot 发布竞争，也不能让所有表特性支持并发写入。最典型的是动态桶：同一分区只允许一个作业写入。

动态桶 Writer 维护 Key 到 Bucket 的索引。两个作业无法共享这份实时分配状态，可能把同一主键分配到不同 Bucket，最终形成重复逻辑行。`write-only=true` 只改变谁做 Compaction，不会让两个动态桶索引变成一个全局一致索引。

固定桶通过稳定 Hash 规则计算 Bucket，更适合多个 Writer，但仍要检查 Merge Engine、sequence、跨分区更新和 Overwrite 是否兼容。

## 6. 分区是最有效的并发边界

推荐让流作业写当前分区，批作业只 Overwrite 已封闭的历史分区。只要二者不修改同一分区和文件，冲突面会小得多。

`INSERT OVERWRITE` 的危险不在 SQL 名称，而在实际覆盖范围。静态分区 Overwrite 只替换明确分区；动态分区 Overwrite 可能根据输出数据决定要替换哪些分区。上线前要用执行计划和测试表确认范围，不要让批作业覆盖仍在流式写入的分区。

对同一分区并发执行流写、Overwrite、Bucket Rescale 或两个 Compactor，需要逐项验证支持关系。乐观并发控制可以拒绝冲突提交，但不能保证所有作业都自动变成无损串行执行。

## 7. 原子发布取决于存储和 Catalog

Paimon 的读者通过 Snapshot 获得一致视图，因此“发布新 Snapshot”必须是原子的：读者只能看到旧 Snapshot 或完整的新 Snapshot，不能看到写到一半的元数据。

Filesystem-managed Snapshot 默认先写临时文件再 Rename。HDFS 提供原子 Rename；S3 一类对象存储不能假设具有相同语义。某些 Paimon 文件系统实现使用原子条件创建，例如官方 OSS 实现。

如果所选存储实现无法在不覆盖现有 Snapshot 的情况下原子发布，需要使用所有 Writer 共享的锁。Hive 或 JDBC Catalog 可以配置：

```sql
'lock.enabled' = 'true'
```

具体还要配置相应锁类型、Metastore 或 JDBC 后端。锁配置必须在创建 Catalog 时统一，不是给单个 SQL Session 临时加一个互不相干的本地锁。

## 8. 所有 Writer 必须看到同一个表

多个 Writer 即使表名相同，只要 Warehouse、Catalog Backend、Metastore URI、认证身份或锁配置不同，就可能没有在协调同一份元数据。

上线前逐项比对：

- Catalog 类型与名称。
- Warehouse 的规范化路径。
- Hive Metastore/REST/JDBC Endpoint。
- 文件系统实现、Region、Endpoint 与认证。
- `lock.enabled`、锁类型和共享锁后端。
- Paimon 与引擎 Connector 版本。

不要让一个 Writer 通过 Hive Catalog 提交，另一个绕过它使用不兼容的 Filesystem Catalog 写同一目录。它们可能各自认为提交成功，却不共享相同的原子发布协议。

## 9. 多 Writer 设计选择

| 写入形态 | 推荐方式 | 主要风险 |
| --- | --- | --- |
| 多作业写不同分区 | 可并发，固定边界 | 分区范围配置错误 |
| 多作业写同一分区、固定桶 | `write-only=true` + 单独 Compactor | Snapshot 竞争、主键顺序 |
| 多作业写同一分区、动态桶 | 不允许 | 同一 Key 被分配到不同 Bucket |
| 流写当前分区 + 批 Overwrite 历史分区 | 推荐 | 动态 Overwrite 越界 |
| 多个 Compactor 处理同一分区 | 避免 | File Conflict 与反复重启 |
| 对象存储多 Writer | 统一 Catalog 与共享锁/原子实现 | Rename 非原子、锁不一致 |

## 10. 排查提交冲突

1. 从 Flink 日志区分 Snapshot ID 竞争和文件变更冲突。
2. 查看 `$snapshots`，确认相同时间有哪些 APPEND、OVERWRITE、COMPACT 提交。
3. 查看 Commit 指标中的 `lastCommitAttempts` 和 `lastCommitDuration`。
4. 列出所有活跃 Writer、Compactor、维护作业及其分区范围。
5. 核对这些作业使用的 Catalog、Warehouse 和锁配置。
6. 动态桶表确认同一分区是否被两个作业写入。
7. 反复重启时不要只增加 restart 次数；先移除持续制造冲突的并发关系。

## 11. 上线检查

- 为每个写作业写明允许修改的分区范围。
- 同分区多 Writer 优先使用固定桶，并验证 sequence/merge 语义。
- `write-only=true` 后明确唯一 Compactor 和 Snapshot 清理作业。
- 对象存储先验证原子提交与共享锁，不按 HDFS Rename 假设设计。
- Batch Overwrite、Rescale 和历史修复设置互斥窗口。
- 监控提交尝试次数、提交耗时、作业重启和 Compaction 积压。
- 故障演练至少包含两个 Writer 同时提交和 Compactor 冲突。

## 相关笔记

- [[Paimon 主键表建模与稳定性检查清单]]
- [[Paimon Snapshot、Manifest、Parquet 与 Compaction 的关系]]
- [[Paimon MOR、COW、MOW 与 Deletion Vector]]
- [[Paimon 系统表、监控指标与故障排查]]

## 官方资料

- [Concurrency Control](https://paimon.apache.org/docs/master/concepts/concurrency-control/)
- [Dedicated Compaction](https://paimon.apache.org/docs/master/maintenance/dedicated-compaction/)
- [Data Distribution](https://paimon.apache.org/docs/master/primary-key-table/data-distribution/)
- [Compaction](https://paimon.apache.org/docs/master/primary-key-table/compaction/)

> 核对日期：2026-09-16。原子提交和锁能力依赖实际 Catalog、文件系统实现及 Paimon 版本。

## 用户原文摘录（原样保留）

大白话版：

两个作业同时盯上一个文件，都想把它删掉。A 手快先删了，提交成功；B 慢了一步，提交时发现文件已经没了，系统直接报错，B 作业被迫重启。重启之后 B 重新看一眼最新状态，发现文件确实没了，就不再傻傻地删它了，一切恢复正常。

专业版：

Files Conflict 是 Paimon 乐观并发提交下的一种硬冲突，本质是“作业要删除的文件在最新快照中已经不存在”。提交方基于某个 base snapshot 构建变更计划，计划中包含对文件 file-x 的逻辑删除（DELETE）。但在真正提交合并 manifest 时，Paimon 发现 file-x 已经被其他作业（通常是 compaction 或分区过期）提前从最新快照中删除了。此时继续提交会产生悬空的 DELETE 条目，破坏 manifest 一致性，所以 FileStoreCommitImpl.assertNoDelete 检测到合并后条目为 DELETE 且没有对应的有效 ADD，于是抛出异常，强制 Flink 作业 failover 重启。

作业重启后从最近 checkpoint 恢复，重新读取最新 snapshot 并重新构建提交计划。由于此时 file-x 已不存在，新计划中不再包含对它的 DELETE，提交便成功了。

总结：

Paimon 固定桶主键表中，Sink 并发与 Bucket 数的关系取决于是否有分区：无分区表按 bucket % numChannels 路由，bucketId 上限固定为 511，当并发开到 800 时 subtask 512\~799 永远收不到数据，实际有效并发被限制在 bucket 数；有分区表则通过 (partition.hashCode() % numChannels + bucket) % numChannels 路由，依靠分区哈希把相同 bucketId 的不同分区打散到全部 800 个 subtask，因此不会出现结构性空闲，但会出现“同一 bucketId 被多个 writer 跨分区写”的现象。多个 Writer 写同一个 Bucket 不会冲突，因为每个 writer flush 产生带 UUID 的全新 Level-0 文件，commit 全部是 ADD 操作、不产生文件级 DELETE，而 Files Conflict 必须依赖 DELETE 已存在的文件才触发；同时 Level-0 天然允许 key range 重叠，LSM 重叠检测只针对 level≥1 文件，所以这种“多写”在源码层面是安全的。write-only=true 的作用是让 writer 不执行 Compaction，提交只包含新增文件，从根上消除写入作业与 Compaction 作业之间的 DELETE 竞争，彻底规避 Files Conflict；但它不解决小文件问题，反而会因为没有自动合并导致 Level-0 文件持续堆积，长期读放大越来越严重。

在此基础上，用三层结构可以更完整地解释整条逻辑链：

第一层，路由分发层：Paimon 通过 (startChannel + bucket) % numChannels 路由。无分区表受 bucketId 上限约束，并发超过 bucket 数时必然出现空闲 subtask，引擎会自动将并发降为 bucket 数；有分区表依靠 partition.hashCode() 打散，才能出现“同一 bucketId 被多 writer 写”，同时也是“并发超过 bucket 数仍能跑满”的唯一场景。

第二层，并发冲突层：多个 writer 写同一 bucket 看似危险，但 Paimon 的 Files Conflict 只有存在 DELETE 操作时才会触发。各 writer flush 产出带 UUID 的独立文件，提交均为 ADD；Level-0 本身允许 key range 重叠，因此这种“多写”不会引发数据冲突，正确性由机制保障，并非靠运气。

第三层，代价与取舍层：write-only=true 将 Compaction 从写入作业剥离，让上述两层安全逻辑更纯粹，但它本身不消除小文件。写入并发越高，同一 bucket 被拆分得越零散，Level-0 文件就越多；叠加没有自动 Compaction，读放大会随时间持续累积。

由此得到两条实践铁律：

1. 写入并发尽量 ≤ bucket 数，或把 bucket 数设为写入并发的整数倍，从源头降低同一 bucket 被多 subtask 拆分写入的概率；
2. 一旦开启 write-only=true，必须配套部署独立 Compaction 作业定期合并小文件，否则今天省下的计算资源，后续会以查询性能恶化的形式连本带利还回来。
