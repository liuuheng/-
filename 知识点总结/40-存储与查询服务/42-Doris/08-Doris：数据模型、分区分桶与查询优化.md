---
tags:
  - doris
  - olap
  - table-design
status: active
---

# Doris：数据模型、分区分桶与查询优化

返回：[[README|企业微信知识地图]]

## 1. Doris 与 Spark 的优化入口不同

Spark 作业优化通常围绕一次计算的扫描、Shuffle、Join、分区和内存；Doris 是长期在线 OLAP 系统，查询性能高度依赖建表时的数据组织。主要杠杆是：

```text
Partition：裁剪扫描范围与管理生命周期
Bucket/Tablet：控制数据分布和扫描并行
Key Model：定义重复 key 的存储/更新语义
Materialized View：预计算常用查询
Bitmap/Bloom/Inverted Index：降低过滤成本
Colocation：让大表 Join 本地化，减少网络 Shuffle
```

因此遇到慢查询时，不应只仿照 Spark 增加并行度，应先检查表模型、分区裁剪、分桶分布、索引命中和 Profile。

## 2. 三种 Key Model

### 2.1 Duplicate Key

保留所有原始记录，Key 主要是排序键，不保证唯一，也不会按 key 自动聚合。适合日志、行为明细和 append-only 数据。

为什么排序键重要？Doris 列式存储仍会按 key 前缀组织数据，查询中高频的前缀过滤可减少数据块扫描。但把 `user_id` 写成 Duplicate Key 不会自动去重。

### 2.2 Unique Key

相同 Key 保留最新版本，适合订单状态、CDC、维表和幂等更新。当前版本通常推荐 Merge-on-Write：导入阶段解决重复，查询直接读取最终结果。

普通整行 UPSERT 缺少的列可能被默认值或 NULL 覆盖；真正部分列更新需要 MOW 并显式使用部分更新能力。不能仅因为 SQL 只写了三列，就假设其他列自动保留。

### 2.3 Aggregate Key

相同 Key 的 Value 列按 SUM、MAX、MIN、REPLACE 等预定义函数在存储层聚合，适合固定口径汇总。

风险在于信息不可逆。例如按用户日粒度 SUM 了金额后，还能向上汇总金额，但若只保存日均价，就不能正确计算月均价，除非同时保存金额与数量。固定聚合模型不适合需要任意明细下钻的场景。

> Doris 的 Key 不能一概等于关系数据库主键。Duplicate Key 中只是排序列；Unique/Aggregate 才具有覆盖或聚合语义。

## 3. Merge-on-Write 与 Merge-on-Read

- MOW 在写入时合并同 key 数据，写入成本更高，查询快，支持更完整的部分更新能力，是大多数读多场景的选择。
- MOR 允许写入更轻，在查询或 compaction 时解决重复，读开销更高，适合极端写多读少且能接受查询成本的场景。

实现方式通常在建表时确定，不能把它当成随时切换的查询参数。

## 4. Partition、Bucket、Tablet、Replica

```text
Table
└── Partition
    └── Tablet（通常对应一个 Bucket）
        └── Replica
```

Partition 常按日期 RANGE/LIST 划分，用于裁剪和生命周期管理。每个 Partition 再按 hash key 分桶成多个 Tablet，决定数据如何分散到 BE 以及并行扫描单位。

查询一个 Tablet 时通常选择其中一个健康 Replica 执行 Scan；副本用于容灾和负载均衡，不表示一次查询会把所有副本都重复计算。

## 5. 为什么 Tablet 热点难以自动修复

HBase Region 可自动 Split，但 Doris Tablet 通常不会因为变大自动拆分。副本均衡只能把整个 Replica 迁到其他 BE，不能把一个超大 Tablet 水平切成两个。因此分桶键和 bucket 数的前期设计非常重要。

热点表现包括：单 Tablet 远大于其他 Tablet、某 BE CPU/IO 高、Compaction backlog、导入和查询同时变慢。若 hash key 本身倾斜，增加副本只能分摊读请求，写入主副本和单 Tablet 大小问题仍存在。

## 6. 分桶键与分桶数

分桶键优先选择高基数、分布均匀、常用于等值 join/filter 的字段。低基数地区字段会让大量数据集中到少数 bucket。组合 key 可以改善分布，但也会改变 colocate join 条件。

“BUCKETS≈BE×2~4”只能作为非常粗的起点，不是公式。还要考虑：

- 每个分区数据量与增长速度。
- 期望单 Tablet 大小。
- 查询并行度与并发。
- BE 数量、磁盘和副本数。
- Tablet 总数对 FE 元数据和 Compaction 的压力。

旧分区 bucket 数通常不能直接原地修改；新分区可以采用新的 bucket 数，但长期混用会影响分布一致性和 Colocation，应设计迁移方案。

## 7. Colocation Group

Colocation Group 约束多张表使用兼容的 hash 分布，使相同 key 的 bucket replica 位于相同 BE，Join 可在本地完成而无需 Exchange。

通常要求分桶键类型/顺序、bucket 数、副本分布等满足 group schema。验证不能只看 DDL，要看：

```sql
DESC SELECT *
FROM fact f
JOIN dim d ON f.user_id = d.user_id;
```

计划中 Hash Join 显示 `colocate: true` 才说明生效；`group is not stable` 和 EXCHANGE 表示数据尚未稳定对齐。

不适用场景：

- 一边非常小，Broadcast 更简单。
- join key 严重倾斜。
- 表经常改变分布或副本配置。
- 多张表无法长期维持一致 bucket schema。

重新绑定 group 不会瞬间重写现有数据，可能需要等待 balance/repair；生产操作前必须核对当前 Doris 版本命令。

## 8. Compute Group 与 Colocation Group

Compute Group 用于存算分离架构下的计算资源隔离，解决不同工作负载争抢 CPU/内存；Colocation Group 约束数据位置，解决 Join 网络传输。两个概念都简称 group，但一个管理计算资源，一个管理物理分布，不能混用。

## 9. Compaction

Doris 每次导入会生成 Rowset，内部包含 Segment。大量小批导入使版本和小 Segment 增多，后台 Compaction 将多个 Rowset 合并，减少查询需要读取的文件和版本。

```text
Tablet
└── Rowset（一次或一段版本）
    └── Segment（列式文件）
```

Compaction backlog 常由高频小批、热点 Tablet、磁盘 I/O 不足或写入速度超过合并能力导致。单纯提高导入并发可能让 backlog 更严重。应调整批次大小、分桶分布、Compaction 资源和写入节奏。

## 10. 分区与副本变更

动态分区适合按日/小时自动创建和回收。副本数不能超过可用 BE 数量；增加副本会触发大量网络和磁盘复制，大表应分批执行并观察 balance 状态。

示例语法随版本变化：

```sql
ALTER TABLE t_order
MODIFY PARTITION p20260714
SET ("replication_num" = "3");
```

新分区采用不同 bucket 数虽可能被支持，但会影响 Colocation 和查询并行稳定性，不能为了临时扩容随意修改。

## 11. NULL 与默认值

Nullable 列需要维护 NULL bitmap，向量化过滤时多一次 bitmap 读取和结果合并，可能增加内存访问与分支。它仍然是向量化执行，不能夸大为“有 NULL 就无法 SIMD”。

是否使用默认值首先是语义问题：未知、未发生、0 和空字符串含义不同。为了少量性能收益把未知金额写成 0，会污染统计。只有业务上确实存在自然默认值时才优先 NOT NULL。

## 12. 索引怎么选

- 前缀/排序组织适合高频 key 前缀过滤。
- Bloom Filter 适合高基数字段等值过滤，用少量误判换取跳过数据块；不适合返回大量命中的低基数字段。
- Bitmap 适合去重计数和某些低/中基数集合运算，具体以 Doris 索引类型和函数支持为准。
- Inverted Index 适合全文、字符串或多条件过滤。
- Materialized View 适合重复出现且可增量维护的聚合/Join 模式。

索引会增加写入、存储和维护成本，不应给每列都建。应从慢查询 Profile 中确认扫描瓶颈和选择性。

## 13. Arrow Flight SQL

Arrow Flight SQL 主要优化查询结果从 Doris 到客户端的列式传输和解析，减少 JDBC 行式转换与拷贝。总耗时可以拆为：

```text
扫描/Join/聚合执行耗时 + 结果序列化/网络传输/客户端解析耗时
```

Flight SQL 主要优化后半段。若查询本身扫描数 TB、Join 倾斜，换协议不会让计算阶段神奇变快。ADBC 偏 Arrow 列式生态，JDBC 通用性和工具兼容性更广。

## 14. FE 元数据

FE 管理数据库、表 schema、partition/tablet/replica、作业、用户权限和节点信息，并通过日志/快照持久化和高可用复制。不能只写“所有元数据都在 FE 内存中”而忽略持久化，否则会误以为 FE 重启后元数据消失。

Tablet 数量过多会增加 FE 内存、调度和元数据日志压力，这也是 bucket 不能无限增多的原因。

## 15. StarRocks/Doris UDF Jar 的工程教训

原记录中的 StarRocks UDF 问题：多个 UDF 共用同一 Jar，更新其中一个并覆盖远程 Jar 后，只重建 UDF-A；UDF-B 的 FE 元数据仍保存旧 checksum，新 CN 拉取新文件时校验失败。

根因是“可变 URL 指向了不同内容”，破坏了内容寻址假设。处理原则：

- 每个发布版本使用不可变文件名或版本路径。
- 多个 UDF 尽量独立打包，减少无关变更。
- 覆盖 Jar 后重建所有引用该 Jar 的函数。
- 把 checksum、函数定义与发布流程纳入版本管理。

## 参考依据

- [Doris 数据模型概览](https://doris.apache.org/docs/dev/table-design/data-model/overview)
- [Doris Unique Key](https://doris.apache.org/docs/dev/table-design/data-model/unique/)
- [Doris 分区与分桶基础](https://doris.apache.org/docs/4.x/table-design/data-partitioning/basic-concepts/)

