---
tags:
  - hbase
  - cdc
  - lakehouse
status: active
---

# HBase、CDC 与湖仓数据链路

返回：[[README|企业微信知识地图]]

## 1. HBase RowKey 设计解决什么问题

HBase 数据按 RowKey 字典序分布在 Region 中。RowKey 同时决定唯一性、数据放在哪个 Region 以及 Scan 能否缩小范围，因此它不是普通主键。

设计流程：

1. 找到能唯一识别记录的业务字段。
2. 列出真实查询模式，决定哪些条件必须成为前缀才能支持范围 Scan。
3. 判断前缀是否单调或热点，决定是否加盐、hash 或反转时间。
4. 控制 RowKey 长度，因为 RowKey 会伴随 Cell 存储并参与索引，过长会放大存储和网络成本。

## 2. 高位散列、低位有序

典型 RowKey：

```text
hash_prefix(user_id) + user_id + reverse_timestamp
```

- 高位 hash 让不同用户分散到多个 Region，避免时间递增写入一直打到最后一个 Region。
- 保留 user_id 让查询可准确构造前缀。
- 反转时间 `Long.MAX_VALUE - timestamp` 让同一用户最新记录排在前面，查询最近记录时少扫描。

这是读写平衡方案，不是免费优化。查询一个用户时，调用方必须能计算相同 hash 前缀；如果用纯随机盐，读时不知道前缀，只能并行扫描所有盐桶，适合极端写多读少。

## 3. 预分区的意义

若表初始只有少量 Region，热点写入可能先集中到一个 Region，等自动 Split 后才逐步均衡。预分区根据 hash 前缀提前创建范围，启动即能并行写入多个 RegionServer。

分区数过多会增加 RegionServer 内存、WAL、Compaction 和调度成本；分区边界必须和 RowKey 前缀编码一致，否则预分区形同虚设。

## 4. RowKey 迁移流程

RowKey 不能原地更新，因为它决定物理位置。安全迁移通常是：

1. 设计新 RowKey 和新表，提前预分区。
2. 在切换前建立双写或可靠 CDC，记录迁移期间增量。
3. 为旧表创建 snapshot，固定全量基线。
4. 用 MapReduce/Spark 读取 snapshot，生成新 RowKey，并以 HFile Bulk Load 导入新表。
5. 校验行数、抽样、关键业务聚合和新旧查询结果。
6. 灰度切换读流量，监控错误和延迟。
7. 停止旧写入、追平最后增量，最终切换；保留回滚窗口后再下线旧表。

Snapshot 主要复制 HFile 引用而不是全量数据，因此创建快。HFile 不可变；被 snapshot 引用的文件在 Compaction 后需要进入 archive 保留，不能简单认为快照后必须完全关闭 Compaction。

## 5. 删除与 Tombstone

HBase 删除先写入 tombstone，真正空间回收通常在后续 major compaction。若删除后又写入更高 timestamp 的版本，新版本仍可见。迁移数据时必须保留版本与 tombstone 语义，否则可能让已删除数据“复活”。

## 6. DataX 按更新时间增量的局限

按 `lastmodified >= start AND lastmodified < end` 抽取容易实现，但它依赖：

- 每次业务更新都可靠修改 lastmodified。
- 时间精度足以区分同一窗口内多次更新。
- 删除有软删标记，物理删除无法被普通 SELECT 发现。
- 源库与调度时钟、时区一致。

为避免边界漏数，常用回看窗口重复抽取，并在下游按主键+版本幂等覆盖。若需要完整更新/删除顺序，CDC 比基于更新时间轮询更可靠。

## 7. MySQL Binlog 顺序

同一 MySQL 实例内，一般可按：

```text
binlog file number → position → row number
```

判断物理变更顺序。文件名应解析数字后比较，不能依赖不定长字符串字典序。一个 event 可能影响多行，row number 用于区分同一 position 内的行。

但 connector 是否把这些 metadata 暴露给 SQL 取决于 Flink CDC/Debezium 版本和 format。不能因为 Debezium 原始 envelope 有 `source.file/pos/row`，就假设当前平台的 Flink SQL 表一定能读到。

拿不到 binlog 位点时，毫秒/微秒 `lastmodified` 仍可能并列。最稳妥是业务提供单调版本号；否则使用事件时间+入仓时间等复合排序，并接受极端并发下最后写胜不完全可靠。

## 8. Binlog 配置与 CDC 数据形态

常见建议是 `binlog_format=ROW`。`binlog_row_image=FULL` 能让 update 事件包含更完整 before/after 行，方便下游重建当前状态；MINIMAL 只记录必要列，节省日志但可能使某些 CDC 处理缺少旧值。

是否能同步完整字段还取决于 connector 的 schema、反序列化 format 和上游权限。上线前应实际打印 create/update/delete 事件，而不是根据配置名推断。

## 9. 追加日志与更新表的存储选择

可以用问题而不是产品名选择：

- 事件是否只追加、按 offset 消费、保留期有限？Kafka 合适。
- 是否需要主键更新、回撤、历史快照和批流统一查询？Paimon/Iceberg/Hudi 等表格式更合适。
- 是否需要低延迟 OLAP 查询？同步到 Doris/StarRocks/ES 等服务引擎。

“事件流碰事件流都放 Kafka、事件碰维度都放 Paimon”是有用的启发，但不是硬规则。维度很小且在外部数据库可 Lookup，未必需要先进入湖仓；两个更新流也可进入主键表做版本合并。

## 10. Paimon 与多模态数据

多模态本体（图片、视频、音频）通常放对象存储/HDFS，表中保存 URI、标签、向量、质量分、版本和血缘。原因是大二进制直接进入行式/列式表会造成扫描、Compaction 和元数据成本，也不利于 CDN/对象存储生命周期管理。

表与本体可能都位于 S3，但逻辑职责不同：表格式管理结构化元数据、快照和事务；对象 URI 指向大文件。必须治理引用完整性、对象删除和表记录生命周期，避免悬空 URI 或误删仍被引用的对象。

## 11. Lakehouse 链路示例

```text
MySQL CDC / Kafka Event
→ Flink 清洗、去重、标准化
→ Paimon 主键表保存可更新明细和历史快照
→ Doris 保存面向分析的服务表/MV
→ BI、API、Agent 查询
```

各层责任：Flink 负责实时计算与状态；Paimon 负责可追溯存储和批流一致；Doris 负责低延迟查询。不要让 Doris 成为唯一原始事实，也不要让 Kafka 无限承担可查询历史仓库角色。

## 12. 数据生命周期与小文件

双生命周期策略示例：

- 年龄超过 B：删除。
- 年龄在 A 与 B 之间：只保留周日或月末快照。
- 年龄小于 A：完整保留。

配置表应包含表名、A/B、周末/月末保留策略、每日删除上限和 dry-run。删除前同时验证 Metastore 分区和 HDFS 路径时间，先输出待执行 SQL 并限制批量，避免配置错误大范围删除。

## 13. 同步与补数的事实边界

当后端接口只能返回当前最新状态时，可以补齐“现在是什么”，不能恢复“历史上第一次是什么”。例如审核单首次提审时间没有日志、CDC 或历史表，就无法由最新状态准确反推。

补数方案应明确三类结论：

- 可恢复：当前快照足以重建的字段。
- 可近似：可用最后修改时间或其他代理字段，但有偏差。
- 不可恢复：历史状态已经被覆盖且没有任何留痕。

数据工程不能通过复杂 SQL 创造源系统从未保存的信息。

