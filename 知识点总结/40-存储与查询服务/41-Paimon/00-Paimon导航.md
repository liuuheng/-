# Paimon 导航

按问题查机制、配置和排查方法时，从“机制与使用场景”进入；需要阅读来源文章的推演、图表、代码和补充批注时，进入“文章批注”。概念主笔记用于按问题查阅，来源批注保留原文章节与细节，两类笔记通过关联阅读连接。

各篇采用的 Paimon、Flink 版本和复核日期不同。参数与结论按对应笔记的版本说明使用；遇到结论差异时，结合实际部署版本和各篇来源复核，不能仅按编号或篇幅判断哪篇适用。

返回 [[知识点总结/00-知识库导航|知识库导航]]。

## 机制与使用场景

- [[知识点总结/40-存储与查询服务/41-Paimon/01-Apache Paimon Flink SQL 建表：Catalog、主键、分区与 Bucket|建表与参数选择]]
- [[知识点总结/40-存储与查询服务/41-Paimon/02-Apache Paimon 表模型、存储组织与读取语义|表模型与读取语义]]
- [[知识点总结/40-存储与查询服务/41-Paimon/05-Paimon CDC 入湖、Schema Evolution 与恢复语义|CDC 入湖与恢复语义]]
- [[知识点总结/40-存储与查询服务/41-Paimon/06-Paimon File Index：存储、读写与选型|File Index 机制与选型]]
- [[知识点总结/40-存储与查询服务/41-Paimon/07-Paimon L0、L1、Snapshot 与查询裁剪|L0 L1 与查询裁剪]]
- [[知识点总结/40-存储与查询服务/41-Paimon/08-Paimon MOR、COW、MOW 与 Deletion Vector|MOR COW MOW 与 Deletion Vector]]
- [[知识点总结/40-存储与查询服务/41-Paimon/09-Paimon Partition 与 Snapshot：写入、读取和过期机制（文章注释版）|Partition 与 Snapshot 关系]]
- [[知识点总结/40-存储与查询服务/41-Paimon/10-Paimon Sequence、RowKind 与删除语义|Sequence RowKind 与删除语义]]
- [[知识点总结/40-存储与查询服务/41-Paimon/11-Paimon Snapshot 历史分区查询|Snapshot 历史分区查询]]
- [[知识点总结/40-存储与查询服务/41-Paimon/12-Paimon Snapshot、Manifest、Parquet 与 Compaction 的关系|Snapshot Manifest 与数据文件]]
- [[知识点总结/40-存储与查询服务/41-Paimon/13-Paimon 主键表建模与稳定性检查清单|主键表建模检查]]
- [[知识点总结/40-存储与查询服务/41-Paimon/14-Paimon 动态 Bucket 的索引机制与单 Writer 限制|动态 Bucket 索引与写入限制]]
- [[知识点总结/40-存储与查询服务/41-Paimon/15-Paimon 多 Writer、Catalog 与原子提交|多 Writer 与原子提交]]
- [[知识点总结/40-存储与查询服务/41-Paimon/17-Paimon 流式读写中的 Snapshot 与 Changelog|流式 Snapshot 与 Changelog]]
- [[知识点总结/40-存储与查询服务/41-Paimon/18-Paimon 系统表、监控指标与故障排查|系统表与故障排查]]
- [[知识点总结/40-存储与查询服务/41-Paimon/19-Kafka 与 Paimon 的流数据存储选型|Kafka 与 Paimon 选型]]
- [[知识点总结/40-存储与查询服务/41-Paimon/Paimon 实时与离线共用维表设计：Tag、Lookup 与历史重跑|实时离线共用维表与历史重跑]]

## 文章批注

以下按来源组织文章批注，精讲系列按原文顺序排列，Lookup 两篇和架构全景单列。

- [[知识点总结/40-存储与查询服务/41-Paimon/16-Paimon 架构全景：写入、快照、索引与变更语义（文章批注版）|架构全景批注]]
- [[知识点总结/40-存储与查询服务/41-Paimon/03-Flink 实时维表性能优化（上）：用对 Paimon Lookup 的完整实践（文章批注版）|Lookup 文章批注 上篇]]
- [[知识点总结/40-存储与查询服务/41-Paimon/04-Flink 实时维表性能优化（下）：用对 Paimon Lookup 的完整实践（文章批注版）|Lookup 文章批注 下篇]]
- [[知识点总结/40-存储与查询服务/41-Paimon/Paimon 精讲（一）：架构、写入、读取与核心元数据（文章批注版）|精讲一 架构与元数据]]
- [[知识点总结/40-存储与查询服务/41-Paimon/Paimon 精讲（二）：Flink SQL 写入与 Changelog 生成机制（文章批注版）|精讲二 写入与 Changelog]]
- [[知识点总结/40-存储与查询服务/41-Paimon/Paimon 精讲（三）：Partition、Snapshot 与数据生命周期（文章批注版）|精讲三 Partition 与 Snapshot]]
- [[知识点总结/40-存储与查询服务/41-Paimon/Paimon 精讲（四）：Fixed、Dynamic 与 Unaware Bucket（文章批注版）|精讲四 Bucket 模式]]
- [[知识点总结/40-存储与查询服务/41-Paimon/Paimon 精讲（五）：Deletion Vector 与 Merge On Write（文章批注版）|精讲五 Deletion Vector]]
- [[知识点总结/40-存储与查询服务/41-Paimon/Paimon 精讲（六）：File Index 的存储与读写链路（文章批注版）|精讲六 File Index]]
- [[知识点总结/40-存储与查询服务/41-Paimon/Paimon 精讲（七）：动态 Bucket 的单写限制与索引一致性（文章批注版）|精讲七 动态 Bucket 写入限制]]
- [[知识点总结/40-存储与查询服务/41-Paimon/Paimon 精讲（八）：查询裁剪、统计信息与 Files Table（文章批注版）|精讲八 查询裁剪与 Files Table]]
- [[知识点总结/40-存储与查询服务/41-Paimon/Paimon 精讲（九）：Append 表的 Bucket Key 与文件索引（文章批注版）|精讲九 Append 表索引]]
- [[知识点总结/40-存储与查询服务/41-Paimon/Paimon 精讲（十）：Flink Sink 并发、Bucket 路由与写入冲突（文章批注版）|精讲十 Sink 并发与写入冲突]]

## 实验

- [[知识点总结/40-存储与查询服务/41-Paimon/实验/01-Paimon 本地 SQL 验证实验手册|本地 SQL 验证实验手册]]
