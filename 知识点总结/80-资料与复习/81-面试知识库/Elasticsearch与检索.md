##### Elasticsearch 与检索

##### Elasticsearch 架构与使用场景

Q：介绍一下 Elasticsearch，你是否参与过部署，什么场景适合使用？
A：Elasticsearch 是基于 Lucene 的分布式检索与分析引擎，索引由主分片和副本组成，文档按路由进入主分片并复制到副本，刷新后近实时可搜索。我会用它承载全文检索、多条件筛选和日志分析，不把它当强事务主库。部署时规划节点角色、分片、副本、JVM 堆和磁盘水位，Mapping 明确 keyword/text、日期与数值，写入使用 bulk 并压测批次。上线监控集群健康、未分配分片、查询/写入延迟、segment、GC 和磁盘，索引生命周期负责 rollover 与删除。

##### 知识点补充

过多小分片会消耗堆和文件句柄。refresh、flush、merge 含义不同，调大 refresh interval 可提高批量写吞吐但增加可见延迟。

##### Elasticsearch 作为二级索引

Q：Elasticsearch 是否需要再建“二级索引”？如何给 HBase 提供检索能力？
A：ES 自身的倒排索引和 doc values 已是检索结构，通常不再像 MySQL 那样手工建二级 B+Tree。所谓“用 ES 做 HBase 二级索引”，是把 HBase 业务主键和可查询字段同步到 ES：查询先在 ES 得到 RowKey，再批量回 HBase 取权威详情。同步用 CDC/Kafka 解耦，消息带版本并幂等 upsert；失败进入重试和对账，定期比较数量及抽样字段。ES 存在近实时延迟，不能承担余额、库存等强一致判断。

##### 知识点补充

Mapping 变更、历史回填和双写一致性是主要风险。只需精确 RowKey/前缀查询时直接使用 HBase，不必引入 ES。

##### NoSQL 数据库选型

Q：Redis、HBase、Elasticsearch 和 ClickHouse 分别适合什么场景？
A：Redis 用于内存级低延迟缓存、计数和短期状态；HBase 适合海量稀疏宽表按 RowKey 随机读写；Elasticsearch 适合全文及多字段检索；ClickHouse 适合列式 OLAP 聚合。选型时我先写清访问模式、规模、延迟、更新、查询维度、一致性和保留期，再做压测。例如画像详情按 user_id 查可放 HBase，热点标签放 Redis，文本搜索放 ES，群体统计放 ClickHouse。不能因为都叫 NoSQL 就互换，也避免同一数据无治理地复制多份。

##### 知识点补充

多存储副本必须声明权威源、同步方式、可接受延迟、校验与重建流程，否则性能收益会转化为一致性债务。
