##### HBase

##### HBase 在实时数仓中的用途

Q：HBase 在项目中用在哪里，为什么适合存实时维表？
A：我通常把 HBase 用在需要按业务键低延迟随机读取、数据量较大且持续更新的维表或明细查询场景。Flink 处理事实流时根据维度 key 异步查询 HBase，热点数据再用 Redis 或本地缓存降低 QPS。选择 HBase 是因为它基于 LSM 思路写入吞吐高、可水平扩展、按 RowKey 查询稳定；但它不适合复杂 Join 和任意条件扫描，所以表设计必须围绕已知访问路径。

##### 知识点补充

HBase 数据先写 WAL 和 MemStore，再 flush 为 HFile，后台 compaction 合并文件。实时维表还要设计 CDC 更新、缓存失效、版本列、空值缓存和查询超时降级。

##### RowKey 设计原则

Q：HBase RowKey 怎么设计，如何避免热点？
A：RowKey 必须从真实查询路径反推，因为 HBase 最擅长完整 key 和前缀范围扫描。比如订单维表主要按 `userId + orderId` 查，我会设计成 `salt_userId_reverseTime_orderId`：用固定桶数的 hash 前缀把写入打散，userId 保证同一用户可查，反转时间让最近订单靠前，并在建表时按 salt 预分区。上线前用日增量和保留期估算单 Region 大小，避免单调时间戳或自增 ID 放最前导致所有新写进入最后一个 Region。代价也会说明：加盐后查某用户可能需要并发扫多个桶，因此桶数不能盲目增大；通过 RegionServer 请求量、写入延迟、Region 大小和 compaction 队列验证是否仍有热点。

##### 知识点补充

RowKey 按字典序排序并存储在每个 Cell 中，过长会增加存储和网络开销。预分区边界必须与 RowKey 分布一致；热点治理还要结合 RegionServer 请求量、Region 大小和 compaction 观察。

##### HBase 架构与读写流程

Q：讲一下 HBase 架构，以及一次读写请求经过哪些组件？
A：HMaster 负责表和 Region 的管理、负载均衡与故障协调，RegionServer 真正承载 Region 读写，ZooKeeper 参与协调和服务发现，数据文件最终在 HDFS。写入先定位 RegionServer，追加 WAL 后写入 MemStore，达到阈值 flush 成 HFile，后台 compaction 合并文件；因此确认写入通常不需要等待 HFile 落盘。读取时客户端根据 meta 缓存定位 Region，RegionServer 依次利用 BlockCache、MemStore 和 HFile 索引/BloomFilter 查找并合并多版本结果。Region 过大后 split，RegionServer 故障时 WAL 拆分和 Region 重新分配完成恢复。回答时我会把“控制面”和“数据面”分开，避免误说所有读写都经过 HMaster。

##### 知识点补充

HBase 的一致性、WAL 与 HDFS 副本是不同层次。列族是物理存储和 flush/compaction 的重要边界，数量不宜随意增加。

##### HBase 性能优化与热点治理

Q：HBase 读写性能下降时如何定位和优化？
A：我先看是单 Region 热点还是全局问题：检查每个 RegionServer 的请求数、P95 延迟、handler queue、GC、MemStore、BlockCache 命中率、compaction queue 和 HDFS 延迟。写热点通常从 RowKey 开始处理，用 salt/hash 前缀打散递增 key，同时预分区，避免所有新数据压到最后一个 Region；批量 put、合理 write buffer 和异步客户端可提高吞吐。读慢则按访问模式设计 RowKey，限制 Scan 的 start/stop row、列族、列和版本数，避免全表 scan；热点点查要提高有效 BlockCache。若 compaction 长期积压，要降低小 HFile 产生速度并评估磁盘吞吐，不能只无限加 handler。

##### 知识点补充

RowKey 打散会增加范围查询的多路合并成本，因此需要在写均衡和读取局部性之间取舍。TTL、版本数和压缩编码也直接影响存储与读放大。

##### Region 预分区、Split 与 Merge

Q：HBase 如何预分区，Region 什么时候切分或合并？
A：建表时可根据预计 RowKey 分布给出 split keys，让初始 Region 分散到多台 RegionServer，避免新表所有写入先压在一个 Region；边界必须与 salt/hash 前缀一致，不能机械等分字符串。Region 达到切分策略阈值后由 RegionServer 生成两个 daughter Region，引用或重组原 HFile，随后完成元数据更新和 compaction；切分会增加 Region 数和管理成本。小 Region 合并通常需要运维判断相邻 Region 数据量和访问热度，可通过 merge 工具处理，不能在高峰盲目自动合并。全过程监控 Region 大小、请求量、热点、compaction queue 和迁移频率。

##### 知识点补充

预分区解决初始并行度，不会自动修复错误 RowKey。Region 过多会增加 HMaster、RegionServer 内存和 compaction 压力。

##### HBase 冷热分层与维表查询边界

Q：HBase 冷热数据如何管理？实时维表为什么用 HBase 而不全放 Redis？
A：热数据需要低延迟高频访问，可保留在 BlockCache、SSD 或 Redis；冷数据通过 TTL、版本数控制，归档到 HDFS/湖表供离线分析。维表量大、需要按主键持久化并承担亿级容量时，HBase 比全内存 Redis 成本更可控，Flink 用异步点查加本地缓存可以达到可接受延迟；Redis 适合更小、更热且对亚毫秒延迟敏感的数据。若业务需要任意属性组合过滤或全文检索，HBase RowKey 点查并不合适，应考虑 OLAP/搜索引擎。选型会量化维表行数、单行字节、QPS、更新频率、P95 和容灾成本。

##### 知识点补充

冷热分层不能只靠时间，还要结合访问频率和合规保留期。Flink 缓存维表需处理更新失效、负缓存和一致性边界。

##### 实时链路数据缺失排查

Q：Flume→Kafka→Flink→HBase 链路在某个时间后查不到数据，如何定位？
A：我以最后一条正常数据的时间和业务主键为线索逐段核对。先查源日志是否继续产生、Flume channel 是否堆积或 sink 失败；再看 Kafka 对应分区最新 offset、消费组 lag 和消息样本；Flink 看 source records、反压、checkpoint、重启和脏数据侧路；最后看 HBase RegionServer 写请求、异常、RowKey、时间范围、TTL 和查询条件。每一段都记录“输入数、输出数、失败数”，能快速确定断点。若只是事件时间停在 9:30，还要排除 Watermark 被空闲分区卡住；若 HBase 有写但查不到，检查 salt 后 RowKey 和时间戳版本。止血后从 Kafka 保存位点回放，sink 按业务主键幂等。

##### 知识点补充

端到端排障不能只重启 Flink。统一 trace id、业务主键和分层计数，才能证明数据在哪一段丢失或延迟。

##### HBase 一级索引、二级索引与 Region 缓存

Q：HBase 的索引怎么工作，什么时候需要二级索引？Region Split 后客户端缓存如何保持正确？
A：HBase 的一级访问路径就是 RowKey：客户端先从本地缓存找 Region 位置，未命中再查 `hbase:meta`，到 RegionServer 后利用 HFile 多级索引、Bloom Filter 和 block index 缩小读取范围。若查询总是按 user_id，就把它设计进 RowKey；需要按手机号、证件号等另一属性查时，可维护“属性值→主 RowKey”的独立索引表，或使用 Phoenix/搜索引擎，但要承担双写一致性、回补和查询二跳。Region Split 后旧 Region 返回 region moved/not serving 等定位错误，客户端清除缓存并重新查 meta 获取 daughter Region，不需要业务手工刷新每个节点。

##### 知识点补充

二级索引不是免费能力，写放大和一致性成本很高。索引表写入应与主表采用幂等事件和对账修复，不能假设跨表原子事务。

##### HBase 与 Redis 缓存一致性

Q：HBase 保存完整维度、Redis 保存热点缓存时，如何保证两者一致？
A：我把 HBase 作为权威数据源，更新先写 HBase 成功，再删除 Redis 缓存；读取 miss 时回源并带版本写缓存。为缩小“写完 HBase、删缓存失败”的窗口，更新事件通过 binlog/消息队列异步重试删除，删除操作幂等；高一致要求可使用版本号，缓存写入只接受不旧于当前版本的数据。热点 key 重建加互斥和随机 TTL，避免击穿。定期对账 HBase 与 Redis 的版本/哈希，发现差异主动失效。不能做“同时双写并希望永远成功”，因为没有跨系统事务，必须有失败记录和补偿路径。

##### 知识点补充

延迟双删只能降低特定并发窗口，不是严格一致性证明。缓存允许的陈旧时间应由业务 SLA 定义，高风险读可绕过缓存。

##### RowKey 的字节序排序

Q：HBase RowKey 底层如何排序，字符串和数字设计有什么坑？
A：HBase 在 Region 和 StoreFile 中按 RowKey 的字节字典序升序排列，而不是业务数字大小，字符串 `10` 会排在 `2` 前。因此需要固定宽度补零、使用有序二进制编码或设计复合 RowKey。范围扫描要求查询前缀靠前，但递增时间直接放前面会形成尾部热点；我会加盐/hash 前缀，时间按倒序编码支持最近优先，再通过多路扫描合并。设计后用真实 key 分布验证 Region 均衡，不能只看格式。

##### 知识点补充

数值转字节要考虑符号位和字节序。RowKey 不能原地修改，业务主键变化相当于删旧行、写新行。

##### HBase 与 Redis 的维度存储选择

Q：维度数据为什么放 HBase 而不是全部放 Redis？如何划分冷热？
A：全量历史维度规模大、需要持久随机读写时我以 HBase 为权威存储，Redis 只缓存高频且容量可控的当前版本。冷热依据访问频次、最近访问、业务等级和对象大小，用监控统计热点与命中率，设置 TTL 和容量，miss 时受控回源。维度更新先写权威源，再删除或按版本刷新缓存；批量重建在新 key 空间校验后切换。若全量很小且强低延迟，也可用 Redis，但必须有持久化和可重建源。

##### 知识点补充

容量估算要包括 key/value、对象开销、复制和碎片率，不能只算原字段字节。冷热策略需要持续重评。

##### WAL、MemStore 刷写与 Compaction

Q：HBase 写入后什么时候刷盘？WAL 是每个 Region 一个吗，Minor/Major Compaction 有什么区别？
A：写请求到 RegionServer 后先追加 WAL，再写对应 Region/列族的 MemStore，达到内存阈值、周期、WAL 压力或人工触发等条件时 flush 成新的 HFile。WAL 通常由 RegionServer 上多个 Region 共享，不是每个 Region 一份；故障后按 WAL 记录恢复未刷写数据。Minor Compaction 合并部分小 HFile，减少读放大；Major Compaction 重写该 Store 的全部 HFile，并真正清理过期版本和删除标记，IO 很重。生产会控制列族数量、写入批次和 compaction 并发，监控 MemStore、WAL、StoreFile 数及 compaction queue，避免高峰手工 major compaction。

##### 知识点补充

每个列族有独立 Store/MemStore/HFile，列族过多会放大 flush 和 compaction。删除在合并前通常只是墓碑标记。

##### HBase 列族设计

Q：一张 HBase 表能有多个列族吗？为什么通常不建议很多列族？
A：可以有多个列族，但我只在字段的访问模式、TTL、版本数或压缩策略确实不同且需要独立管理时拆分。每个列族在每个 Region 中都有独立 MemStore 和 StoreFile，flush、compaction、缓存及文件数都会被放大；某个列族触发 flush 也可能带来其他列族的小文件。大多数业务一到三个列族足够，强相关且总是一起读取的字段放同一族。建表前我用真实读写路径验证，而不是按关系库“字段分类”机械拆族。

##### 知识点补充

列可动态增加，列族属于较重的物理设计。TTL、VERSIONS、BLOOMFILTER 等配置主要在列族级生效。
