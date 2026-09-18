
```
Doris 的数据是按照 tablet 为单元存储的。
Table -> Partition -> Tablet（Bucket）-> Replica

同一个 Partition 中，不同的 Tablet 可以在同一个 BE 中。
同一个 Tablet 的不同 Replica 不会运行在同一个 BE 中（副本会尽量分散到不同 BE，提高容灾与查询可用性）。

查询时，一个 Tablet 通常只有一个 Replica 会真正参与计算。
FE 会根据：
- BE 当前负载
- 网络情况
- 副本健康状态
- 本地性（local shuffle/local scan）

选择一个最优 Replica 参与 Scan，因此热点 Tablet 往往会演变为热点 BE。

如果单个 Bucket（Tablet）数据越来越大，会导致：
- 数据倾斜（Storage Skew）
- 查询热点（Hot Tablet）
- Compaction 压力巨大

例如：
- 某个 Tablet 数据量远超其他 Tablet
- 某些热点 Key 永远命中同一个 Tablet
- Compaction 长时间 backlog
- 导致导入、查询、BE CPU/IO 都出现瓶颈

Doris 的解决方式：只能部分缓解，无法根治。

1. Replica Balance（副本迁移）
- 不会对 Tablet 自动 Split
- 只会将 Replica 迁移到不同 BE
- 本质是“换机器存储”
- Tablet 本身大小不会变化

2. 查询层负载均衡（Scan Replica Balance）
- FE 会优先选择低负载 Replica
- 多次查询可能轮询不同 Replica
- 能缓解部分查询热点
- 但无法解决单个 Tablet 数据过大的问题
- Scan 范围本质上仍然是同一个 Tablet

3. 数据导入并发调度
- Routine Load / Stream Load 支持并发写多个 Tablet
- 可以提升整体导入吞吐
- 但如果 Hash Key 本身倾斜：
  - 数据仍然会集中写入热点 Tablet
  - 无法解决热点问题

Doris 最大的问题之一：Tablet 不会自动 Split。
（因此 Doris 非常依赖 Bucket / Hash Key / Partition 的前期设计）

与 HBase 不同：
- HBase Region 可以自动 Split
- Doris Tablet 不会自动水平拆分

因此 Doris 更偏向：
- 提前建模
- 提前规划数据分布
- 而不是运行期动态切分

生产经验：
- 单个 Tablet 建议控制在：
  1GB ~ 10GB

- Bucket 数量不能过少：
  - 容易产生超大 Tablet

- Bucket 数量也不能过多：
  - FE Metadata 压力大
  - Compaction 增多
  - 调度开销增大

Hash Key 设计非常重要：

避免低基数字段：
HASH(province)

更推荐高基数字段：
HASH(user_id)

对于超大热点用户：
HASH(user_id, rand_bucket)

或者：
user_id + salt

用于将热点用户打散到多个 Tablet。

动态分区（Dynamic Partition）也非常重要：
- 避免单个 Partition 无限增大
- 通常按天/小时分区：
  PARTITION BY RANGE(dt)

- 避免一个超大历史分区长期存在
```