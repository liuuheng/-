
```
Doris 新分区理论上可以使用不同 hash key，但会破坏统一数据分布模型，影响 join locality 和查询稳定性，生产中通常不推荐在同一表内混用不同 hash key，而是通过建新表或数据分层解决。

Doris 建议在新分区中变动bucket的数量，对于旧分区不做改动：
ALTER TABLE t_order
ADD PARTITION p20260515 VALUES LESS THAN ('2025-05-16')
DISTRIBUTED BY HASH(user_id) BUCKETS 64;

标准写法：
ALTER TABLE t_order
ADD PARTITION p20260516 VALUES LESS THAN ('2025-05-17')
PROPERTIES (
    "replication_num" = "3"
)
DISTRIBUTED BY HASH(user_id) BUCKETS 128;

Doris 可以通过 ALTER TABLE / ALTER PARTITION SET ("replication_num"=N) 来动态调整副本数，支持全表或分区级别修改，但会触发后台数据复制任务，需要谨慎在大表上操作。
修改 某一个分区的副本数：副本数不能超过 BE 数量、增加副本是“重 IO 操作”
ALTER TABLE table_name
MODIFY PARTITION p20260514
SET ("replication_num" = "3");
```