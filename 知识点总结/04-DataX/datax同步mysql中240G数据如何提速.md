目前同步后端表240G大约需要11小时，下面是一些优化方式
DataX 提高速率，核心就是一句话：

> **让它并发读、批量写，同时别把源库和目标库打挂。**

常见优化方式如下。

**1. 提高 channel 并发数**

`channel` 是 DataX 最直接的提速参数。

```json
"setting": {
  "speed": {
    "channel": 8
  }
}
```

一般可以从：

```text
2 -> 4 -> 8 -> 16
```

逐步压测。

但注意：  
`channel` 越高，对源库查询压力、目标端写入压力、网络压力越大。

**2. 配置 splitPk**

如果是 MySQL 大表，同步时一定优先看有没有主键或递增字段。

```json
"reader": {
  "name": "mysqlreader",
  "parameter": {
    "splitPk": "id"
  }
}
```

DataX 会按 `id` 范围切分任务，并发读取。

没有 `splitPk` 时，很多情况下只能单通道或低效读取，大表会很慢。

要求：

```text
splitPk 字段最好是主键、唯一索引、数字类型、自增或分布较均匀
```

**3. 增大 writer 批量写入**

如果目标是 MySQL、Doris、ClickHouse 等，要调大批量写入参数。

例如 MySQL writer：

```json
"writer": {
  "name": "mysqlwriter",
  "parameter": {
    "batchSize": 5000
  }
}
```

可以根据情况测试：

```text
1000 -> 3000 -> 5000 -> 10000
```

批量越大，提交次数越少，但单批失败回滚成本也更高。

**4. 去掉不必要的限速**

DataX 配置里如果有这些参数，可能限制了速度：

```json
"speed": {
  "byte": 10485760,
  "record": 10000
}
```

如果想跑满，可以不配 `byte` / `record`，只配 `channel`。

不过生产环境建议适度限速，避免影响线上库。

**5. 只同步需要的字段和数据**

不要 `select *`，只取数仓需要的字段：

```json
"column": [
  "id",
  "user_id",
  "amount",
  "status",
  "create_time",
  "update_time"
]
```

如果是分区同步，可以加 `where`：

```json
"where": "update_time >= '2026-05-23 00:00:00' and update_time < '2026-05-24 00:00:00'"
```

数据量少了，速度自然提升。

**6. 避免复杂 SQL**

尽量不要在 DataX reader 里写复杂 SQL，比如：

```sql
join
group by
order by
函数计算
子查询
```

DataX 最适合做简单搬运：

```sql
select columns from table where ...
```

复杂清洗建议放到数仓里做。

**7. 从从库或备库读**

如果直接读线上主库，不敢开太高并发。

如果从只读从库同步，可以适当提高：

```text
channel
batchSize
byte speed
```

这样既能提速，也降低对线上业务的影响。

**8. 优化源表索引**

如果使用：

```json
"splitPk": "id"
```

`id` 必须有索引。

如果使用：

```json
"where": "update_time >= ... "
```

`update_time` 最好有索引。

否则 DataX 每次都可能全表扫。

**9. 优化目标端写入**

不同目标端重点不一样：

| 目标端 | 优化点 |
|---|---|
| HDFS/Hive | 控制文件大小，避免小文件，开启压缩 |
| MySQL | batchSize、事务提交、索引数量 |
| Doris/StarRocks | stream load batch、并发、tablet 分布 |
| ClickHouse | batch 写入、分区设计、减少小批量 |
| Kafka | batch、linger、压缩、分区数 |
| HBase | batch put、预分区、rowkey 设计 |

很多时候慢不是 reader，而是 writer 写入慢。

**10. 拆分大表任务**

240G 这种大表，可以按时间或 ID 范围拆：

```text
任务1：id 1 - 5000万
任务2：id 5000万 - 1亿
任务3：id 1亿 - 1.5亿
```

或者：

```text
2026-01
2026-02
2026-03
```

好处是：

```text
失败重跑成本低
并发可控
便于定位慢分片
```

简单优先级建议：

```text
第一优先：splitPk
第二优先：channel
第三优先：writer batchSize
第四优先：去掉限速
第五优先：从库同步 + 索引优化
```

结合你现在的情况：

```text
240G / 11小时 ≈ 6MB/s
```

可以先检查这几个配置：

```json
"setting": {
  "speed": {
    "channel": 4
  }
}
```

```json
"splitPk": "id"
```

```json
"batchSize": 5000
```

如果现在没有 `splitPk` 或 `channel` 很低，这是最可能的瓶颈。