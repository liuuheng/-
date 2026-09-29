
doris 优化和spark 并不相同，主要从一下方面进行优化：

```text
partition	分区
bucket		分桶
key model	建模：明细模型、聚合模型、唯一模型
materialized view	物化视图
bitmap / bloom filter	索引优化
colocation	--Colocation Group(数据共位组) = 一组“强约束的分布规则绑定”，让多张表在物理层完全按同一种方式分布，从而实现本地 Join（避免 Shuffle）。=
```

简单来说：
```text
Partition：减少扫描范围
Bucket：提升并行与分布均匀性
Key Model：定义数据如何存储与更新
MV：提前计算减少查询成本
Bitmap / Bloom：减少过滤成本
Colocation：减少 join shuffle
```


多张表想要实现可优化的join
```text
1. 相同分桶键（distribution key）
2. 相同分桶方式（HASH）
3. 相同 bucket 数量
4. 相同副本数（通常要求一致）
5. 相同 Colocation Group（如果启用）
```
