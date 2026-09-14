# Spark SQL Cache Table 机制

## 1. Temporary View 不会自动缓存

```sql
create or replace temporary view dim_style_tag as
select ...
```

`CREATE OR REPLACE TEMPORARY VIEW` 只是给一段查询逻辑起一个临时名字，类似 CTE 或子查询封装。

特点：

- 不会立即执行查询
- 不会物化结果
- 不会自动写入内存或磁盘缓存
- 后续引用 view 时，Spark 会展开原始 SQL 逻辑重新参与计算
- 生命周期通常只在当前 Spark session 内

如果只是创建 temporary view，后续每次使用它时，通常仍会重新读取源表并执行过滤、窗口函数、排序等逻辑。

## 2. 真正缓存需要显式 CACHE TABLE

```sql
cache table dim_style_tag;
```

`CACHE TABLE` 才会将表或临时视图的结果注册为可缓存对象。

但需要注意：`CACHE TABLE` 通常只是登记缓存，不一定立刻计算。真正填充缓存一般要等后续 action 触发。

常见写法：

```sql
cache table dim_style_tag;

select count(*) from dim_style_tag;
```

`count(*)` 用来触发实际计算，让 Spark 把结果写入缓存。

## 3. Cache 后数据存在哪里

缓存结果不会保存在 Driver 内存中，而是按 partition block 分布在各个 Executor 上，由 Executor 的 BlockManager 管理。

结构可以理解为：

```text
Executor JVM
└─ BlockManager
   └─ Storage Memory
      └─ cached table blocks
```

也就是说：

- Driver 负责 SQL 解析、优化、生成执行计划、调度 task、跟踪状态
- Executor 负责读取 cached blocks 和执行具体计算
- 缓存数据主要占用 Executor 的 Storage Memory
- 内存不够时，默认可能落到 Executor 本地磁盘

Spark SQL 的 `CACHE TABLE` 默认缓存级别通常类似：

```text
MEMORY_AND_DISK
```

含义是优先放内存，内存不够时部分 block 可以落到本地磁盘。

## 4. Cache 是 Executor 级别，不是 Task 级别

缓存 block 占用的是 Executor 级别的 Storage Memory，不是某个 task 私有的内存。

Task 的作用是：

- 计算某个 partition
- 把结果 block 存入所在 Executor 的 BlockManager
- 后续其他 task 可以复用这些 block

Task 结束后：

- task 使用的 execution memory 会释放
- cached block 仍可能保留在 executor 的 storage memory 中
- 后续查询可以继续复用缓存

缓存存在时间取决于：

- 当前 Spark session 是否还在
- executor 是否还存活
- cache 是否被 uncache
- 是否因为内存不足被 evict

## 5. 一张缓存表通常对应多个 Block

缓存表不是整张表一个 block，而是按 Spark partition 缓存。

例如：

```text
dim_style_tag partition 0 -> cached block 0
dim_style_tag partition 1 -> cached block 1
dim_style_tag partition 2 -> cached block 2
...
```

这些 blocks 会分布在多个 executor 上：

```text
executor-1: block 0, block 7, block 13
executor-2: block 1, block 5, block 20
executor-3: block 2, block 8, block 31
```

默认一般没有副本。除非显式使用带 replication 的 StorageLevel，例如 `MEMORY_ONLY_2`。

## 6. 不广播时，Cache 数据通常不经过 Driver

如果后续多个地方都使用 cached table，例如：

```sql
select ...
from fact_a a
join dim_style_tag d
on a.brandgood_id = d.brandgood_id;

select ...
from fact_b b
join dim_style_tag d
on b.brandgood_id = d.brandgood_id;
```

如果没有发生 broadcast join，executor 会直接读取 cached blocks 参与计算。

数据路径大致是：

```text
Executor cached blocks
-> Executor task 读取
-> Executor task 计算
```

Driver 仍然需要参与：

- 生成执行计划
- 调度 task
- 跟踪任务状态
- 汇总最终小结果

但 Driver 不承载整张缓存表的数据。

## 7. Broadcast Join 时 Driver 会参与承载广播侧数据

如果 Spark 选择 broadcast join，或者显式指定：

```sql
select /*+ broadcast(dim_style_tag) */ ...
```

那么广播侧数据会被收集到 Driver，构造成 broadcast relation，再分发或供 executor 拉取。

路径大致是：

```text
cached blocks on executors
-> 收集广播侧结果
-> Driver 构造 broadcast relation
-> executors 获取广播数据
-> executor 本地 join
```

因此：

- cache 可以避免重复计算上游逻辑
- 但如果使用 broadcast join，广播侧仍会被收集到 Driver 构造广播变量
- 广播表过大时，Driver 内存可能有压力

## 8. Cache 不等于 Broadcast

| 概念 | 作用 |
|---|---|
| `CACHE TABLE` | 复用中间结果，减少重复读表或重复计算 |
| `BROADCAST JOIN` | Join 优化策略，把小表发送到 executor 本地参与 join |

两者可以同时发生，但不是同一件事。

已经 cache 的表，如果被选择为 broadcast join 的广播侧，Spark 仍需要构造 broadcast relation。

## 9. 使用 Cache 前的判断

适合 cache 的场景：

- 中间结果会被多次复用
- 上游计算比较昂贵
- 缓存结果规模可控
- executor storage memory 相对充足
- session 内源数据不需要频繁变化

不太适合 cache 的场景：

- 只使用一次
- 结果非常大
- 上游计算不贵
- executor 内存紧张
- 底层数据频繁变化，容易读到旧缓存

## 10. 底层数据变化与缓存失效

如果底层源表数据发生变化，已有 cache 不一定自动反映最新数据。

稳妥做法是先取消缓存，再重新创建和缓存：

```sql
uncache table dim_style_tag;

create or replace temporary view dim_style_tag as
select ...;

cache table dim_style_tag;

select count(*) from dim_style_tag;
```

也可以清空当前 session 中所有缓存：

```sql
clear cache;
```

## 11. 如何确认是否命中缓存

可以使用：

```sql
explain formatted
select ...
from dim_style_tag;
```

重点看执行计划中是否出现：

```text
InMemoryTableScan
```

如果出现：

```text
BroadcastExchange
BroadcastHashJoin
```

说明发生了 broadcast join。

如果出现：

```text
SortMergeJoin
```

通常说明是 shuffle join。

也可以在 Spark UI 的 Storage 页查看：

- cached table 是否存在
- cached partitions 数量
- memory size
- disk size
- storage level

## 12. 常用命令

创建临时视图：

```sql
create or replace temporary view dim_style_tag as
select ...;
```

缓存表：

```sql
cache table dim_style_tag;
```

触发缓存填充：

```sql
select count(*) from dim_style_tag;
```

取消指定表缓存：

```sql
uncache table dim_style_tag;
```

清空所有缓存：

```sql
clear cache;
```

查看执行计划：

```sql
explain formatted
select ...
from dim_style_tag;
```

## 总结

`temporary view` 只是查询逻辑别名，不会自动缓存。`cache table` 才会把结果按 partition block 缓存在 executor 的 Storage Memory 或本地磁盘中。

缓存是 executor 级别，不是 task 级别。普通复用缓存时，数据主要在 executor 中读取和计算，不经过 Driver 承载整表。如果发生 broadcast join，广播侧数据会被收集到 Driver 构造 broadcast relation，再分发给 executors。
