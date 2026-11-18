---
aliases:
  - "01-Doris Colocation Group：建组、约束、重分布与验证"
  - "Doris Colocation Group：建组、约束、重分布与验证"
tags:
  - doris
  - doris-精讲
---

# Doris 精讲（九）：从两张表的桶布局推导 Colocation Join

两张表都按用户分成 16 桶，为什么执行 Join 时仍可能看到 Exchange？相同的分桶规则只能保证相同用户映射到相同桶号。要在 BE 本地关联，还要让两边对应桶的副本具备共同的执行位置。

以下副本放置与语法适用于 Doris 4.x 的存算一体原生 OLAP 表。

## 一、用两张表的第 3 桶看一次 Join

假设 `user_action` 和 `user_profile` 的 `user_id` 都是 `BIGINT`，两边同用 `HASH(user_id) BUCKETS 16`。假设用户 42 映射到第 3 桶。

| 第 3 桶示意 | 行为表的副本位置 | 用户表的副本位置 | 本地 Join 条件 |
| --- | --- | --- | --- |
| 各表独立放置 | BE-1、BE-2、BE-3 | BE-4、BE-5、BE-6 | 桶号相同，两边该桶仍无法在同一节点读取 |
| 共位组稳定后 | BE-1、BE-2、BE-3 | BE-1、BE-2、BE-3 | 可在符合条件的节点读取两边第 3 桶并执行 Join |

Colocation Group 对齐的是同一桶序号的副本放置。同组表需要有兼容的分布列数量、对应类型和 Hash 顺序，Bucket 数及各分区副本数要符合组 Schema。可以分别叫 `user_id` 和 `uid`，等值连接需要按对应位置建立关系。分区数量、范围和分区列类型可以不同。

## 二、从建组到组稳定，经历了什么

建表时设 `"colocate_with" = "group1"`。第一张表可自动建组，第二张表加入时先校验组约束。下图中的行为表保存 `user_id BIGINT` 和 `cost DECIMAL(18,2)`，用户表保存 `user_id BIGINT` 和 `name STRING`，都按 `user_id` 分 16 桶。图中也给出了已有表的绑定方式。

![[99-images/企业微信截图_17786667082439.png]]

下面每段命令分别对应查组、绑定、解绑与迁组。先确认目标表与组兼容，再选需要的操作。

```sql
-- 查看已有组与稳定状态。
SHOW PROC '/colocation_group';

-- 将已有表加入 group1；不存在则自动创建。
ALTER TABLE user_action SET ("colocate_with" = "group1");
ALTER TABLE user_profile SET ("colocate_with" = "group1");

-- 移除某张表的共位属性。
ALTER TABLE user_action SET ("colocate_with" = "");

-- 将另一张表迁往 group_new；也可以直接从旧组迁往新组。
ALTER TABLE user_profile SET ("colocate_with" = "group_new");
```

对于已有数据，表属性变更后，系统还要在后台修复或均衡副本位置。组处于 `IsStable=false` 时，查询可能改用普通 Join。迁组可以直接设置新组，不要求每次先解绑。直接迁组和解绑后重新绑定都需要等待布局稳定。

普通组属于数据库。自 Doris 2.0 起可用 `__global__` 前缀建立跨库组，例如 `"colocate_with" = "__global__group1"`。跨库组名称在全局唯一。

![[99-images/企业微信截图_17786673127403.png]]

查组使用 `SHOW PROC '/colocation_group'`，`SHOW TABLES` 仅列出表。`DROP COLOCATE GROUP IF EXISTS group1` 和 `ADMIN REBALANCE TABLE user_action` 不属于上述通用流程，使用前应确认版本是否支持。组内最后一张表被完全删除后，组会随之自动删除，完全删除还涉及回收站生命周期。

## 三、用组状态和查询计划分别判断

在组列表中核对成员、`BucketsNum`、`ReplicationNum`、`DistCols` 和 `IsStable`。然后针对实际要执行的 Join 看计划。下例假设两张表均按对应的 `k2` 分桶并满足组约束。

```sql
DESC SELECT *
FROM tbl1
INNER JOIN tbl2 ON tbl1.k2 = tbl2.k2;
```

| 计划输出 | 如何解释 |
| --- | --- |
| Hash Join 显示 `colocate: true` | 这个 Join 采用共位，减少为该连接进行的跨节点重分布 |
| `colocate: false, reason: group is not stable` | 组未稳定，该 Join 可能通过 Exchange 重分布数据 |
| 其他节点仍有 Exchange | 可能服务于另一个 Join 或聚合，应按算子归属判断 |

若其中一侧很小，Broadcast 可能已经足够，仍需看过滤后构建侧的大小和内存。若第 3 桶对应的热点 Key 占大量记录，共位只解决位置对齐，热点仍由该桶承担。见 [[07-Doris 精讲（七）：Tablet 热点如何形成，怎样定位和缓解|Tablet 热点如何形成，怎样定位和缓解]]。频繁更换分布列、桶数或副本属性也会增加维护组兼容性的成本。一次普通非分布列变更不必然破坏共位。
