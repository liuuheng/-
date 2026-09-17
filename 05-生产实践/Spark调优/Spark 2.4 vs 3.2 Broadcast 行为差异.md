## 测试背景

测试场景是：**首次创建一张表，用 `INSERT INTO TABLE xxx VALUES` 插入数据**。

这种情况下 metastore 里不会自动生成统计信息（Statistics），是一个"裸表"状态。我们想搞清楚：在这种状态下，两个版本对于 broadcast join 的判断逻辑有什么不同。

---

## Spark 2.4 的行为

### 现象

用 hint 提示广播后，即使表没有 Statistics，广播仍然成功触发了。

更有意思的是：**在 job 运行期间**去查 `DESCRIBE EXTENDED xxx`，会发现 Statistics 突然出现了。

### 这是执行了 ANALYZE TABLE 吗？

不是。Spark 并不会主动帮你跑 `ANALYZE TABLE`，但确实发生了一件类似的事：

> Spark 2.4 在处理 hint 广播的执行计划时，如果发现 metastore 里没有统计信息，会通过 `CommandUtils.updateTableStats` 或 `HiveClientImpl` **把本次收集到的 stats 回写到 metastore**。这是执行计划阶段的一个副作用，不是主动触发的 ANALYZE。

所以你看到 `DESCRIBE EXTENDED` 里出现了 Statistics，是这个副作用"顺手"写进去的。

### 为什么没有 Statistics 就不敢自动广播？
![[images/企业微信截图_1777268757765.png]]
Spark 2.4 的逻辑比较保守：

- 如果 metastore 里没有 totalSize / numRows，`sizeInBytes` 会直接退化成 `Long.MaxValue`（一个接近无穷大的值）
- 优化器一看这个值，觉得表太大了，就**不敢自动触发广播**
- 所以 2.4 非常依赖 metastore 里的统计信息，没有就基本不会自动广播

---

## Spark 3.2 的行为

### 现象

同样缺少 Statistics，hint 广播照样成功。但和 2.4 不同的是：**job 跑完之后去查 `DESCRIBE EXTENDED`，Statistics 仍然不存在**。

说明 3.2 在广播时完全不依赖也不回写 metastore 的统计信息，走的是另一套逻辑。

### 3.2 有两条完全独立的广播路径

理解 3.2 的行为，首先要分清这两条路径，它们互不干扰：

**路径一：静态估算 + hint（跟 AQE 无关）**

执行计划阶段，Spark 会尝试从文件索引或文件大小直接推算出 `sizeInBytes`，不需要 metastore 里有统计信息。只要估出来的值小于 `autoBroadcastJoinThreshold`，就触发广播。

```
执行计划阶段
  → 从文件大小 / 文件索引推断 sizeInBytes
  → sizeInBytes < autoBroadcastJoinThreshold
  → 触发 broadcast join
```

**路径二：AQE 动态转换（运行时，不需要 hint）**

AQE 不在执行计划阶段做决定，而是等 shuffle 真正写完之后，拿到真实的数据量，再动态决定要不要把 sort-merge join 改成 broadcast join。

```
两表 Shuffle 写出落盘
  → AQE 读取真实数据量
  → 发现小表够小 → 动态改为 Broadcast Join
```

### 为什么 3.2 即使没有 Statistics 也能推算出 sizeInBytes？

这是 3.2 引入的**激进推断机制**。和 2.4 直接返回 `Long.MaxValue` 不同，3.2 会依次尝试：

1. 基于 schema 推算
2. 基于 rowSize 推算
3. 基于 children 递归估算

最终走到这个公式：

```
sizeInBytes = outputRowSize × rowCount
```

听起来很合理，但有一个**隐藏的陷阱**：

> 当 rowCount 未知时，默认值是 **1**。
> 
> 这意味着一张几十 GB 的大表，在没有统计信息的情况下，`sizeInBytes` 可能被估成一个极小的值，从而被误判为"小表"触发 broadcast，最终导致 **executor OOM**。

这是 3.2 在无统计信息场景下最需要警惕的风险。

---

## 两个版本行为对比

||Spark 2.4|Spark 3.2|
|---|---|---|
|无 stats 时 sizeInBytes|`Long.MaxValue`，保守，不广播|激进推断，可能严重低估|
|hint broadcast 触发条件|依赖 metastore stats，无 stats 会回写副作用|从文件大小 / schema 直接推算，不回写|
|跑完后 `DESCRIBE EXTENDED` 有 Statistics|✅ 有（副作用写回）|❌ 无|
|AQE 动态广播|❌ 不支持|✅ 支持（基于 shuffle 后真实数据量）|
|主要风险|没有 stats 就不广播，容易错失优化|没有 stats 可能低估大表，触发误广播 OOM|

---

## 本质差异一句话总结

两个版本的根本区别在于**"面对未知时选择保守还是激进"**：

- **Spark 2.4**：不知道表有多大 → 当作无限大 → 不广播，安全但可能错失优化
- **Spark 3.2**：不知道表有多大 → 尽力推算，实在不知道 rowCount 就默认为 1 → 可能广播，但有 OOM 风险

因此在 Spark 3.2 中，对于**没有统计信息的表**，建议：

- 手动执行 `ANALYZE TABLE xxx COMPUTE STATISTICS` 补充统计信息
- 或者合理设置 `spark.sql.autoBroadcastJoinThreshold`，避免因低估 size 触发意外的大表广播


---

来源的地方：cf文档。主要的重点在于 broadcast失效的问题，这个没有遇到过  
![[images/企业微信截图_17772600392028.png]]
![[images/企业微信截图_17772600481828.png]]![[images/企业微信截图_17772600766662.png]]![[images/企业微信截图_17772601211799.png]]