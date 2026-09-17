##  AQE相关参数 （3.2默认开启）

### `spark.sql.files.maxPartitionBytes=256MB`

- 读取文件时，每个输入分区的目标上限是 256MB。
- 值越大：分区更少、task 更少、调度开销更低；但单 task 更重。
- 常见范围 128MB~512MB，256MB 属于偏稳妥。
- > ⚠️ 注意：此参数**只影响读取阶段**（scan 时的分区数），对 shuffle 后的分区数没有影响，两者独立控制。

---

### `spark.sql.adaptive.enabled=true`

- 开启 AQE 总开关。
- Spark 会在运行时根据真实统计信息重写执行计划（比如改 join 策略、合并分区）。
- > ⚠️ 补充：AQE 是 Spark 3.0 正式启用的特性，**Spark 3.2+ 默认已为 `true`**，若集群版本 ≥ 3.2，此配置可省略（显式写上也无害）。

---

### `spark.sql.adaptive.coalescePartitions.enabled=true`

- AQE 子功能：自动合并 shuffle 后的小分区。
- 作用是减少"很多很小的 task"，提升整体吞吐和稳定性。
- > ⚠️ 注意：此功能依赖 AQE 总开关开启，且**只作用于 shuffle 后的分区合并**，不影响读取阶段的分区数。

---

### `spark.sql.adaptive.advisoryPartitionSizeInBytes=256MB`

- AQE 在合并/调整 shuffle 分区时的"目标分区大小建议值"。
- > ⚠️ 勘误："`advisoryPartitionSizeInBytes` 和 `maxPartitionBytes` 对齐，整体分区策略更一致"的说法**容易产生误导**：
  >   - `maxPartitionBytes` 作用于**读取文件阶段**（输入端）
  >   - `advisoryPartitionSizeInBytes` 作用于 **AQE shuffle 后**的分区合并（中间/输出端）
  >   - 两者作用阶段完全不同，数值相同只是巧合，**并不代表"策略一致"**
- 推荐值通常在 64MB~256MB 之间，应结合下游 task 实际数据量调整，无需刻意与 `maxPartitionBytes` 对齐。

---

### `spark.sql.adaptive.localShuffleReader.enabled=true`

- 在可能情况下优先本地读取 shuffle 数据，减少跨节点网络拉取。
- 通常可降低网络开销、提高读取效率，尤其在 join/聚合阶段。
- > ⚠️ 补充：此优化在 **broadcast join 转换后效果最显著**，普通 sort-merge join 场景下收益相对有限。

---

### `spark.sql.adaptive.skewJoin.enabled=true`

- AQE 子功能：自动识别并处理倾斜 join（某些 key 数据量特别大）。
- 会把大分区拆分处理，避免个别 task 拖慢全局（长尾问题）。
- > ⚠️ 补充：skew join 的拆分会引入**额外的 shuffle 开销**，对于数据量不大、key 分布均匀的场景可能略有负担，并非适用所有情况的银弹，建议在确认存在数据倾斜时再依赖此功能。



---


# Spark AQE 相关知识点 （详细讲解）

注：AQE本质上是运行时优化，一些原数据是在运行时才能获取，然后在进行优化，和hint提示有区别

> spark.sql.adaptive.localShuffleReader.enabled=true 下面的话主要是针对此参数进行进一步描述

## localShuffleReader 与 Broadcast Join 的关系

### 疑问：broadcast 不是已经将小表放到 executor 了吗，读取是不是都在本地？

理解**部分正确**，但混淆了两个阶段。

**Broadcast Join 的过程：**

1. 小表被 broadcast 广播到所有 executor —— 这部分确实是"小表本地化"
2. 但大表在 join **之前**往往经历过 shuffle，shuffle 产生的中间数据块分散在各个节点上
3. `localShuffleReader` 的作用是：在读取**这部分 shuffle 中间数据**时，优先从本地磁盘读，而不是跨节点网络拉取

**为什么 broadcast join 转换后效果最显著？**

AQE 有一个能力：在运行时发现某张表足够小，**动态地把原本计划的 sort-merge join 转换成 broadcast join**。

转换发生后，原来为 sort-merge join 准备的 shuffle 数据已经写到磁盘了（这个 shuffle 无法撤销），此时 `localShuffleReader` 就能介入，让后续读取这批已有 shuffle 数据时尽量走本地，避免不必要的网络传输。

> **一句话总结：** broadcast 解决的是"小表不需要 shuffle"，`localShuffleReader` 解决的是"已经产生的 shuffle 数据尽量本地读"——两者作用对象不同，并不冲突。

---

## AQE 动态转换的完整流程

### 场景背景

假设有两张表做 join：

- **大表 A**：订单表，10GB
- **小表 B**：用户维度表，50MB

---

### 没有 AQE 时的 Sort-Merge Join 流程

```
大表 A ──► Shuffle ──► 按 join key 分区排序 ──┐
                                              ├──► Sort-Merge Join
小表 B ──► Shuffle ──► 按 join key 分区排序 ──┘
```

两张表都要经历完整的 shuffle，数据按 join key 哈希分区，**跨节点网络传输**，每个 executor 拉取自己负责的那部分数据，再做归并排序 join。

---

### AQE 介入后的动态转换流程

**第一阶段：shuffle 已经发生了**

```
大表 A ──► Shuffle 写出 ──► shuffle 文件落盘（分散在各节点）
小表 B ──► Shuffle 写出 ──► shuffle 文件落盘（分散在各节点）
```

> ⚠️ 注意：此时 shuffle 数据**已经写到磁盘了，无法撤销**。

---

**第二阶段：AQE 在运行时统计数据量**

AQE 在 shuffle 写出完成后，会收集真实的统计信息：

```
AQE 统计：小表 B shuffle 后实际只有 45MB
          ↓
判断：45MB < spark.sql.autoBroadcastJoinThreshold（默认 10MB，可调）
          ↓
决策：动态将 Sort-Merge Join 改为 Broadcast Join
```

---

**第三阶段：执行 Broadcast Join**

```
小表 B 的数据 ──► broadcast 广播到所有 executor（内存中）
                                    ↓
大表 A 的 shuffle 文件 ──► localShuffleReader 介入
                           ↓
                 优先读取本节点上的 shuffle 文件
                 （不再需要跨节点拉取大表数据）
                                    ↓
                        本地直接与广播的小表 B 做 join
```

---

### localShuffleReader 的关键作用

||没有 localShuffleReader|有 localShuffleReader|
|---|---|---|
|大表 shuffle 数据读取|跨节点网络拉取|优先读本地磁盘|
|网络开销|高|低|
|适用前提|—|AQE 已将 join 转为 broadcast|

**为什么转成 broadcast join 之后才有效？**

因为 sort-merge join 本身要求数据必须按 key 重新分区对齐，数据该去哪个节点是固定的，`localShuffleReader` 没有介入空间。而一旦转成 broadcast join，大表的 shuffle 数据**在哪个节点读都行**，这时候就可以优先读本地的，不用再跨节点拉取了。

---

### 完整流程总结

```
两表 Shuffle 写出（落盘）
        ↓
AQE 收集统计信息
        ↓
发现小表足够小 ──► 动态改为 Broadcast Join
        ↓
小表广播到所有 executor
        ↓
localShuffleReader：大表 shuffle 数据优先本地读
        ↓
本地完成 Join，无需跨节点网络传输大表数据
```

> **一句话记忆：** broadcast 负责把小表"推"到每个节点，localShuffleReader 负责让大表的 shuffle 数据"不用跑"——两个优化方向相反但相互配合，共同消灭网络传输。

---

## 为什么 AQE 的判断在 shuffle 之后？

AQE 的设计原则是**"运行时"优化**，它依赖的是真实执行后的统计数据：

```
执行计划制定时：Spark 只知道"预估"大小
                ↓ 预估可能严重不准（压缩、过滤、倾斜等）
Shuffle 写出后：Spark 才知道每个分区"实际"多大
                ↓ 此时统计数据才可靠
AQE 介入：基于真实数据做决策
```

所以 AQE 宁愿"先 shuffle 再改计划"，也不愿意基于不可靠的预估贸然决策。

---

## 显式指定广播小表（`BROADCAST hint`）

这走的是**完全不同的路径**，Spark 会在 shuffle 之前就决定用 broadcast join：

```sql
SELECT /*+ BROADCAST(b) */ *
FROM a JOIN b ON a.id = b.id
```

```
执行计划阶段：看到 BROADCAST hint
                ↓
直接规划为 Broadcast Join，跳过小表的 shuffle
                ↓
小表 B ──► 直接读取 ──► driver 收集 ──► broadcast 到所有 executor
大表 A ──► 正常 scan ──► 每个 executor 本地与广播数据 join
```

**小表完全不经过 shuffle**，大表也不需要 shuffle，整个 join 没有 shuffle 阶段。

---

## AQE 动态转换 vs 显式 BROADCAST hint 对比

||AQE 动态转换|显式 BROADCAST hint|
|---|---|---|
|决策时机|shuffle 写出后|执行计划阶段|
|小表是否 shuffle|✅ 已经 shuffle 了|❌ 完全跳过|
|大表是否 shuffle|✅ 已经 shuffle 了|❌ 完全跳过|
|localShuffleReader 是否有用|✅ 有用（大表 shuffle 已存在）|❌ 没有 shuffle 数据，无从介入|
|依赖统计信息|是（运行时真实数据）|否（用户强制指定）|
|风险|几乎没有|小表预估错误可能 OOM|

> **一句话总结：** 显式 `BROADCAST hint` 是"事前规划"，整个 join 没有 shuffle，性能最优但需要你自己保证小表够小；AQE 动态转换是"事后补救"，shuffle 已经发生了再改策略，`localShuffleReader` 才有用武之地。因此，如果**明确知道某张表很小**，直接用 hint 比依赖 AQE 效果更好。