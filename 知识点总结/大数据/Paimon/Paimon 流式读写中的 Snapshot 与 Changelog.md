# Paimon 流式读写中的 Snapshot 与 Changelog

## 核心结论

Snapshot 与 Changelog 解决的是两个不同问题：

```text
Snapshot：这批变化属于哪一次已提交的表版本
Changelog：这一次提交包含哪些行级新增、更新和删除
```

Changelog 不是 Reader 确定启动 Snapshot 时临时开启的，而是在写入或 Compaction 阶段由 `changelog-producer` 决定是否生成。Snapshot 负责原子发布并引用相关文件；流式 Reader 先确定启动位置，再依次消费后续 Snapshot 携带的增量数据或 Changelog。

## 一、完整链路

```text
写入侧
────────────────────────────
上游数据
  → Writer 生成 Data File
  → changelog-producer 决定是否生成 Changelog File
  → Flink Checkpoint 成功
  → 提交 Snapshot
       ├── 引用 Data Manifest
       └── 可能引用 Changelog Manifest

读取侧
────────────────────────────
确定启动 Snapshot
  → 读取初始全量或指定增量
  → 监控后续 Snapshot
  → 每发现一个新 Snapshot
       ├── 有 Changelog File：读取 Changelog
       └── 无 Changelog File：从增量 Data File 获取 Upsert 变化
  → 保存当前消费进度
  → 等待下一个 Snapshot
```

## 二、写入侧

### 1. Writer 先生成数据文件

上游记录持续进入 Paimon Writer。Writer 根据 Partition 和 Bucket 缓冲、排序数据，并生成 Parquet、ORC 或 Avro 等 Data File。

```text
上游记录
  → Partition
  → Bucket
  → Writer Buffer
  → Data File
```

文件写出并不等于已经对查询可见。只有成功提交的 Snapshot 引用了这些文件，它们才正式进入表状态。

### 2. Changelog Producer 决定是否生成 Changelog

`changelog-producer` 是表的写入侧配置，不是流式 Reader 的启动开关。

```text
none
  → 不额外生成完整 Changelog File

input
  → 保存上游已经提供的 +I/-U/+U/-D

lookup
  → Compaction 时查询旧值并生成完整 Changelog

full-compaction
  → 比较两次 Full Compaction 的表状态生成 Changelog
```

### 3. Checkpoint 驱动提交

Flink Checkpoint 成功后，Paimon Committer 原子提交本轮文件和元数据，并形成 Snapshot。

```text
Checkpoint N
  → 收集各 Writer 的待提交文件
  → 提交 Manifest
  → 提交 Snapshot N
  → 本轮数据正式可见
```

### 4. Snapshot 如何引用文件

```text
Snapshot
├── Data Manifest
│   └── Data File
├── Changelog Manifest（可选）
│   └── Changelog File
└── Index Manifest（可选）
    └── Index File
```

Snapshot 不保存业务数据，它保存 Schema、Manifest 引用、提交类型、提交时间等表版本元数据。

## 三、读取侧

### 1. 先确定启动位置

流式 Reader 首先决定从哪里开始消费。启动位置可能来自：

- 默认最新 Snapshot；
- `scan.mode`；
- `scan.snapshot-id`；
- Flink Checkpoint 或 Savepoint 中恢复的 Source State；
- Consumer ID 保存的消费进度。

启动位置与是否生成完整 Changelog 是两个独立配置维度。

### 2. 默认流式读取

```sql
SET 'execution.runtime-mode' = 'streaming';

SELECT *
FROM orders;
```

首次启动且没有历史消费状态时，可以理解为：

```text
找到启动时最新 Snapshot 100
  → 读取 Snapshot 100 的完整表状态
  → 等待 Snapshot 101
  → 读取 Snapshot 101 的变化
  → 等待 Snapshot 102
  → 读取 Snapshot 102 的变化
  → 持续运行
```

初始全量不是重放 Snapshot 1 到 Snapshot 100 的所有历史 Changelog，而是输出 Snapshot 100 对应的当前表状态。

### 3. 只读取启动后的变化

```sql
SELECT *
FROM orders
/*+ OPTIONS('scan.mode' = 'latest') */;
```

```text
跳过当前完整表状态
  → 等待后续 Snapshot
  → 持续读取新变化
```

### 4. 从指定 Snapshot 的完整状态开始

```sql
SELECT *
FROM orders
/*+ OPTIONS(
    'scan.mode' = 'from-snapshot-full',
    'scan.snapshot-id' = '100'
) */;
```

```text
读取 Snapshot 100 的完整状态
  → 读取 Snapshot 101、102……的变化
```

### 5. 从指定 Snapshot 的变化位置开始

```sql
SELECT *
FROM orders
/*+ OPTIONS('scan.snapshot-id' = '100') */;
```

在流模式中，单独指定 `scan.snapshot-id` 表示从相应提交位置读取变化；如果需要先输出该 Snapshot 的完整表内容，应使用 `from-snapshot-full`。

## 四、有无 Changelog File 时怎样读取

### 有 Changelog File

例如 `changelog-producer = input`，Snapshot 101 引用了完整变更：

```text
-U order_id=1, amount=100
+U order_id=1, amount=150
```

读取过程：

```text
Reader 发现 Snapshot 101
  → 找到 Changelog Manifest
  → 读取 Changelog File
  → 向下游输出 -U 100、+U 150
```

### 没有完整 Changelog File

例如 `changelog-producer = none`：

```text
Reader 发现 Snapshot 101
  → 根据增量 Data Manifest 找到新数据文件
  → 读取 Upsert 变化
  → 可能只得到 +U amount=150
```

如果下游计算必须获得旧值，Flink 可能通过 Normalize 状态保存历史值并补齐 `UPDATE_BEFORE`，代价是增加下游状态。

## 五、四种 Changelog Producer

| 配置 | 旧值来源 | 读取延迟 | 主要代价 |
| --- | --- | --- | --- |
| `none` | 不额外生成旧值；必要时由下游状态补齐 | 低 | 下游可能需要 Normalize 状态 |
| `input` | 上游完整 CDC | 低 | 增加 Changelog 文件，且依赖上游完整性 |
| `lookup` | Compaction 时查询表中旧值 | 受 Lookup Compaction 影响 | 查询、缓存、本地磁盘和 Compaction 成本 |
| `full-compaction` | 比较两次完整表状态 | 较高 | Full Compaction 读写放大，中间更新可能折叠 |

选择原则：

```text
下游只需最终状态或可以按主键覆盖
  → none

上游已经提供完整 CDC
  → input

上游没有旧值，但下游必须撤回旧值
  → lookup

只关心周期性状态差异且能接受高延迟
  → full-compaction
```

## 六、Snapshot 与 Changelog 的对应关系

每个 Snapshot 都可以从两个视角理解：

```text
表状态视角
────────────────────────────
Snapshot 101
  → 当前表有哪些有效 Data File
  → 用于批查询和当前状态查询

增量变化视角
────────────────────────────
Snapshot 101
  → 相比前一次提交发生了哪些变化
  → 可能来自 Changelog File
  → 也可能来自增量 Data File
```

一个 Snapshot 不一定包含独立的 Changelog File，但一定代表一次已经发布的表状态变化。

## 七、常见误区

### 误区一：Reader 启动时才开启 Changelog

错误。Reader 只决定从哪个 Snapshot 开始读取。完整 Changelog 是否存在，由写入侧 `changelog-producer` 和 Compaction 策略决定。

### 误区二：Snapshot 就是 Changelog

错误。Snapshot 是提交版本元数据；Changelog 是行级变化。Snapshot 可以引用 Changelog，也可以只引用 Data File 的变化。

### 误区三：初始全量会回放全部历史变化

错误。默认初始全量读取的是启动时最新 Snapshot 的完整表状态，不是从第一版开始回放所有更新过程。

### 误区四：有主键表就天然有完整 Before/After

错误。主键表能够形成最终逻辑状态，但完整 `UPDATE_BEFORE` 是否可供流式下游读取，仍取决于 Changelog Producer。

## 八、最终记忆模型

```text
写入侧决定“产生什么变化”
  → changelog-producer

Checkpoint决定“何时提交”
  → 原子提交文件和元数据

Snapshot决定“变化属于哪个表版本”
  → 引用Data/Changelog/Index Manifest

读取侧决定“从哪个版本开始”
  → scan.mode / scan.snapshot-id / 恢复状态

Reader决定“如何消费这个版本的变化”
  → 有Changelog读Changelog
  → 无完整Changelog读增量Data File
```

一句话总结：

> Changelog 不是在 Reader 确定 Snapshot 时临时开启的，而是在写入或 Compaction 阶段由 `changelog-producer` 决定是否生成；Snapshot 负责发布并引用这些变化，流式 Reader 先确定从哪个 Snapshot 开始，再逐个读取后续 Snapshot 所携带的增量或 Changelog。

## 相关笔记

- [[Paimon Snapshot、Manifest、Parquet 与 Compaction 的关系]]
- [[Apache Paimon 表模型、存储组织与读取语义]]
- [[paimon-create-table-research]]

## 官方资料

- [Apache Paimon 2.0 Flink SQL Query](https://paimon.apache.org/docs/2.0/flink/sql-query/)
- [Apache Paimon 2.0 Changelog Producer](https://paimon.apache.org/docs/2.0/primary-key-table/changelog-producer/)
- [Apache Paimon 2.0 Basic Concepts](https://paimon.apache.org/docs/2.0/concepts/basic-concepts/)
