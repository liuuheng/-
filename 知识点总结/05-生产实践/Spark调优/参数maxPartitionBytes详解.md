# `spark.sql.files.maxPartitionBytes`、列裁剪与 Spark UI Input Size

配置示例：`spark.sql.files.maxPartitionBytes=512MB`

## 1. `maxPartitionBytes` 的作用（核心）
`spark.sql.files.maxPartitionBytes` 用于 Spark 在做文件扫描规划时，控制**单个扫描分区（partition / task）期望覆盖的“数据字节规模上限（估算）”**。  
Spark 会据此把文件（或文件切片）组合成多个分区，从而影响扫描阶段的 **task 粒度**。

> 直觉：值越大，越倾向于把更多数据合进同一个分区（task 可能变少）；值越小，分区更碎（task 可能变多）。

## 2. 为什么“理论分了 512MB 文件”，Spark UI 的 Input Size 可能只有几十 MB？
扫描规划阶段会把一批文件/切片分配给一个 task（估算目标可能接近 512MB），但**执行时不一定读取这些文件的全部数据**，常见原因：

- **列裁剪（Column Pruning）**  
  只读取查询所需的列；像 Parquet/ORC 这类列式存储，会显著减少实际读取字节数。
- **过滤下推/分区裁剪（Predicate Pushdown / Partition Pruning）**  
  条件过滤可以让 Spark 跳过部分 row group/stripe 或整段文件区域，进一步减少读取量。

因此，即使一个 task “负责”的文件集合在规划上看起来很大，**实际从存储系统读到的字节数**仍可能远小于 512MB。

![[99-images/企业微信截图_17768382675458.png]]

## 3. Spark UI 里的 Input Size 通常表示什么？
在 Spark UI 的 stage/task 指标里，**Input Size 通常表示 task 实际从底层数据源读取到的字节数**（偏“实际 I/O”口径），会受到以下因素影响：

- **列裁剪**：只读需要的列 → Input Size 变小
- **过滤下推/裁剪**：跳过不需要的数据块 → Input Size 变小
- **文件格式与压缩**：列式格式与压缩会改变“落盘字节”与“解压后字节”的关系，UI 指标通常更接近“从存储读到的字节”这一侧

> 结论：Input Size 反映的是“实际读了多少”，而 `maxPartitionBytes` 主要影响的是“扫描任务怎么分、分得多大”（规划层面的粒度），两者不必一致。