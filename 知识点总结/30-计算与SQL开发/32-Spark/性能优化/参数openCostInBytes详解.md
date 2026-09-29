# `spark.sql.files.openCostInBytes` 

配置示例：`spark.sql.files.openCostInBytes=67108864`（约 **64MB**）默认是4MB。
还有 用此参数 优化读取表时启动task的数量时，效果不佳。
**主要是控制 每个task中读取小文件的数量上限**

## 1. 参数作用（一句话）
`spark.sql.files.openCostInBytes` 用于在 Spark 扫描文件并做**切分/合并规划**时，把“**打开一个文件的固定成本**”折算成“等价字节数”，参与分区（partition/split）大小估算，从而影响**每个 task 内会合并多少文件**（尤其是小文件）。

## 2. 直觉理解
读取阶段除了真实数据字节，还存在固定开销（例如 open/seek、读取 footer、初始化 reader 等）。  
Spark 用 `openCostInBytes` 把这类固定开销“算进成本”，以避免出现“单个 task 里塞了海量小文件导致固定开销叠加爆炸”的情况。

## 3. 对 task 数与每 task 文件数的影响（重点）
- `openCostInBytes` **不是一个直接控制 task 数的开关**，它是通过影响切分/合并策略来**间接**改变 task 数与每 task 文件数。
- 常见趋势（小文件很多时更明显）：
  - **`openCostInBytes` 越大**：Spark 越“害怕”把很多文件合到同一个 partition ⇒ 单个 partition 里文件数倾向更少 ⇒ **partition/task 倾向变多**
  - **`openCostInBytes` 越小**：更愿意合并更多小文件到同一个 partition ⇒ **partition/task 倾向变少**，但单个 task 内文件数可能增多

## 4. 为什么会出现“想减少 task 但又怕每个 task 文件太多”的两难？
两类问题目标相反：

### A. task 太碎（task 太多）→ 调度/管理开销大
- 目标：合并成更少的 task
- 常用手段：
  - 增大 `spark.sql.files.maxPartitionBytes`（提高单个分片目标大小）
  - 适当降低 `spark.sql.files.openCostInBytes`（更激进地合并小文件）

### B. 单个 task 内文件太多 → 固定开销叠加（open/seek/footer）明显
- 目标：限制单个 task 内文件数量
- 常用手段：
  - 适当提高 `spark.sql.files.openCostInBytes`（更保守地合并文件）
  - 从数据侧治理小文件（合并文件、优化写入并发与分区策略等）

## 5. 与 `spark.sql.files.maxPartitionBytes` 的关系（建议一起记）
- `spark.sql.files.maxPartitionBytes`：更像“每个 partition 目标读取多少数据字节”的主旋钮  
- `spark.sql.files.openCostInBytes`：给“每个文件”额外加一笔成本，用来修正“小文件很多时的固定开销”  
二者共同决定扫描时的切分/合并效果，从而影响 task 数与每 task 文件数。

## 6. 对 `64MB` 这个值的理解
当 `openCostInBytes=64MB` 时，可理解为：规划时每个文件都会被额外“算作”约 64MB 的成本。  
在小文件极多的场景，这会让 Spark 更保守地合并文件，降低单 task 文件数爆炸的风险；同时也可能带来 task 数上升的倾向。

## 7. 速记
- **`openCostInBytes` 越大 → 越不愿把很多小文件塞进一个 task → task 倾向变多**
- **它调的是“合并小文件的意愿”，不是“task 数量开关”**