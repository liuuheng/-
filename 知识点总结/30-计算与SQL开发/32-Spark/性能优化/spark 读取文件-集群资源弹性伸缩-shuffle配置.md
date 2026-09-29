## 1、执行预测

> 个人感觉不是很好，参数开后，会出现许多killed的task 并且数据可能会丢失（不知道是不是还有参数没有开启）
### `spark.speculation=false`

- **推测执行关闭**：不会对同一个 partition 的 task 再起一份并行执行。
- **收益**：更省 CPU/内存；减少重复计算与重复 shuffle 写入；降低副作用场景下的重复执行风险。
- **代价**：若个别 task 因节点慢、磁盘抖动、GC 等原因长尾拖慢，stage 完成时间可能被拉长。
- > ⚠️ 补充：适合作业整体稳定、资源紧张或对幂等/副作用敏感的场景；若经常出现长尾 task，可评估开启推测执行并结合相关阈值参数收敛误触发。

---
## 2、读取文件

>   目前来看，没有很好的参数 可以保证 每个task中读取文件的数量的 均匀性；最好的办法是 读取后 repartition 或则是上游的 进行distribute by
### `spark.sql.files.maxPartitionBytes=268435456`（256MB）

- 控制文件扫描时**单个分区（读路径上通常对应一个 task）目标数据规模的上限（估算）**。
- **偏大**：读 task 更少、调度开销通常更低，但单 task 更重，极端时可能 OOM/GC 压力更大或单 task 耗时过长。
- **偏小**：分区更碎、task 更多，可能增加调度与 task 启动开销。
- > ⚠️ 补充：这是**规划口径**；Spark UI 的 Input Size 还受**列裁剪、过滤下推**等影响，与“分区大小估算”不必一致，排障时要分开看。

---

### `spark.sql.files.openCostInBytes=8388608`（8MB）

- 在扫描分区规划时，为**每个文件**增加一笔“打开成本”的虚拟字节数（如 open/seek/footer 等固定开销的折算）。
- **偏大**：更不愿意把很多（尤其小）文件合并到同一分区 → 单分区文件数倾向更少，**task 可能变多**。
- **偏小**：更愿意合并小文件 → **task 可能变少**，但单 task 内可能打开过多文件导致固定开销叠加。
- > ⚠️ 补充：常与 `maxPartitionBytes` **联动**；小文件很多时，该参数对“每 task 文件数”影响通常更明显。

---

### `spark.sql.orc.enableVectorizedReader=true`

- ORC 扫描优先走**向量化读取**路径（批量解码，后续算子衔接更高效）。
- **常见收益**：扫描吞吐提升，尤其在列裁剪明显、过滤可下推时更有体感。
- > ⚠️ 补充：若遇到特定类型/表达式兼容性问题，可作为排查项临时关闭做对比；多数 ORC 分析型负载建议保持开启。


---
---
## 集群资源配置（spark3.2 默认开启）

> 下面是spark充分利用集群资源的配置。sprak3.2 默认开启 2.4需要手动设定 ；
> 此处这些参数 对于spark2.4 版本的优化效果很好
> 在集群空闲时，spark2.4不会申请创建额外的executor 开启后才会申请，优化之处就在此处

### `spark.shuffle.service.enabled=true`

- 启用 **External Shuffle Service**（常见于 YARN：由集群侧服务持有 shuffle 中间数据）。
- **作用**：允许在 executor 动态回收/替换时，其它 executor 仍可通过 shuffle 服务读取 shuffle 数据，降低 shuffle 数据不可用风险。
- > ⚠️ 补充：与 **`spark.dynamicAllocation.enabled=true`** 强配套；若集群未正确部署 shuffle service，动态缩容可能导致 shuffle 不稳定（以集群实际部署为准）。

---

### `spark.dynamicAllocation.enabled=true`

- 打开 **Executor 动态分配**：根据调度积压、空闲等信号自动增减 executor。
- **典型收益**：负载波动场景下更省资源（闲时收缩），峰值可自动扩容。
- > ⚠️ 补充：需要队列/集群允许弹性申请资源；扩缩过于频繁可能带来调度开销，需结合监控调参。

---

### `spark.dynamicAllocation.initialExecutors=1`

- 作业启动时的**初始 executor 数量**（受 min/max 约束）。
- **偏小**：冷启动更省资源，但若首个 stage 很重可能需要先扩容才有足够并行度。
- > ⚠️ 补充：若作业一开始就大规模扫描/shuffle，可适当提高 initial（或 min）减少“先慢后快”的爬坡。

---

### `spark.dynamicAllocation.minExecutors=1` / `spark.dynamicAllocation.maxExecutors=25`

- **min**：至少保留 1 个 executor（避免无可用执行资源）。
- **max**：最多扩展到 25，防止无限扩容占满集群或触发队列上限。
- > ⚠️ 补充：`max` 是硬上限；最终并行度仍受 **partition 数、每个 executor 的 cores、队列配额**等约束。

---

### `spark.dynamicAllocation.executorIdleTimeout=60s`

- executor **无运行 task 且无缓存数据**时，空闲超过该时间可被回收。
- **60s 偏激进**：资源回收快，但 stage 间短停顿也可能触发频繁回收与再申请。
- > ⚠️ 补充：若观察到 stage 边界频繁“掉 executor 再拉起”，可适当放宽 idle timeout 降低 churn。

---

### `spark.dynamicAllocation.cachedExecutorIdleTimeout=30min`

- 对仍 **cache/persist** 了数据的 executor，允许空闲更久再回收，降低**反复重算缓存**概率。
- > ⚠️ 补充：大量缓存可能导致 executor 长期占坑、降低资源利用率；需要结合缓存价值与成本权衡。

---

### `spark.dynamicAllocation.schedulerBacklogTimeout=2s`

- 当调度出现 **task backlog（待运行任务积压）**并持续超过该时间，会倾向于触发扩容。
- **2s 很短**：对负载上升响应快，也可能对瞬时抖动更敏感。
- > ⚠️ 补充：建议与 `sustainedSchedulerBacklogTimeout`、`executorAllocationRatio` 一起看“敏感度 vs 稳定性”。

---

### `spark.dynamicAllocation.sustainedSchedulerBacklogTimeout=2s`

- 用于确认 backlog **持续存在**后再强化扩容倾向（与上一项配合，降低一过性抖动引发猛扩容）。
- **同样偏短**：整体扩容策略偏积极。
- > ⚠️ 补充：共享集群上过短窗口可能导致短时间申请偏多 executor；建议结合 pending tasks、executor 申请速率监控调参。

---

### `spark.dynamicAllocation.executorAllocationRatio=0.5`

- 估算“还需要多少 executor”时乘以 **0.5**，扩容更保守，避免一次性加太多。
- **收益**：扩缩更平滑，降低 overshoot 与资源争抢。
- **代价**：峰值阶段达到理想并行度可能更慢（需要多轮扩容）。
- > ⚠️ 补充：延迟敏感且资源充足时可适当提高 ratio；集群资源紧张时 0.5 往往更稳。
