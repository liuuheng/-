# Flink CDC Pipeline YAML 完整指南：配置、路由、分库分表、条件过滤与 Schema 演进

> 本文以 Apache Flink CDC Pipeline YAML 3.x 为主，重点参考 3.6 行为。Flink CDC、Flink 以及各个 Pipeline Connector 的配置会随版本变化，生产配置必须以实际使用版本的官方文档为准。

---

## 1. 两个核心问题

### 1.1 表结构变更如何处理

Flink CDC 不仅采集 INSERT、UPDATE、DELETE 数据事件，还可以产生 SchemaChangeEvent，例如建表、增加字段、修改类型、重命名字段、删除字段、清空表和删除表。

结构变化能否同步，要经过四层判断：

1. **Source 能否捕获 DDL**：受数据库日志、Connector 能力和 Source 参数控制。
2. **Pipeline 采用什么演进策略**：由 `pipeline.schema.change.behavior` 决定。
3. **事件是否被过滤**：由 `sink.include.schema.changes` 和 `sink.exclude.schema.changes` 决定。
4. **Sink 能否应用**：受 Sink Connector、目标数据库能力和账号 DDL 权限控制。

所以它不是把源端 ALTER SQL 原样复制到目标端，而是把源端变化转换成统一 SchemaChangeEvent，再由 Sink 翻译成目标系统支持的操作。

### 1.2 多 Source 如何处理

必须区分：

- **一个 Source Connector 读取多张表**：支持，属于常规能力。
- **多个分库分表合并成一张逻辑表**：支持，通常用 Source 正则订阅和 Route 合并。
- **一个 YAML 定义多个异构 Source Connector**：标准 Pipeline YAML 通常只有一个顶层 `source:`。不同 MySQL 集群、MySQL + PostgreSQL 等情况通常拆成多个 Pipeline，或使用 Flink SQL/DataStream API 自定义。
- **一个 Sink Connector 写多张表**：支持。
- **一个 YAML 同时写 Doris 和 Kafka 等不同 Sink 系统**：标准单 Sink Pipeline 通常不直接支持，常见做法是拆 Job或使用更灵活的 API。

---

## 2. Pipeline 的准确含义

Pipeline 有两层含义。第一层是整个数据作业：

```text
Source：初始快照 + 增量日志
  ↓
Transform：选列、改名、计算、过滤、主键调整
  ↓
Route：源 Table ID 映射到目标 Table ID
  ↓
Schema 协调：传播并应用表结构变化
  ↓
Sink：写数据和 DDL
```

这是职责关系，不代表所有版本内部算子的严格物理顺序。

第二层是 YAML 的 `pipeline:` 配置块，只保存作业级参数。它不是用来匹配 Source 数据和 Sink 表的。

| 模块 | 职责 |
|---|---|
| `source` | 连接源系统、选择表、控制快照和日志读取 |
| `transform` | 改变行列：投影、计算、过滤、主键 |
| `route` | 改变表去向 |
| `sink` | 连接目标系统、建表、改表和写数据 |
| `pipeline` | 作业名、并行度、时区、运行模式和演进策略 |

---

## 3. YAML 基础

YAML 使用空格缩进，不能混用 Tab。`-` 表示列表项：

```yaml
transform:
  - source-table: app_db.orders
    projection: id, amount
  - source-table: app_db.products
    projection: id, name
```

真正的注释是：

```yaml
parallelism: 4  # 全局并行度
```

`description` 不是 YAML 注释，而是框架识别的可选字段：

```yaml
description: filter and enrich order records
```

它主要提高配置可读性，也可能出现在日志、调试或配置展示中，但不改变字段、路由、过滤或写入结果。官方没有保证所有版本都在 Flink UI 展示它，不能依赖它实现监控标签。

表 ID 中的点号也是正则特殊字符，应按 Connector 文档转义：

```yaml
tables: app_db\.orders_[0-9]+
```

不要随意使用过宽的 `.*`，否则可能捕获 gh-ost、pt-osc 临时表。

---

## 4. 一份完整 YAML 示例

```yaml
source:
  type: mysql
  name: mysql-orders-source
  hostname: mysql.example.com
  port: 3306
  username: cdc_user
  password: MYSQL_CDC_PASSWORD_FROM_SECRET
  tables: app_db\.orders_[0-9]+

  # 排除在线 DDL 工具的影子表、旧表
  tables.exclude: app_db\._.*_gho,app_db\._.*_ghc,app_db\._.*_new,app_db\._.*_old

  server-id: 5400-5408
  server-time-zone: Asia/Shanghai
  metadata.list: op_ts
  schema-change.enabled: true

sink:
  type: doris
  name: doris-orders-sink
  fenodes: doris-fe-1:8030,doris-fe-2:8030
  username: cdc_writer
  password: DORIS_PASSWORD_FROM_SECRET

  include.schema.changes:
    - create.table
    - add.column
    - alter.column.type
    - rename.column

  # exclude 优先级高于 include
  exclude.schema.changes:
    - drop.column
    - drop.table
    - truncate.table

  table.create.properties.replication_num: 3

transform:
  - source-table: app_db\.orders_[0-9]+
    projection: >
      id,
      user_id,
      amount,
      status,
      __table_name__ AS source_table,
      op_ts AS source_op_ts
    filter: status = 'PAID'
    primary-keys: source_table,id
    description: Keep paid orders and record source metadata

route:
  - source-table: app_db\.orders_[0-9]+
    sink-table: ods_db.paid_orders
    description: Merge all order shards into one Doris table

pipeline:
  name: mysql-sharded-paid-orders-to-doris
  parallelism: 4
  local-time-zone: Asia/Shanghai
  execution.runtime-mode: STREAMING
  schema.change.behavior: try_evolve
  operator.uid.prefix: paid-orders-v1
```

注意：

- 密码示例表示“从秘密管理系统注入”，不是保证占位符能被所有版本自动展开。
- `__table_name__` 等隐藏字段名称有版本差异。
- Sink 是否支持特定 DDL 仍要查 Connector 和目标数据库。
- 将来源表放入主键会改变下游数据模型，不能机械照抄。

---

## 5. Source 配置详解

### 5.1 通用结构

| 参数 | 通常是否必填 | 含义 |
|---|---:|---|
| `type` | 是 | Connector 类型，如 mysql、postgres |
| `name` | 否 | 可读名称，通常无数据语义 |
| 地址和认证 | 是 | hostname、port、username、password 等 |
| 表选择 | 是 | 要采集的表，通常支持正则 |
| Connector 专属参数 | 视情况 | 快照、日志、时区、元数据、SSL 等 |

详细参数不能脱离 Connector 回答。MySQL、PostgreSQL 的日志和启动位点机制不同。

### 5.2 MySQL 常见参数

| 参数 | 说明 |
|---|---|
| `hostname` / `port` | MySQL 地址和端口 |
| `username` / `password` | CDC 账号 |
| `tables` | 捕获表，支持正则和多表 |
| `tables.exclude` | 从命中范围排除表 |
| `server-id` | replication client ID 或候选范围 |
| `server-time-zone` | MySQL 服务端时区 |
| `schema-change.enabled` | 是否发送 Schema Change Event |
| `metadata.list` | 要读取的 Connector 元数据 |
| `scan.incremental.snapshot.enabled` | 是否启用增量快照 |
| `scan.incremental.snapshot.chunk.size` | 快照分块大小 |
| `scan.incremental.snapshot.chunk.key-column` | 快照切分列 |
| `scan.startup.mode` | 初始快照、最新位点或特定位点等启动模式；枚举查版本文档 |
| `debezium.*` | 透传给 Debezium 的高级参数，需谨慎 |

---

## 6. server-id：5400-5404 的含义

```yaml
server-id: 5400-5404
```

这是包含首尾的范围：

```text
5400、5401、5402、5403、5404
```

共 5 个候选 ID，不是一个名为“5400-5404”的单值。

ID 数值由使用方规划，本身没有业务含义。约束是：

1. 每个同时连接 MySQL 的 CDC Reader 需要唯一 ID；
2. 不能与同一 MySQL 集群中的其他复制客户端冲突；
3. 多个 CDC Job的范围不能重叠；
4. 范围要能覆盖并行 Reader 数量。

范围大小主要与并行 Reader/连接数量相关，不与表数量一一对应。不是“10 张表就必须 10 个 server-id”。大表会被切成很多 Snapshot Split，Split 再由有限的 Reader 调度执行。

典型示例：

```yaml
source:
  server-id: 5401-5404

pipeline:
  parallelism: 4
```

提供 4 个 ID 供 4 个并行 Reader 使用。生产还要考虑扩容、恢复和其他 Job的统一 ID 规划。

---

## 7. Sink 配置详解

一个 Sink Connector 可以管理多张目标表，例如同一 Doris 集群中的：

```text
ods.orders
ods.products
ods.shipments
```

它们可以结构不同，因为是不同逻辑表。但一个 Sink 管多表不等于一个 YAML 原生同时定义 Doris Sink 和 Kafka Sink。

Sink 常见配置类别：

| 类别 | 内容 |
|---|---|
| Connector | `type`、`name` |
| 连接认证 | FE、Broker、Catalog、用户名和密码 |
| 建表属性 | 副本数、分桶、表模型 |
| 写入参数 | 批量、Flush、重试、一致性 |
| Schema 过滤 | include/exclude schema changes |
| Connector 专属参数 | 必须查对应版本文档 |

---

## 8. Pipeline 作业级参数

| 参数 | 含义 |
|---|---|
| `name` | 提交到 Flink 集群的 Job 名 |
| `parallelism` | Pipeline 全局并行度，默认通常为 1 |
| `local-time-zone` | 作业会话时区 |
| `execution.runtime-mode` | STREAMING 或 BATCH，CDC 通常为 STREAMING |
| `route-mode` | ALL_MATCH 或 FIRST_MATCH，按版本支持 |
| `schema.change.behavior` | exception、evolve、try_evolve、lenient、ignore |
| `schema-operator.rpc-timeout` | 等待下游应用 Schema 事件的超时 |
| `operator.uid.prefix` | 稳定算子 UID 前缀，利于 Savepoint 升级 |
| `schema.operator.uid` | 旧参数，较新版本已弃用 |
| `user-defined-function` | 注册 Transform 中使用的 UDF |

至少要给 `pipeline` 配置一个字段，不能是空对象。

---

## 9. Transform 参数与例子

| 参数 | 含义 |
|---|---|
| `source-table` | 匹配源表，支持正则 |
| `projection` | 类似 SELECT，选列、改名、计算 |
| `filter` | 类似 WHERE，过滤行 |
| `primary-keys` | 指定转换后主键 |
| `partition-keys` | 指定转换后分区键 |
| `table-options` | 传递目标建表属性 |
| `table-options.delimiter` | 属性分隔符 |
| `converter-after-transform` | Transform 后转换事件，如 SOFT_DELETE |
| `description` | 说明文字，无数据逻辑 |

### 9.1 三张不同结构的表分别处理

```yaml
transform:
  - source-table: app_db.orders
    projection: id, user_id, amount, status, op_ts AS source_op_ts
    filter: amount > 0
    primary-keys: id
    description: Normalize orders

  - source-table: app_db.products
    projection: product_id AS id, UPPER(product_name) AS product_name, category_id, price
    filter: price IS NOT NULL
    primary-keys: id
    description: Normalize products

  - source-table: app_db.shipments
    projection: shipment_id AS id, order_id, carrier, tracking_no, shipped_at
    primary-keys: id
    description: Normalize shipments
```

对应路由：

```yaml
route:
  - source-table: app_db.orders
    sink-table: ods_db.orders
  - source-table: app_db.products
    sink-table: ods_db.products
  - source-table: app_db.shipments
    sink-table: ods_db.shipments
```

结果：

```text
orders    → ods_db.orders
products  → ods_db.products
shipments → ods_db.shipments
```

这证明 Pipeline 不仅能做分库分表，也能在同一个 Source/Sink Connector 范围内，将不同结构的表分别同步到不同目标表。

### 9.2 同表多条 Transform 的陷阱

不要把多条 Transform 当作 if/else：

```yaml
transform:
  - source-table: app_db.orders
    filter: province = 'SHANGHAI'
    projection: ...
  - source-table: app_db.orders
    filter: province = 'BEIJING'
    projection: ...
```

较新版本对同一张表通常采用第一条表匹配规则，不能期待第一条 filter 为 false 后自动进入第二条。稳妥做法是同表使用一条 Transform、统一计算分类字段，或者拆 Pipeline/使用 Flink SQL。

---

## 10. metadata.list、op_ts 和元数据

### 10.1 metadata.list 的过程

```yaml
source:
  metadata.list: op_ts
```

可理解为：

```text
数据库日志
→ Source Connector 解析事件
→ 按 metadata.list 提取 op_ts
→ 放入 DataChangeEvent 元数据
→ Transform 引用
→ projection 输出
→ Sink 将其保存为目标列
```

“先声明元数据，后面 Transform 使用；需要落表时再投影输出”的理解总体正确，但有两个限定：

1. `metadata.list` 是 Source Connector 配置，不是 Pipeline 全局元数据注册中心；
2. Transform 还有内置隐藏元数据，未必都要经 `metadata.list`。

### 10.2 op_ts

`op_ts` 是 operation timestamp，即变更发生时间，不是枚举，也不表示 INSERT/UPDATE/DELETE。

初始快照记录并非某一条实时日志变更，官方通常约定其 `op_ts=0`。具体类型可能是 TIMESTAMP_LTZ 或毫秒 BIGINT，取决于 Connector/API/版本。

```yaml
source:
  metadata.list: op_ts

transform:
  - source-table: app_db.orders
    projection: id, amount, op_ts AS source_operation_time
```

### 10.3 元数据有哪些

Transform 内置元数据可能包括：

- namespace_name
- schema_name
- table_name
- data_event_type

实际表达式名称可能是：

```text
__namespace_name__
__schema_name__
__table_name__
__data_event_type__
```

Connector 元数据可能包括：

- op_ts
- table_name
- database_name
- schema_name
- Connector 专属字段

操作类型常见语义：

| 类型 | 含义 |
|---|---|
| INSERT / +I | 插入 |
| DELETE / -D | 删除 |
| UPDATE_BEFORE / -U | 更新前 |
| UPDATE_AFTER / +U | 更新后 |

Pipeline DataChangeEvent 可能用一个 UPDATE 同时携带 before/after；Flink SQL RowKind 可能拆成 -U/+U。要区分 API 层。

---

## 11. primary-keys 不是元数据声明

```yaml
primary-keys: source_database,source_table,id
```

它声明转换后目标逻辑表的复合主键，不是元数据清单。来源字段可能先由元数据生成：

```yaml
projection: >
  __namespace_name__ AS source_database,
  __table_name__ AS source_table,
  id,
  amount

primary-keys: source_database,source_table,id
```

假设：

```text
orders_0: id=100
orders_1: id=100
```

若目标只用 id，记录可能互相覆盖；使用 `source_table,id` 可区分。

如果业务 ID 本来全局唯一，则通常只用 id。判断原则是：

> 合并后的目标范围内，主键是否仍全局唯一？

---

## 12. Route：一对一、多对一、一对多

### 一对一

```yaml
route:
  - source-table: app_db.orders
    sink-table: ods_db.orders
```

### 多个分表合并为一张表

```yaml
route:
  - source-table: app_db\.orders_[0-9]+
    sink-table: ods_db.orders
```

要求分片 Schema 可兼容，主键在合并范围内唯一，并排除临时表。

### 多张不同表分别写多张目标表

```yaml
route:
  - source-table: app_db.orders
    sink-table: ods_db.orders
  - source-table: app_db.products
    sink-table: ods_db.products
```

### 一张源表写多张目标表

支持 `route-mode: ALL_MATCH` 的版本可以这样配置：

```yaml
route:
  - source-table: app_db.orders
    sink-table: ods_db.orders
  - source-table: app_db.orders
    sink-table: audit_db.orders_copy

pipeline:
  name: duplicate-orders
  parallelism: 2
  route-mode: ALL_MATCH
```

含义：

```text
app_db.orders ─┬→ ods_db.orders
               └→ audit_db.orders_copy
```

`FIRST_MATCH` 只采用第一条匹配 Route。两张目标表仍由同一个 Sink Connector 管理，不等于同时写两个不同 Sink 系统。

---

## 13. 分库分表与不同结构多表

分库分表：

```text
db_0.orders_00
db_0.orders_01
db_1.orders_00
db_1.orders_01
```

物理上是 4 张表，业务上是一张“订单”逻辑表，可合并到 `ods.orders`。

但不能因此认为 Pipeline 只能处理“一张逻辑表到另一张逻辑表”。它也可以同时处理 orders、products、shipments 等不同结构表，只要分别 Transform 和 Route。

不同结构表要合并到同一目标，必须先统一 Schema 和业务语义：

```yaml
transform:
  - source-table: db_a.orders
    projection: order_id AS id, buyer_id AS user_id, amount, 'db_a' AS source
    primary-keys: source,id

  - source-table: db_b.trade_order
    projection: id, user_id, total_price AS amount, 'db_b' AS source
    primary-keys: source,id

route:
  - source-table: db_a.orders
    sink-table: ods.orders
  - source-table: db_b.trade_order
    sink-table: ods.orders
```

字段改成一致不代表业务语义一定一致，必须先完成数据建模，并验证版本的 Schema 合并能力。

---

## 14. 带条件同步：UPDATE 跨过滤边界

设：

```yaml
filter: status = 'PAID'
```

目标表语义应是“源表当前满足条件的动态结果集”，不能只看每条事件的新值。

| before | after | 正确下游事件 |
|---|---|---|
| PAID | PAID | UPDATE |
| CREATED | CANCELLED | 不输出 |
| CREATED | PAID | INSERT |
| PAID | CANCELLED | DELETE |

### PAID → CANCELLED

目标此前已有 `id=100,status=PAID`。源端执行：

```sql
UPDATE orders
SET status = 'CANCELLED'
WHERE id = 100;
```

如果只是忽略新值，目标会永久残留旧 PAID 记录。正确做法是 DELETE。

### CREATED → PAID

目标此前没有该记录。它第一次进入结果集，对目标应是 INSERT，而不是简单保留源端 UPDATE 类型。

### 版本差异

Flink CDC 3.5 及以前的相关实现只对 UPDATE 的 after 应用 Transform filter，可能造成：

- PAID → CANCELLED 被丢弃，目标残留；
- CREATED → PAID 仍按 UPDATE 发送，而目标没有该行。

FLINK-39230 描述了这个问题。Flink CDC 3.6.0 的相关修改处理了 before/after 跨边界：

- before 满足、after 不满足 → DELETE；
- before 不满足、after 满足 → INSERT。

即使使用 3.6+，仍需验证 Source before image、目标主键以及 Sink 的 UPDATE/DELETE 能力。

---

## 15. Schema Evolution 详细配置

Schema Change Event 常见枚举：

| 枚举 | 含义 |
|---|---|
| `create.table` | 创建表 |
| `add.column` | 增加列 |
| `alter.column.type` | 修改列类型 |
| `rename.column` | 重命名列 |
| `drop.column` | 删除列 |
| `truncate.table` | 清空表 |
| `drop.table` | 删除表 |

这些是框架支持的枚举，不是用户任意命名。部分版本支持 `column`、`drop` 等类别匹配，但生产配置建议显式列出完整事件。

```yaml
sink:
  include.schema.changes:
    - create.table
    - add.column
    - alter.column.type
    - rename.column
  exclude.schema.changes:
    - drop.column
    - drop.table
    - truncate.table
```

exclude 优先级高于 include。

### schema.change.behavior

#### exception

遇到 Schema 变化就失败。适用于下游 DDL 必须人工审核的环境。

#### evolve

严格尝试同步上游变化。Sink 不支持、类型不兼容或权限不足时可能失败。

#### try_evolve

尝试应用并进行一定容错，但不是“永不失败”；实际降级行为依赖版本和 Sink。

#### lenient

用更宽松方式保持可写，例如不物理删除旧列、把重命名转换成兼容操作等。各 Sink 行为不同。

#### ignore

忽略 Schema 事件。但新数据若已包含新字段，目标表没有对应列，写入仍可能失败。因此 ignore 不等于安全。

---

## 16. gh-ost、pt-online-schema-change

普通开发环境可能执行：

```sql
ALTER TABLE orders ADD COLUMN coupon_id BIGINT;
```

生产在线 DDL 工具通常会：

```text
1. 创建新结构的影子表
2. 复制历史数据
3. 同步增量变化
4. 通过 RENAME 完成切换
5. 清理旧表和辅助对象
```

pt-osc 常依赖触发器；gh-ost 主要读取 binlog。

CDC 可能看到：

```text
CREATE 临时表
临时表大量 INSERT/UPDATE
RENAME 原表
RENAME 影子表为正式表
DROP 旧表
```

如果正则过宽，`_orders_gho`、`orders_new`、`orders_old` 等也可能被采集并命中正式 Route，造成重复写、Schema 冲突或错误 Drop。

上线前：

1. 明确实际在线 DDL 工具和临时表规则；
2. 用 `tables.exclude` 排除临时表、旧表；
3. 避免无边界 `.*`；
4. 在测试库运行完整 gh-ost/pt-osc，而非只测普通 ALTER；
5. 观察完整 Schema/Data 事件序列；
6. 检查 Route 是否误合并影子表；
7. 对 drop、truncate 保持谨慎；
8. 准备 Savepoint、回滚和目标修复方案。

---

## 17. 调度与部署

CDC Pipeline 通常是长期运行的 Streaming Job：

```text
启动 → 初始快照 → binlog/WAL → 持续运行
                  ↓
             Checkpoint 保存状态
                  ↓
               失败恢复
```

通常使用类似命令提交：

```bash
bin/flink-cdc.sh mysql-to-doris.yaml
```

并可通过参数增加 Connector/UDF JAR。作业可提交到 Standalone、YARN、Kubernetes 或企业 Flink 平台。

| 层级 | 负责内容 |
|---|---|
| Flink Scheduler | 算子、Subtask、Slot、并行度、失败恢复 |
| CDC Source | Snapshot Split 和日志读取 |
| YARN/Kubernetes | 进程、容器和资源 |
| Airflow/DolphinScheduler | 定时启动、依赖、补数、审批 |
| Pipeline YAML | 描述作业，不提供 Cron 调度 |

### Checkpoint

保存 Source 位点、快照进度、有状态算子和 Sink 一致性状态，用于自动故障恢复。

### Savepoint

用于人工升级、迁移和回滚。`operator.uid.prefix` 有助于新作业重新映射旧状态。

---

## 18. 并行度、表数和 server-id

```text
多张表
  → 每张表切成多个 Snapshot Split
  → Split 分配给有限数量 Reader
  → 每个并行 Reader 使用唯一 server-id
```

因此：

- 表数量不是 server-id 数量；
- 一张大表也可能产生很多 Split；
- 提高 `pipeline.parallelism` 不保证线性加速；
- Snapshot 可并行，binlog 增量读取可能有拓扑限制；
- Sink 写入、主键冲突和小批量也可能成为瓶颈。

---

## 19. 容易遗漏的生产问题

### 初始快照与增量衔接

检查全量 + 增量模式、快照期间日志回放、Checkpoint 恢复、连接超时和 binlog 保留时间。

### 主键

检查 Source 是否有主键、Transform 是否保留主键、主键是否被修改、多分片合并后是否全局唯一、Sink 是否为主键/Unique Key 模型。

### 时区

同时核对 MySQL server-time-zone、Pipeline local-time-zone、JVM 时区、Sink 会话时区及 TIMESTAMP/TIMESTAMP_LTZ。错误时区可能整体偏移 8 小时而不报错。

### Exactly-once

端到端一致性依赖 Checkpoint、Source 位点、Sink 两阶段提交或幂等写和主键。Sink 只有至少一次且无幂等主键时，恢复可能产生重复。

### 删除

确认 Sink 是否支持物理 DELETE，是否要软删除，是否保存操作类型，以及 filter 跨边界 DELETE 能否落地。

### 规则重叠

列出每张实际表命中的 Source、Transform、Route 规则，检查 Transform 首条匹配和 Route ALL_MATCH/FIRST_MATCH，防止意外重复写。

### 故障域

一个 Pipeline 同步很多表时，一张表 DDL 失败可能拖垮整个 Job。重要性、DDL 频率、吞吐、权限不同的表应考虑拆 Job。

---

## 20. 端到端测试清单

### 数据事件

1. INSERT；
2. UPDATE 非主键；
3. UPDATE 主键；
4. DELETE；
5. 相同主键重复写；
6. NULL 转换；
7. 大字段和特殊字符；
8. 时间与时区；
9. DECIMAL 精度；
10. 失败并从 Checkpoint 恢复。

### Filter 状态迁移

1. PAID → PAID；
2. CREATED → CANCELLED；
3. CREATED → PAID；
4. PAID → CANCELLED。

### Schema 事件

1. CREATE TABLE；
2. ADD COLUMN nullable；
3. ADD COLUMN not null with default；
4. ALTER COLUMN TYPE；
5. RENAME COLUMN；
6. DROP COLUMN；
7. TRUNCATE TABLE；
8. DROP TABLE；
9. gh-ost/pt-osc 完整流程；
10. 多分片错峰执行同一 DDL。

### 分库分表

- 相同 ID 出现在不同分片；
- 分片字段顺序不同；
- 某分片多一列；
- 动态新增分片；
- 分片重命名；
- 正则是否命中新分片；
- 来源库/表元数据是否正确落列。

---

## 21. 对话问题逐项结论

### `5400-5404` 是一个还是多个？

多个，是 5400 到 5404，共 5 个。

### server-id 是自己定义的吗？

数值由使用方规划，核心是 MySQL 集群范围内唯一，并且范围容量能覆盖并行 Reader。

### server-id 与并行度还是表数有关？

主要与并行 Reader/连接数有关，不与表数一一对应。

### `op_ts` 是枚举吗？

不是，是操作时间。INSERT、UPDATE、DELETE 才属于操作类型。

### 元数据是不是先声明再在 Transform 使用？

Connector 专属元数据通常如此；Transform 内置元数据可能无需 `metadata.list`。要落表通常需 projection 输出。

### 元数据有哪些？

包括来源库、Schema、表名、操作时间、数据事件类型等；精确字段按 Connector 和版本查文档。

### `primary-keys: source_database,source_table,id` 是元数据吗？

不是，是复合主键；其中来源字段可能由元数据投影产生。

### description 是注释吗？

不是 YAML 注释，是说明字段；通常只有可读性作用，不影响数据。

### include/exclude 的事件名是枚举吗？

是框架支持的 Schema Change Event 类型，不能随意自定义。

### Pipeline 是匹配 Source 与 Sink 吗？

不是。Pipeline 块管作业级参数；Transform 管行列；Route 管表映射。

### 能否一张源表写多张目标表？

支持 ALL_MATCH 的版本可复制到同一 Sink Connector 管理的多张目标表；不同 Sink 系统一般拆 Job。

### Pipeline 是否只适合分库分表？

不是。它能把多张结构不同的源表分别同步到多张结构不同的目标表。分片合并只是 Route 的典型场景。

### 带条件同步 PAID → CANCELLED 为什么要 DELETE？

因为目标此前已有该 PAID 记录。它离开过滤结果集时若不删除，目标会残留脏数据。

### gh-ost、pt-osc 为什么特殊？

它们通过影子表、数据复制和 RENAME 切换完成 DDL，CDC 看到的是一串表和数据事件，不是单条 ALTER。

### 条件跨边界处理实现了吗？

Flink CDC 3.5 及以前存在只判断 after 的问题；相关逻辑在 3.6 中处理。生产仍需端到端验证 Sink 主键和 DELETE/UPDATE 能力。

---

## 22. 最终判断原则

1. 先固定精确版本，再写 YAML。
2. 分清物理表、逻辑表、Connector 和 Pipeline。
3. Source 管“从哪里读”，Transform 管“行列怎么变”，Route 管“表到哪里”，Pipeline 管“作业怎么运行”。
4. 分片合并前先统一 Schema 和全局主键。
5. 不同结构表可以分别同步，不等于必须合并。
6. 一个 Source 多表不等于多个异构 Source。
7. 所有 filter 都要测试 UPDATE 跨条件边界。
8. 自动 DDL 要同时验证 Source、策略、白名单、Sink 和目标权限。
9. gh-ost/pt-osc 必须用真实工具完整演练。
10. 上线前验证 Checkpoint 恢复、重复、删除、时区和 Schema 故障。
11. 正则必须可审计。
12. 同一 Pipeline 表越多，故障域越大，必要时拆 Job。

---

## 23. 官方资料

- [Flink CDC 3.6 Data Pipeline](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.6/docs/core-concept/data-pipeline/)
- [Flink CDC 3.6 Data Source](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.6/docs/core-concept/data-source/)
- [Flink CDC 3.6 Route](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.6/zh/docs/core-concept/route/)
- [Flink CDC Transform](https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/core-concept/transform/)
- [Flink CDC MySQL Pipeline Connector](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.6/docs/connectors/pipeline-connectors/mysql/)
- [Flink CDC Schema Evolution](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.4/docs/core-concept/schema-evolution/)
- [FLINK-39230：Transform filter 的 UPDATE before/after 问题](https://www.mail-archive.com/dev%40flink.apache.org/msg85285.html)
- [Apache Flink CDC 3.6.0 Release Announcement](https://flink.apache.org/2026/03/30/apache-flink-cdc-3.6.0-release-announcement/)

