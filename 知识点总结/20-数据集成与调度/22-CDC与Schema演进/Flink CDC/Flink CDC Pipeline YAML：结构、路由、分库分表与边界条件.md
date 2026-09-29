# Flink CDC Pipeline YAML：结构、路由、分库分表、元数据与边界条件

> 适用范围：以 Flink CDC Pipeline YAML 3.x 为主。配置项会随版本和具体 Source/Sink Connector 变化；使用时必须查对应版本文档，不能把文章示例当成所有版本均支持的固定语法。

## 1. 核心认知：Pipeline 到底是什么

Flink CDC 中的 Data Pipeline 是一整个数据同步作业及其算子链，通常为：

```text
Source（采集快照和变更日志）
  → Transform（字段投影、计算、过滤）
  → Route（决定逻辑表写到哪个目标表）
  → Schema Operator（协调表结构变化）
  → Sink（建表、改表、写入 INSERT/UPDATE/DELETE）
```

YAML 的顶层通常包含：

- `source`：从哪个外部系统读取以及读取哪些表，必填。
- `sink`：写入哪个外部系统，必填。
- `transform`：字段选择、字段计算、过滤、目标主键等，可选。
- `route`：源逻辑表 ID 到目标逻辑表 ID 的映射，可选。
- `pipeline`：整个 Flink 作业的名称、并行度、时区、运行模式、Schema 演进策略等作业级配置，必填且不能是空对象。

必须纠正的误解：`pipeline:` 配置块本身不负责匹配 Source 数据和 Sink 表；匹配关系主要由 `route` 完成，字段形态由 `transform` 完成。`pipeline` 既可指整个 ETL 作业，也可指 YAML 中的作业级参数块，需要结合语境区分。

## 2. YAML 使用基础

- 使用空格缩进，不能用 Tab；同一层级缩进一致。
- `key: value` 表示键值；`-` 表示列表项。
- 正则表达式中的点是任意字符，匹配数据库与表名中的字面点时要按文档示例转义，例如 `app_db\.orders_.*`。
- 布尔值、数字和字符串类型要符合 Connector 文档；不确定时不要仅凭 YAML 能解析就认为 Connector 能接受。
- `# comment` 才是 YAML 注释，会被解析器忽略。
- `description: ...` 不是注释，而是框架认识的可选配置字段；通常只提升规则可读性，并可能出现在配置对象、日志或展示信息中，不参与路由、过滤和数据计算。具体显示位置随版本和实现变化，不能依赖它实现业务逻辑。

## 3. 通用配置骨架与参数职责

```yaml
source:
  type: mysql
  name: mysql-source
  hostname: mysql.example.com
  port: 3306
  username: cdc_user
  password: xxx
  tables: app_db\.orders_[0-9]+
  tables.exclude: app_db\._.*          # 可选，避免临时表
  server-id: 5400-5408
  server-time-zone: Asia/Shanghai
  metadata.list: op_ts

sink:
  type: doris
  name: doris-sink
  fenodes: doris-fe:8030
  username: writer
  password: xxx
  include.schema.changes: [create.table, add.column, alter.column.type, rename.column]
  exclude.schema.changes: [drop.column, drop.table, truncate.table]

transform:
  - source-table: app_db\.orders_[0-9]+
    projection: "*, __table_name__ AS source_table, op_ts"
    filter: "status = 'PAID'"
    primary-keys: source_table,id
    description: filter and enrich order records

route:
  - source-table: app_db\.orders_[0-9]+
    sink-table: ods_db.orders
    description: merge order shards

pipeline:
  name: mysql-orders-to-doris
  parallelism: 4
  local-time-zone: Asia/Shanghai
  execution.runtime-mode: STREAMING
  schema.change.behavior: try_evolve
  operator.uid.prefix: mysql-orders-v1
```

这只是结构示例，不是可直接照抄的万能配置。Source 和 Sink 下绝大多数参数都是 Connector 专属参数，例如 MySQL、PostgreSQL、Doris、Kafka、Paimon 的必填项完全不同。正确做法是先确定 Flink CDC 版本、Source 类型和 Sink 类型，再查这三个版本完全对应的文档。

### pipeline 常见作业级参数

- `name`：提交到 Flink 集群的 Job 名称。
- `parallelism`：Pipeline 全局并行度，默认通常为 1；实际某些算子可能受 Connector 能力、分区数或显式配置限制。
- `local-time-zone`：作业会话时区，影响时间函数和部分时间类型转换。
- `execution.runtime-mode`：`STREAMING` 或 `BATCH`；CDC 通常是长期运行的 STREAMING。
- `route-mode`：3.6 等版本可配置 `ALL_MATCH` 或 `FIRST_MATCH`。前者让一个源表应用所有匹配路由，后者只用第一条匹配路由。
- `schema.change.behavior`：Schema 变更总策略，常见枚举为 `exception`、`evolve`、`try_evolve`、`lenient`、`ignore`；语义见第 8 节。
- `schema-operator.rpc-timeout`：Schema Operator 等待下游应用 SchemaChangeEvent 的超时时间。
- `operator.uid.prefix`：为算子生成稳定 UID 的前缀，有利于基于 Savepoint 的有状态升级和 Flink UI 排障。
- `schema.operator.uid`：旧版本参数，较新版本已弃用，优先用 `operator.uid.prefix`。
- `user-defined-function`：按版本注册 UDF，供 transform 表达式调用。

## 4. server-id：5400-5404 究竟表示什么

`server-id: 5400-5404` 是一个包含两端的整数范围，即候选 ID 为 5400、5401、5402、5403、5404，共 5 个；不是一个名叫“5400-5404”的单一 ID。

MySQL CDC Reader 以类似 MySQL replication client/server 的身份连接 MySQL，每个同时运行的 Reader 必须使用不同的 server-id，并且还不能和同一 MySQL 集群里的其他复制客户端冲突。因此：

- ID 数值可以由使用方规划，本身没有固定业务含义；核心约束是 MySQL 集群范围内当前运行实例唯一。
- 范围大小主要与并行 Reader/连接需求有关，不直接由同步表数量决定。
- 表数量会影响快照 Split 数量，但不是“一张表对应一个 server-id”。
- 开启增量快照并并行读取时，应提供足够大的范围；官方建议范围容量至少覆盖并行 Reader，生产上还应留余量。
- 同一个 MySQL 集群上的多个 CDC Job 不能复用重叠的 server-id 范围。
- 单值写法如 `server-id: 5400` 只提供一个 ID，通常不适合需要多个并行 Reader 的配置。

## 5. 元数据、op_ts 与 primary-keys

### metadata.list 是什么

`metadata.list` 是 Source Connector 的配置，用来要求 Connector 从 SourceRecord/变更事件中解析并携带指定元数据。它不是 Pipeline 的全局枚举，也不是 Transform 自动生成的字段。可选值由具体 Connector 和版本决定。

正确理解可以概括为：

1. Source 先通过 `metadata.list` 声明要读取哪些 Connector 元数据；
2. Transform 中才能引用这些字段进行投影、过滤或计算；
3. 如果希望把元数据实际落入目标表，需要在 `projection` 中将其输出，或者采用该版本支持的直接透传方式；
4. 只声明但不投影/不透传，通常不等于一定会成为目标表的物理列。

### op_ts 是什么

`op_ts` 是 operation timestamp，即数据库中变更事件发生的时间，不是操作类型枚举。MySQL/PostgreSQL 等 Connector 对它的具体数据类型可能不同，必须看对应版本文档。快照阶段的历史记录不是从实时日志变更产生的，官方文档通常约定其 `op_ts` 为 0。

操作类型是另一类元数据，例如 `data_event_type`、`__data_event_type__` 或 SQL Connector 中的 `row_kind`，名称和可用值随 API 层和版本变化。常见事件语义包括 INSERT、DELETE、UPDATE_BEFORE、UPDATE_AFTER，但不要把它们和 `op_ts` 混淆。

Flink CDC Transform 还可能提供内置隐藏元数据字段，例如 namespace/schema/table 名和 data event type；字段名称在不同版本文档中可能带双下划线。Connector 元数据与 Transform 内置元数据存在重叠，例如 table_name 既可能由 Connector 暴露，也可能通过 Transform 隐藏字段取得。

### primary-keys 不是元数据声明

```yaml
primary-keys: source_database,source_table,id
```

它是在 Transform 结果 Schema 中指定下游逻辑表的主键，不是“声明元数据”。其中 `source_database`、`source_table` 之所以看起来像元数据，是因为它们可能先通过投影由数据库名、表名元数据生成，然后被纳入复合主键。

分库分表合并时，多个分片可能都有 `id=1`。如果源端 ID 不是全局唯一，可以使用 `source_database + source_table + id` 区分；如果业务 ID 本来就是全局唯一，则不应机械加入来源字段，否则会改变下游数据模型和去重语义。

## 6. Transform 的能力与边界

常见参数：

- `source-table`：此 Transform 规则匹配的源逻辑表 ID，支持正则。
- `projection`：类似 SQL SELECT 列表，完成选列、改名、常量列、表达式和元数据落列。
- `filter`：类似 SQL WHERE，只让满足条件的数据进入该 Transform 结果。
- `primary-keys`：覆盖/指定转换后逻辑表主键。
- `partition-keys`：指定转换后表的分区键，是否生效还依赖 Sink。
- `table-options` 与 `table-options.delimiter`：把建表属性传给支持的 Sink。
- `converter-after-transform`：较新版本可在 Transform 后改变 DataChangeEvent，例如 `SOFT_DELETE`。
- `description`：人类可读说明，无数据语义。

多个结构不同的源表可以在同一个 YAML 中分别写多条 Transform 和 Route，分别同步到多张不同目标表。例如 orders、shipments、products 可各自投影为不同结构，再分别路由。因此“Pipeline 只能处理分库分表，本质永远是一张逻辑表到另一张逻辑表”并不完整。

更准确的判断是：

- 每一条具体 DataChangeEvent 始终属于某个源逻辑表，并最终写入一个或多个目标逻辑表；
- 一个 Pipeline 可以同时承载多张彼此无关、结构不同的表；
- 每张表可以有独立的 Transform 和 Route；
- 分库分表合并只是 Route 最典型的使用场景，不是唯一场景；
- 多张结构不同的表不能不经设计就合并为同一张目标表，合并前必须解决字段、类型、主键和 Schema 演进兼容性；
- 一个 YAML 通常只有一个 `source` Connector 实例和一个 `sink` Connector 实例。它可以从该 Source 实例读取多表，也可由该 Sink 实例写多表，但这不等于一个 YAML 原生连接多个异构 Source 系统或多个不同 Sink 系统。异构多 Source、多 Sink 通常拆成多个 Pipeline，或改用 Flink SQL/DataStream 自定义拓扑。

### 3.6 的 Transform 首条匹配行为

较新版本中，一张表命中多条 Transform 规则时通常只应用第一条按表匹配的规则，不能把多条 Transform 当成 if/else，并期待第一条 filter 为 false 后自动落到第二条。规则顺序因此具有业务含义；应尽量让同一张表只有一条清晰 Transform，复杂分类使用更明确的作业设计。

## 7. Route、多表与一表多下游

Route 的职责是把源 Table ID 映射为目标 Table ID：

```yaml
route:
  - source-table: mydb\.orders_[0-9]+
    sink-table: ods.orders
```

这是典型的“多个物理分表 → 一张目标逻辑表”。

同一个 YAML 也可以定义多组独立映射：

```yaml
route:
  - source-table: mydb.orders
    sink-table: ods.orders
  - source-table: mydb.products
    sink-table: ods.products
```

这就是“多张不同表 → 多张不同目标表”，并非只能合并分片。

“一张源表写多张目标表”表示同一个上游表的事件被复制到多个下游逻辑表。在支持 `route-mode: ALL_MATCH` 的版本中，如果同一源表同时命中多条 Route，所有规则都可应用；`FIRST_MATCH` 则只采用第一条。这里的“多个下游”仍由同一个 Sink Connector 实例管理，例如同一个 Doris 集群的多张表，不等价于同时写 Doris 和 Kafka 两种 Sink。

使用一表多目标时要验证：

- 每个目标表的 Schema 是否兼容；
- 下游是否都支持 DELETE/UPDATE；
- 一条路由失败是否会拖累整个 Job；
- Schema ChangeEvent 是否会同时传播到全部目标；
- 是否会因为重叠正则意外重复写入。

## 8. 表结构变更（Schema Evolution）

CDC 不只传数据事件，也可以传 SchemaChangeEvent。常见事件类型是固定枚举：

- `create.table`
- `add.column`
- `alter.column.type`
- `rename.column`
- `drop.column`
- `drop.table`
- `truncate.table`

`include.schema.changes` 和 `exclude.schema.changes` 通常配置在 `sink` 块中，用于选择允许传给 Sink 的 Schema 事件；`exclude` 优先级高于 `include`。部分版本还支持按类别部分匹配，例如 `column` 或 `drop`，但生产配置应优先写清晰的完整事件名，并核对目标版本。

`pipeline.schema.change.behavior` 决定整体处理策略：

- `exception`：遇到 Schema 变化直接失败，最保守。
- `evolve`：要求把上游 Schema 变化应用到下游；Sink 不支持时可能失败。
- `try_evolve`：尝试应用，不支持时尽可能容错，但必须根据版本确认具体降级行为。
- `lenient`：通过更宽松的方式保持数据可写，例如不物理删除旧列、把重命名转为兼容操作等；准确行为还取决于 Sink。
- `ignore`：忽略 Schema 事件；如果随后数据事件已经带新字段，仍可能因为上下游 Schema 不一致而写入失败。

只配置 include/exclude 并不能保证目标数据库真的支持该 DDL。最终结果取决于 Source 是否能捕获、Pipeline 策略是否允许、Sink Connector 是否实现、目标系统是否允许，以及账号 DDL 权限。

## 9. gh-ost、pt-online-schema-change 为什么特殊

生产 MySQL 常用 gh-ost 或 pt-online-schema-change 执行在线变更，而不是直接对原表执行单条 `ALTER TABLE`。这些工具的本质通常是：

1. 创建一张带新结构的影子/临时表；
2. 复制原表历史数据；
3. 将增量变更持续同步到影子表（pt-osc 常用触发器，gh-ost 主要读取 binlog）；
4. 在切换阶段通过 RENAME 让影子表替代原表；
5. 删除或保留旧表及辅助对象。

因此 CDC 看到的可能不是“orders 加了一列”，而是一串 create/rename/drop 事件，还可能采集到临时表的数据。如果 `tables` 正则写得过宽，影子表也可能被当作业务表同步，造成重复表、错误路由、Schema 冲突甚至误传播 drop。

上线前应：

- 明确 gh-ost/pt-osc 的临时表命名规则；
- 使用 `tables.exclude` 或精确正则排除临时表、旧表；
- 在测试环境完整演练 create → copy → cutover → cleanup；
- 检查 Source 对 rename/drop 的捕获方式和 Sink 对相应事件的支持；
- 确认 route 的正则不会把影子表合并进正式目标表；
- 不要仅凭普通 `ALTER TABLE` 测试通过就认定在线变更工具也安全。

## 10. 带条件同步最容易忽略的问题：UPDATE 跨越过滤边界

设 Transform 条件：

```yaml
filter: status = 'PAID'
```

目标表的正确语义应是“源表当前满足条件的动态结果集”，而不是“只把当下满足条件的变更事件留下”。对于 UPDATE，必须同时判断 before 和 after：

| before 是否 PAID | after 是否 PAID | 正确下游事件 |
|---|---|---|
| 是 | 是 | UPDATE |
| 否 | 否 | 不输出 |
| 否 | 是 | INSERT |
| 是 | 否 | DELETE |

关键案例：

```sql
UPDATE orders
SET status = 'CANCELLED'
WHERE id = 100;
```

假设目标表已有 `id=100,status=PAID`。更新后记录不再满足过滤条件，正确结果不是“忽略这个 UPDATE”，而是从目标表删除旧记录。否则目标表会永久残留一条已不满足条件的脏数据。

版本差异：

- Flink CDC 3.5 及以前的相关实现只对 UPDATE 的 `after` 应用过滤条件，可能出现 PAID → CANCELLED 被直接丢弃、CREATED → PAID 仍按 UPDATE 发出的语义问题。
- FLINK-39230 专门记录了该缺陷；Flink CDC 3.6.0 合入相关修复，按 before/after 跨边界生成 DELETE 或 INSERT。
- 即使使用 3.6+，仍需端到端验证 Sink 的主键、UPDATE/DELETE 支持和实际 Connector 版本，不能只验证 Pipeline 中间事件。
- 如果 Source 不能提供完整 before image，框架可能无法无状态地判断边界变化，需要额外状态或 Source 配置支持。

## 11. 调度与运行方式

CDC Pipeline 通常是持续运行的 Streaming Job，不是每隔几分钟启动一次的传统离线任务。常见流程是：

1. 使用 `flink-cdc.sh <pipeline.yaml>` 等命令把 YAML 编译为 Flink Job；
2. 提交到 Standalone、YARN 或 Kubernetes；
3. Source 先读取初始快照，再持续消费 binlog/WAL；
4. Flink Checkpoint 保存读取位点和算子状态；
5. 失败后由 Flink 或部署平台按重启策略恢复。

因此“调度”要区分：

- 作业内并行调度：由 Flink 调度 Source、Transform、Schema、Sink 算子和 Subtask。
- 长期运行任务的部署/拉起：由 Flink Standalone、YARN、Kubernetes Operator 等管理。
- 定时启动、停止、补数、依赖编排：需要 Airflow、DolphinScheduler、企业调度平台或 Kubernetes CronJob 等外部系统；Pipeline YAML 自身不是 Cron 调度器。
- 一次性全量批任务可考虑 BATCH，但是否支持以及能否与 CDC 增量语义配合，要看 Connector 和版本。

生产环境至少配置和验证：Checkpoint 存储与周期、重启策略、Savepoint 升级、资源与并行度、监控告警、日志、Source 权限、Sink 幂等/Exactly-once 能力、Schema 变更超时。

## 12. 分库分表设计检查清单

### 多分表合并为一张表

- 各分片字段名、类型、可空性是否一致；
- 主键是否全局唯一；
- 是否需要把来源库/表作为普通列和复合主键；
- 多分片 DDL 是否同步执行，过渡期 Schema 是否兼容；
- 正则是否误匹配临时表；
- 多 Source Reader 的 server-id 范围是否足够且唯一；
- 下游写入是否可能出现同主键覆盖。

### 多张不同结构表分别同步

- 为每张表定义明确的 Transform 和 Route；
- 不要求不同表转换后 Schema 彼此一致，只要求每张源表与自己的目标表兼容；
- 规则顺序不能产生意外命中；
- Sink Connector 必须能管理多张目标表；
- 一张表的 DDL 失败是否会导致整个 Job 失败，需要按作业隔离需求决定是否拆 Pipeline。

### 多源与多 Sink 的判定

- “一个 Source 读取多张表”不等于“多个 Source Connector”；
- “一个 Sink 写多张表”不等于“多个不同 Sink 系统”；
- 同一 MySQL Connector 连接范围内的多库多表可以由一条 Source 规则订阅；
- 不同 MySQL 集群、MySQL + PostgreSQL、Doris + Kafka 等异构拓扑，通常拆分多个 Pipeline 或使用 Flink SQL/DataStream；
- 是否拆 Job还要考虑故障域、吞吐、Schema 风险、升级频率和权限边界。

## 13. 最终判断原则

1. 先确定 Flink CDC 精确版本，再讨论 YAML 参数和运行语义。
2. 先区分“Connector 实例”“逻辑表”“物理分片”“外部系统”，避免把多表误叫多 Source。
3. `pipeline` 管作业级行为，`transform` 管行和列，`route` 管表 ID 映射，`source/sink` 管外部系统连接与 Connector 能力。
4. 分库分表合并要求 Schema 和主键可统一；结构不同的表可以在同一 Pipeline 内分别处理，但不能无设计地合并。
5. 带 filter 的 CDC 必须测试 UPDATE 前后跨条件边界，不能只测试 INSERT。
6. DDL 能否同步由 Source 捕获、Pipeline 策略、事件白名单、Sink 能力、目标系统和账号权限共同决定。
7. gh-ost/pt-osc 要按完整在线变更事件序列测试，不能等同于普通 ALTER。
8. 一切生产结论以对应版本官方文档、端到端事件测试和目标 Sink 实际行为为准。

## 14. 版本核验参考

- Flink CDC 3.6 Data Pipeline：https://nightlies.apache.org/flink/flink-cdc-docs-release-3.6/docs/core-concept/data-pipeline/
- Flink CDC 3.6 MySQL Pipeline Connector：https://nightlies.apache.org/flink/flink-cdc-docs-release-3.6/docs/connectors/pipeline-connectors/mysql/
- Route：https://nightlies.apache.org/flink/flink-cdc-docs-release-3.6/zh/docs/core-concept/route/
- Transform：https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/core-concept/transform/
- Schema Evolution：https://nightlies.apache.org/flink/flink-cdc-docs-release-3.4/docs/core-concept/schema-evolution/
- FLINK-39230 过滤 UPDATE 边界问题：https://www.mail-archive.com/dev%40flink.apache.org/msg85285.html

