# Paimon CDC 入湖、Schema Evolution 与恢复语义

> 版本与验证边界：2026-11-07 补充端到端演练与 DDL 对照。表存储规则参考 Paimon 2.0 文档，Source 示例参考 Flink CDC 3.1 MySQL 文档；两者分别用于说明表能力与 Source 配置，不能据此推定是一套已认证的依赖组合。本地已有环境是 Flink 1.18.1 + Paimon 1.3.1，本文新增 CDC、DDL 与恢复步骤未执行。

## 结论

CDC 入湖是否正确，要同时检查四层：Source 是否读取了完整变更，传输格式是否保留主键和 Before/After，Paimon Sink 如何提交，目标表的 Merge Engine 如何解释重复、乱序和删除。

Paimon CDC Action 可以自动建表并执行一部分 Schema Evolution，但它不是数据库 DDL 的完整复制工具。新增列和兼容的类型扩大可以传播；删除列、重命名列、重命名表、主键和分区键变更不能按“源端改了，目标自动完全一致”理解。

## 1. 先区分四条入湖路径

| 路径 | Schema 从哪里来 | 适用场景 | 需要特别检查 |
| --- | --- | --- | --- |
| Flink SQL `INSERT INTO` | SQL 中声明 | 自定义转换、已有 Flink 作业 | 源新增列不会自动改 SQL 和目标表 |
| Paimon CDC Action | Source 元数据与事件 | 单表或整库同步 | Action 参数、路由、支持的演进范围 |
| Flink CDC YAML Pipeline | Pipeline 定义与 Connector | Flink CDC Pipeline 体系 | 使用该体系自己的 Schema Change 规则 |
| Kafka/Pulsar CDC Action | 消息格式中的 Schema 和变更 | 已有 CDC 消息总线 | Format 是否真的携带类型、主键和删除 |

Paimon Action 的 `--table_conf`、`--catalog_conf` 等参数属于 Paimon Action CLI，不能原样当作 Flink CDC YAML 的配置。普通 JSON 通常只表达 Insert-only；Canal、Debezium、Maxwell、OGG 等格式能表达哪些变更，也取决于消息实际包含的字段。

## 2. 初始快照与增量日志如何衔接

数据库 CDC Source 通常先读取已有数据，再持续读取 binlog/WAL。正确衔接要求 Source 在一致性边界上记录日志位点，并把位点纳入 Flink checkpoint。Paimon 不会在 Sink 端补救 Source 漏读的日志区间。

验证时不要只看“初始数据条数正确”。至少要覆盖：初始快照期间发生 UPDATE/DELETE、Source 切换到增量后是否重复或漏失、故障恢复是否从 checkpoint 位点继续。

Source 侧的一致性快照算法、启动模式和 Connector 版本属于 Flink CDC 或源 Connector 的能力，不由 Paimon 表参数决定。Paimon 负责接收已经产生的变更并在 checkpoint 成功时提交 Snapshot。

## 3. Checkpoint 与 Paimon Snapshot

Flink 流式写 Paimon 时，完成的 checkpoint 驱动提交，数据只有进入已提交的 Paimon Snapshot 后才对普通读取可见。看到 Source 已消费某个 binlog offset，不代表相应数据已经提交到表；还要检查 checkpoint 和 `$snapshots`。

发生失败时，Flink 从最近成功 checkpoint 恢复 Source、算子状态和 Sink 提交边界，部分记录可能重新处理。正确协调的框架恢复不应重复提交同一贡献，不能把 `aggregation` 简单视为故障后必然重复累加。需要另外防范的是源端把同一业务事件作为两条独立输入发送，或人为将旧 Source/算子状态与较新的目标表状态拼接：`deduplicate` 可能覆盖相同最终行，而 `aggregation` 的主键不会自动按事件 ID 消除重复贡献。

所以“Flink exactly-once”不等于所有业务 Merge Engine 都天然幂等。要区分：传输与提交没有丢失/重复提交，以及同一业务事件被源端重复产生后目标结果是否仍然正确。

## 4. CDC 事件是否真的完整

检查实际记录，而不是根据“CDC”标签推断：

- INSERT 是否包含完整新行。
- UPDATE 是否包含 `-U/+U`，还是只有更新后镜像。
- Before/After 是完整行，还是仅包含变化字段。
- DELETE 是否包含完整旧行，还是只有主键。
- 事件是否携带可比较的 binlog offset、LSN 或源端版本。
- DDL 和数据事件是否共用同一条有序通道。

完整 `RowKind` 不代表完整字段镜像。只有主键的 DELETE 对 `deduplicate` 可能足够定位记录，但 partial-update、aggregation、跨分区更新或基于 sequence 的乱序判断可能需要更多字段和版本信息。

## 5. Schema Evolution 能做什么

Paimon CDC Action 会比较输入 Schema 与目标 Schema，对支持的变化修改目标表。下表细项沿用原笔记的开发版 Schema Evolution 资料；不同发布版对类型映射的默认行为与支持范围可能不同，应按目标 Action 版本核对，不能把它当作本地 1.3.1 的实测清单：

| 源端变化 | Paimon 目标行为 |
| --- | --- |
| 新增列 | Source 能把 Schema 传到 Sink 时，新增目标字段 |
| 字符串、二进制长度扩大 | 扩大目标类型 |
| 整数或浮点类型扩大 | 在同类型族内扩大，如 `INT` 到 `BIGINT` |
| Decimal、时间精度变化 | 兼容且通过 Paimon 校验时演进 |
| 非字符串改字符串 | 默认禁用，需要 `allow-non-string-to-string` |
| 删除列 | 目标旧列保留 |
| 重命名列 | 新名称按新列处理，旧列保留 |
| 重命名表 | 不自动重命名原 Paimon 表 |

类型缩小不会自动把已有目标列缩小；不兼容的变化可能使作业失败，不能假设 Sink 会静默忽略。

`to-string` 影响初始类型映射，`allow-non-string-to-string` 允许已有列后续演进到字符串，两者不是同一个开关。

## 6. 主键和分区键不是普通列演进

Schema Evolution 不负责迁移主键和分区键。目标表已存在时，Action 会基于目标 Schema 和参数检查兼容性；需要改变主键或分区键时，通常要建新表、重写数据并切换作业；固定桶数量调整则有专门的离线 Rescale 流程，不能等同于普通字段演进，详见 [[23-Paimon 多 Writer 冲突验证与 Bucket 扩缩容]]。

如果分区键来自 computed column，例如从 `create_time` 计算 `dt`，还要确认时区、格式和空值行为。源字段类型变化可能同时影响路由到哪个分区，不能只检查目标列能否新增。

## 7. 多表合并与路由

多个源表或分表可以同步到同一张 Paimon 表，但它们的 Schema 必须兼容，目标主键必须在所有来源之间唯一。

假设 `shop_1.orders` 和 `shop_2.orders` 都有 `id=100`。如果目标主键只有 `id`，两条记录会被当作同一逻辑行互相覆盖。应把来源标识加入目标主键，或确保源端主键已经全局唯一。

整库同步保留多张目标表时，要明确表名映射、包含/排除规则以及新表发现机制。源表重命名后是否继续被路由，取决于 Source 选择与映射规则，不等于原目标表被自动 Rename。

## 8. Merge Engine 与 CDC 的匹配

- 上游每次提供完整最终行：通常使用 `deduplicate`，并用可靠 sequence 处理乱序。
- 上游只提供变化字段：使用 partial-update 前确认 `NULL` 是“不更新”还是“清空”，并设计 sequence group。
- 上游字段代表可加减贡献：才使用 aggregation，同时处理重放和 retract。
- 只保留第一次到达：使用 first-row，但它不是按事件时间取最早记录。

存在流式下游时，还要单独选择 Changelog Producer。只有 Writer 实际收到的记录已经是完整最终变化，`input` 才合适；partial-update 或 aggregation 的合并后完整行通常需要 `lookup` 或 `full-compaction`。

## 9. 恢复测试

上线前至少执行这些故障用例：

1. 初始快照中途杀掉作业，从 checkpoint 恢复后核对行值和主键唯一性。
2. 在快照切换增量期间持续 UPDATE/DELETE，确认没有日志空洞。
3. Sink 写完但 checkpoint 未完成时故障，确认框架重放与提交恢复不会造成聚合重复；再单独测试源端重复发送业务事件，二者不能混作一个用例。
4. 新增列后恢复旧 checkpoint，确认 Source 状态和目标 Schema 兼容。
5. 发送乱序 UPDATE 与迟到 DELETE，确认 sequence 能阻止旧事件覆盖新状态。
6. 多表合并制造相同源主键，验证是否发生目标主键冲突。

排查顺序是：Source offset 与反序列化日志、Flink checkpoint、Paimon `$snapshots`、目标 Merge Engine 结果。只查最终表很难判断问题发生在哪一层。

## 10. 上线检查

- 固定并记录 Source Connector、Flink、Paimon 的兼容版本。
- 明确启动模式和初始快照期间的资源需求。
- 抽样保存真实 INSERT、UPDATE、DELETE 事件，确认 RowKind 和字段镜像。
- 定义主键在分表、租户和数据库之间是否全局唯一。
- 把每种 DDL 变化分成“自动支持、作业失败、人工迁移”三类。
- 给 aggregation 等非幂等语义增加事件级去重或对账。
- 同时监控 checkpoint 成功时间和 Paimon Snapshot 提交时间。

## 11. 手工 ALTER 与 CDC 自动演进不是同一项能力

Paimon 支持某条 ALTER，不表示 CDC Action 会自动把对应 MySQL DDL 翻译过去。手工 ALTER 由使用者主动改变目标表；CDC 演进由 Source 实际传出的字段名称、类型与 Action 的匹配规则驱动。两者的控制入口和支持范围都不同。[Paimon SQL Alter](https://paimon.apache.org/docs/2.0/flink/sql-alter/)、[CDC 演进说明](https://paimon.apache.org/docs/2.0/cdc-ingestion/)

| 操作 | 手工修改目标 Paimon 表 | 源库变化经 CDC 自动传播 |
| --- | --- | --- |
| 新增普通列 | 支持相应 ADD 语句；旧文件按新 Schema 解释 | 需要 Schema 信息抵达支持演进的 Sink；普通固定 Schema SQL 不自动完成 |
| 重命名普通列 | 有 RENAME 语句，需同时检查读写作业 | 不能当作自动重命名；可能形成新旧两列 |
| 删除普通列 | 有 DROP 语句，且需检查 Catalog 和在运行作业 | CDC 目标旧列可能继续保留 |
| 修改列类型 | 按目标版本校验支持的转换 | 只有兼容演进与已启用的映射规则才能通过 |
| 重命名表 | 有对应操作，但对象存储移动可能不是原子的 | Source 路由变化不等于目标表同步重命名 |
| 修改主键、分区键 | 需要按表迁移设计 | 不属于普通列自动演进 |

下面是**手工 Schema 实验**，与源库自动演进实验分开运行。先选中已配置好的 Paimon Catalog，使用独立库，不在运行中的 CDC 目标表上直接练习：

```sql
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
CREATE DATABASE IF NOT EXISTS paimon_schema_lab;
USE paimon_schema_lab;

CREATE TABLE schema_orders (
    order_id BIGINT,
    amount INT,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('bucket' = '2');

INSERT INTO schema_orders VALUES (1001, 100);
ALTER TABLE schema_orders ADD channel STRING;
ALTER TABLE schema_orders MODIFY amount BIGINT;
INSERT INTO schema_orders VALUES (1002, 120, 'app');

SELECT order_id, amount, channel FROM schema_orders ORDER BY order_id;
SELECT * FROM `schema_orders$schemas`;
```

预期旧行 `1001` 的 `channel` 为 NULL，新行 `1002` 为 `app`。这说明旧文件可在新 Schema 下读取，不表示 ALTER 已经把所有历史文件重写。此处不配置默认值，避免把默认值行为与旧列映射混为一谈。

若继续测试 RENAME、DROP、类型缩小和 NULL 约束，应在独立表上逐项进行，并分别核对旧 Snapshot、最新 Schema 与在运行 SQL 的兼容性。DDL 返回成功不等于旧作业已经重新规划字段，也不等于它的 Savepoint 能用新拓扑恢复。

## 12. 从源库到湖表的最小 CDC 演练

### 12.1 先固定依赖和两套时间窗口

实验记录应包含 MySQL、Flink、Paimon、CDC Connector、JDBC Driver 的精确版本，以及使用 SQL、Action 还是 YAML Pipeline。Paimon 2.0 的 MySQL Action 文档列出的 CDC 依赖要求不能直接套给本地旧版 Paimon；应从所选 Action 的版本文档核对依赖。[Paimon MySQL Action](https://paimon.apache.org/docs/2.0/cdc-ingestion/mysql-cdc/)

还要区分两个独立的保留窗口：源端 Binlog 覆盖“Source 从旧位点继续”的需求；Paimon Snapshot、Changelog 和 Consumer 保留覆盖“湖表下游从旧版本继续”的需求。目标表保留一个 Tag，不能找回源库已经清理的 Binlog。

源端准备至少核对 Binlog 开启、ROW 格式、完整行镜像、读取权限和唯一的 Server ID 范围；并行 Source 需要足够的 ID，且不能与其他复制客户端冲突。实际要求按所选 MySQL CDC 版本执行，不要通过减小行镜像来节省带宽后继续假设完整 Before/After。[Flink CDC MySQL Source](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/mysql-cdc/)

### 12.2 构造可以人工算清的数据

以下 SQL 只在隔离 MySQL 库执行。库名与 Paimon 目标库可以相同，但它们位于两个不同系统，不共享元数据：

```sql
CREATE DATABASE paimon_cdc_lab;
USE paimon_cdc_lab;
CREATE TABLE orders (
    order_id BIGINT PRIMARY KEY,
    amount DECIMAL(12, 2),
    status VARCHAR(16),
    update_version BIGINT NOT NULL
);
INSERT INTO orders VALUES
    (1001, 100.00, 'CREATED', 1),
    (1002, 50.00, 'CREATED', 1);
```

先用普通 SQL CDC 链路验证固定 Schema 的插入、更新与删除，再单独使用 CDC Action 验证自动演进。两条链路使用不同目标表和不同 Server ID，不同时写同一目标表。

普通 SQL 路径的 Source 定义如下。主机、凭据和 Connector 依赖是部署模板；实际凭据由提交环境注入，不保存进笔记、仓库或日志：

```sql
USE CATALOG default_catalog;
USE default_database;
CREATE TEMPORARY TABLE mysql_orders_source (
    order_id BIGINT,
    amount DECIMAL(12, 2),
    status STRING,
    update_version BIGINT,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '<MySQL地址>',
    'port' = '3306',
    'username' = '<只读CDC账号>',
    'password' = '<由提交环境注入>',
    'database-name' = 'paimon_cdc_lab',
    'table-name' = 'orders',
    'server-id' = '7100-7107',
    'server-time-zone' = 'Asia/Shanghai',
    'scan.startup.mode' = 'initial'
);
```

示例 ID 范围必须先确认未被占用。若 Source 并行度或 Connector 要求变化，也要重新核对范围；这里不把它作为通用默认值。

目标侧先选中已配置的 Paimon Catalog，建立同构的 `paimon_cdc_lab.orders_state`，主键为 `order_id`，固定桶数示例为 2，Merge Engine 为 `deduplicate`。普通 MySQL CDC 已提供主键变更顺序时，不为追求“多一个保险”随意加不匹配的 `sequence.field`；若确有额外乱序，按 [[10-Paimon Sequence、RowKind 与删除语义]] 设计所有更新和删除都能解释的版本规则。

目标 DDL 与写入模板如下。为避免前一个代码块停留在 `default_catalog`，这里显式建立并切换到 `paimon_cdc_catalog`。Warehouse 占位符必须替换为独立实验目录；Source 临时表与 INSERT 保持在同一 SQL Client 会话：

```sql
CREATE CATALOG paimon_cdc_catalog WITH (
    'type' = 'paimon',
    'warehouse' = '<隔离CDC实验Warehouse的完整URI>'
);
USE CATALOG paimon_cdc_catalog;
CREATE DATABASE IF NOT EXISTS paimon_cdc_lab;
CREATE TABLE paimon_cdc_lab.orders_state (
    order_id BIGINT,
    amount DECIMAL(12, 2),
    status STRING,
    update_version BIGINT,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'bucket' = '2',
    'merge-engine' = 'deduplicate',
    'changelog-producer' = 'input'
);

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '30 s';
INSERT INTO paimon_cdc_lab.orders_state
SELECT order_id, amount, status, update_version
FROM default_catalog.default_database.mysql_orders_source;
```

30 秒只是演练采样间隔；生产环境还要按可见延迟、文件大小和恢复目标调整。`input` 用于观察完整 CDC 输入，不负责给字段补旧值。若当前 Source 不能提供约定的输入变化，先修正链路契约。

### 12.3 在初始快照或增量阶段制造变化

在另一个 MySQL 会话中执行：

```sql
USE paimon_cdc_lab;
START TRANSACTION;
UPDATE orders SET amount = 120.00, status = 'PAID', update_version = 2
WHERE order_id = 1001;
DELETE FROM orders WHERE order_id = 1002;
INSERT INTO orders VALUES (1003, 80.00, 'CREATED', 1);
COMMIT;
```

为了确保“快照中途变化”用例真的发生，应先增加足够多的填充行，观察 Source 阶段，再对这两个测试 Key 操作。只有两行的小表可能在操作者提交事务前已结束快照，不能把该次运行记为快照衔接验证。

待 Source 进入增量阶段、Paimon 完成后续提交，并在写入静止边界取样后，`1001` 应为 120 元、`1002` 应不存在、`1003` 应为 80 元。只对测试 Key 统计时，预期行数 2、金额和 200 元；填充数据不纳入这个断言。

在独立批查询会话中，先执行上面相同的 `CREATE CATALOG paimon_cdc_catalog` 定义，使用同一个 Warehouse，再选择 Catalog 核对；Catalog 名称是会话对象，不会因为 Writer 会话创建过就自动出现在新会话中：

```sql
USE CATALOG paimon_cdc_catalog;
SET 'execution.runtime-mode' = 'batch';
SELECT order_id, amount, status, update_version
FROM paimon_cdc_lab.orders_state
WHERE order_id IN (1001, 1002, 1003)
ORDER BY order_id;

SELECT COUNT(*) AS row_count, SUM(amount) AS amount_sum
FROM paimon_cdc_lab.orders_state
WHERE order_id IN (1001, 1002, 1003);
```

跨源库和湖表比较时，先暂停实验写入并等待湖表追上，或使用可关联的一致性边界。两边在持续变化时各执行一次 COUNT，出现差异不能立即断定漏数；只比较 COUNT 相同，也不能证明字段和删除正确。

## 13. 自动 DDL 演进与恢复应怎样单独验证

使用 Paimon CDC Action 时，源库和目标表分别写在 `--mysql_conf` 与目标 `--database/--table` 参数中；命令从目标版本的 MySQL Action 文档生成，不能把普通 SQL Connector 配置或 YAML 配置原样拼接。自动演进用 `orders_evolution` 等独立目标表，避免改坏上一节的固定 Schema 链路。

依次测试下列用例，每次记录源端 DDL、事件顺序、目标 `$schemas`、最新行值、作业日志和恢复点。失败用例应使用独立表，不让上一个失败阻断后面的判断：

| 用例 | 需要观察的结果 |
| --- | --- |
| 新增可空列并写新值 | 目标是否新增字段；旧行和新行分别怎样读取 |
| INT 扩为 BIGINT | Schema 是否演进；超过 INT 范围的新值是否正确 |
| 列重命名 | 目标是否保留旧列并新增新列；不得记成等价 Rename 成功 |
| 删除源列 | 目标旧列是否保留，历史值如何解释 |
| 缩小类型或引入不兼容类型 | 是否明确失败；不能只看任务还能运行 |
| DDL 后从旧 Checkpoint 恢复 | Source Schema 历史、目标最新 Schema、反序列化和 Sink 是否兼容 |
| 新增表或重命名表 | Source 选择与路由规则是否实际消费，不能只查目标库列表 |

普通 Checkpoint 故障恢复交给框架协调重放，不先手工回滚目标表。主动恢复较早 Savepoint 时，则需要对齐源位点、算子状态和表版本，可能涉及对应 Tag 的回滚；操作步骤见 [[20-Paimon Checkpoint、Savepoint 与一致性恢复]]。

## 14. 如何记录实测，避免把模板当作通过

现有学习项目 `15-cdc-multiwriter-template.sql` 与 `22-external-integrations-template.sql` 只提供外部 CDC 模板；2026-10-04 的项目记录明确未验证真实 MySQL/Kafka/Pulsar、外部 Catalog 和恢复流程。上面的新增实验也尚未执行。

一次可复核的实测记录应至少包含：精确依赖版本与入口、隔离库表、输入变更及预期、Source 阶段与位点、成功 Checkpoint、Paimon Snapshot、逐键对账、失败与恢复日志。不要把源端日志位点和表 Snapshot ID 直接等同，也不要用系统表的物理记录数替代逻辑业务行数。

通过普通 DML 链路，只能说明该组固定 Schema 输入的结果正确；通过一次新增列，也不能推断所有 DDL 和旧 Savepoint 恢复都兼容。验证范围应按具体用例登记。

## 相关笔记

- [[10-Paimon Sequence、RowKind 与删除语义]]
- [[17-Paimon 流式读写中的 Snapshot 与 Changelog]]
- [[13-Paimon 主键表建模与稳定性检查清单]]
- [[18-Paimon 系统表、监控指标与故障排查]]
- [[20-Paimon Checkpoint、Savepoint 与一致性恢复]]
- [[21-Paimon Changelog 下游聚合与消费恢复验证]]
- [[25-Paimon 多引擎读写、Catalog 与存储部署]]

## 官方资料

- [CDC Ingestion](https://paimon.apache.org/docs/master/cdc-ingestion/)
- [Schema Evolution and Type Mapping](https://paimon.apache.org/docs/master/cdc-ingestion/schema-evolution/)
- [Kafka CDC](https://paimon.apache.org/docs/master/cdc-ingestion/kafka-cdc/)
- [Sequence and Row Kind](https://paimon.apache.org/docs/master/primary-key-table/sequence-rowkind/)

- [Paimon 2.0 CDC Ingestion](https://paimon.apache.org/docs/2.0/cdc-ingestion/)
- [Paimon 2.0 MySQL Action 与依赖](https://paimon.apache.org/docs/2.0/cdc-ingestion/mysql-cdc/)
- [Paimon 2.0 SQL Alter](https://paimon.apache.org/docs/2.0/flink/sql-alter/)
- [Flink CDC 3.1 MySQL Source](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/mysql-cdc/)

> 原笔记核对日期为 2026-09-16，保留原开发文档来源用于追溯；新增内容核对日期为 2026-11-07。Schema Evolution 专页属于开发文档，不能将其细项默认当作 Paimon 2.0 或本地 1.3.1 已有能力。Source 的增量快照与恢复细节以实际 Connector 版本为准。
