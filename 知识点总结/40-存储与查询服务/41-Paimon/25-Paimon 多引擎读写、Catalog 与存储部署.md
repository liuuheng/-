# Paimon 多引擎读写、Catalog 与存储部署

> 核对日期：2026-11-07。Paimon 侧参考 2.0 文档，Doris 侧参考官方 Paimon Catalog 文档及其中逐项版本标注。本地已有实验环境仍是 Flink 1.18.1 + Paimon 1.3.1；本文没有安装新连接器、升级环境或完成跨引擎实测。

## 核心结论

多引擎共用 Paimon，不只是让它们连接同一个对象存储目录。还要统一表身份、Catalog 后端、提交协议、数据类型和读取版本，并分别验证每个引擎支持的表特性。

Flink、Spark 和 Doris 的 `CREATE CATALOG` 语法及属性命名不同。Catalog 在各引擎中的本地名称可以不同，但最终必须解析到同一个元数据后端和同一张表。

能列出表、能执行一次 `COUNT(*)`、能正确读取更新和删除，是三个不同的验证层级。部署验收至少要覆盖最后一个层级。

## 一、把部署拆成四层

| 层次 | 负责什么 | 典型失败 |
| --- | --- | --- |
| 引擎与连接器 | SQL、执行计划、Paimon Reader/Writer | JAR 与引擎/Scala 不匹配、类冲突 |
| Catalog | 库表发现、Schema 和元数据服务 | 连到不同 HMS/REST/JDBC 后端 |
| 文件系统 | 数据与元数据文件访问 | Endpoint、Region、路径或权限错误 |
| 表语义 | Merge Engine、Sequence、DV、Snapshot | 只按 Parquet 读文件导致更新/删除错误 |

查到表名但打开数据失败，通常要继续检查执行节点的文件系统依赖与权限；查到数据但更新前后结果不同，则要追到表语义和读取版本，不能只归因于网络。

Catalog 类型的定义见[Paimon Catalog](https://paimon.apache.org/docs/2.0/concepts/catalog/)，文件系统依赖另见[Filesystems](https://paimon.apache.org/docs/2.0/maintenance/filesystems/)。

## 二、先写清楚版本组合和验证等级

下面只记录文档依据与候选部署组合，不构成已经通过的兼容认证：

| 组合或能力 | 官方依据 | 本文的使用边界 |
| --- | --- | --- |
| Flink 1.18 + Paimon 2.0.0 | 2.0 Quick Start 列出对应 `paimon-flink-1.18-2.0.0.jar` | 说明存在对应集成包；不代表本机已经从 1.3.1 升级 |
| Spark 3.5 / Scala 2.12 + Paimon 2.0.0 | 2.0 Quick Start 列出 `paimon-spark-3.5_2.12-2.0.0.jar` | Spark、Scala、JDK 与集群依赖一起确认 |
| Doris 查询 Paimon | Doris 文档定义为读取外表 | 不据此安排 Doris 回写 Paimon |
| Doris 历史版本、Tag/Branch、系统表 | Doris 页面标注相关能力自 3.1.0 起 | 实际补丁版本和读取路径仍需逐项验证 |
| DV、partial-update、aggregation 等组合 | 表特性文档与引擎集成共同决定 | 单独建立更新/删除样例验收，不用一张“全支持”表概括 |

Paimon 的两组包名来自[Flink Quick Start](https://paimon.apache.org/docs/2.0/flink/quick-start/)与[Spark Quick Start](https://paimon.apache.org/docs/2.0/spark/quick-start/)。包名中的引擎版本和 Scala 二进制版本都属于兼容边界。

Doris 的[官方 3.x 文档页面](https://doris.apache.org/docs/3.x/lakehouse/catalogs/paimon-catalog/)也包含明确标为更高版本的新能力，例如 JDBC Catalog 标注从 4.1.0 起支持。阅读时应按每一项能力的版本标注判断，不能看到 URL 有 `3.x` 就把整页功能套到任意 3.x 集群。

## 三、Catalog 的选择取决于已有元数据治理

| 类型 | 表身份从哪里解析 | 部署时需要统一什么 |
| --- | --- | --- |
| Filesystem | Warehouse 下的元数据 | 规范化路径、文件系统实现与提交原子性 |
| Hive | Hive Metastore 与表目录 | HMS URI、数据库、表位置、认证和共享锁 |
| JDBC | 关系数据库中的 Catalog 元数据 | JDBC 服务、Catalog 标识、驱动与锁机制 |
| REST | REST Catalog 服务 | 服务地址、Warehouse/Catalog 身份、认证与服务能力 |

上表是 Paimon Catalog 类型，不表示每个外部引擎都已经支持全部类型。接入前先取引擎自己的支持清单，再选能够共同使用的后端。

已有 HMS、统一权限和跨引擎发现需求时，可以优先评估 Hive Catalog。仅做隔离 HDFS 实验，Filesystem Catalog 更直接；对象存储生产并发写入，还必须单独确认原子发布和共享锁要求。

不应为了让某个引擎“看见表”，就另建一个名称相同但后端不同的 Catalog 来写原目录。迁移 Catalog 属于需要核对元数据和切换写入协议的独立操作。

## 四、依赖应按运行位置分发

Flink 的 SQL Client 能加载 JAR，不代表所有 TaskManager 都有相同依赖；Spark Driver 能访问 HMS，也不代表 Executor 可以读取 HDFS 或对象存储。

准备连接器时需要记录：

- Paimon 版本、对应引擎版本、Spark 的 Scala 版本和 JDK。
- 文件系统插件、Hadoop/Hive/JDBC 依赖以及加载方式。
- Driver、JobManager、TaskManager、Executor 或 Doris FE/BE 各自的访问路径。
- 已有集群中是否残留另一版本的 Paimon、Hadoop 或存储 SDK。

Bundled JAR 与 Action JAR 用途不同；前者用于集成读写，后者提供维护命令。不能只放 Action JAR 就假定 SQL Connector 已就绪。[Flink 集成文档](https://paimon.apache.org/docs/2.0/flink/quick-start/)说明了这一区别。

Paimon 的文件系统文档还区分 Flink 插件和其他引擎使用的依赖，不宜直接把一整套 Flink `lib` 复制给 Spark 或 Hive 来解决类缺失。

## 五、用隔离 HDFS Warehouse 建立最小对照

以下是待执行示例，前提是各节点已安装匹配依赖、加载同一 HDFS 配置并具有相应权限。`nameservice1` 是占位逻辑名称；实际环境必须替换或配置。Warehouse 与现有学习区分开。

先在 Flink 创建 Catalog 和测试表：

```sql
CREATE CATALOG paimon_writer WITH (
    'type' = 'paimon',
    'warehouse' = 'hdfs://nameservice1/warehouse/paimon-multi-engine-lab'
);
USE CATALOG paimon_writer;
CREATE DATABASE IF NOT EXISTS paimon_engine_lab;
USE paimon_engine_lab;

SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';

CREATE TABLE engine_orders (
    order_id BIGINT,
    amount BIGINT,
    update_version BIGINT,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'bucket' = '2',
    'merge-engine' = 'deduplicate',
    'sequence.field' = 'update_version'
);

INSERT INTO engine_orders VALUES (1, 100, 1), (2, 50, 1);
INSERT INTO engine_orders VALUES (1, 120, 2);
DELETE FROM engine_orders WHERE order_id = 2;
```

这是故意简化的 deduplicate 表。每条 DML 完成后再进行下一步，目标最终状态只有 `order_id=1, amount=120, update_version=2`。Flink 更新与删除支持边界见[SQL Write](https://paimon.apache.org/docs/2.0/flink/sql-write/)。

初次验证只安排一个 Writer，其他引擎只读。确认读一致性后，才另开跨引擎写入实验，并应用 [[23-Paimon 多 Writer 冲突验证与 Bucket 扩缩容]] 的并发边界。

## 六、Spark 连接同一张表

下面命令以 Spark 3.5 / Scala 2.12 / Paimon 2.0.0 为文档示例，JAR 路径必须替换为实际部署位置。Hadoop 配置与认证通过集群方式提供。

```bash
spark-sql \
  --jars /opt/connectors/paimon-spark-3.5_2.12-2.0.0.jar \
  --conf spark.sql.catalog.paimon_reader=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon_reader.warehouse=hdfs://nameservice1/warehouse/paimon-multi-engine-lab \
  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions
```

在 Spark SQL 中检查明细和聚合：

```sql
SELECT order_id, amount, update_version
FROM paimon_reader.paimon_engine_lab.engine_orders
ORDER BY order_id;

SELECT COUNT(*) AS rows_count, SUM(amount) AS total_amount
FROM paimon_reader.paimon_engine_lab.engine_orders;
```

预期为一行、金额 `120`。这只能验收当前示例，不足以证明复杂类型、DV、其他 Merge Engine 或流式读取全部通过。

Spark 支持的查询与历史读取语法应查[Spark SQL Query](https://paimon.apache.org/docs/2.0/spark/sql-query/)，不能直接复制 Flink 的 `OPTIONS` Hint 或 Flink 的 DDL 主键语法。

## 七、Doris 作为读取端接入

下面使用 Doris 文档中的 Filesystem Catalog 形态，前提同样是 FE/BE 能访问 HDFS，并有目标版本所需配置。它不是 Flink 的 `WITH` 语法：

```sql
CREATE CATALOG paimon_reader PROPERTIES (
    'type' = 'paimon',
    'paimon.catalog.type' = 'filesystem',
    'warehouse' = 'hdfs://nameservice1/warehouse/paimon-multi-engine-lab'
);

SELECT order_id, amount, update_version
FROM paimon_reader.paimon_engine_lab.engine_orders
ORDER BY order_id;
```

Catalog 创建成功后，仍要用同样的更新、删除和聚合结果验证 Reader。Doris 官方文档将 Paimon Catalog 定义为读取能力；写入 Doris 内表是另一条数据链路，不能视为回写 Paimon。

如果集群版本具备相应历史版本语法，可以固定相同 Snapshot 对账；否则在最小实验中停止写入并确认元数据刷新后再读。若启用 Read-optimized 路径，要单独标明读取的新鲜度，不能与最新完整状态直接混比。[Doris 集成入口](https://paimon.apache.org/docs/2.0/ecosystem/doris/)给出读取优化与 DV 的版本提示。

## 八、持续写入时怎样做跨引擎对账

两个查询先后运行时，最新 Snapshot 可能已经变化。一个引擎得到 100 行、另一个得到 110 行，不足以证明兼容性错误。

可按以下顺序排除时间差：

1. 在写入端记录已发布 Snapshot ID、提交时间与 Schema ID。
2. 为验收窗口保留对应版本；支持历史读取的引擎显式选择同一个版本。
3. 核对主键明细、更新版本、删除键、金额汇总，而不只核对行数。
4. 检查时区、Decimal 精度、空值及嵌套类型的映射。
5. 对不支持同样历史选择的引擎，使用停写窗口或稳定的隔离测试表。

同一个 Snapshot 对每张表提供版本视图，不等于多个表天然存在跨表事务。多表报表若要求统一业务日切，需要额外定义各表版本清单和调度边界。

元数据刷新、对象存储读取和 Paimon 提交各有自己的延迟。应记录“写入完成、Snapshot 发布、引擎识别、查询返回”四个时间点，才能判断延迟来自哪里。

## 九、存储与认证必须覆盖全部执行节点

对象存储部署除 Warehouse 外，还需要核对 Endpoint、Region、寻址方式、网络出口、TLS 和凭据来源。不同引擎可能使用不同属性名以及不同 SDK；相同的参数文本不保证相同的实际访问路径。

HDFS 部署则检查 NameService/HA 配置、用户身份、Kerberos、票据续期和节点时钟。容器中的本地路径也不等于所有节点共享路径；生产 Warehouse 不能指向某个 SQL Client 的私有 `file:/tmp`。

凭据通过集群凭据服务、角色、密钥挂载或受管配置注入，笔记和 Git 只保留属性名与说明。不要把真实密钥填入 SQL 示例、日志附件或跨引擎对账文件。

按职责分别验证权限：Reader 能否访问所需元数据和数据，Writer 能否创建并提交，Compactor/维护身份能否执行其维护操作。每个角色权限不同，不能以管理员测试成功替代实际运行身份验收。

## 十、原子提交不由 Catalog 名称自动保证

读者需要看到完整发布的 Snapshot。HDFS 的原子 Rename 与对象存储的行为不同；Paimon 2.0 的[Concurrency Control](https://paimon.apache.org/docs/2.0/concepts/concurrency-control/)要求针对相应对象存储提交路径使用合适的 Catalog 和共享锁配置。

所有 Writer 应采用经过确认的同一提交协议。不能让一个 Writer 经 HMS 锁提交，另一个绕过它直接以不兼容的 Filesystem 方式写同一表目录。

共享锁解决的是原子发布和协调，不会把动态桶不允许的多 Writer 组合变成合法，也不会补齐业务事件的 Sequence。表语义、并发约束和存储提交应分别验证。

## 十一、部署验收应留下哪些证据

| 验收项 | 需要保存的证据 |
| --- | --- |
| 依赖 | 引擎、Paimon、JDK/Scala、文件系统插件版本 |
| 表身份 | Catalog Backend、Warehouse、表位置、数据库与表名 |
| 读取 | 明细与聚合对账，更新和删除后的预期结果 |
| 特性 | 实际启用的 DV、Merge Engine、Tag/Branch 与系统表测试 |
| 提交 | Writer/Compactor 清单、共享锁/原子实现、Snapshot 与失败恢复记录 |
| 权限 | 运行身份下的访问结果，凭据续期与失败后的诊断位置 |

本文示例、版本对照与部署方法已按来源静态核对；跨引擎对账、权限和并发提交仍是待执行验收项。只有保存实际结果，才能把相应组合标为“本环境已验证”。

## 相关笔记

- [[01-Apache Paimon Flink SQL 建表：Catalog、主键、分区与 Bucket]]
- [[05-Paimon CDC 入湖、Schema Evolution 与恢复语义]]
- [[08-Paimon MOR、COW、MOW 与 Deletion Vector]]
- [[15-Paimon 多 Writer、Catalog 与原子提交]]
- [[18-Paimon 系统表、监控指标与故障排查]]
- [[Paimon 实时与离线共用维表设计：Tag、Lookup 与历史重跑]]

## 官方资料

- [Paimon 2.0 Catalog](https://paimon.apache.org/docs/2.0/concepts/catalog/)
- [Paimon 2.0 Flink Quick Start](https://paimon.apache.org/docs/2.0/flink/quick-start/)
- [Paimon 2.0 Spark Quick Start](https://paimon.apache.org/docs/2.0/spark/quick-start/)
- [Paimon 2.0 Flink SQL Write](https://paimon.apache.org/docs/2.0/flink/sql-write/)
- [Paimon 2.0 Spark SQL Query](https://paimon.apache.org/docs/2.0/spark/sql-query/)
- [Paimon 2.0 Filesystems](https://paimon.apache.org/docs/2.0/maintenance/filesystems/)
- [Paimon 2.0 Concurrency Control](https://paimon.apache.org/docs/2.0/concepts/concurrency-control/)
- [Paimon 2.0 Doris 集成入口](https://paimon.apache.org/docs/2.0/ecosystem/doris/)
- [Apache Doris Paimon Catalog](https://doris.apache.org/docs/3.x/lakehouse/catalogs/paimon-catalog/)
