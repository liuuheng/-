##### Hive

##### UDF、UDAF 与 UDTF

Q：Hive 的 UDF、UDAF、UDTF 有什么区别，各举例说明。
A：UDF 是一行输入一行输出，例如把手机号统一脱敏；UDAF 是多行聚合成一行，例如实现可合并的 bitmap 去重计数；UDTF 是一行展开为多行或多列，例如 `explode` 拆分商品数组。实际开发时我先确认内置函数能否完成，避免每行调用 Java 自定义逻辑。确实需要自定义时，会明确输入输出 ObjectInspector、null 和异常数据处理；UDAF 要实现 iterate、terminatePartial、merge、terminate，保证 Map 端局部结果能正确合并；UDTF 在 `process` 中 `forward` 多行并与 `LATERAL VIEW` 配合。发布时把 Jar 版本纳入任务依赖，在小数据集验证结果、在大表比较 CPU 和序列化开销，禁止 UDF 内逐行访问远程数据库。

##### 知识点补充

UDAF 需要考虑 partial aggregation 和 merge，保证聚合逻辑可组合。UDTF 常与 `LATERAL VIEW` 使用。自定义函数应处理 null、类型转换和确定性，并避免逐行调用外部服务。

##### Hive JOIN 过滤位置与大表关联

Q：Hive 中 JOIN 的 `ON` 和 `WHERE` 有什么区别？两张大表关联如何优化？
A：内连接中很多过滤条件能被优化器下推，结果可能相同；外连接不能混用。`ON` 决定右表哪些记录参与匹配，`WHERE` 在关联结果生成后再过滤，把右表字段条件放到 WHERE 往往会过滤掉补出的 NULL，使 LEFT JOIN 实际变成 INNER JOIN。大表关联我会先做列裁剪、分区过滤和预聚合，确认 key 类型一致；两边都大时按 key shuffle，并通过采样检查倾斜。小表才使用 map-side join/broadcast，不能只凭“维表”名称判断，需看过滤后的实际大小。热点 key 会拆成普通 key 与热点 key 两路，热点加盐并在关联后去盐聚合；同时检查一对多数据膨胀，避免把重复维表当成性能问题。

##### 知识点补充

LEFT JOIN 若要保留左表全部记录，右表过滤条件通常写在 ON 中。`EXPLAIN`、执行引擎 UI 和实际输入输出行数比背参数更能定位问题。

##### Hive Join 策略、分桶与数据分布

Q：Hive 的 Common Join、MapJoin、Bucket/SMB Join，以及数据分布子句分别怎么用？
A：Common Join 按 key shuffle 两边并在 Reduce 端关联；MapJoin 把过滤后的小表加载到 Mapper 内存哈希表，省掉 Reduce，但必须按序列化后大小评估 OOM。Bucket Map Join 要求关联键分桶且桶数兼容，SMB Join 还要求桶内有序，能像归并一样连接；我会用 explain 验证计划，不能只有表属性。`DISTRIBUTE BY` 决定相同 key 进入同一 Reducer，`SORT BY` 保证 Reducer 内有序，`CLUSTER BY` 是同字段 distribute 加升序 sort。分桶是写入时长期组织文件，不等同目录分区；落地前核对桶数、哈希和写入是否真正执行分桶。

##### 知识点补充

`ORDER BY` 是全局有序，通常需要单 Reducer，海量数据代价高。范围分区要依赖边界或 TotalOrderPartitioner 思路，Hive 常规 `DISTRIBUTE BY` 默认是哈希分发。

##### Hive 参数与执行时效优化

Q：Hive 任务执行慢或要求提高时效，你会调哪些参数、按什么顺序处理？
A：我不会先背一串参数，而是从执行计划和 Stage 指标判断瓶颈：扫描量大先做分区裁剪、列裁剪并改成 ORC/Parquet；小文件多先治理上游写入和合并；Join 慢看广播条件、倾斜 key 与数据膨胀；Reducer 过少或过多再根据总输入量调整每个 Reducer 处理字节数和最大 Reducer 数。执行引擎使用 Tez/Spark 时还要看容器内存、并行度、动态资源和 JVM GC。每次调优用基线 SQL 对比输入字节、shuffle、运行时间和资源消耗，并确保结果行数、金额等校验不变，避免“跑快了但口径错了”。

##### 知识点补充

参数名会随 Hive 与执行引擎版本变化，面试中应说明调节目标而非承诺固定值。自动 MapJoin、CBO、向量化需要正确统计信息和兼容的数据格式才能发挥作用。

##### Hive 文件格式与压缩选择

Q：Hive 常见文件格式和压缩方式有哪些，生产中如何选择？
A：TextFile 可读性强但没有列裁剪和高效编码，适合临时交换；SequenceFile 是 Hadoop 键值二进制格式；Avro 偏行式并携带 Schema，适合消息和模式演进；ORC、Parquet 是列式格式，支持列裁剪、谓词下推、统计信息和压缩，数仓事实表通常优先选。压缩上 gzip 压缩率高但普通 gzip 不可切分，适合最终小结果；Snappy/LZ4 解压快，常用于中间与热数据；Zstd 在压缩率和速度间较均衡。实际会按查询列数、写入频率、引擎兼容和 CPU/I/O 瓶颈做样本压测，并检查文件大小在合理区间，避免压缩后仍产生海量小文件。

##### 知识点补充

文件格式决定数据组织和统计能力，压缩算法决定字节编码，两者不是同一概念。是否可切分还取决于格式容器和压缩方式的组合。

##### 分区表、分桶表与范围管理

Q：Hive 单值分区和范围分区有什么区别？分区表与分桶表分别什么时候使用？
A：Hive 原生分区通常是目录键值，例如 `dt=2026-08-10`，查询带分区条件即可跳过其他目录；所谓范围分区更多是按年/月或自定义区间把范围映射成目录，Hive 并不像某些数据库自动维护通用 RANGE 边界，需要 ETL 明确写入。分桶则在分区内部按列哈希到固定数量文件，适合稳定的抽样、Join 和数据分布，不替代分区裁剪。例如订单表先按天分区控制扫描与生命周期，再按 user_id 分桶支持特定关联。上线前会确认动态分区数量、桶数、写入是否真正按桶排序，以及文件大小；桶太多会重新制造小文件。

##### 知识点补充

分区是目录级裁剪，分桶是分区内文件组织。范围边界变更和迟到数据回写要纳入调度规则，不能只建 DDL 不管写入。

##### Hive ACID 与元数据

Q：Hive 元数据存在哪里？事务表如何实现更新和删除？
A：表、列、分区、存储位置和统计信息保存在 Metastore 后端关系数据库中，例如 MySQL/PostgreSQL；真正数据仍在 HDFS/对象存储，HiveServer2 通过 Metastore 服务访问元数据。ACID 表把写入组织为 base 和 delta 文件，读取时按 write id 合并可见版本，更新/删除生成 delta/delete_delta，后台 minor/major compaction 合并文件。使用前要确认表格式、分桶/事务属性和并发配置满足当前 Hive 版本要求。生产上我会监控 compaction backlog、失败事务和小 delta 数，做好 Metastore 数据库备份与高可用；大量高频单行更新仍不是 Hive 的优势场景。

##### 知识点补充

Metastore 是逻辑元数据中心，不保存事实数据内容。事务隔离与锁机制随 Hive 版本演进，回答时应以实际版本为准，不照搬旧配置。

##### Hive 小文件与异常慢任务排查

Q：Hive SQL 半年前能正常跑，现在变慢甚至跑不动，应该怎么查？
A：我先用历史运行记录对比输入分区、文件数、总字节、Stage/Task 数、最大 Task 时长和资源队列等待。若数据增长但文件更碎，重点查上游并行写入和 compaction；若少数 Task 长尾，统计热点 key、NULL key 和 Join 膨胀；若所有 Task 都慢，检查队列资源、存储 I/O、Metastore、执行引擎版本和配置变更。再看 `EXPLAIN` 是否因统计信息过期导致 Join 策略变化，分区过滤是否失效。修复可能是合并小文件、更新统计信息、预聚合、广播小表、热点拆分或调整并行度。每一步都与历史基线对照，并校验结果口径，不能只把内存调大。

##### 知识点补充

“以前能跑”只说明旧数据和旧环境可行。数据规模、分布、代码、依赖、资源和元数据任一变化都可能改变执行计划与容量需求。

##### Hive 时间与窗口函数

Q：Hive 常用时间函数和窗口函数有哪些，什么时候必须写 `ORDER BY`？
A：时间处理我常用 `to_date`、`date_add/date_sub`、`datediff`、`unix_timestamp/from_unixtime`，但会先统一时区和字符串格式。窗口聚合如 `sum(x) over(partition by dept)` 不依赖顺序，可以不写 order by；`row_number/rank/dense_rank`、`lag/lead` 以及累计和需要确定先后，必须写 order by，并最好加唯一键保证同值稳定。`first_value/last_value` 还要注意默认窗口边界，求整组最后值常显式写 `rows between unbounded preceding and unbounded following`。实际 SQL 会先去重到业务粒度再开窗，避免重复明细把登录天数和金额放大。

##### 知识点补充

窗口中的 `ORDER BY` 会引入分区内排序成本；`ROWS` 按物理行，`RANGE` 按排序值范围，遇到重复排序值时结果可能不同。

##### 动态分区、静态分区与分区修复

Q：Hive 动态分区和静态分区有什么区别？HDFS 已有文件如何补全 Hive 分区元数据？
A：静态分区在 INSERT 时明确给出分区值，适合单天重跑，风险可控；动态分区从查询结果列生成多个目录，适合一次写多天或多地区，但要限制最大动态分区数，防止脏值制造海量目录。若文件已按规范放到 `table_path/dt=...`，可以逐个 `ALTER TABLE ADD PARTITION ... LOCATION ...`，或在受控目录使用 `MSCK REPAIR TABLE` 扫描发现分区；外部表删除元数据不会必然删除文件。生产补数我优先显式 add partition 并校验文件格式、Schema 和行数，避免 repair 扫描巨大目录或误加载临时路径。

##### 知识点补充

删除分区使用 `ALTER TABLE ... DROP PARTITION (...)`，内部表与外部表的数据删除行为受版本和表属性影响，执行前必须确认并备份。

##### Hive on Spark、Spark SQL 与 Metastore

Q：Hive on Spark 和 Spark SQL 访问 Hive 有什么区别，元数据链路怎样工作？
A：Hive on Spark 仍由 HiveServer2 解析 HQL、应用 Hive 语义并生成任务，只把执行引擎换为 Spark；Spark SQL 启用 Hive 支持后，则由 Catalyst 解析优化，通过 Metastore 读取 Hive 表。已有 Hive 作业只换引擎时前者迁移小，需要 DataFrame API 和统一 Spark 工程时选择后者。两者都应通过独立 Metastore Service 访问后端关系库中的表、分区、列和统计信息，不能让业务客户端直改元数据库。上线要固定 Hive/Spark/Hadoop、JDK、SerDe 和 UDF 兼容组合，并分别查看 HS2、Metastore、Driver、Executor 日志和 explain。

##### 知识点补充

Metastore 后端可用 MySQL、PostgreSQL 等受支持关系库，嵌入式 Derby 只适合单用户测试。两种入口的 SQL 方言和计划并不完全相同，数据文件仍位于 HDFS/对象存储。

##### ORC 与 Parquet 底层结构

Q：ORC、Parquet 为什么查询效率高，它们的底层结构有什么差异？
A：两者都按列组织，并把文件切成可跳过的较大单元。Parquet 以 row group→column chunk→page 组织，支持 definition/repetition level 表达嵌套结构；ORC 以 stripe 组织列 stream，并带 row index、统计信息和可选 Bloom Filter。查询只读取需要列，利用 min/max、字典等统计跳过不匹配数据，再通过 RLE、字典和压缩降低 I/O，配合向量化批量处理减少对象开销。实际选择看 Hive/Spark/Flink 兼容、嵌套数据、写入和查询表现；无论哪种，都要控制文件/row group 大小并保证统计信息可用。

##### 知识点补充

列式格式适合分析扫描，不适合高频单行更新。谓词下推只能跳过统计上确定不匹配的数据，不等于建立了传统行级 B+Tree 索引。

##### Hive 字段类型与 ODS 保留策略

Q：Hive 建表怎样选择字段类型，ODS 数据应该保存多久？
A：类型从业务语义和源端精度出发：金额使用明确精度的 decimal，不用 double；事件时间统一 timestamp 并保留原始时区/字符串；枚举用 string 或受控编码，复杂 JSON 高频查询时拆成 struct/map/array，低频则保留原文。ODS 作为可追溯原始层，保存周期由源数据可重放能力、补数窗口、合规和成本决定，例如 Kafka 仅 7 天而 ODS 保留 1—2 年；敏感字段按法规缩短或脱敏。分区按业务日期组织，生命周期由表策略自动执行，删除前验证下游血缘和备份，不能所有表统一永久保存。

##### 知识点补充

源 Schema 变化要兼容旧分区。过宽 string 虽省建模时间，却会把类型错误推迟到查询阶段并降低统计与压缩效果。

##### Hive 内外部表与索引误区

Q：Hive 内部表和外部表有什么区别？为什么很少用 Hive 传统索引？
A：核心是生命周期：删除 managed table 通常删除元数据和受管数据，external table 默认只删元数据并保留外部位置的数据，但具体行为受 Hive 版本和 purge 配置影响，drop 前必须核查类型与 location。传统 Hive 索引维护成本高、优化收益有限，很多版本已不推荐；生产更常依靠分区裁剪、分桶、ORC/Parquet 统计、列裁剪和数据跳过，不能照搬 MySQL B+Tree 索引思路。

##### 知识点补充

外部表适合多引擎共享，受管表适合 Hive 管理生命周期。元数据操作是否删除底层对象必须以当前版本和配置验证。

##### Hive Metastore、HiveServer2 与 JDBC

Q：Hive 元数据包含什么？脚本通过 JDBC 和直接客户端执行有什么区别？
A：Metastore 保存库表、字段、分区、存储格式、SerDe、Location、统计信息和权限相关元数据，通常落在 MySQL/PostgreSQL；HDFS 保存实际数据。应用通过 JDBC/Beeline 连接 HiveServer2，HS2 负责会话、认证、SQL 编译和提交，适合远程多用户访问；旧 CLI 可能直接访问 Metastore 并在本地驱动，不适合作为受控服务入口。生产脚本使用 Beeline/JDBC，传队列、执行引擎和业务日期，检查退出码与结果日志；元数据库只由 Metastore 服务访问，不让业务脚本直连修改。

##### 知识点补充

Hive 角色包括 Server、Metastore、执行引擎和底层存储的职责，不等于权限系统中的 role。元数据服务高可用和数据库备份决定表定义可恢复性。

##### Hive 权限管理

Q：Hive 权限怎样管理，用户和角色如何落地？
A：企业环境我会用 Kerberos 做身份认证，Ranger/Sentry 等做库、表、列和行级授权，HDFS ACL 控底层文件访问；仅在 Hive 里 grant 而 HDFS 目录开放，会被绕过。权限按角色授予，例如开发、分析、运维，不直接给个人长期高权；敏感列做脱敏或列级策略，服务账号使用 keytab 和最小权限。所有申请、审批和访问记录进入审计，离职/转岗自动回收，并定期扫描越权与长期不用权限。

##### 知识点补充

SQL 标准授权、Ranger 策略和存储权限的能力取决于部署模式。外部引擎直读 HDFS 时也必须纳入统一鉴权。

##### order by、sort by、distribute by 与 cluster by

Q：Hive 中 order by、sort by、distribute by 和 cluster by 有什么区别？
A：order by 为全局有序，通常把最终结果汇到单个 Reducer，适合小结果；sort by 只保证每个 Reducer 内有序。distribute by 决定相同 key 进入同一 Reducer，常与 sort by 组合实现按 key 分区并在区内排序；cluster by key 等价于对同一列 distribute by 加 sort by，且默认升序。生产大表不会为展示随意全局排序，而是先过滤聚合或取 TopN；落桶/有序文件还要结合写表语义验证，不能只看查询输出。

##### 知识点补充

SQL 的窗口 order by 与查询末尾 order by 作用域不同。Reducer 数量和倾斜会影响局部排序文件大小。

##### Hive JSON 解析与数据导入导出

Q：Hive 怎样解析 JSON，并在 HDFS、本地和表之间导入导出数据？
A：稳定 JSON 我会用 JsonSerDe 建外部表，字段经常变化或只取少量路径时保留原文，再用 `get_json_object`/`json_tuple` 解析；解析失败写脏数据表，不能静默变 null。导入已有 HDFS 数据可建 external table 指向 location 或 `load data inpath`，本地文件用 `load data local inpath`；查询导出用 `insert overwrite directory` 并明确分隔符和压缩。跨集群迁移更适合 DistCp 或数据同步工具，操作前区分 load 是移动还是复制，避免误删源文件。

##### 知识点补充

频繁按 JSON 路径查询会增加 CPU 且难利用列式统计，DWD 应转成强类型 ORC/Parquet。导出目录通常要求不存在或会被覆盖，要加路径保护。
