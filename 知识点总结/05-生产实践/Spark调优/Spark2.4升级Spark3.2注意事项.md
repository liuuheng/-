

> 说明：以下差异主要来自 Spark 3.0 引入的行为变化，Spark 3.2 通常延续/包含这些变化。  
> 建议策略：优先修正依赖旧行为的 SQL/数据；必要时用 legacy 参数临时兜底；稳定后逐步移除 legacy 以避免长期技术债。

---

## 1. SQL 语法 / 类型系统变化

### 1.1 INTERVAL 字符串解析更严格
**现象**

```sql
select interval '2 10:20' hour to minute
```

- **Spark 2.4**：可能可解析（示例：`interval 10 hours 20 minutes`）
- **Spark 3.x**：对 `hour to minute` 的输入格式要求更严格，可能直接报错

**兜底参数（恢复旧解析）**
- `spark.sql.legacy.fromDayTimeString.enabled=true`

**影响**
- 依赖“宽松 interval 文本解析”的存量 SQL 可能升级后失败；建议统一 interval 字符串写法或改用更显式的写法。

---

### 1.2 DECIMAL 不再允许负 scale（负指数）
**现象**
- **Spark 2.4**：可能出现 `DecimalType(2, -9)` 这类负 scale
- **Spark 3.x**：会收敛/修正到非负 scale（示例：`DecimalType(11, 0)`）

**兜底参数（允许负 scale）**
- `spark.sql.legacy.allowNegativeScaleOfDecimal=true`

**影响**
- 影响 schema 推断/表达式类型推导；以及依赖特定 decimal 精度/scale 的下游（例如写入表 schema、对账规则等）。

---

### 1.3 String 与 (DATE / TIMESTAMP) 比较：类型提升方向改变
**现象**
- **Spark 2.4**：倾向把 Date/Timestamp 转成 String 再比较
- **Spark 3.x**：倾向把 String 转成 Date/Timestamp 再比较（按日期时间语义比较）

**兜底参数（恢复旧的类型提升）**
- `spark.sql.legacy.typeCoercion.datetimeToString.enabled=true`

**影响**
- 可能导致过滤条件命中范围变化，进而影响结果集；尤其当字符串日期不规范、存在时区/格式差异时更明显。

---

## 2. 文件扫描 / 目录变动容错变化

### 2.1 扫描目录时子目录/文件消失：从“忽略”变为“报错”
**场景**
- 扫描/列目录过程中，子目录或文件消失（并发删除、对象存储最终一致性导致的短暂不可见等）

**行为差异**
- **Spark 2.4**：忽略并继续
- **Spark 3.x**：抛异常导致任务失败

**兜底参数（忽略缺失文件）**
- `spark.sql.files.ignoreMissingFiles=true`

**影响**
- 对对象存储（S3/OSS 等）或并发清理场景更敏感；开启兜底需评估数据完整性与可追溯性。

---

## 3. 数据源行为变化（JSON / CSV / Avro）

### 3.1 JSON：字符串可能被推断为 TimestampType
**变化说明**
在 Spark 3.x 中：当 JSON 字符串值与 `timestampFormat` 匹配时，JSON 数据源与 `schema_of_json` 可能把字段推断为 `TimestampType`。

**控制开关**
- JSON 选项：`inferTimestamp=false`（禁用上述类型推断）

**影响**
- schema 推断变化可能导致字段类型不一致、下游写入/对齐失败或隐式转换开销增加。

---

### 3.2 CSV（PERMISSIVE 模式）坏数据行的返回结果更“部分成功”
**行为差异**
- **Spark 2.4**：格式错误的 CSV 字符串 → 整行各列都为 null
- **Spark 3.x**：若部分列解析成功 → 返回行可能包含非 null 字段（坏列按策略处理）

**影响**
- 数据质量/坏数据统计口径会变化：2.4 更像整行作废，3.x 更像局部字段作废。

---

### 3.3 CSV：带 BOM 文件的编码自动检测行为改变
**行为差异**
- **Spark 2.4**：某些情况下可自动检测编码（尤其 `multiLine=true` 场景）
- **Spark 3.x**：按 CSV 选项 `encoding` 读取（默认 UTF-8）。编码不匹配可能导致乱码/解析失败  
  处理：显式指定正确 `encoding`；或将其设为 `null` 以回退到 3.0 之前的自动检测行为（如环境支持）

**影响**
- 历史依赖“自动识别编码”的任务在升级后容易出问题；建议把编码作为数据契约的一部分明确下来。

---

### 3.4 Avro：写入时字段匹配从“按位置”改为“按字段名”
**变化说明**
Spark 3.x：当使用用户提供的 schema 写 Avro 时，字段匹配以 **catalyst schema 与 Avro schema 的字段名**为准，而非位置。

**影响**
- 依赖“位置对齐”的写入链路可能写错列/写失败；建议统一字段名与 schema 管理策略。

---

## 4. SparkSession / Hive 相关变化

### 4.1 `cloneSession()` 初始化配置优先级变化
**行为差异**
- **Spark 2.4**：新 session 继承 parent SparkContext 的配置（即使 parent SparkSession 有不同值）
- **Spark 3.x**：parent SparkSession 的配置优先于 parent SparkContext

**兜底参数（恢复旧行为）**
- `spark.sql.legacy.sessionInitWithConfigDefaults=true`

**影响**
- 同一 SparkContext 下多 session 的配置继承逻辑变化，可能造成“同名参数取值变化/不生效”。

---

### 4.2 `hive.default.fileformat` 的回退来源变化
**变化说明**
Spark 3.x：若 Spark SQL 配置中找不到 `hive.default.fileformat`，会回退到 SparkContext 的 Hadoop 配置（`hive-site.xml`）读取。

**影响**
- 环境间 `hive-site.xml` 差异可能“隐式影响”默认表/写入格式；建议显式配置或统一环境。

---

### 4.3 Decimal 的字符串表现：尾随零填充差异
**示例**

```sql
SELECT CAST(1 AS decimal(38, 18));
```

- **Spark 2.4**：`1`
- **Spark 3.x**：`1.000000000000000000`

**影响**
- 影响字符串比较、导出/对账、以及依赖精确字符串格式的下游系统或测试用例。

---

### 4.4 内置 Hive 从 1.2 升级到 2.3（Spark 3.x）
**影响点（常见）**
- 外部 metastore 版本适配（按实际环境设置）：
  - `spark.sql.hive.metastore.version`
  - `spark.sql.hive.metastore.jars`（常见取值如 `maven`）
- 自定义 SerDe 可能需要迁移适配 Hive 2.3，或自行构建使用 Hive 1.2 的 Spark
- SQL 使用 `TRANSFORM` 时，十进制字符串表示可能不同：
  - Hive 1.2：可能省略尾随零
  - Hive 2.3：可能补齐到固定小数位（常见到 18 位）

---

## 5. 升级落地建议（最小化风险）
- **优先定位高风险链路**：interval/decimal/string-date 比较、CSV/JSON 推断、对象存储缺失文件、Hive/SerDe
- **双轨策略**：先开 legacy 兜底确保不中断，同时改造 SQL/数据以适配新语义
- **回归验证**：对比升级前后核心指标（行数、空值率、分布、抽样校验），重点关注“比较语义改变”导致的筛选差异
- **逐步收敛**：稳定后逐步关闭 legacy，避免长期依赖旧行为


## 6. 一些案例
### spark 2.4 将时间变成string
![[images/企业微信截图_17766548078255.png]]
### spark 3.2 将string 变成时间
![[images/企业微信截图_17766548331441.png]]![[images/企业微信截图_1776655070632.png]]
** 将string变成时间 解析失败返回为null**

### 一些情况spark2.4返回null 而3.2 报错
![[images/企业微信截图_17766556502031.png]]![[images/企业微信截图_17766556622728.png]]
### 月末日期调整
![[images/企业微信截图_1776656861931.png]]![[images/企业微信截图_17766568731360.png]]![[images/企业微信截图_1776656941812.png]]超过的还是会变的（上图）

![[images/企业微信截图_17766569646110.png]]