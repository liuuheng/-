---
title: "Doris 入门篇04：数据类型体系"
author: "外星人的知识杂谈"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484332&idx=1&sn=a46b927ffaa23edf5cabfdf2b70ddd49&chksm=c1bf09b1f6c880a78c40fd59a7ca5b35f0988197755a6bc01a126f840a776d5ea9d328418cb5"
published_at: "2026-07-26T12:20:00+08:00"
annotated_at: "2026-11-18"
cssclasses:
  - wechat-article
  - doris-intro-annotated
tags:
  - doris
  - 文章批注
---

# Doris 入门篇04：数据类型体系

<div class="article-meta">外星人的知识杂谈 · 2026-07-26</div>

[原文链接](https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484332&idx=1&sn=a46b927ffaa23edf5cabfdf2b70ddd49&chksm=c1bf09b1f6c880a78c40fd59a7ca5b35f0988197755a6bc01a126f840a776d5ea9d328418cb5)

<!-- 正文按原文顺序保留；“批注”“核对说明”为新增。移除装饰封面及篇末导航。示例命令未在集群运行。 -->

> **定位：** 入门篇第 4 篇 | 掌握 Doris 特殊数据类型的使用场景
>
> **阅读前提：** 已了解三种数据模型（第 03 篇）

---

## 一、背景动机

Doris 除了支持常规的 INT/VARCHAR/DATETIME 等基础类型外，还提供了一组针对 OLAP 场景深度优化的特殊类型。选对类型能带来数量级的性能提升，选错则可能导致存储膨胀或查询超时。

---

## 二、基础类型速览

| 类别 | 类型 | 说明 |
| --- | --- | --- |
| 整数 | TINYINT / SMALLINT / INT / BIGINT / LARGEINT | 1~16 字节整数 |
| 浮点 | FLOAT / DOUBLE | 近似浮点 |
| 定点 | DECIMAL(P, S) | 精确小数，P 最大 38 |
| 字符串 | VARCHAR(N) / STRING / CHAR(N) | 变长/超长/定长字符串 |
| 时间 | DATE / DATETIME / DATEV2 / DATETIMEV2 | 日期时间 |
| 布尔 | BOOLEAN | true/false |
| 二进制 | — | Doris 无独立 BINARY 类型，用 STRING 存储 |

基础类型选型原则很简单：**能用小类型就不用大类型**（节省存储、提高压缩率、加速扫描）。

> [!warning] 核对说明：DECIMAL 和二进制类型有版本条件
>
> `DECIMAL(P,S)` 的 `P` 是总位数，`S` 是小数位数。Doris 4.x 默认最大精度为 38，开启 `enable_decimal256` 后可到 76。选小类型前先用业务上限估算整数位数和汇总结果，避免单行能存、聚合后溢出。4.x 也识别 `VARBINARY`，但官方文档限定了用途：可用于外部 Catalog 映射和查询，当前不能作为 Doris 内表的存储列。[DECIMAL](https://doris.apache.org/docs/4.x/sql-manual/basic-element/sql-data-types/numeric/DECIMAL/) · [VARBINARY](https://doris.apache.org/docs/4.x/sql-manual/basic-element/sql-data-types/binary-type/VARBINARY/)

---

## 三、特殊类型详解

### 3.1 BITMAP — 精确去重

**解决的问题：**`COUNT(DISTINCT user_id)` 在亿级数据上极慢。

**原理：** 将每个元素映射为 Bitmap 中的一位，合并（UNION）即可得去重计数（BIT COUNT）。Doris 使用 Roaring Bitmap 实现，支持高效压缩。


```sql
-- 建表
CREATE TABLE page_uv (
    dt       DATE,
    page_id  VARCHAR(128),
    uv       BITMAP  BITMAP_UNION
)
AGGREGATE KEY(dt, page_id)
DISTRIBUTED BY HASH(page_id) BUCKETS 8;

-- 写入（将 user_id 转为 bitmap）
INSERT INTO page_uv
SELECT dt, page_id, TO_BITMAP(user_id)
FROM user_behavior_log;

-- 查询 UV
SELECT dt, BITMAP_UNION_COUNT(uv) AS uv
FROM page_uv
WHERE dt = '2026-07-01'
GROUP BY dt;

```


**适用场景：**

- 用户去重（UV）
- 标签交并集运算
- 漏斗/留存分析的底层计算

**限制：**

- 只支持非负整数（INT/BIGINT），字符串需先映射为 ID
- 必须配合 Aggregate Key 使用（聚合函数为 BITMAP\_UNION）
- 单个 Bitmap 可能很大（高基数场景每个 bitmap 可达 MB 级）

> [!note] 批注：精确 UV 要合并集合，再算基数
>
> 同一用户访问多个页面时，把各页面 UV 相加会重复计数。示例按 `dt, page_id` 保存可合并的用户集合；按日查询时先对各页面集合取并集，再得到日 UV。精确性还要求原始用户到整数 ID 的映射一一对应；对字符串做有限位宽哈希存在碰撞，不能按严格精确统计验收。

### 3.2 HLL（HyperLogLog）— 近似去重

**解决的问题：** 当精确去重不是必须，且数据基数极高时，BITMAP 存储开销大。

**原理：** HyperLogLog 算法通过概率统计估计基数，固定占用约 16KB 内存，误差率约 1~2%。


```sql
-- 建表
CREATE TABLE app_uv_approx (
    dt         DATE,
    app_name   VARCHAR(64),
    uv_approx  HLL  HLL_UNION
)
AGGREGATE KEY(dt, app_name)
DISTRIBUTED BY HASH(app_name) BUCKETS 4;

-- 写入
INSERT INTO app_uv_approx
SELECT dt, app_name, HLL_HASH(user_id)
FROM user_events;

-- 查询近似 UV
SELECT dt, HLL_UNION_AGG(uv_approx) AS approx_uv
FROM app_uv_approx
WHERE dt = '2026-07-01'
GROUP BY dt;

```


**BITMAP vs HLL 对比：**

| 维度 | BITMAP | HLL |
| --- | --- | --- |
| 精度 | 精确 | 近似（误差 ~1-2%） |
| 存储空间 | 随基数增长（可达 MB） | 固定 ~16KB |
| 适用基数 | < 1 亿 | 无上限 |
| 交集运算 | 支持 | 不支持 |
| 适合场景 | 精确 UV、标签运算 | 超高基数近似统计 |

> [!note] 批注：合并估算状态，不相加估算值
>
> `HLL_HASH` 产生可合并的状态，`HLL_UNION_AGG` 在合并后才给出近似人数。需要跨天去重时，应保留每日 HLL 状态供月度查询合并，单独保留每日人数会丢掉跨天重合信息。原文的误差和空间数字适合做量级参考；小集合可以用更紧凑的表示，不能据此假设每行始终占 16 KB，也没有可按业务承诺的固定误差上界。[HLL 近似去重](https://doris.apache.org/docs/4.x/query-acceleration/distinct-counts/hll-approximate-deduplication/)

### 3.3 QUANTILE\_STATE — 近似分位数

**解决的问题：** 计算 P50/P90/P99 等分位数，精确计算需要全量排序，代价极高。

**原理：** 基于 t-digest 算法，将数据分布压缩为一组质心（centroid），支持高精度分位数估计。


```sql
-- 建表
CREATE TABLE api_latency (
    dt          DATE,
    api_name    VARCHAR(128),
    latency_q   QUANTILE_STATE  QUANTILE_UNION
)
AGGREGATE KEY(dt, api_name)
DISTRIBUTED BY HASH(api_name) BUCKETS 4;

-- 写入
INSERT INTO api_latency
SELECT dt, api_name, TO_QUANTILE_STATE(latency_ms, 2048)
FROM api_logs;

-- 查询 P99
SELECT dt, api_name,
       QUANTILE_PERCENT(QUANTILE_UNION(latency_q), 0.99) AS p99
FROM api_latency
GROUP BY dt, api_name;

```

> [!note] 批注：分位数要从合并后的分布求
>
> 不同 API 或不同时间窗口出现的请求量可能相差几个数量级，因此不能平均各组的 P99 来求总体 P99。保留 `QUANTILE_STATE` 后，查询可以先用 `QUANTILE_UNION` 合并统计状态，再对合并结果取 `0.99`。示例的 `2048` 是压缩参数，不是样本大小；增大参数会提高精度，也会增加计算成本。[TO_QUANTILE_STATE](https://doris.apache.org/docs/4.x/sql-manual/sql-functions/scalar-functions/quantile-functions/to-quantile-state/)


### 3.4 VARIANT — 半结构化数据

**解决的问题：** JSON 数据 Schema 不固定，传统做法是存成 VARCHAR 后`json_extract`，性能极差。

**原理：** VARIANT 类型在写入时自动分析 JSON 结构，将高频出现的字段提取为独立子列（subcolumn），以列式方式存储。低频字段保留在稀疏列中。


```sql
-- 建表
CREATE TABLE event_log (
    event_time  DATETIME,
    event_type  VARCHAR(64),
    properties  VARIANT          -- 半结构化属性
)
DUPLICATE KEY(event_time, event_type)
DISTRIBUTED BY HASH(event_type) BUCKETS 16;

-- 写入 JSON 数据
INSERT INTO event_log VALUES
('2026-07-01 10:00:00', 'click', '{"page":"home", "duration":320, "button":"play"}'),
('2026-07-01 10:00:01', 'play',  '{"song_id":12345, "quality":"hq", "duration":180}');

-- 查询（直接用路径访问，走列式扫描）
SELECT properties['page'], properties['duration']
FROM event_log
WHERE event_type = 'click' AND properties['duration'] > 100;

```


**VARIANT vs JSON vs VARCHAR：**

| 维度 | VARIANT | JSON (JSONB) | VARCHAR + json\_extract |
| --- | --- | --- | --- |
| 存储方式 | 列式（subcolumn 提取） | 二进制 JSON | 纯文本 |
| 查询性能 | 高（列式扫描） | 中（二进制解析） | 低（每行解析文本） |
| Schema 要求 | 无 | 无 | 无 |
| 索引支持 | 支持倒排索引 | 有限 | 不支持 |
| 适合场景 | 日志/埋点属性 | 嵌套复杂 JSON | 历史兼容 |

> [!note] 批注：字段能取出来，不代表类型稳定
>
> 埋点升级时，`duration` 可能从数值变成字符串，也可能缺失。用于过滤或聚合时，按目标版本验证子路径的推断类型和转换规则；可用 `CAST(properties['duration'] AS BIGINT)` 明确数值口径，再决定缺失或转换失败的记录怎么处理。对长期稳定、频繁筛选的字段，也可以单独建列。[VARIANT](https://doris.apache.org/docs/4.x/sql-manual/basic-element/sql-data-types/semi-structured/VARIANT/)

### 3.5 ARRAY / MAP / STRUCT — 复杂类型


```sql
-- ARRAY：存储变长列表
tags       ARRAY<VARCHAR(64)>       -- 用户标签列表
scores     ARRAY<INT>               -- 评分列表

-- MAP：键值对
ext_info   MAP<VARCHAR(64), VARCHAR(256)>  -- 扩展属性

-- STRUCT：固定结构嵌套
address    STRUCT<city:VARCHAR(32), district:VARCHAR(32), street:VARCHAR(128)>

```


**使用场景：**

- ARRAY：标签列表、多值属性、时序采样点
- MAP：动态扩展字段（已知 key 集合有限）
- STRUCT：固定结构的嵌套字段（替代 JSON 中的固定子对象）

**注意：** 复杂类型在聚合模型中只支持`REPLACE` 聚合，不支持 SUM/MAX 等操作。

> [!note] 批注：复杂列的替换粒度
>
> 在聚合表中，`REPLACE` 表示替换整个数组、映射或结构体。它不会逐元素相加，也不会把两个 `MAP` 按键合并。需要对集合成员独立更新时，要重新定义表的行粒度，或在写入端计算合并后的完整列值。

### 3.6 JSONB — 二进制 JSON


```sql
-- 建表
detail  JSONB

-- 查询
SELECT detail->'name', detail->'age'
FROM users
WHERE detail->'city' = '"杭州"';

```


与 VARIANT 的关系：

- JSONB 保留完整的 JSON 结构，以二进制格式存储，解析比 VARCHAR 快
- VARIANT 在存储层做结构提取，查询性能更优
- 新场景推荐 VARIANT；JSONB 适合需要完整保留 JSON 结构且查询不频繁的场景

> [!warning] 核对说明：区分 SQL 类型名和二进制存储格式
>
> Doris 4.x 文档使用 `JSON` 声明列；`JSONB` 是二进制存储格式，也是 1.2.x 使用过的类型名。原文的箭头表达式保留作示意；按 4.x 文档，字符串比较可写成 `JSON_EXTRACT_STRING(detail, '$.city') = '杭州'`，避免把 JSON 值和 SQL 字符串直接比较。[JSON 类型](https://doris.apache.org/docs/4.x/sql-manual/basic-element/sql-data-types/semi-structured/JSON/) · [JSON_EXTRACT_STRING](https://doris.apache.org/docs/4.x/sql-manual/sql-functions/scalar-functions/json-functions/json-extract-string/)

---

## 四、类型选型速查表

| 场景 | 推荐类型 | 原因 |
| --- | --- | --- |
| 精确 UV 统计 | BITMAP | 预聚合 + 高效交并集 |
| 超高基数近似 UV | HLL | 固定空间，无上限基数 |
| 分位数统计（P99） | QUANTILE\_STATE | 预聚合，无需全量排序 |
| 半结构化日志属性 | VARIANT | 列式存储，自动 schema 推断 |
| 固定 Schema 的 JSON | STRUCT / 展开为独立列 | 性能最优 |
| 不固定但查询少的 JSON | JSONB | 保留完整结构 |
| 标签列表/多值 | ARRAY | 原生支持 ARRAY 函数 |
| 动态 KV 扩展属性 | MAP / VARIANT | 视查询频率选择 |

---

## 五、使用注意事项

1. **BITMAP/HLL/QUANTILE\_STATE 只能用于 Aggregate Key 表**

   （聚合函数分别是 BITMAP\_UNION / HLL\_UNION / QUANTILE\_UNION）
2. **VARIANT 不能作为 Key 列，**

   只能作为 Value 列
3. **ARRAY/MAP/STRUCT 不能作为 Partition 或 Distribution 列**
4. **BITMAP 的输入必须是非负整数，**

   字符串需通过字典表映射为 ID
5. **类型选择影响 Compaction：**

   BITMAP 列的 Compaction 需要做 bitmap union，比普通列慢

> [!warning] 核对说明：区分列类型和表内聚合配置
>
> `BITMAP_UNION` 等列级聚合声明属于 Aggregate Key 表的写法，但不能由此断定这些类型只能用于 Aggregate Key 表。4.x 的 `BITMAP` 和 `QUANTILE_STATE` 文档也列出 Unique / Duplicate 表。建表时分别检查目标类型允许的表模型、列角色和聚合函数；用 `SHOW CREATE TABLE` 核对实际定义。[BITMAP](https://doris.apache.org/docs/4.x/sql-manual/basic-element/sql-data-types/aggregate/BITMAP/) · [QUANTILE_STATE](https://doris.apache.org/docs/4.x/sql-manual/basic-element/sql-data-types/aggregate/QUANTILE-STATE/)

---

## 六、小结

| 要点 | 内容 |
| --- | --- |
| BITMAP | 精确去重，预聚合，支持交并集 |
| HLL | 近似去重，固定空间，超高基数 |
| QUANTILE\_STATE | 近似分位数，t-digest 算法 |
| VARIANT | 半结构化列式存储，自动提取子列 |
| ARRAY/MAP/STRUCT | 复杂嵌套类型，替代部分 JSON 场景 |
| 核心原则 | 能预聚合就预聚合，能列式就不用文本 |
