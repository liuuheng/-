---
title: "Doris 进阶篇06：索引进阶与实战"
author: "外星人的知识杂谈"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484369&idx=1&sn=4e674dec1929e5194186c37cddba3bd4&chksm=c1bf09ccf6c880da89cd9a029dc9accda48cc14106f2c966e83af90be2f5103da2e6098ab92b#wechat_redirect"
published_at: "2026-08-28T19:23:16+08:00"
annotated_at: "2026-11-18"
cssclasses:
  - wechat-article
  - doris-intro-annotated
tags:
  - doris
  - 文章批注
---

# Doris 进阶篇06：索引进阶与实战

<div class="article-meta">外星人的知识杂谈 · 2026-08-28</div>

[原文链接](https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484369&idx=1&sn=4e674dec1929e5194186c37cddba3bd4&chksm=c1bf09ccf6c880da89cd9a029dc9accda48cc14106f2c966e83af90be2f5103da2e6098ab92b#wechat_redirect)

<!-- 保留原文正文、顺序与示例；批注和核对说明为新增。装饰封面与篇末导航已移除；代码未在集群运行。 -->

> **定位：** 进阶篇第 6 篇 | 深入各类索引的内部原理与选型实战
>
> **阅读前提：** 入门篇第 07 篇（索引体系与向量检索入门）

---

## 一、背景动机

入门篇介绍了 Doris 索引的“是什么”和基本用法，本篇回答“为什么这样设计”和“怎么选最优”。理解索引的内部原理，才能准确判断：

- 什么时候建索引能带来显著收益
- 什么时候索引反而是负担（写入放大、Compaction 变慢）
- 多种索引可选时如何决策

---

## 二、Bloom Filter 深入

### 2.1 原理回顾

Bloom Filter 是一个概率数据结构，用于快速判断“某元素是否不在集合中”：


```text
写入过程：
  element → k 个 hash 函数 → 在 bit 数组中标记 k 个位置为 1

查询过程：
  element → k 个 hash 函数 → 检查 k 个位置
  如果任一位为 0 → 一定不在集合中（精确跳过）
  如果全部为 1 → 可能在集合中（需进一步验证，存在误判）

```


### 2.2 误判率与参数关系

误判率公式：


```text
FPR ≈ (1 - e^(-k×n/m))^k

其中：
  n = 插入元素数量
  m = bit 数组大小（bits）
  k = hash 函数数量
  FPR = False Positive Rate（误判率）

```


Doris 中每个 Page（默认 64KB 数据）维护一个 Bloom Filter：

| 参数 | Doris 默认值 | 说明 |
| --- | --- | --- |
| 每 Page 行数 | ~1024 行（视数据类型） | n |
| Bloom Filter 大小 | 由`bloom_filter_fpp` 控制 | m |
|`bloom_filter_fpp` | 0.05（5%误判率） | 建表时可指定 |

### 2.3 大小估算


```text
目标 FPR = 1%，n = 1024 行
m = -(n × ln(FPR)) / (ln2)^2
  = -(1024 × ln(0.01)) / 0.48
  ≈ 9830 bits ≈ 1.2 KB

目标 FPR = 5%，n = 1024 行
m ≈ 6235 bits ≈ 0.76 KB

```

> [!warning] 核对说明｜公式估算理论 bit 数，不等于磁盘上每页大小
>
> 这个估算假设所用哈希次数接近最优，用来判断目标误判率降低后需要多少额外空间。实际页内行数、重复值比例和过滤器实现都会影响结果。不要把约 1024 行当成 Bloom Filter 页的固定行数，也不要把理论 1% 误判率理解为每次查询都能跳过 99% 的页。[BloomFilter 索引](https://doris.apache.org/docs/4.x/table-design/index/bloomfilter/)


### 2.4 Bloom Filter 索引实战


```sql
-- 建表时声明（适合高基数等值过滤列）
CREATE TABLE user_events (
    dt        DATE,
    user_id   BIGINT,
    event_id  VARCHAR(64),
    device_id VARCHAR(64),
    ...
) DUPLICATE KEY(dt, user_id)
PROPERTIES (
    "bloom_filter_columns" = "event_id, device_id",
    "bloom_filter_fpp" = "0.01"  -- 追求更低误判率
);

```


### 2.5 适用场景与收益

| 场景 | 是否适用 | 原因 |
| --- | --- | --- |
|`WHERE device_id = 'xxx'` （高基数） | 非常适用 | 绝大多数 Page 会被跳过 |
|`WHERE status = 1` （低基数，如只有 0/1） | 不适用 | 几乎每个 Page 都包含，Filter 无法跳过 |
|`WHERE user_id > 1000` （范围查询） | 不适用 | Bloom Filter 只支持等值 |
|`WHERE device_id IN ('a','b','c')` | 适用 | 多次 BF 查询取交集 |

**经验法则：** 列的 NDV > 总行数的 1%，且查询以等值过滤为主，适合建 Bloom Filter。

> [!warning] 核对说明｜IN 不是求交集
>
> 对于 `IN ('a','b','c')`，页面只要可能包含任意一个值就必须保留；三个值都确定不存在才可跳过。值列表越长，保留的候选页可能越多。全表 NDV 仅可作初筛，实际收益取决于查询值在数据页上的分布，用 Profile 中的 `RowsBloomFilterFiltered` 验证。[BloomFilter 索引](https://doris.apache.org/docs/4.x/table-design/index/bloomfilter/)

---

## 三、Bitmap 索引深入

### 3.1 Roaring Bitmap 基础

Doris 的 Bitmap 索引底层使用 Roaring Bitmap 实现，对比传统 Bitmap：


```text
传统 Bitmap（适合稠密数据）：
  值=1 的行: [1,0,1,0,0,1,0,0,1,...]  → 每行 1 bit

Roaring Bitmap（适合稀疏数据）：
  将 32-bit 空间划分为 65536 个 Container（高 16 位分桶）
  每个 Container 内根据密度选择：

  - Array Container（稀疏，< 4096 个元素）：存储排序数组
  - Bitmap Container（稠密，>= 4096 个元素）：传统 bit 数组
  - Run Container（连续段多）：存储 [start, length] 对

```


### 3.2 Bitmap 索引在 Doris 中的实现


```text
为 status 列（值域：0,1,2,3,4）建 Bitmap 索引：

status=0: Roaring Bitmap [row_0, row_5, row_8, ...]
status=1: Roaring Bitmap [row_1, row_3, row_6, ...]
status=2: Roaring Bitmap [row_2, row_4, row_7, ...]
status=3: Roaring Bitmap [row_9, row_12, ...]
status=4: Roaring Bitmap [row_10, row_11, ...]

查询 WHERE status = 1：
  直接取出 status=1 的 Roaring Bitmap → 精确定位所有行号

查询 WHERE status IN (1, 3)：
  Bitmap[1] OR Bitmap[3] → 合并后精确定位

```


### 3.3 Bitmap 索引适用场景


```sql
-- 创建 Bitmap 索引
CREATE INDEX idx_status ON table1 (status) USING BITMAP;
CREATE INDEX idx_platform ON table1 (platform) USING BITMAP;

```


| 场景 | 是否适用 | 原因 |
| --- | --- | --- |
| 低基数列（NDV < 100） | 非常适用 | 每个值的 Bitmap 小且精确 |
| 多条件组合过滤 | 适用 | Bitmap AND/OR 运算高效 |
| 高基数列（NDV > 10000） | 不适用 | 太多 Bitmap，存储爆炸 |
| 频繁写入的表 | 慎用 | Bitmap 索引写入时需要更新 |

> [!warning] 核对说明｜4.x 不按原文 USING BITMAP 建索引
>
> 位图示例解释按值找行号及集合运算。Doris 4.x 的 `CREATE INDEX` 文档只列出 `INVERTED`、`NGRAM_BF` 和 `ANN`，不应直接执行上面的 `USING BITMAP`。也不要和保存集合的 `BITMAP` 数据类型混用。低基数条件可测试无分词倒排索引，但大量匹配行仍要读取，收益不能只靠 NDV 推断。[CREATE INDEX](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/table-and-view/index/CREATE-INDEX/)

### 3.4 Bitmap 索引 vs Bloom Filter 决策

| 维度 | Bloom Filter | Bitmap 索引 |
| --- | --- | --- |
| 适合基数 | 高（NDV 大） | 低（NDV 小） |
| 精确度 | 有误判 | 精确 |
| 支持查询 | 等值 | 等值、IN、组合 |
| 存储开销 | 小（固定大小/Page） | 与基数成正比 |
| 写入影响 | 小 | 中 |
| 过滤粒度 | Page 级 | 行级 |

---

## 四、倒排索引深入

### 4.1 底层实现

Doris 倒排索引基于 CLucene（C++ 版 Lucene），为每个 Segment 维护独立索引文件：


```text
Segment 文件结构：
├── data.dat         ← 列式数据
├── index.idx        ← 前缀索引
├── bloom.bf         ← Bloom Filter
└── inverted/        ← 倒排索引目录
    ├── _0.tii       ← Term Index（跳表）
    ├── _0.tis       ← Term Dictionary
    ├── _0.frq       ← Term Frequency
    └── _0.prx       ← Position（全文检索用）

```

> [!warning] 核对说明｜图示表达索引与 Segment 的关系
>
> 倒排索引按 Segment 建立，用词项到行号列表的映射减少读数据的范围。文件命名和多列索引打包方式依版本和索引存储格式而变；不要用示例目录推断实际文件的组成，更不要按示意路径删除文件。[倒排索引](https://doris.apache.org/docs/4.x/key-features/inverted-index/)


### 4.2 分词器选择

| 分词器 | 语法 | 适用场景 | 示例 |
| --- | --- | --- | --- |
| none（不分词） | 无 | 精确匹配（如 ID、状态码） |`WHERE col = 'abc123'` |
| english | TOKENIZE(“english”) | 英文文本 |`WHERE col MATCH 'hello world'` |
| chinese | TOKENIZE(“chinese”) | 中文文本 |`WHERE col MATCH '网易云音乐'` |
| unicode | TOKENIZE(“unicode”) | 混合文本（按 Unicode 标点分词） | 日志场景 |

### 4.3 倒排索引创建与使用


```sql
-- 创建倒排索引（等值 + 范围）
CREATE INDEX idx_city ON table1 (city) USING INVERTED;

-- 创建倒排索引（全文检索）
CREATE INDEX idx_content ON table1 (content) USING INVERTED 
PROPERTIES ("parser" = "chinese");

-- 全文检索查询
SELECT * FROM logs WHERE content MATCH_ANY 'error timeout';    -- 匹配任一词
SELECT * FROM logs WHERE content MATCH_ALL 'error AND timeout'; -- 匹配全部词
SELECT * FROM logs WHERE content MATCH_PHRASE 'connection refused'; -- 短语匹配

-- 等值查询（倒排索引同样加速）
SELECT * FROM logs WHERE level = 'ERROR';

-- 范围查询
SELECT * FROM logs WHERE response_time > 1000;

```

> [!warning] 核对说明｜MATCH_ALL 的参数不是 SQL 布尔表达式
>
> `MATCH_ALL 'error timeout'` 表示匹配分词后的所有词项；把 `AND` 写进字符串不会在这里变成 SQL 运算符。等值过滤和范围过滤示例也分别需要在 `level` 和 `response_time` 上有适用的索引，前面创建的 `content` 索引不能替它们工作。[倒排索引查询操作符](https://doris.apache.org/docs/4.x/table-design/index/inverted-index/search-operators/)


### 4.4 倒排索引 vs Bloom Filter（等值场景）

| 维度 | 倒排索引 | Bloom Filter |
| --- | --- | --- |
| 精确度 | 100% 精确 | 有误判 |
| 过滤粒度 | 行级 | Page 级 |
| 存储开销 | 较大（完整倒排结构） | 小 |
| 写入影响 | 中-高 | 小 |
| 支持功能 | 等值/范围/全文/LIKE | 仅等值 |
| Compaction 影响 | 中（需合并倒排文件） | 小 |

**结论：** 如果只需等值过滤且不想增加太多写入开销，用 Bloom Filter；如果需要全文检索或范围查询，用倒排索引。

---

## 五、NGram Bloom Filter 深入

### 5.1 解决的问题

标准 Bloom Filter 不支持`LIKE '%keyword%'` 查询。NGram BF 通过对字符串做 N-Gram 分解来支持子串匹配：


```text
字符串 "hello world" 的 3-gram 分解：
["hel", "ell", "llo", "lo ", "o w", " wo", "wor", "orl", "rld"]

查询 LIKE '%worl%' 时：
"worl" 的 3-gram: ["wor", "orl"]
检查 Bloom Filter 中是否包含这些 N-Gram
全部命中 → 该 Page 可能包含目标（需验证）
任一缺失 → 该 Page 一定不包含（跳过）

```

> [!note] 批注｜多个 gram 都在，仍不能证明原子串命中
>
> 例如 `wor` 和 `orl` 可以来自同一数据块的不同记录，甚至各自只是 Bloom 假阳性。过滤器决定是否要读块，SQL `LIKE` 仍负责验证连续子串并保证最终结果准确。


### 5.2 gram\_size 调优

| gram\_size | 查询模式 | 效果 |
| --- | --- | --- |
| 2 (bigram) |`LIKE '%ab%'` （短关键词） | 适用，但误判率较高 |
| 3 (trigram) |`LIKE '%abc%'` （3字符以上） | 平衡精度和空间 |
| 4 (4-gram) |`LIKE '%abcd%'` （4字符以上） | 低误判，但短关键词无法过滤 |

**关键约束：** 查询关键词长度必须 >= gram\_size，否则 NGram BF 无法过滤。


```sql
-- 创建 NGram BF 索引
CREATE INDEX idx_url ON access_log (url) USING NGRAM_BF 
PROPERTIES ("gram_size" = "3", "bf_size" = "65536");

-- 查询（关键词长度 >= 3 才能利用索引）
SELECT * FROM access_log WHERE url LIKE '%api/v2%';

```


### 5.3 NGram BF vs 倒排索引（模糊查询）

| 维度 | NGram BF | 倒排索引 + MATCH |
| --- | --- | --- |
| 查询方式 |`LIKE '%xxx%'` |`MATCH_ANY 'xxx'` |
| 精确度 | 有误判（BF 特性） | 精确（基于 token） |
| 支持模式 | 任意子串 | 分词后的 token |
| 存储开销 | 小 | 大 |
| 中文支持 | 按字符 N-Gram | 按分词结果 |

> [!warning] 核对说明｜子串命中和词项命中是不同语义
>
> 如果 `timeout` 是 `connection_timeout_ms` 的一段，`LIKE '%timeout%'` 会匹配；`MATCH_ANY 'timeout'` 是否匹配由选用的分词器决定。应先用业务样本确定要找子串、词项还是短语，再选索引。Bloom Filter 的误判只留下额外候选块，不会把错行返回给查询。[NGram BloomFilter](https://doris.apache.org/docs/4.x/table-design/index/ngram-bloomfilter-index/)

---

## 六、索引选型决策矩阵

### 6.1 按查询模式选型

| 查询模式 | 推荐索引 | 备选 |
| --- | --- | --- |
|`WHERE col = 'value'` （高基数） | Bloom Filter | 倒排索引 |
|`WHERE col = 'value'` （低基数） | Bitmap 索引 | 倒排索引 |
|`WHERE col IN (...)` | Bitmap 索引 | 倒排索引 |
|`WHERE col > X AND col < Y` | 倒排索引 | ZoneMap（自动） |
|`WHERE col LIKE '%xxx%'` | NGram BF | 倒排索引（需改写为 MATCH） |
| 全文检索 | 倒排索引 | - |
| 多条件组合（AND/OR） | 倒排索引 | Bitmap 索引 |
| Key 列前缀等值 | 前缀索引（自动） | - |

### 6.2 按场景选型

| 业务场景 | 推荐方案 |
| --- | --- |
| 日志检索（全文 + 时间范围） | 倒排索引（分词） + 分区裁剪 |
| 用户行为分析（user\_id 点查） | Bloom Filter + 前缀索引 |
| 标签筛选（status/type 多值） | Bitmap 索引 |
| URL/路径模糊搜索 | NGram BF |
| 高并发报表（维度组合查询） | Rollup + 倒排索引 |

### 6.3 索引开销分析

| 索引类型 | 存储膨胀 | 写入延迟影响 | Compaction 影响 |
| --- | --- | --- | --- |
| Bloom Filter | 1~3% | 极小 | 小 |
| Bitmap 索引 | 5~20%（视基数） | 小 | 中 |
| 倒排索引 | 10~50%（视文本量） | 中 | 中-大 |
| NGram BF | 3~10% | 小 | 小 |

> [!note] 批注｜增量开销要用相同负载做对照
>
> 原表百分比没有给出版本、行宽、词项数量或写入模式，只可作为估算参考。测试时保留同一批数据和查询条件，分别记录索引体积、写入吞吐、合并耗时和查询裁剪量，再决定是否接受写入代价。

---

## 七、索引实战案例

### 7.1 案例一：日志查询优化

场景：1 天 50 亿行日志，查询`WHERE dt = 'xxx' AND level = 'ERROR' AND message LIKE '%timeout%'`


```sql
-- 优化前：全分区扫描，耗时 45s
-- 优化后：
CREATE TABLE logs (
    dt DATE,
    level VARCHAR(16),
    service VARCHAR(64),
    message TEXT,
    trace_id VARCHAR(64)
) DUPLICATE KEY(dt, level, service)
PARTITION BY RANGE(dt) (...)
DISTRIBUTED BY RANDOM BUCKETS 32
PROPERTIES (
    "bloom_filter_columns" = "trace_id",    -- trace_id 高基数等值
    "inverted index" = "..."
);

-- 为 level 建 Bitmap 索引（低基数）
CREATE INDEX idx_level ON logs (level) USING BITMAP;

-- 为 message 建倒排索引（全文检索）
CREATE INDEX idx_msg ON logs (message) USING INVERTED 
PROPERTIES ("parser" = "unicode");

-- 优化后查询
SELECT * FROM logs 
WHERE dt = '2026-07-01' 
  AND level = 'ERROR' 
  AND message MATCH_ANY 'timeout';
-- 耗时：0.8s（分区裁剪 + Bitmap 过滤 + 倒排索引）

```

> [!warning] 核对说明｜性能对照前先校验两条查询返回同一批行
>
> 原案例将子串匹配替换为分词匹配，不能直接把 45 秒到 0.8 秒全归功于索引。若业务仍要求任意子串，用 `LIKE` 保持口径并评估 NGram BF；若允许词项查询，则用按预期分词的倒排索引。原例的 `"inverted index"` 是占位符，需按官方 DDL 声明实际索引。


### 7.2 案例二：用户画像标签筛选

场景：圈选同时满足多个标签条件的用户


```sql
CREATE TABLE user_profile (
    user_id BIGINT,
    gender TINYINT,          -- 0/1/2
    age_group VARCHAR(16),   -- '18-24','25-34',...
    city VARCHAR(32),
    is_vip BOOLEAN,
    music_preference VARCHAR(32)
) DUPLICATE KEY(user_id);

-- Bitmap 索引适合低基数多条件组合
CREATE INDEX idx_gender ON user_profile (gender) USING BITMAP;
CREATE INDEX idx_age ON user_profile (age_group) USING BITMAP;
CREATE INDEX idx_vip ON user_profile (is_vip) USING BITMAP;

-- 查询：18-24岁的VIP女性用户
SELECT user_id FROM user_profile
WHERE gender = 2 AND age_group = '18-24' AND is_vip = true;
-- Bitmap 索引对每个条件精确定位行号，然后做 AND 运算

```


### 7.3 案例三：分布式追踪 trace\_id 查询


```sql
-- trace_id 是高基数列（每个请求唯一），查询模式是精确等值
-- 方案：Bloom Filter（存储开销小，等值过滤效果好）

CREATE TABLE traces (
    dt DATE,
    trace_id VARCHAR(64),
    span_id VARCHAR(64),
    service VARCHAR(32),
    duration_ms INT
) DUPLICATE KEY(dt, trace_id)
PROPERTIES (
    "bloom_filter_columns" = "trace_id, span_id",
    "bloom_filter_fpp" = "0.01"
);

-- 查询
SELECT * FROM traces WHERE trace_id = 'abc123def456';
-- Bloom Filter 跳过 99%+ 的 Page，快速定位目标数据

```

> [!note] 批注｜点查先看已裁剪范围，再看 Bloom Filter
>
> 原查询只有 `trace_id`，没有示例表排序键左侧的 `dt` 条件，因此不应假设前缀索引或日期分区裁剪能生效。若业务能给出时间窗，先用它缩小范围；再看 Bloom Filter 在保留分区里实际减少多少读取。1% 的配置不等于现场一定跳过 99% 的页。


---

## 八、索引运维

### 8.1 索引管理操作


```sql
-- 查看表的索引信息
SHOW CREATE TABLE table_name;
SHOW INDEX FROM table_name;

-- 添加索引（Schema Change，在线异步执行）
ALTER TABLE table_name ADD INDEX idx_name (col) USING INVERTED;

-- 删除索引
ALTER TABLE table_name DROP INDEX idx_name;

-- 查看 Schema Change 进度
SHOW ALTER TABLE COLUMN;

```

> [!warning] 核对说明｜看见索引定义不等于历史数据已构建
>
> 新写入数据和历史数据可能在不同阶段获得索引。对于需要单独 `BUILD INDEX` 的场景，用 `SHOW BUILD INDEX` 跟踪；若目标版本和部署形态走 Schema Change，则检查对应任务达到完成状态。选一个历史分区做查询，用 Profile 确认实际读取及过滤情况。[倒排索引构建](https://doris.apache.org/docs/4.x/table-design/index/inverted-index/overview/)


### 8.2 索引大小估算


```sql
-- 查看表的存储详情（含索引占用）
SHOW DATA FROM db.table;

-- 查看 Tablet 级详情
SHOW TABLET FROM db.table;

```


---

## 九、小结

| 要点 | 内容 |
| --- | --- |
| Bloom Filter | 高基数等值过滤，存储开销小，有误判但 Page 级跳过效果好 |
| Bitmap 索引 | 低基数列精确过滤，多条件组合 AND/OR 高效 |
| 倒排索引 | 最强大（等值/范围/全文），但存储和写入开销最大 |
| NGram BF |`LIKE '%xxx%'`  场景的轻量级加速方案 |
| 选型原则 | 根据基数/查询模式/写入频率综合决策，不盲目堆索引 |
