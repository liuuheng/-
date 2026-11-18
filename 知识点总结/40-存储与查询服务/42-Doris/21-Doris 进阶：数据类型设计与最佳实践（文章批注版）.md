---
title: "Doris 进阶篇02：数据类型设计与最佳实践"
author: "外星人的知识杂谈"
platform: "wechat"
source_url: "https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484345&idx=1&sn=544bdb4caf104879af9f1e7150d12e05&chksm=c1bf09a4f6c880b2c80f6ad06f0d433f13c172aef015c93765e912ec38f91c8f39553f998a63#rd"
published_at: "2026-08-01T12:20:00+08:00"
annotated_at: "2026-11-18"
cssclasses:
  - wechat-article
  - doris-intro-annotated
tags:
  - doris
  - 文章批注
---

# Doris 进阶篇02：数据类型设计与最佳实践

<div class="article-meta">外星人的知识杂谈 · 2026-08-01</div>

[原文链接](https://mp.weixin.qq.com/s?__biz=MzkxNzU5Mzk3MA==&mid=2247484345&idx=1&sn=544bdb4caf104879af9f1e7150d12e05&chksm=c1bf09a4f6c880b2c80f6ad06f0d433f13c172aef015c93765e912ec38f91c8f39553f998a63#rd)

<!-- 保留原文正文、顺序与示例；批注和核对说明为新增。装饰封面与篇末导航已移除；代码未在集群运行。 -->

> **定位：** 进阶篇第 2 篇 | 类型选型的性能影响与生产经验

---

## 一、背景动机

入门篇介绍了各类型“是什么”，本篇回答“怎么选”。类型选错的代价不仅是存储浪费，还会引发 Compaction 变慢、查询超时、内存不足等连锁反应。

---

## 二、BITMAP vs COUNT DISTINCT：精确去重选型

### 2.1 为什么 COUNT DISTINCT 慢


```sql
-- 方案 A：Duplicate Key + count distinct（运行时计算）
SELECT dt, COUNT(DISTINCT user_id) FROM events GROUP BY dt;
-- 扫描全部明细行，运行时排序去重，亿级数据耗时分钟级

```

```sql
-- 方案 B：Aggregate Key + BITMAP（预聚合）
SELECT dt, BITMAP_UNION_COUNT(uv) FROM daily_uv GROUP BY dt;
-- 扫描预聚合后的行（可能只有几十行），毫秒级返回

```

> [!warning] 核对说明｜耗时来自参与聚合的明细量和跨节点去重
>
> `COUNT(DISTINCT user_id)` 不一定要排序。Doris 可以先在各 BE 上按分组键和 `user_id` 去重，再交换中间数据并做全局去重。`BITMAP` 方案预先保存可合并的用户集合，查询时可以少扫明细、少传中间行；收益要看每个分组实际保留的状态量。[精确去重](https://doris.apache.org/docs/4.x/query-acceleration/distinct-counts/bitmap-precise-deduplication/)


### 2.2 真实案例：99s → 284ms

网易云音乐 VIP 可视化场景（参见`Doris落地/总结/最佳实践/Bitmap UV 慢查询优化`）：

| 阶段 | 方案 | 耗时 | 瓶颈 |
| --- | --- | --- | --- |
| 原始 | Duplicate Key + count distinct | 99s | 全量扫描 + 运行时聚合 |
| 优化后 | Aggregate Key + BITMAP\_UNION + Rollup | 284ms | 无（预聚合 + 降维） |

**关键差异：** 扫描行数从 290 万 → 27 行。

> [!note] 批注｜案例同时改变了三项，不能把全部提速归给 BITMAP
>
> 案例同时更换了数据模型、去重状态和查询所扫的 Rollup。比较结果前要确认两边过滤条件、分组粒度和用户 ID 口径相同，再用 `EXPLAIN` 与实际 Profile 核对命中的扫描对象和行数。若查询需要 Rollup 已省略的维度，不能照用这个降维结果。

### 2.3 BITMAP 适用边界

| 场景 | 是否适用 | 原因 |
| --- | --- | --- |
| UV 统计（用户ID为整数） | 适用 | 经典场景 |
| 标签交并集运算 | 适用 | Bitmap AND/OR/XOR |
| 去重对象是字符串 | 需映射 | 先通过字典表转为 INT |
| 单个 Bitmap 基数 > 1亿 | 评估存储 | 单 Bitmap 可达数十 MB |
| 只需近似结果 | 用 HLL 更好 | 固定 16KB vs 可变 MB |

> [!note] 批注｜精确去重要用稳定的一一映射
>
> 同一个用户在各天、各业务分区都要映射成同一非负整数，不同用户不能共用一个 ID，否则集合合并的结果会失真。普通字符串哈希存在碰撞，不能当作严格精确的字典。评估 Bitmap 大小还要看值域分布、连续性以及位图组合数，原文的 1 亿不是切换阈值。

---

## 三、HLL 误差控制与适用场景

### 3.1 HLL 精度

HLL 标准误差率约 1.04/√(2^p)，Doris 默认 p=14（16384 个寄存器），误差约 0.81%。


```text
-- 实际测试：1000万 distinct user_id
-- HLL 估计值：9,921,347（误差 0.79%）
-- 精确值：10,000,000

```


### 3.2 HLL vs BITMAP 决策


```text
精确度要求？
├── 必须精确 → BITMAP
└── 允许 1~2% 误差
    ├── 基数 < 1亿 → BITMAP（存储可控）
    └── 基数 > 1亿 或 维度组合极多 → HLL（固定空间）

```

> [!warning] 核对说明｜标准误差不是每次查询的上限
>
> 原式 `1.04 / √(2^14)` 给出约 0.81% 的相对标准误差量级；单次样本的实际误差可能更大。约 16 KB 对应稠密状态的量级，小集合的序列化状态可以更小。选型时把业务能否容忍误差放在首位，并对多组真实数据对照精确计数。[HLL 近似去重](https://doris.apache.org/docs/4.x/query-acceleration/distinct-counts/hll-approximate-deduplication/)


---

## 四、VARIANT vs JSONB vs 展开列

### 4.1 性能对比

针对日志场景的查询`SELECT properties['duration'] FROM logs WHERE properties['page'] = 'home'`：

| 方案 | 存储方式 | 查询耗时（1亿行） | 存储空间 |
| --- | --- | --- | --- |
| VARCHAR + json\_extract | 文本 | 45s | 100% |
| JSONB | 二进制 JSON | 12s | 80% |
| VARIANT | 列式 subcolumn | 1.8s | 60% |
| 展开为独立列 | 原生列 | 0.9s | 55% |

> [!warning] 核对说明｜对照同一查询口径，再看子列和扫描成本
>
> 原表数值没有提供版本、索引和缓存等复现实验条件，不能用来承诺倍数。对比时让四种写法返回相同类型和相同过滤结果，再分别测冷、热缓存，并用 Profile 查看扫描字节及是否实际读取目标子列。4.x 里 SQL 类型名是 `JSON`，`JSONB` 指二进制存储格式。[JSON 类型](https://doris.apache.org/docs/4.x/sql-manual/basic-element/sql-data-types/semi-structured/JSON/)

### 4.2 选型逻辑


```text
Schema 是否固定？
├── 完全固定 → 展开为独立列（性能最优）
├── 基本固定但偶有新增 → VARIANT（自动提取高频字段为 subcolumn）
├── 完全动态且查询多 → VARIANT
└── 完全动态且查询少 → JSONB（保留完整结构）

```


### 4.3 VARIANT 的链式影响

VARIANT 类型对底层的影响：

- **存储：**

  高频字段自动提取为独立列存储，享受列式压缩
- **Compaction：**

  涉及 subcolumn 合并，比普通列稍慢
- **索引：**

  支持为 subcolumn 创建倒排索引
- **Schema Evolution：**

  新字段写入时自动扩展 subcolumn，无需 ALTER TABLE

> [!note] 批注｜自动推断并不保证每个新路径都有独立列
>
> 可提取的路径取决于目标版本、子列上限和路径配置；不能按访问频率假定任意查询字段都已物化。同一路径有数值和字符串混写时，先统一写入类型，或查询时显式转换并核对缺失及失败值。高频而稳定的过滤字段还可以单独建列。[VARIANT](https://doris.apache.org/docs/4.x/sql-manual/basic-element/sql-data-types/semi-structured/VARIANT/)

---

## 五、复杂类型（ARRAY/MAP/STRUCT）的取舍

### 5.1 ARRAY 适用场景


```sql
-- 用户标签列表
tags ARRAY<VARCHAR(64)>

-- 查询：包含某标签的用户
SELECT user_id FROM users WHERE array_contains(tags, 'vip');

```


**注意：** ARRAY 列不能作为 Key 列，不能建前缀索引，过滤效率依赖倒排索引或全扫。

> [!note] 批注｜成员查询与按成员展开是两种工作量
>
> 只问某一行是否包含 `vip` 时，`array_contains` 不必先展开数组。要使用倒排索引，还需元素类型受支持、索引已构建，查询形态也能利用它；建了 ARRAY 列不等于有索引。若每个标签都需要 JOIN 维表，展开后每个标签都产生一行，成本随总元素数增长。[ARRAY_CONTAINS](https://doris.apache.org/docs/4.x/sql-manual/sql-functions/scalar-functions/array-functions/array-contains/)

### 5.2 何时用 ARRAY vs 行展开

| 场景 | ARRAY | 行展开（每个元素一行） |
| --- | --- | --- |
| 元素数量不定 | 天然适合 | 行数膨胀 |
| 需要聚合元素 | array\_agg 支持 | 直接 GROUP BY |
| 需要 JOIN | 需 UNNEST 展开 | 直接 JOIN |
| 存储效率 | 紧凑 | 冗余（Key 列重复） |

**经验法则：** 如果 90% 的查询都需要 UNNEST 才能用，说明应该行展开而非用 ARRAY。

> [!warning] 核对说明｜按元素聚合前先定义展开语义
>
> `ARRAY_AGG` 收集输入行生成数组，不会替代对数组元素的 `GROUP BY`。Doris 可用 `LATERAL VIEW EXPLODE(tags) e AS tag` 展开；要核对空数组、NULL 和重复成员，避免把人次误算为人数。原文的 90% 是经验说法，是否预展开取决于更新成本、元素总数和查询形态。[LATERAL VIEW](https://doris.apache.org/docs/4.x/query-data/lateral-view/)

### 5.3 MAP vs VARIANT

|  | MAP | VARIANT |
| --- | --- | --- |
| Schema | 必须声明 KV 类型 | 完全自由 |
| 嵌套 | 不支持 | 支持任意嵌套 |
| 列式存储 | 否（整体存储） | 是（subcolumn 提取） |
| 查询性能 | 中 | 高（高频字段） |
| 适合 | Key 集合有限且已知 | Key 集合未知或频繁变化 |

> [!warning] 核对说明｜MAP 有类型约束，也支持部分嵌套
>
> 4.x 文档允许 MAP 的值类型为 `ARRAY`、`MAP` 或 `STRUCT`；原表的“不支持嵌套”不适用。MAP 的键和值类型在声明时确定，同一列的键集合仍可以逐行变化。原表的“整体存储”也不能理解为不参与 Doris 列式存储；比较时看查询能否按路径只取所需数据。[MAP](https://doris.apache.org/docs/4.x/sql-manual/basic-element/sql-data-types/semi-structured/MAP/)

---

## 六、类型对存储/Compaction/查询的链式影响

| 类型 | 存储开销 | Compaction 开销 | 查询开销 |
| --- | --- | --- | --- |
| INT/BIGINT | 低 | 低 | 低 |
| VARCHAR | 中 | 低 | 中（解码） |
| BITMAP | 高（单值可达 MB） | 高（需 bitmap union） | 低（预聚合后） |
| HLL | 固定 16KB | 中（merge 操作） | 低 |
| VARIANT | 中 | 中-高（subcolumn 合并） | 低-中 |
| ARRAY | 中 | 低 | 中（遍历元素） |

**关键教训：** BITMAP 列的 Compaction 代价远高于普通列。如果一张表有多个 BITMAP 列且写入频繁，Compaction Score 会持续偏高。

> [!warning] 核对说明｜Compaction Score 反映合并压力，不能单独定位 BITMAP
>
> 合并包含 BITMAP 的聚合行时，确实可能多做集合并集；Score 持续偏高还可能是写得太碎、桶倾斜或合并资源不足。对照相同写入速率下的每 Tablet Rowset 增长、合并吞吐和合并耗时，再判断瓶颈来自写入端还是状态合并。[Data Compaction](https://doris.apache.org/docs/4.x/key-features/data-compaction/)

---

## 七、生产经验总结

1. **UV 统计优先 BITMAP + Aggregate Key + Rollup，**

   这是 Doris 相比 CK 的核心优势
2. **日志属性用 VARIANT 而非 VARCHAR + json\_extract，**

   性能差距 20 倍以上
3. **不要用 STRING 存储可以用 INT 表示的数据**

   （如 user\_id），存储和查询效率差 3~5 倍
4. **DECIMAL 精度够用就好，**

   DECIMAL(38,18) 比 DECIMAL(10,2) 多占 3 倍空间
5. **DATETIMEV2 优于 DATETIME**

   （V2 支持微秒精度且存储更紧凑）

> [!warning] 核对说明｜DECIMAL 和时间精度按实际类型声明看
>
> 4.x 的 `DECIMAL` 按精度档位使用 4、8、16 或 32 字节，`DECIMAL(10,2)` 与 `DECIMAL(38,18)` 的类型宽度不应按原文的“三倍”计算，压缩后的磁盘占用也不等于宽度之比。需要微秒时写明 `DATETIME(6)`，再用 `SHOW CREATE TABLE` 核对目标版本的定义。性能倍数和跨引擎结论只保留为原文观点。[Doris 数据类型](https://doris.apache.org/docs/4.x/table-design/data-type/)

---

## 八、小结

| 要点 | 内容 |
| --- | --- |
| BITMAP | UV 场景的杀手级方案，配合 Rollup 实现 100x 加速 |
| HLL | 超高基数 + 允许误差时的最佳选择 |
| VARIANT | 半结构化日志的标配，远优于 VARCHAR+json |
| 类型链式影响 | 类型选择影响存储→Compaction→查询全链路 |
