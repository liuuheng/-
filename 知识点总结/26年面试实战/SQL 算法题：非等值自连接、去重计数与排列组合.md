---
title: SQL 算法题：非等值自连接、去重计数与排列组合
aliases: [SQL 自连接算法题, 学生分数配对, SQL 排列组合]
tags: [面试, SQL, SparkSQL, 算法, 自连接, 排列组合]
created: 2026-11-16
updated: 2026-11-16
---

# SQL 算法题：非等值自连接、去重计数与排列组合

这类题需要把自然语言转换成记录之间的关系，再确定结果粒度和计数对象。自连接用于比较同一集合中的记录，去重规则解决“哪些结果在业务上算同一个”，计数则取决于题目要求的是组合、参与者还是每个人的匹配人数。

相关原题：[[知识点总结/26年面试实战/KIMI 大数据开发三轮面试：完整题目与参考回答#C7 网吧用户对 SQL：明确时间条件与去重粒度|网吧用户对 SQL]]。

学生题及其变题来自本次讨论，是教学示例；网吧题的题意与口径取自上述已有笔记。SQL 按 Spark SQL 编写，样例临时视图需要在同一会话中创建。本笔记的 SQL 已做 Spark 方言解析，并用本地 SQLite 核对关系运算结果；未在 Spark 集群执行，不包含性能验证。

## 1. 解题时持续跟踪“一行代表什么”

拿到题目后，先确定输入粒度：学生表的一行代表一名学生，还是一名学生的一次考试？如果同一个学生有多行，必须先明确是按考试记录配对，还是按学生配对；需要取哪次成绩，也由题目决定。

本例使用同一张表的两个别名，所以是自连接；`a.student_id < b.student_id` 和分数范围判断是非等值条件，所以属于非等值自连接。自连接描述的是表的使用方式，非等值描述的是匹配条件，两者可以同时成立。

再确定配对是否有方向。“两人组成小组”中，张三与李四交换位置仍是同一组；“张三指导李四”中，左右位置表示不同角色。只有题目不区分角色，且交换两侧后业务匹配条件仍然成立，才适合用 ID 大小关系消除镜像组合。

连接之后，一行通常已经变成“一对记录”。此时要分别判断：题目是在数配对行、不同参与者，还是每个参与者的匹配记录。不能看到“去重”就直接添加 `DISTINCT`，也不能看到“人数”就直接数连接行。

面试时可以这样推导：“输入一行是一名学生；连接后一行是一对学生；交换左右属于重复，所以固定 ID 顺序；最终要求参与人数，因此需要收集两侧 ID 后去重。”

## 2. 完整例题：分数差不超过 10 的学生配对

### 2.1 题目、数据和成立前提

从学生表中找出两名不同学生，要求分数差不超过 10。两人的先后顺序不影响组合，分别统计组合数和参与配对的学生人数。

本例约定：每名学生只有一行，`student_id` 唯一且非空，`score` 非空；不同学生同分时仍然可以配对。`student_name` 只用于展示，不能代替 ID 判断是不是同一个人。

| student_id | student_name | score |
|---:|---|---:|
| 1 | 张三 | 80 |
| 2 | 李四 | 85 |
| 3 | 王五 | 90 |
| 4 | 赵六 | 95 |
| 5 | 孙七 | 60 |

先创建一次样例临时视图，后续学生题 SQL 都使用它。

```sql
CREATE OR REPLACE TEMP VIEW student_pair_demo AS
SELECT 1 AS student_id, '张三' AS student_name, 80 AS score
UNION ALL
SELECT 2, '李四', 85
UNION ALL
SELECT 3, '王五', 90
UNION ALL
SELECT 4, '赵六', 95
UNION ALL
SELECT 5, '孙七', 60;
```

### 2.2 同时统计组合数和参与人数

```sql
WITH pairs AS (
    SELECT
        a.student_id AS student_a,
        b.student_id AS student_b
    FROM student_pair_demo a
    JOIN student_pair_demo b
        ON a.student_id < b.student_id
       AND ABS(a.score - b.score) <= 10
),
participants AS (
    SELECT student_a AS student_id FROM pairs
    UNION
    SELECT student_b AS student_id FROM pairs
)
SELECT
    (SELECT COUNT(*) FROM pairs)        AS pair_count,
    (SELECT COUNT(*) FROM participants) AS participant_count;
```

结果是 `pair_count = 5`、`participant_count = 4`。`pairs` 每行代表一个组合；`participants` 每行代表一名参与过配对的学生。若没有任何组合，两个计数都会返回 0。

### 2.3 自连接的完整中间结果

两个别名 `a`、`b` 各自表示同一张学生表。逻辑上，5 行与 5 行可以产生 25 个有序候选，包括自己与自己。为了看清两个条件的作用，先只应用分数差条件，再检查 ID 条件；数据库不一定真的按这个顺序生成中间表。

下面的 SQL 可以查看只满足分数差条件的 15 行。

```sql
SELECT
    a.student_id   AS student_a,
    a.student_name AS name_a,
    a.score        AS score_a,
    b.student_id   AS student_b,
    b.student_name AS name_b,
    b.score        AS score_b,
    ABS(a.score - b.score) AS score_diff
FROM student_pair_demo a
JOIN student_pair_demo b
    ON ABS(a.score - b.score) <= 10
ORDER BY
    a.student_id,
    b.student_id;
```

| a：学生（ID / 分数） | b：学生（ID / 分数） | 分数差 | 再检查 `a.student_id < b.student_id` |
|---|---|---:|---|
| 张三（1 / 80） | 张三（1 / 80） | 0 | 不保留 |
| 张三（1 / 80） | 李四（2 / 85） | 5 | 保留 |
| 张三（1 / 80） | 王五（3 / 90） | 10 | 保留 |
| 李四（2 / 85） | 张三（1 / 80） | 5 | 不保留 |
| 李四（2 / 85） | 李四（2 / 85） | 0 | 不保留 |
| 李四（2 / 85） | 王五（3 / 90） | 5 | 保留 |
| 李四（2 / 85） | 赵六（4 / 95） | 10 | 保留 |
| 王五（3 / 90） | 张三（1 / 80） | 10 | 不保留 |
| 王五（3 / 90） | 李四（2 / 85） | 5 | 不保留 |
| 王五（3 / 90） | 王五（3 / 90） | 0 | 不保留 |
| 王五（3 / 90） | 赵六（4 / 95） | 5 | 保留 |
| 赵六（4 / 95） | 李四（2 / 85） | 10 | 不保留 |
| 赵六（4 / 95） | 王五（3 / 90） | 5 | 不保留 |
| 赵六（4 / 95） | 赵六（4 / 95） | 0 | 不保留 |
| 孙七（5 / 60） | 孙七（5 / 60） | 0 | 不保留 |

这 15 行包括 5 个自身配对和 10 个有方向的非自身配对。孙七虽然没有其他可匹配的学生，仍然能与自己满足分数差条件；需要通过 ID 条件排除自身。

加入 ID 条件后，保留 `(1, 2)`、`(1, 3)`、`(2, 3)`、`(2, 4)`、`(3, 4)`，一共 5 个组合。

### 2.4 `<` 怎样去重，参与人数为什么合并两侧 ID

张三和李四会形成 `(1, 2)` 与 `(2, 1)` 两行。`a.student_id < b.student_id` 约定较小 ID 放左边、较大 ID 放右边，因此只保留 `(1, 2)`。任何两个不同且非空的唯一 ID，恰好只有一个方向满足 `<`；相同 ID 则两个方向都不满足。

这里“固定顺序”只表示固定组合内部的左右位置。ID 的顺序不代表分数顺序，也不保证查询结果的显示顺序；显示排序需要 `ORDER BY`。换成 `>` 也能达到相同的无序组合去重目的，只是固定方向相反。仅写 `<>` 可以排除自己，但会保留两个方向。

最终五行中，左侧 ID 是 `1, 1, 2, 2, 3`，去重后为 `{1, 2, 3}`；右侧 ID 是 `2, 3, 3, 4, 4`，去重后为 `{2, 3, 4}`。只数左侧会漏掉赵六，只数右侧会漏掉张三；两侧去重人数直接相加，又会重复计算李四和王五。

`UNION` 收集两侧 ID 并去重后得到 `{1, 2, 3, 4}`，所以参与人数是 4。若使用 `UNION ALL`，必须在合并之后再按 ID 去重，否则每次出现在组合中都会计数。

## 3. 五道常见变题及完整 SQL

### 3.1 每名学生有多少名匹配同学，保留零匹配学生

题目要求一行代表“一名学生及其匹配人数”。同一学生需要看到自己的全部匹配同学，因此保留两个方向，使用 `<>` 排除自己。`LEFT JOIN` 保留没有匹配的学生。

```sql
SELECT
    a.student_id,
    a.student_name,
    COUNT(b.student_id) AS matched_student_count
FROM student_pair_demo a
LEFT JOIN student_pair_demo b
    ON a.student_id <> b.student_id
   AND ABS(a.score - b.score) <= 10
GROUP BY
    a.student_id,
    a.student_name
ORDER BY a.student_id;
```

按 ID 顺序，匹配人数为 `2、3、3、2、0`。孙七仍有一行连接结果，只是右侧为 `NULL`。`COUNT(b.student_id)` 不计这个空值；如果改成 `COUNT(*)`，孙七会错误地得到 1。

不能把 ID 条件改成 `<`：那样每个人只会统计 ID 比自己大的匹配同学，赵六甚至会被错误计为 0。

### 3.2 找出至少有 3 名匹配同学的学生

先按学生计算匹配人数，再筛选分组后的计数。`HAVING` 筛选聚合结果，`WHERE` 不能直接承担这里的计数条件。

```sql
SELECT
    a.student_id,
    a.student_name,
    COUNT(b.student_id) AS matched_student_count
FROM student_pair_demo a
JOIN student_pair_demo b
    ON a.student_id <> b.student_id
   AND ABS(a.score - b.score) <= 10
GROUP BY
    a.student_id,
    a.student_name
HAVING COUNT(b.student_id) >= 3
ORDER BY a.student_id;
```

结果是李四和王五，匹配人数均为 3。题目只要求至少 3 人，零匹配学生本来就不可能满足，因此使用内连接即可。

### 3.3 找出没有任何其他匹配同学的学生

先保留全部左侧学生，再选择右侧没有匹配记录的行。

```sql
SELECT
    a.student_id,
    a.student_name,
    a.score
FROM student_pair_demo a
LEFT JOIN student_pair_demo b
    ON a.student_id <> b.student_id
   AND ABS(a.score - b.score) <= 10
WHERE b.student_id IS NULL
ORDER BY a.student_id;
```

结果只有孙七。这里能用右侧 ID 是否为空判断匹配，是因为源表 ID 非空。也可以用相关 `NOT EXISTS` 表达“不存在其他匹配同学”，但目标引擎对相关子查询的支持需要另行确认。

### 3.4 找出左侧学生比右侧学生恰好高 10 分的配对

左右角色分别代表高分学生和低分学生，条件有方向。

```sql
SELECT
    a.student_id   AS higher_student_id,
    a.student_name AS higher_student_name,
    a.score        AS higher_score,
    b.student_id   AS lower_student_id,
    b.student_name AS lower_student_name,
    b.score        AS lower_score
FROM student_pair_demo a
JOIN student_pair_demo b
    ON a.score - b.score = 10
ORDER BY
    a.student_id,
    b.student_id;
```

结果为王五与张三，即 `(3, 1)`；赵六与李四，即 `(4, 2)`。分数条件本身排除了自己。不应再加 `a.student_id < b.student_id`，因为 ID 大小与分数高低无关；本例两个合法结果都会被这个额外条件删除。

### 3.5 三名学生组成一组，任意两人分差都不超过 10

三个人没有角色区别，使用严格递增的 ID 固定一种表示。需要检查全部三对分数差。

```sql
SELECT
    COUNT(*) AS triple_count
FROM student_pair_demo a
JOIN student_pair_demo b
    ON a.student_id < b.student_id
   AND ABS(a.score - b.score) <= 10
JOIN student_pair_demo c
    ON b.student_id < c.student_id
   AND ABS(a.score - c.score) <= 10
   AND ABS(b.score - c.score) <= 10;
```

结果是 2 组，分别为 `(1, 2, 3)` 和 `(2, 3, 4)`。三个不同学生原本有 6 种位置排列，严格递增的 ID 只保留其中一种。

“任意两人分差不超过 10”等价于三人的最高分减最低分不超过 10。只检查相邻两对不够，例如 80、90、100 的相邻分差都是 10，但两端相差 20。按 ID 递增也不意味着按分数递增，因此不能据此省略分数约束。

## 4. ON 与 WHERE：匹配条件和结果筛选

`ON` 定义两行是否匹配；连接类型决定未匹配行是否保留；`WHERE` 再筛选连接结果。这是理解语义的逻辑顺序，不等于物理执行顺序。

不能简单理解成“关联写在 ON，额外条件写在 WHERE”。学生 ID 条件和分数差条件都可以共同定义匹配；单张表上的条件也可能放在 `ON` 中参与判断。

对于本例的内连接，`ON a.student_id < b.student_id AND ABS(a.score - b.score) <= 10`，与在 `ON` 中保留 ID 条件、在 `WHERE` 中检查分数差，结果相同。这个结论针对这里的普通内连接及同样的筛选表达式，不应直接套到外连接。

### 4.1 把分数条件移到 WHERE，为什么会丢掉孙七

下面的 SQL 语法有效，但不满足“保留零匹配学生”的题意。

```sql
SELECT
    a.student_id,
    a.student_name,
    COUNT(b.student_id) AS matched_student_count
FROM student_pair_demo a
LEFT JOIN student_pair_demo b
    ON a.student_id <> b.student_id
WHERE ABS(a.score - b.score) <= 10
GROUP BY
    a.student_id,
    a.student_name
ORDER BY a.student_id;
```

`ON` 只检查 ID 不同，因此孙七先与另外四名学生连接；这些行随后全部因分数差超过 10 而被 `WHERE` 删除。外连接不会在筛选之后重新给孙七补一行。结果只剩前四名学生，无法得到孙七匹配人数为 0。

完整分数条件放在 `ON` 时，孙七没有任何满足完整条件的右侧记录，`LEFT JOIN` 才会为他保留右侧为空的一行。

### 4.2 WHERE 中可以有右表条件

第 3.3 题中的 `WHERE b.student_id IS NULL` 就是有意从连接结果中筛选未匹配学生。应根据题目判断筛选后的语义，不能归纳成“右表条件永远不能写在 WHERE”。一般而言，右侧为 `NULL` 时，`ABS(a.score - b.score) <= 10` 不为真，因此这类条件放在 `WHERE` 中不会保留空扩展行。

## 5. 排列组合、关联条件与各自场景

### 5.1 为什么 `<` 对应二人组合

同一张含有 $n$ 个对象的表与自己配对，可以得到 $n^2$ 个有序候选。排除 $n$ 个自身配对后，剩下 $n(n-1)$ 个，对应排列数 $A_n^2$。

无序二人组合中，`(A, B)` 与 `(B, A)` 算同一组。用唯一且非空 ID 的 `<` 或 `>` 保留一个方向，数量减半：

$$
C_n^2 = \binom{n}{2} = \frac{n(n-1)}{2}
$$

本例有 5 人，只应用 `<` 时得到 $C_5^2=10$ 个组合；增加分数差条件之后只剩 5 个。组合公式表示尚未增加额外业务限制的候选数量，不能直接当成筛选后的答案。

### 5.2 七类常见情况

下表用 `id` 简写对象 ID。假设每个对象一行、ID 唯一且非空；同一集合有 $n$ 个对象，不同集合分别有 $m$、$n$ 个对象。公式均未增加分数、资格等额外过滤。

| 枚举目标 | 关联写法或逻辑条件 | 候选数量 | 对应场景 |
|---|---|---|---|
| 两人有序，允许自己与自己 | `CROSS JOIN` | $n^2$ | 枚举完整的站点起终点矩阵，包括本站到本站；去程和返程是不同方向 |
| 两人有序，两人不同 | `a.id <> b.id` | $A_n^2=n(n-1)$ | 从同一名单选队长和副队长，同一人不能兼任，交换职责算不同安排 |
| 两人无序，两人不同 | `a.id < b.id` | $C_n^2=n(n-1)/2$ | 两名学生组成学习小组，不分角色 |
| 两人无序，保留自己与自己 | `a.id <= b.id` | $n(n+1)/2$ | 对称距离矩阵只保存一半，同时保留自身距离 0 |
| 三人有序，三人各不相同 | `a.id <> b.id AND a.id <> c.id AND b.id <> c.id` | $A_n^3=n(n-1)(n-2)$ | 选主持人、记录员、计时员，三人不能兼任，交换职责算不同安排 |
| 三人无序，三人各不相同 | `a.id < b.id AND b.id < c.id` | $C_n^3=n(n-1)(n-2)/6$ | 三名学生组成讨论小组，不分角色 |
| 不同集合各取一个对象 | `CROSS JOIN` | $m\times n$ | 枚举学生和课程的全部候选选课搭配，左右分别代表学生和课程 |

“有序”指位置代表不同角色，不是查询结果已经排序。三表条件是最终需同时满足的关系，实际 SQL 中应在对应别名参与连接后再引用它。

`<=` 比 `<` 多保留的是 $n$ 个自身配对，不是允许源表重复记录。三人排列必须检查三对 ID 均不相同；仅有 `a.id <> b.id AND b.id <> c.id`，仍然允许 `a.id = c.id`。

不同集合中的 ID 数值大小通常没有去重含义。例如学生 ID 为 5，课程 ID 为 2，仍是合法搭配；增加 `student_id < course_id` 会删掉它。`CROSS JOIN` 这里只枚举候选，实际是否允许选课，还要增加资格、容量等题目条件。

### 5.3 同组配对与有方向的业务条件

“同班两名学生组成一组”可以使用 `a.class_id = b.class_id AND a.id < b.id`。分组 ID 非空时，候选数量是各班组合数之和 $\sum_g C_{n_g}^2$。普通 `=` 不会让两个 `NULL` 班级值匹配；是否将未知班级视为一组，需要单独约定。

“左侧分数高于右侧”使用 `a.score > b.score`，已经定义了方向。不能再随意叠加 ID 顺序限制；只有业务关系不区分方向时，ID 大小才适合用来固定唯一表示。

## 6. 实际面试题迁移：三家网吧出现过的用户对

相关原笔记的题目是：两名用户在同一家网吧上下线时间相差不超过 10 分钟，视为可能认识；至少在三家网吧出现，则给出题目定义的“认识”标签。原题没有给完整表结构，时间条件也有歧义，不能直接写 SQL 后默认所有解释都成立。

这里沿用相关笔记的明确口径：输入表 `cafe_session` 的一行是一段完整会话，包含 `user_id`、`cafe_id`、`login_s`、`logout_s`，时间是同一基准下的整数秒；同一网吧内，两名不同用户的上线时间差、下线时间差都不超过 600 秒，才算该网吧的一次有效证据。要求至少三个不同网吧，不是三次会话。

```sql
WITH valid_sessions AS (
    SELECT DISTINCT
        user_id,
        cafe_id,
        login_s,
        logout_s
    FROM cafe_session
    WHERE user_id IS NOT NULL
      AND cafe_id IS NOT NULL
      AND login_s IS NOT NULL
      AND logout_s IS NOT NULL
      AND logout_s >= login_s
),
pair_cafe AS (
    SELECT DISTINCT
        a.user_id AS user_a,
        b.user_id AS user_b,
        a.cafe_id
    FROM valid_sessions a
    JOIN valid_sessions b
        ON a.cafe_id = b.cafe_id
       AND a.user_id < b.user_id
       AND ABS(a.login_s - b.login_s) <= 600
       AND ABS(a.logout_s - b.logout_s) <= 600
)
SELECT
    user_a,
    user_b,
    COUNT(*) AS cafe_count
FROM pair_cafe
GROUP BY
    user_a,
    user_b
HAVING COUNT(*) >= 3;
```

这道题的去重分为两个层次。`a.user_id < b.user_id` 消除同一用户对的镜像方向；`SELECT DISTINCT user_a, user_b, cafe_id` 则把同一用户对在同一家网吧的多次匹配合成一条证据。去重后每行已经代表“某用户对在某网吧出现过”，`COUNT(*)` 才等于网吧数量。

这里一名用户可以有多次会话，因此“ID 大小条件能消除镜像方向”和“每个用户对只剩一行”不能等同。即使原始会话完全不重复，多次合法会话仍然可能产生多个匹配。

如果题目改成只看上线时间，就不应保留下线时间约束；如果改成会话区间重叠，也要重写时间条件。输入若是一行一次上线或下线事件，需要先按真实会话标识还原会话，不能随意把相邻事件拼起来。题目中的关系标签不证明现实中两个人一定认识。

## 7. 复核 SQL 时检查哪些边界

- 实体粒度：同一学生是否只有一行？如果分数冲突，需要先确定有效成绩规则，`DISTINCT` 无法替代这个规则。ID 大小条件也不能修复源表重复行。
- 同分不同人：通过 ID 排除自己，不应通过 `a.score <> b.score` 排除自己，否则会误删不同学生的同分配对。
- 空值：本例约定分数非空。如果实际分数为 `NULL`，分数差条件不为真；左连接可能显示匹配数为 0，“无匹配”查询也可能返回该学生。分数未知与已知没有合格同学是否等价，必须明确。
- 零匹配和空结果：检查左连接是否保留了应保留的学生，检查 `COUNT(*)` 是否把空扩展行计为 1；组合与参与人数查询在没有配对时应返回 0。
- 对称性与角色：交换左右是否还是同一个结果、是否仍满足业务条件？需要统计每个人的匹配数时，不要沿用无序组合的 ID 限制。
- 计数单位：一行是会话对、用户对、用户对加网吧，还是参与者？确认去重层级之后再选择 `COUNT(*)`、`COUNT(DISTINCT ...)` 或两侧 ID 合并。
- 三人约束：检查三人是否各不相同，全部业务条件是否覆盖三对关系；ID 顺序与分数顺序没有必然关系。

## 核对范围

学生样例已核对：分数条件下 15 行；无序二人组合 5 组、参与人数 4 人；每人匹配数 `2、3、3、2、0`；至少 3 名匹配同学的是李四和王五；零匹配的是孙七；有向高 10 分配对为 `(3, 1)`、`(4, 2)`；三人组合 2 组。网吧拓展使用本地构造数据检查了“同一网吧多次匹配只计一家”与“至少三家”的边界。

核对结果用于确认本文示例的关系逻辑，不代表 Spark 执行计划、集群资源消耗或大规模自连接性能结论。
