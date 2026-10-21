---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 21
source_sections: "主教材第10.6.12节；优化第3.3.4、6.1节"
---

# Flink 2.3 SQL Join 选型与历史维度一致性

选 Join 的关键是“匹配哪一版数据、等多久、保存多久”。相同关联字段并不意味着 Regular、Interval、Lookup 和 Temporal Join 可以互换。

| Join | 业务含义 | 状态或外部依赖 |
|---|---|---|
| Regular | 动态表之间持续关联 | 通常长期保留两侧可关联历史 |
| Interval | 两侧事件落在有限时间间隔 | 依时间进度清理匹配缓冲 |
| Window | 相同窗口内关联 | 窗口关闭后清理 |
| Lookup | 处理到事件时查询外部维度 | 外部 QPS、缓存与连接器能力 |
| Event-time Temporal | 查询事件发生时的维度版本 | 版本表主键、事件时间与 watermark |

## Interval Join 的 SQL 核心

```sql
SELECT o.order_id, o.amount, p.payment_id
FROM orders AS o JOIN payments AS p
ON o.order_id = p.order_id
AND p.et BETWEEN o.et AND o.et + INTERVAL '15' MINUTE;
```

两侧 et 需满足事件时间属性要求；按订单号和时间区间约束匹配。不能把日期格式化成字符串比较后，仍期待 planner 自动识别同样的 interval 清理机制。

## Regular 外连接会修正旧行

左记录先到时，LEFT JOIN 可先输出 `(order,NULL)`；右记录到达后，需要撤回或更新此前的空匹配，再输出完整关联。Sink 必须接得住更新。右侧同 key 有多行时，一条左记录可能匹配多条右记录。

为 Regular Join 配 TTL 是资源与语义的折中：到期后更晚到的对侧记录可能无法匹配。若业务天然只允许十五分钟内关联，优先用时间条件表达，而不是只靠一个与事件时间无关的处理时间 TTL。

## Lookup 与历史版本不是一回事

```sql
SELECT o.order_id, d.user_level
FROM orders AS o
LEFT JOIN user_dim FOR SYSTEM_TIME AS OF o.pt AS d
ON o.user_id = d.user_id;
```

这里 pt 为处理时间属性，user_dim 需要连接器提供 lookup 能力。缓存可降低外部压力，也可能暂时返回旧维度；失败重放时外部维度已经变化，同一订单可能查到不同值。Flink 内部 exactly-once 无法替外部查询冻结历史。

若要求按订单发生时的会员等级，使用具有有效主键和版本时间的事件时间 Temporal Join，而不是把上例的 pt 机械改成普通 timestamp。版本变更必须被正确建模，并保证主键条件和两侧 watermark。

## Window Join 的核心约束

对两侧先做相同定义的 TVF，在 ON 中包含业务 key、相等 window_start 和相等 window_end。匹配窗口边界相同不代表两侧业务主键唯一；仍要预防一对多放大。

## 验证有意义的反例

同一用户先为普通会员后升级，分别回放升级前后的订单。处理时间 lookup 可能都查到当前等级；事件时间版本关联应得到各自发生时等级。再交换两路到达顺序并观察 watermark，确认结果受所选业务语义约束。

外部 Lookup 示例属于接入模板：核验日官网尚无明确支持 2.3 的 JDBC 发布包，本系列没有把它标记为已完成外部端到端验证。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：主教材第10.6.12节；优化第3.3.4、6.1节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [joins](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/joins/)
- [window-join](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-join/)
- [versioned tables](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/sql-table-concepts/versioned_tables/)

前篇：[[13-Flink 2.3 SQL聚合、窗口TVF与去重排名]]

后篇：[[15-Flink 2.3 UDF、Catalog与SQL作业组织]]
