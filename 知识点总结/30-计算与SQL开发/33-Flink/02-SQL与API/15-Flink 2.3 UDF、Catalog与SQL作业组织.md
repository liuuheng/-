---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 22
source_sections: "主教材第10.6.13、10.7—10.9节"
---

# Flink 2.3 UDF、Catalog 与 SQL 作业组织

内置函数不足时才扩展 UDF；表与函数元数据由 Catalog/Module 组织；SQL Client 或程序入口负责加载配置并提交作业。三层各司其职，不能把注册一张表当作已经运行数据流水线。

## 标量函数和表函数

```java
public static class SafeRatio extends ScalarFunction {
    public Double eval(Long numerator, Long denominator) {
        if (numerator == null || denominator == null || denominator == 0) return null;
        return numerator.doubleValue() / denominator;
    }
}
t.createTemporarySystemFunction("safe_ratio", SafeRatio.class);
// SELECT safe_ratio(pv, uv) FROM metrics
```

标量函数一行产出一个值；UDTF 可将一行展开多行，通过 LATERAL TABLE 关联。类型推导不明确时用 `DataTypeHint/FunctionHint` 描述，不能依赖 Java 的 `Object` 自动生成稳定 SQL schema。

函数应明确 null、空串、除零和异常行为。把外部 HTTP 调用塞进同步 scalar eval 会阻塞算子，并且可能破坏重放确定性；需要 I/O 的函数应另行选择受支持的异步能力或先做维度关联。

## UDAF 的核心：累加、撤回、合并

```java
public static class AvgAcc {
    public long sum;
    public long count;
}
public static class AvgMoney extends org.apache.flink.table.functions.AggregateFunction<Double, AvgAcc> {
    public AvgAcc createAccumulator() { return new AvgAcc(); }
    public Double getValue(AvgAcc a) { return a.count == 0 ? null : (double)a.sum/a.count; }
    public void accumulate(AvgAcc a, Long v) { if(v != null) { a.sum += v; a.count++; } }
    public void retract(AvgAcc a, Long v) { if(v != null) { a.sum -= v; a.count--; } }
    public void merge(AvgAcc a, Iterable<AvgAcc> others) {
        for (AvgAcc b : others) { a.sum += b.sum; a.count += b.count; }
    }
}
```

SQL 聚合函数与 DataStream 的同名 AggregateFunction 属于不同 API。支持 changelog 更新时可能需要 retract；Local-Global 优化需要可合并的累加器。整数金额范围要经过评估，不能让 long 溢出后仍输出看似正常的平均值。

## Catalog 保存的是定义

Catalog 管理数据库、表、函数等元数据。内存 Catalog 随当前环境生命周期存在，不应承诺重启后永久保留；持久化 Catalog 需要匹配其实现与外部服务。表的定义和表内实际数据分别由元数据系统与连接器/存储承担。

名称解析依赖当前 catalog/database；临时表可遮蔽同名持久表。复杂作业可使用完整三段名减少歧义，部署时记录 DDL 与依赖版本。

Module 提供函数集合及函数解析顺序，不负责持久化业务数据。启用 Hive 等模块前，先核对当前版本兼容性，不能只复制一组旧 jar 到 lib 目录。

## SQL 作业组织与计划检查

```sql
SET 'parallelism.default' = '2';
EXPLAIN PLAN FOR SELECT user_id, SUM(amount) FROM events GROUP BY user_id;
```

`WITH` CTE 提高可读性，不保证物化或只计算一次。多个输出可评估 StatementSet 统一优化，提交后仍要核对实际作业数量和共享计划。动态 OPTIONS Hint 只适合连接器允许覆盖的选项，不能认为它永久修改了 Catalog 表定义。

UDF 验证包含空输入、重复值、撤回、局部累加器合并，以及与内置 SUM/COUNT 得出的基准结果对比。仅对 append-only 正常数据成功，不足以承诺它适用于更新流。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：主教材第10.6.13、10.7—10.9节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [udfs](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/functions/udfs/)
- [catalogs](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/catalogs/)
- [modules](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/modules/)
- [sql-client](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/interfaces/sql-client/)

前篇：[[14-Flink 2.3 SQL Join选型与历史维度一致性]]

后篇：[[16-Flink 2.3 PTF与物化表的职责边界]]
