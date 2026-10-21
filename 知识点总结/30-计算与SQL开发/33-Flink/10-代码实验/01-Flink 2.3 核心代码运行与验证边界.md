---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 34
source_sections: "本系列综合实验"
---

# Flink 2.3 核心代码运行与验证边界

本实验将正文中的核心 Java 实现保存在 `flink23-lab`，可以独立查看 imports、编译并运行。输入有限且确定，便于核对状态、事件时间和 SQL 结果；不会连接已有的 Kafka、数据库或本机生产集群。

## 工程组成

`Flink23Lab.java` 包含 Event、窗口结果、计数聚合、TTL 去重、事件时间定时器、窗口 Top-N 和 SQL 窗口实验。`PtfExamples.java` 保存 PTF 类型模板。`CoreApiExamples.java` 包含文件源、DataGen、脏数据分流、Interval Join、允许迟到、广播规则、文件 Sink、Async I/O 接口及 UDF 模板。`SqlPlanChecks.java` 对当前 SQL 模式生成执行计划。

Flink 依赖固定为 2.3.0，Java 编译目标为 17。核心依赖使用 provided；本地运行脚本显式构建包含这些依赖的 classpath，因此无需改成 fat jar 才能做本地实验。

## 运行方法

在 `flink23-lab` 目录使用：

```bash
bash run.sh wordcount
bash run.sh windows
bash run.sh dedup
bash run.sh timers
bash run.sh topn
bash run.sh sql
bash run.sh plans
```

首次运行 Maven 会下载依赖，需要可用网络。Java 17 模块开放参数在脚本中处理；若 Maven/JVM 选择不正确，先检查 `java -version`、`mvn -version`。每个运行模式采用本地有限输入，并在结果不匹配时抛出断言错误。

## 已执行的结果断言

| 实验 | 已核对结果 |
|---|---|
| wordcount | flink=2，sql=1，state=1 |
| windows | 首窗 /a=3、/b=1，次窗 /b=1 |
| dedup | 两条相同标识仅保留一条，另一个标识保留 |
| timers | 两用户分别输出 21000、14000 的事件时间截止提醒 |
| topn | 首窗排名 /a=3、/b=1，次窗 /b=1 |
| sql | 十秒窗口中 /a 金额40，/b 金额20 |

以上六组在本次整理时于 **Flink 2.3.0、Java 17.0.19** 的本地 MiniCluster 运行通过。有限输入结束会发终止 watermark，不能据此证明无界源空闲时仍会继续触发窗口。

另外，17 组 SQL 计划检查通过，覆盖 GROUP BY、TUMBLE/HOP/CUMULATE/SESSION、OVER、窗口 Top-N、去重、Interval/Regular Join、标量与聚合 UDF、FROM_CHANGELOG/TO_CHANGELOG、Local-Global、Split Distinct 和 FILTER DISTINCT。它们验证规划可行；Local-Global 和 Split Distinct 还核对了对应执行节点。生成的计划保存在 `verification/plans`，后续运行 plans 模式会写入 `target/plans`。规划检查不能替代运行数据正确性、实际优化收益或生产恢复测试。

## 核心窗口代码

```java
public static class CountAgg implements AggregateFunction<Event,Long,Long> {
    public Long createAccumulator() { return 0L; }
    public Long add(Event event, Long acc) { return acc+1; }
    public Long getResult(Long acc) { return acc; }
    public Long merge(Long a, Long b) { return a+b; }
}
public static class AddWindow extends ProcessWindowFunction<Long,WindowCount,String,TimeWindow> {
    public void process(String key, Context ctx, Iterable<Long> values,
                        Collector<WindowCount> out) {
        out.collect(new WindowCount(key,ctx.window().getStart(),
            ctx.window().getEnd(),values.iterator().next()));
    }
}
static SingleOutputStreamOperator<WindowCount> windows(DataStream<Event> input) {
    return input.keyBy(e->e.url)
        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
        .aggregate(new CountAgg(), new AddWindow()).uid("url-window-v1");
}
```

## 尚未当作完成的验证

允许迟到的修正时序、TTL 到期和 TTL 迁移、真实故障恢复、Savepoint 升级、外部事务一致性、YARN/Kubernetes 部署及压力测试，均在相应笔记给出验证方法，本次不声称已经部署验证。

Kafka/JDBC 2.3 对应发布包仍受官网兼容矩阵限制，因此实验工程不混入其他 Flink 版本的连接器。ForSt 在官方文档中仍属实验能力，也未作为默认运行后端。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：本系列综合实验。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [java compatibility](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/java_compatibility/)
- [testing](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/testing/)

前篇：[[09-Flink 2.3 故障排查与证据保留]]
