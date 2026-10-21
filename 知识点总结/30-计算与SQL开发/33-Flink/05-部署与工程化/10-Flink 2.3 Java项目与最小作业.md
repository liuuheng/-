---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 2
source_sections: "主教材第2章；优化第5.1节"
---

# Flink 2.3 Java 项目与最小作业

先建立一个输入可控、输出可断言的最小作业，再接外部系统。这样出现问题时，可以区分是业务逻辑、Flink API，还是网络和连接器造成的。本系列实验工程位于 `10-代码实验/flink23-lab`，包含完整 imports 和运行入口；正文代码突出核心逻辑，未逐段重复完整工程骨架。

## 依赖必须按角色区分

```xml
<properties>
  <maven.compiler.release>17</maven.compiler.release>
  <flink.version>2.3.0</flink.version>
</properties>
<dependencies>
  <dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-java</artifactId>
    <version>${flink.version}</version>
    <scope>provided</scope>
  </dependency>
  <dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-clients</artifactId>
    <version>${flink.version}</version>
    <scope>provided</scope>
  </dependency>
</dependencies>
```

集群提供 Flink 核心依赖，业务 jar 应避免再打入另一套核心类。IDE 或普通 `java` 运行时则需要把 `provided` 依赖加入本地运行 classpath。`provided` 影响打包和运行类路径，不意味着该依赖不需要下载，也不意味着可以用另一版本的集群随意运行。

SQL 程序还需要 Table API、planner 和运行时；文件和 DataGen 按各自模块引入。Kafka/JDBC 等独立发布的连接器不能机械地把版本号设为 `${flink.version}`。

## WordCount 的核心执行链

```java
static DataStream<Tuple2<String,Long>> wordCount(StreamExecutionEnvironment env) {
    return env.fromData("flink sql", "flink state")
        .flatMap((String line, Collector<Tuple2<String,Long>> out)-> {
            for (String word:line.split("\\s+")) out.collect(Tuple2.of(word,1L));
        }).returns(Types.TUPLE(Types.STRING,Types.LONG))
        .keyBy(x->x.f0).sum(1).uid("word-count-v1");
}
```

切词产生 `(word, 1)`，`keyBy` 将相同单词路由到同一聚合实例，`sum(1)` 保存并更新各单词累计值。`returns` 补全 Lambda 泛型擦除后无法可靠推断的类型。`uid` 为后续状态恢复提供稳定标识。

输入两行 `flink sql`、`flink state`，最终应为 `flink=2`、`sql=1`、`state=1`。流式累计可能输出 `flink=1` 和 `flink=2` 两条中间记录；验证时按 key 取最终值，不能把所有中间值再求和。

## 所有示例使用同一事件模型

`Event` 含 `eventId、userId、url、ts、amount`。`ts` 统一为毫秒；金额示例用最小货币单位的整数，避免浮点累计误差。Java 类保留无参构造器和公共字段，避免把不可识别的数据对象无意间退化成通用序列化。

解析前检查字段数量、空值和时间戳范围。无法解析的数据进入侧输出，保留原始行及原因；把异常行改成零金额或零时间戳，会让作业看似稳定却污染业务统计。

## 验证边界

最小作业确认的是图能提交、算子能运行、结果符合断言。有限集合源结束时会推进终止 watermark，因此能触发剩余事件时间窗口；这不代表持续无界源在长期空闲时也会自动推进。检查点恢复、连接器事务和生产吞吐需要另做实验。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：主教材第2章；优化第5.1节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [overview](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/overview/)
- [types serialization](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/serialization/types_serialization/)

前篇：[[04-Flink 2.3 流批执行与有状态计算]]

后篇：[[05-Flink 2.3 执行图、算子链与Slot]]
