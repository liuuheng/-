---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 1
source_sections: "主教材第1、2、5章"
---

# Flink 2.3 流批执行与有状态计算

Flink 把输入事件经过转换写到下游；遇到需要记住历史的计算，就把历史保存为状态；用时间进度确定何时给出结果，用检查点在失败后恢复这个进度。理解这四件事，才能把 API、窗口、容错和性能放进同一条逻辑链。

本系列以 **Flink 2.3.0、Java 17** 为基线，保留原教材仍成立的机制并重写示例。版本核验日为 2026-10-21。实际部署还要核对连接器兼容矩阵，核心版本最新不意味着每种连接器都有对应发布包。

## 有界性和执行模式是两个维度

有界输入有确定的终点，例如一批固定文件；无界输入会持续增长，例如不设终止 offset 的消息订阅。`STREAMING` 是执行模式，可以处理有界和无界输入；`BATCH` 要求所有输入都有界；`AUTOMATIC` 才会根据输入有界性选择执行方式。不要把“文件”直接等同于批执行，也不要把“用了 DataStream”直接等同于无界流。

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setRuntimeMode(RuntimeExecutionMode.BATCH);
env.fromData("a", "b", "a")
   .map(x -> Tuple2.of(x, 1L))
   .returns(Types.TUPLE(Types.STRING, Types.LONG))
   .keyBy(x -> x.f0).sum(1).print();
env.execute("bounded-count");
```

这段代码的输入有界，所以可以用批执行。换成持续订阅的数据源后，需要重新选择模式。批执行能利用输入最终会结束这一条件安排调度、排序和状态处理，不应把流式检查点的行为直接套到批任务上。

## 状态决定计算是否依赖历史

解析 JSON、字段映射和过滤通常只依赖当前事件。累计金额、去重、Join、窗口都需要历史。比如订单总额不是“本条金额”，而是“旧累计值 + 本条金额”；若累计值只是普通成员变量，子任务重启后就不能从检查点自动恢复。

状态的范围也必须说清楚：`keyBy(userId)` 后的用户总额属于用户；整个子任务的读取任务分配属于算子实例。并行度变化时，前者按 key group 分配，后者按各类算子状态的重分配规则处理。

## main、执行图和实际计算

`main()` 中的转换调用主要构建执行图。普通 DataStream 作业由 `execute()` 或收集结果的执行 API 提交运行；不是 Java 执行到 `map()` 就已经处理完全部数据。子任务部署后才持续消费输入。

SQL 也会转成可执行的算子图。选择 SQL 还是 DataStream，主要看业务能否用关系运算清晰表达，以及是否需要精细状态、定时器或自定义协议；两种 API 不代表两个不同的运行引擎。

## 一个需要贯穿后续笔记的问题

“过去十分钟每个页面的访问量”至少包含四个决定：按哪个业务字段分组、用事件时间还是处理时间、如何处理迟到、输出覆盖旧值还是追加新版本。API 写法必须在这些语义确定之后选择。

实验时分别用 `STREAMING`、`BATCH` 处理同一份有限输入，比较中间输出与最终结果。最终计数一致不表示调度方式、输出时点和失败恢复机制相同。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：主教材第1、2、5章。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [execution mode](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/execution_mode/)
- [java compatibility](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/java_compatibility/)

后篇：[[10-Flink 2.3 Java项目与最小作业]]
