---
tags: [flink, flink-2-3, 尚硅谷整理]
flink_version: "2.3.0"
java_version: "17"
verified_at: "2026-10-21"
series_order: 5
source_sections: "主教材第5.3节；优化第7.13节"
---

# Flink 2.3 转换、分区与数据质量

转换先确定每条记录的含义，分区再决定记录由哪个子任务处理。只有类型和质量稳定，后续状态与窗口才有可靠输入。

## 选择转换函数

`map` 一进一出，适合字段转换；`filter` 保留或丢弃原记录；`flatMap` 可以一进零出或多出，适合拆分明细。累计函数需要历史状态：`reduce` 输入输出同型，`AggregateFunction` 允许输入、累加器和输出使用不同类型。

`min` 只修改所选字段，`minBy` 返回该字段最小的整条记录。若需要“最低金额对应的原订单”，选 `minBy` 才符合记录整体的业务含义。

## 脏数据进入可追踪的侧输出

```java
final OutputTag<String> dirty = new OutputTag<String>("dirty-lines") {};
SingleOutputStreamOperator<Event> valid = raw.process(
    new ProcessFunction<String, Event>() {
        @Override
        public void processElement(String line, Context ctx, Collector<Event> out) {
            try {
                String[] f = line.split(",", -1);
                if (f.length != 5 || f[0].isBlank())
                    throw new IllegalArgumentException("invalid fields");
                long ts = Long.parseLong(f[3]);
                if (ts < 0) throw new IllegalArgumentException("negative timestamp");
                out.collect(new Event(f[0], f[1], f[2], ts, Long.parseLong(f[4])));
            } catch (IllegalArgumentException ex) {
                ctx.output(dirty, line + " | " + ex.getMessage());
            }
        }
    });
valid.getSideOutput(dirty).print("dirty");
```

匿名 `OutputTag` 保留泛型类型信息。捕获预期的数据校验错误，系统连接失败等异常仍应传播；用 `catch (Exception)` 吞掉所有错误会把故障变成静默数据丢失。

## 分区规则与顺序代价

| 规则 | 使用意图 | 主要边界 |
|---|---|---|
| `keyBy` | 相同 key 集中以访问状态 | 热 key 仍集中到同一实例 |
| `rebalance` | 全局轮询分散无状态负载 | 增加 shuffle，不保留原分区顺序 |
| `rescale` | 在上下游子集间局部轮询 | 通信更少，不保证消除所有倾斜 |
| `broadcast` | 每个下游都接收一份 | 流量、内存按并行度放大 |
| `global` | 发往一个下游实例 | 容易形成单点瓶颈 |

这些规则不能互换。对用户累计金额，在 keyed 聚合前用 rebalance 不会打散最终的热用户；keyBy 会再次把同一用户集中起来。

## union 和 connect 的不同职责

`union` 合并同类型流，无法保证来自不同输入的全局顺序；`connect` 保留两种输入类型，配合 CoProcess 或广播函数实现关联。`connect` 本身不会自动完成 Join，匹配和清理状态仍由函数实现。

## 富函数和序列化

资源在 `open(OpenContext)` 初始化，在 `close()` 尽量释放。关闭回调不能承担事务正确性的唯一保证，进程被杀时未必有机会执行。不要在构造函数建立数据库连接后指望 Flink 把这个连接序列化到集群。

验证时输入空字段、负时间戳、超大金额和正常记录，检查“主流条数 + 脏数据条数”是否与输入一致。随后再对主流做金额对账，确认错误记录没有被伪装成正常事件。

## 来源与核对

原始资料：尚硅谷《大数据技术之 Flink-1.17》及《Flink 优化 V2.0》；本篇原文定位：主教材第5.3节；优化第7.13节。正文按 2.3 现行接口重写，官网链接固定到 2.3 文档分支，避免 stable 链接将来漂移。

- [overview](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/overview/)
- [side output](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/side_output/)

前篇：[[04-Flink 2.3 Source读取、Split与恢复位置]]

后篇：[[06-Flink 2.3 事件时间与Watermark推进]]
