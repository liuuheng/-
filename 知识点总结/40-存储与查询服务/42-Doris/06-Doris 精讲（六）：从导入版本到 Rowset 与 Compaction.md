---
aliases:
  - "02-Doris Compaction：Rowset、Segment 与版本合并"
  - "Doris Compaction：Rowset、Segment 与版本合并"
tags:
  - doris
  - doris-精讲
---

# Doris 精讲（六）：从导入版本到 Rowset 与 Compaction

假设订单明细表 5 月 15 日第 3 桶连续收到几个小批次。查询能读到每批新增的订单，但每次读取都要处理越来越多版本。这是 Compaction 要解决的问题。它在 Tablet 内合并 Rowset，并按表模型整理记录。

## 一、从一个 Tablet 看每次导入留下的状态

Doris 使用列式存储，写入有类 LSM 的追加和后台合并特征。表先分区，分区内再分桶。在存算一体中，对应 Tablet 的各副本保存该分片数据。副本内的 Rowset 关联一个或一段版本区间，内部可能有零个或多个 Segment。Segment 才是承载列式数据的文件。

假设一个 Tablet 的三个可见增量版本如下。版本号、Rowset 名和订单号用于说明层级和合并结果。

| 可见版本 | Rowset | Segment | Duplicate Key 明细 |
| --- | --- | --- | --- |
| 6 | R6 | S6a | 订单 1001 |
| 7 | R7 | S7a、S7b | 订单 1002、1003 |
| 8 | R8 | S8a | 订单 1004 |

一次导入触及多个 Tablet 时，会为各相关 Tablet 形成各自的写入结果。客户端请求、全表导入作业和单 Tablet 的版本不能直接按 1:1 换算。写入、提交和版本可见也需要区分。

## 二、合并 6～8 后，查询结果和文件分别怎样变

后台可以选取连续的版本 6～8，产出覆盖该区间的新 Rowset。对于表中的 Duplicate Key 明细，新 Rowset 仍含四笔订单，查询可以通过更少的 Rowset 处理同一范围。新 Rowset 的 Segment 数取决于具体写出，旧文件也要等并发读取及回收机制允许后再清理。

如果同样的输入属于 Aggregate Key，Compaction 按 Value 列的聚合规则合并同 Key。若是 Unique Key，合并还需遵循旧版本和失效行的处理规则。[[05-Doris 精讲（五）：Unique Key 如何覆盖旧行与更新部分列|Unique Key 如何覆盖旧行与更新部分列]]说明 MOW 在写入时已经决定旧行是否可见，Compaction 之后再整理版本并清理旧数据。Compaction 的范围仍是原 Tablet，用户的 Hash 路由不随文件重写变化。

Cumulative Compaction 常处理较新的增量 Rowset，Base Compaction 整理较早或已累计的数据。两者选择哪些版本以及何时触发，受版本、配置和表属性影响。

## 三、怎样确认小批导入已经造成积压

如果增量长期产生得比合并快，版本和小 Segment 持续累积，扫描与合并成本可能一起上升。观察同一 Tablet 的 Rowset 或版本趋势、Compaction 任务进度与失败、导入速率和对应节点的 CPU、磁盘 I/O。Compaction Score 也需要结合策略解释，不能直接当作文件数、字节数或耗时。

若许多 Tablet 同时累积，检查批次大小和合并资源。若主要集中在用户 42 所在的少数 Tablet，还需查写入分布。提高导入并发可能使生成速率更高，进一步扩大积压。见 [[07-Doris 精讲（七）：Tablet 热点如何形成，怎样定位和缓解|Tablet 热点如何形成，怎样定位和缓解]]。
