# Spark UI 界面理解

相关：[[Spark知识地图]] · [[02-Spark/spark 运行相关的理解 （stage、task、job）|Job、Stage 与 Task]]

## Stage 下的 Event Timeline

![[images/企业微信截图_17780384096868.png]]

### 问题

为什么页面显示有 1800 个 Task，但 Event Timeline 中看起来只有少数几个？

### 当前理解

Stages 页面展示 Task 的执行详情，绿色条表示各 Task 的执行情况。页面可能只显示部分 Task，可通过翻页箭头切换展示范围，因此时间线中可见的条目不一定等于 Task 总数。
