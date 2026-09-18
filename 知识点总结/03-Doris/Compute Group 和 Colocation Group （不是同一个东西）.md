```
Compute Group (计算组)	资源隔离，在存算分离的 集群中  类似消费者组，控制集权资源不被某个大型任务全部占用 
（数据导入中 routine load 中 建表后无法改动 compute group）

Colocation Group (位置协同组)   消除join 中的网络shuffle
```