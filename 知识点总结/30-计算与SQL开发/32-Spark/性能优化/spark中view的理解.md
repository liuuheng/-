
|类型|作用域|生命周期|是否持久化|
|---|---|---|---|
|TEMP VIEW|当前 session|session 结束消失|❌|
|GLOBAL TEMP VIEW|所有 session|Spark 应用存活期间|❌|
|VIEW（普通视图）|所有用户|永久|✅（存 metastore）|

view的一些 理解：

大多数情况下：
1 Application ≈ 1 SparkSession

但是：一个 Application
可以创建多个 SparkSession

val spark2 = spark1.newSession()

```
CREATE TEMP VIEW view_name AS   -- session 内
SELECT ...
```

```
CREATE GLOBAL TEMP VIEW view_name AS  -- 多个session 公用
SELECT ...
```

```
CREATE VIEW view_name AS   -- 都可以用
SELECT ...
```


