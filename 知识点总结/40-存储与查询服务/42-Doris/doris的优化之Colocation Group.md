
简称：Colocation Group（CG）
Colocation Group 创建的方式，另外一张表如何绑定group 

```text
  一些要求：分桶键必须相同、bucket数相同、副本数相同，hash分布规则相同，表必须在同一个 colocation group  
  那些情况不适合用：表结构经常变化、hash ky 倾斜、大表 join 小表 （broadcast join 更合适）
```
![[99-images/企业微信截图_17786667082439.png]]

一些具体的操作流程：
```text
查询当前 group ：SHOW TABLES;
解除绑定group：ALTER TABLE user_action SET ("colocate_with" = "");
删除 group：DROP COLOCATE GROUP IF EXISTS group1;
创建group：重新绑定即可自动创建
重新绑定新 group：ALTER TABLE user_profile SET ("colocate_with" = "group_new");

数据重分布：ADMIN REBALANCE TABLE user_action;

具体的流程如下：
“解绑旧分布约束 → 重新绑定 group → 重新对齐数据分布（rebalance 或重写数据）”
```

```text
如果指定的 Group 不存在，则 Doris 会自动创建一个只包含当前这张表的 Group。如果 Group 已存在，则 Doris 会检查当前表是否满足 Colocation Group Schema。如果满足，则会创建该表，并将该表加入 Group。
```

![[99-images/企业微信截图_17786673127403.png]]

如何验证是否生效：
```text
查看查询计划：
DESC SELECT * FROM tbl1 INNER JOIN tbl2 ON (tbl1.k2 = tbl2.k2);
如果 Colocation Join 生效，则 Hash Join 节点会显示 colocate: true。
如果没有生效：HASH JOIN 节点会显示对应原因：colocate: false, reason: group is not stable。同时会有一个 EXCHANGE 节点生成。
```