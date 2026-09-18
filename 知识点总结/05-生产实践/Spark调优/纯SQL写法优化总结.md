# SQL 优化总结

## 1. FROM 和 WHERE 的优化

### FROM 的优化

FROM 的优化，就是优化 SQL 读取数据的过程，这块其实类似小文件的优化。

**核心问题**：大量的小文件，意味着数据寻址需要花费更多时间，"from" 的过程就会很慢。小文件的优化主要通过参数调优。

### WHERE 的优化

WHERE 的优化，主要就是过滤模式的优化。

**核心手段**：利用谓词下推，将 where 操作提前，提前过滤数据（分区过滤），避免全盘扫描。

### 谓词下推

**定义**：这里的 "谓词"，其实就是 SQL 中的 where 过滤语句；谓词下推就是在不影响计算结果的前提下，尽量将过滤语句前移，以减少后续计算步骤的数据量。

**启动参数**：`set hive.optimize.ppd=true;`

### 分区过滤

**本质**：避免全表扫描。

虽然分区过滤的条件写在 where 子句中，但分区过滤在 Tablescan 之前就完成了；其他 where 里的过滤条件发生在 map 阶段，通过 MapTask 实现一行行过滤（属于全表扫描）。

分区过滤相当于直接指定路径读取文件，是分区表目录结构带来的天然优势。

分区过滤有可能失效，这会带来灾难性问题（触发全表扫描）。例如要读取的分区表数据量为 320GB，而表总大小为 88TB，若过滤失效则需读取 88TB 全量数据。因此需重点关注 "分区过滤是否生效"。

### 不同 Join 类型中分区过滤的生效情况

#### 1. Inner Join

规则：无论是把分区过滤条件放到 where 还是 on 中，都会生效。

#### 2. Left Join（特殊情况）

- **条件放 where 中**：主表生效；副表会先进行全表扫描，分区裁剪在 Join 后（Reduce 阶段最后）进行（副表不是不裁剪，是在 shuffle 后裁剪）。
- **条件放 on 中**：副表生效；主表会先进行全表扫描，分区裁剪在 Join 时进行过滤（主表不是不裁剪，是在 shuffle 后裁剪）。

**最优方案**：若想同时实现 Map 前主表 + 副表的分区裁剪，把主表的分区裁剪放到 where 子句里，把从表的分区裁剪放到 on 里。

#### 3. Full Join（特殊情况）

规则：无论是把分区过滤条件放到 where 还是 on 中，都不会生效。

**解决方案**：若需要分区裁剪，只能通过子查询（将 t1 和 t2 写成子查询再进行 Join）。

---

## 2. GROUP BY 的优化

当我们在代码中使用 group by 进行分组聚合时，由于分组键分布不均匀，可能导致数据倾斜问题。

### 怎么知道分组键分布不均匀？

定位到 group by 倾斜之后，对分组键做个探查就行，例如定位到分组键是 shop_id，对应的数据源是 us_base.dwd_cro_uid_xxx_di，分区是 20250302：

```sql
select shop_id, count(1) as cnt
from us_base.dwd_cro_uid_xxx_di
where p_date = '20250302'
group by shop_id
order by cnt desc
limit 100;
```

### 什么情况下可能会出现分组键分布不均匀的情况呢？

- null 值
- 特殊含义的 0 or 1
- 异常值，例如空字符串 ""、"未知" 这种等
- 在业务角度，高热出现的商品 / 工具 / 区域等

### 怎么处理？

如果这部分热点值和最终聚合得到的结果没有任何关系，那么最好处理，直接使用 where 干掉。

如果这部分热点值和最终聚合得到的结果有关系，那么得利用负载均衡的思路，或者单独处理热点。

- **负载均衡**：先局部聚合，再全局聚合
- **单独处理热点值**：单独处理热点值，使用 union all 拼接起来

### 优化案例

**业务背景**：需要计算平台过去 7 天，每个直播间的总打赏金额，如果直播间过去 7 天从未有人打赏，则直播间 id 会被自动处理成 000000，表示不需要业务关注。假设我们不需要保留这些从未有人打赏的直播间，但是在所有人打赏的直播间中，打赏金额分布差异极大！热门直播间打赏不断，但其余直播间可能 7 天打赏金额为个位数，需要考虑性能问题。

**常规写法**：

```sql
select a.room_id, sum(a.room_order_amount/1000) as room_order_amount
from us_base.dwd_cro_live_room_xxx_di as a
where p_date between '${p_date-6}' and '${p_date}'
and room_id != 000000 -- 过滤掉不需要业务关注的直播间id
group by a.room_id
```

**优化方法一：负载均衡**

```sql
select room_id
      ,sum(room_order_amount) as room_order_amount
from 
(
    select  a.room_id
            ,cast(rand()*10000 as bigint) as group_by_tmp_key
            ,sum(a.room_order_amount/1000) as room_order_amount
    from us_base.dwd_cro_live_room_xxx_di as a
    where p_date between '${p_date-6}' and '${p_date}'
    and room_id != 000000 -- 过滤掉不需要业务关注的直播间id
    group by a.room_id, cast(rand()*10000 as bigint)
) as a
group by room_id
```

**优化方法二：单独处理热点值**

```sql
-- 假设room_id = 122024541321的直播间占了平台总打赏金额的36%以上，那么可以单独处理

select  a.room_id                as room_id
        ,sum(a.room_order_amount/1000)  as room_order_amount
from us_base.dwd_cro_live_room_xxx_di as a
where p_date between '${p_date-6}' and '${p_date}'
and room_id != 000000 -- 过滤掉不需要业务关注的直播间id
and room_id != 122024541321
group by a.room_id

union all

select  122024541321             as room_id
        ,sum(room_order_amount)  as room_order_amount
from
(
    select  cast(rand()*10000 as bigint) as group_by_tmp_key
            ,sum(a.room_order_amount/1000) as room_order_amount
    from us_base.dwd_cro_live_room_xxx_di as a
    where p_date between '${p_date-6}' and '${p_date}'
    and room_id != 000000 -- 过滤掉不需要业务关注的直播间id
    and room_id = 122024541321
    group by cast(rand()*10000 as bigint)
) as a
```

---

## 3. JOIN 的优化

### 3.1 通用思想：提前减少 Join 时 Shuffle 的数据量

核心思路：在 Join 前通过 where 过滤或 group by 提前聚合，降低数据量，提升 Join 效率。

似乎一个主表 left join 从表1 left join 从表2 left join 从表3 ... 这种写法是最优的，主表只需要扫描一次 排序一次。

### 优化案例

**业务背景**：计算过去 180 天，年龄在 18-25 岁的人中，不同省份的人数、总支付订单金额和总支付订单次数。

**常规写法**

```sql
select t2.user_province
        ,sum(pay_amount) as pay_amount
        ,count(if(pay_status = 'success', 1, null)) as pay_cnt
from dwd_asx_order_detail_df as t1
left join dim_user_info_df as t2
on t1.user_id = t2.user_id
where t1.date = '${p_date}'
and t1.pay_date between '${p_date - 179}' and '${p_date}' -- 过去180天订单数据
and t2.age between 18 and 25 -- 年龄在18-25岁的人
and t2.date = '${p_date}'
group by t2.user_province
```

**优化思路一：先过滤，后 Join**

原逻辑问题：将用户订单明细和全量用户属性关联后再过滤，Shuffle 数据量大。

优化方向：把单表的过滤操作提前到子查询里，减少 Join 时的 Shuffle 数据量。

**1. 放到子查询里进行过滤**

```sql
select 
    t2.user_province
    ,sum(pay_amount) as pay_amount
    ,count(1) as pay_cnt
from 
(
    select user_id, pay_amount
    from dwd_asx_order_detail_df
    where date = '${p_date}'
    and pay_date between '${p_date - 179}' and '${p_date}' -- 过去180天订单数据
    and pay_status = 'success'
) as t1
inner join 
(
    select user_id, user_province
    from dim_user_info_df
    where date = '${p_date}'
    and age between 18 and 25 -- 年龄在18-25岁的人
) as t2
on t1.user_id = t2.user_id
group by t2.user_province
```

**2. 放到 where 和 on 中进行过滤**

```sql
select t2.user_province
    ,sum(pay_amount) as pay_amount
    ,count(if(pay_status = 'success', 1, null)) as pay_cnt
from dwd_asx_order_detail_df as t1
left join dim_user_info_df as t2
on t1.user_id = t2.user_id
-- 副表的过滤条件放到on中
and t2.age between 18 and 25 -- 年龄在18-25岁的人
and t2.date = '${p_date}'
-- 主表的过滤条件放到where中
where t1.date = '${p_date}'
and  t1.pay_date between '${p_date - 179}' and '${p_date}' -- 过去180天订单数据
group by t2.user_province
```

**优化思路二：先聚合，后 Join**

如果整个全量的用户属性信息表特别大的话，那么可以先去聚合主表减少一部分数据量，再去关联用户的维度属性。

```sql
select  t2.user_province
        ,sum(pay_amount)    as pay_amount
        ,count(1)          as pay_cnt
from
(
  -- 提前按照用户id聚合一次，减少后续关联时的Shuffle数据量
  select  user_id
          ,sum(pay_amount) as pay_amount
  from    dwd_asx_order_detail_df
  where   date = '${p_date}'
  and     pay_date between '${p_date - 179}' and '${p_date}' -- 过去180天订单数据
  and     pay_status = 'success'
  and     user_id > 0
  group by user_id
) as t1
inner join
(
  select  user_id, user_province
  from    dim_user_info_df
  where   date = '${p_date}'
  and     age between 18 and 25 -- 年龄在18-25岁的人
) as t2
on t1.user_id = t2.user_id
group by t2.user_province 
-- 二次聚合，拿到最终的结果
```

**优化思路三：调整 Join 位置，避免数据膨胀后再进行 Join**

```sql
select  t1.*
        ,t2.shop_id
        ,t2.cust_id
        ,t2.xxx
        ,t3.cust_name
        ,t3.cust_industry
        ,t3.xxx
from dw.dwd_table_a_di as t1 -- 2亿数据量
left join (
  -- 店铺维表
  select  shop_id
          ,cust_id
          ,xxx
  from dw.dim_shop_info_df
  where p_date = '${p_date}'
) as t2
left join (
  -- 商家维表
  -- 4000w行的量级，基本很难走广播
  select  cust_id
          ,cust_name
          ,cust_industry
          ,xxx
  from dw.dim_cust_info_df
  where p_date = '${p_date}'
) as t3
on coalesce(t2.cust_id, cast(rand()*100000 as bigint)) = t3.cust_id 
where t1.p_date='${p_date}'
```

具体而言，就是把关联 dw.dim_cust_info_df 的位置提前（因为 dw.dim_cust_info_df 其实是通过 dw.dim_shop_info_df 来间接和主表关联）：

```sql
select  t1.*
        ,t2.shop_id
        ,t2.cust_id
        ,t2.xxx
        ,t2.cust_name
        ,t2.cust_industry
from dw.dwd_table_a_di as t1 -- 2亿数据量
left join (
  -- 店铺维表
  select  a.shop_id
          ,a.cust_id
          ,a.xxx
          ,b.cust_name
          ,b.cust_industry
  from dw.dim_shop_info_df as a
  left join (
    -- 商家维表
    select  cust_id
            ,cust_name
            ,cust_industry
            ,xxx
    from dw.dim_cust_info_df
    where p_date = '${p_date}'
  ) as b
  on a.cust_id = b.cust_id
  where a.p_date = '${p_date}'
) as t2
where t1.p_date = '${p_date}'
```

### 3.2 大表 Join 小表：让 Join 走 "广播"

核心原理：大表与小表 Join 时，若进行 Shuffle 会产生大量网络 IO，而广播 Join 可避开 Shuffle，提升 Join 效率。其本质是通过参数设置减少右表（小表）的数据量，使其满足广播阈值，从而避免 Shuffle。

**相关参数**：
- `set spark.sql.adaptive.join.enabled=true;`：开启自动广播，默认开启。
- `set spark.sql.autoBroadcastJoinThreshold=-1;`：用于设置广播阈值。

**优势**：即使源数据 Join Key 分布不均匀，也不会因数据倾斜影响任务执行效率。

### 3.3 大表 Join 大表 / 小表：随机数打散 or 单独处理热点值

适用场景：当广播 Join 因小表大小超阈值、平台 / 引擎问题等无法使用，且 Join Key 出现热点值（某几个枚举量级特别大）时，需根据热点值具体情况选择优化手段。

#### 情况一：热点值为无实际意义的值（如 -1/""/0/null 等），且对应行结果不需要保留

优化方法：在 Join 之前直接过滤掉这些热点值即可，后续可基于 "先过滤，后 Join" 的思路进行修改。

```sql
select  t2.user_province
        ,sum(pay_amount) as pay_amount
        ,count(1) as pay_cnt
from 
(
    select user_id, pay_amount
    from dwd_asx_order_detail_df
    where date = '${p_date}'
    and pay_date between '${p_date - 179}' and '${p_date}' -- 过去180天订单数据
    and pay_status = 'success'
    -- 假设 user_id = -1 和 user_id is null的数据为异常值，且结果行不需要保留，则直接过滤即可
    and user_id != -1
    and user_id is not null
) as t1
inner join 
(
    select user_id, user_province
    from dim_user_info_df
    where date = '${p_date}'
    and age between 18 and 25 -- 年龄在18-25岁的人
    and user_id != -1
    and user_id is not null
) as t2
on t1.user_id = t2.user_id
group by t2.user_province
```

#### 情况二：热点值为无实际意义的值，比如 -1/""/0/null 等等，但是对应行的结果也【需要保留】

```sql
-- 假设 user_id = 0 为异常值，但是结果行需要保留，则需要将热点值打散成随机值(需要确保不会和正常值重复)
-- 假设题目不要求年龄在18-25岁的人，关联维表仅仅是为了拿到省份信息，方便进行分组，所以 join 改为了 left join
select  t2.user_province
        ,sum(pay_amount) as pay_amount
        ,count(1) as pay_cnt
from 
(
    select user_id, pay_amount
    from dwd_asx_order_detail_df
    where date = '${p_date}'
    and pay_date between '${p_date - 179}' and '${p_date}' -- 过去180天订单数据
    and pay_status = 'success'
) as t1
left join 
(
    select user_id, user_province
    from dim_user_info_df
    where date = '${p_date}'
) as t2
-- 将热点值打散成随机值(需要确保不会和正常值重复)
on if(t1.user_id = 0, cast(rand()*1000000000 as bigint), t1.user_id) = t2.user_id
group by t2.user_province
```

#### 情况三：热点值为有实际意义的值，例如 user_id = 1001 为热点值，且维表数据量不太大（允许膨胀多倍）

```sql
-- 这种情况就要复杂一些，需要将Join的维表膨胀若干倍之后，再将热点值所在的表加随机数进行打散，最后关联时再加上随机值
-- 假设题目不要求年龄在18-25岁的人，关联维表仅仅是为了拿到省份信息，方便进行分组，所以 join 改为了 left join
select  t2.user_province
        ,sum(pay_amount) as pay_amount
        ,count(1) as pay_cnt
from 
(
    select user_id, pay_amount, cast(rand()*10 as bigint) as tmp_rand_t1
    from dwd_asx_order_detail_df
    where date = '${p_date}'
    and pay_date between '${p_date - 179}' and '${p_date}' -- 过去180天订单数据
    and pay_status = 'success'
) as t1
left join 
(
    select user_id, user_province, tmp_rand_t2
    from dim_user_info_df
    lateral view explode(array(0,1,2,3,4,5,6,7,8,9,10)) as tmp_rand_t2
    where date = '${p_date}'
) as t2
on t1.user_id = t2.user_id
-- 进一步使用处理完的随机数key进行关联
and t1.tmp_rand_t1 = t2.tmp_rand_t2
group by t2.user_province
```

#### 情况四：热点值为有实际意义的值，例如 user_id = 1001 为热点值，且维表数据量比较大（膨胀多倍后需要考虑效率问题）

相比于情况三，这种情况下单独处理热点 key 比较好一些，也就是对倾斜的 Key 单独进行处理（和膨胀后的维表进行关联），其余的数据正常关联。但是需要提前探查一下，看看热点值的具体枚举，这里假设 user_id = 1001 为热点值。

```sql
-- 假设题目不要求年龄在18-25岁的人，关联维表仅仅是为了拿到省份信息，方便进行分组，所以 join 改为了 left join
select  t2.user_province
        ,sum(pay_amount) as pay_amount
        ,count(1) as pay_cnt
from 
(
    select user_id, pay_amount, cast(rand()*10 as bigint) as tmp_rand_t1
    from dwd_asx_order_detail_df
    where date = '${p_date}'
    and pay_date between '${p_date - 179}' and '${p_date}' -- 过去180天订单数据
    and pay_status = 'success'
    and user_id = 1001 -- 过滤完user_id = 1001的数据后，主表的数据量会少很多，和膨胀后的维表关联也会变快
) as t1
left join (
    select user_id, user_province, tmp_rand_t2
    from dim_user_info_df
    lateral view explode(array(0,1,2,3,4,5,6,7,8,9,10)) as tmp_rand_t2
    where date = '${p_date}'
    and user_id = 1001 -- 只选择膨胀的key的用户信息
) as t2
on t1.user_id = t2.user_id
-- 进一步使用处理完的随机数key进行关联
and t1.tmp_rand_t1 = t2.tmp_rand_t2
group by t2.user_province

union all

select  t2.user_province
        ,sum(pay_amount) as pay_amount
        ,count(1) as pay_cnt
from 
(
    select user_id, pay_amount
    from dwd_asx_order_detail_df
    where date = '${p_date}'
    and pay_date between '${p_date - 179}' and '${p_date}' -- 过去180天订单数据
    and pay_status = 'success'
    and user_id != 1001
) as t1
left join 
(
    select user_id, user_province
    from dim_user_info_df
    where date = '${p_date}'
    and user_id != 1001
) as t2
on t1.user_id = t2.user_id
group by t2.user_province
```

---

## 4. 开窗函数的优化

### 长倾斜 Key 开窗函数的优化

**业务场景**：需统计每间直播房间的打赏总金额，若房间过去7天 从未有人打赏，则直播间id会被自动处理为 null。

**优化前代码**

```sql
select a.room_id,
       sum(a.room_order_amount/10000) over (partition by a.room_id) as room_order_amount_tag 
from dw.dwd_dau_cre_live_room_xxx_df as a
where p_date = '${p_date}'
```

（假设 room_id is null 的数据占整体数据量的 40% 以上，存在倾斜）

**优化思路**：通过 coalesce 结合随机数，将倾斜的 partition key 打散，避免数据倾斜。

**优化后代码**

```sql
-- 长倾斜Key如果想实现类似group by 效果的开窗聚合，同样可以使用随机打散的方式来解决！
select a.room_id,
       -- 假设 room_id is null 的数据占了整体数据量的40%以上，很容易倾斜
       sum(a.room_order_amount/10000) over (partition by coalesce(a.room_id, cast(rand()*1000000000 as bigint))) as room_order_amount_tag
from dw.dwd_dau_cre_live_room_xxx_df as a
where p_date = '${p_date}'
```

### 排序开窗函数的优化

**上案例**：不同部门 按入职顺序 排序

```sql
select employee_id
      ,department_id
      ,hire_date
      ,row_number() over(partition by department_id order by hire_date) as rn
from table_a
```

-- 注：假如部门A的人数特别多，上述代码使用 department_id 分区，会产生严重倾斜问题，那么怎么解决呢？

**思路**：

```sql
with hire_table As (
SELECT 1001 as employee_id, 'a' as department_id, '2024-11-16' as hire_date
UNION ALL
SELECT 1002 as employee_id, 'a' as department_id, '2024-11-25' as hire_date
UNION ALL
SELECT 1003 as employee_id, 'a' as department_id, '2024-11-30' as hire_date
UNION ALL
SELECT 1004 as employee_id, 'a' as department_id, '2024-12-01' as hire_date
UNION ALL
SELECT 1005 as employee_id, 'a' as department_id, '2024-12-02' as hire_date
UNION ALL
SELECT 1006 as employee_id, 'a' as department_id, '2024-12-01' as hire_date
UNION ALL
SELECT 1007 as employee_id, 'a' as department_id, '2024-12-02' as hire_date
)
-- 1.先按department_id 入职年月进行分区， 局部编号
select
    employee_id
    ,department_id
    ,hire_date
    ,date_format(hire_date, 'yyyy-MM') as months
    ,row_number() over(partition by department_id,date_format(hire_date, 'yyyy-MM') order by hire_date) as local_m
from hire_table

-- 2.计算每个 department_id 入职年月分区，对应的起始编号
, tmp2 as (
select department_id
        ,months
        ,month_cnt
        ,sum_month_cnt
        ,lag(sum_month_cnt,1,0) over (partition by department_id order by months) as lag_sum_month_cnt
from (
    select department_id
            ,months
            ,month_cnt
            ,sum(month_cnt) over (partition by department_id order by months rows between unbounded preceding and current row) as sum_month_cnt
    from (
        select department_id,date_format(hire_date, 'yyyy-MM') as months
                ,count(1) as month_cnt
        from hire_table
        group by department_id,date_format(hire_date, 'yyyy-MM')
    ) as t1
) as t2
)
select t1.employee_id
        ,t1.department_id
        ,t1.hire_date
        ,t1.local_m + t2.lag_sum_month_cnt as res
from local_m_table as t1
left join tmp2 as t2
on t1.department_id = t2.department_id
and t1.months = t2.months
```

### 案例2

有一张用户登录表 table_2，包含用户ID，登陆日期，在线时长。表的粒度为用户ID+登陆日期，需求是想要拿到每个用户最早一次登陆日期及对应的在线时长。

**表结构如下**：

| USER_ID | LOGIN_DATE | ONLINE_TIME(MIN) |
|---------|------------|------------------|
| 1001    | 2024-12-01 | 10               |
| 1001    | 2024-12-02 | 20               |
| 1001    | 2024-12-03 | 30               |
| 1002    | 2024-11-01 | 50               |
| 1002    | 2024-11-05 | 10               |

**思路一**：

```sql
select
    user_id
    ,login_date
    ,online_time
from
(
    select
        user_id
        ,login_date
        ,online_time
        ,row_number() over(partition by user_id order by login_date) as rn
    from user_log
) a
where rn = 1
```

-- PS: 如果存在在分头部用户数据量特别大，使用 row_number() 容易出现数据倾斜问题，该如何解决呢？

**思路二**：

```sql
with table_2 as (
SELECT 1001 as user_id ,'2024-12-01' as login_date, 10 as online_time
UNION ALL
SELECT 1001 as user_id ,'2024-12-02' as login_date, 20 as online_time
UNION ALL
SELECT 1001 as user_id ,'2024-12-03' as login_date, 30 as online_time
UNION ALL
SELECT 1002 as user_id ,'2024-11-01' as login_date, 50 as online_time
UNION ALL
SELECT 1002 as user_id ,'2024-11-05' as login_date, 10 as online_time
)
select
user_id
,substr(first_login_online_time, 1, 10) as login_date
,cast(substr(first_login_online_time, 12) as bigint) as online_time
from
(
select
user_id
,min(concat(login_date, '_', online_time)) as first_login_online_time
from table_2
group by user_id
) a
```

这里利用了 Min/Max 函数聚合的优化，减少 Shuffle 过程中拉取的数据量，提升计算效率。此外，后续遇到【首次发生某种行为】的场景，也首先思考一下，是否能通过 max/min 的方式解决，尽量避免开窗函数带来的开销。

---

## 5. COUNT（DISTINCT）的优化

### 单个 count (distinct) 的优化

**优化思路**：通过提前按 user_from_v1 和 user_id 分组，将 count(distinct) 转换为普通 count，减少 distinct 操作的性能开销。

**源代码**

```sql
select user_from_v1, count(distinct user_id) as uv
from table_a
where p_date = '${p_date}'
group by user_from_v1
```

**优化后代码**

```sql
select user_from_v1, count(1) as uv
from (
    select user_from_v1, user_id
    from table_a
    where p_date = '${p_date}'
    group by user_from_v1
) a
group by user_from_v1
```

### 多个 count (distinct) 的优化

**源代码**

```sql
select count(distinct if(user_from_v1 = 1, user_id, null)) as from_11_uv,
       count(distinct if(user_from_v1 = 2, user_id, null)) as from_12_uv,
       count(distinct if(user_from_v1 = 3, user_id, null)) as from_13_uv,
       count(distinct if(user_from_v1 = 4, user_id, null)) as from_14_uv,
       count(distinct if(user_from_v1 = 5, user_id, null)) as from_15_uv,
       count(distinct if(user_from_v1 = 6, user_id, null)) as from_16_uv,
       count(distinct if(user_from_v1 = 7, user_id, null)) as from_17_uv,
       count(distinct if(user_from_v1 = 8, user_id, null)) as from_18_uv
from table_a
where p_date = '${p_date}'
```

**优化思路**：聚合前先给用户打标，然后判断当前用户是否符合每个 UV 中的统计条件，最终直接 sum 即可。

**优化后代码**

```sql
select  sum(is_from_11_uv) as is_from_11_uv,
        sum(is_from_12_uv) as is_from_12_uv,
        sum(is_from_13_uv) as is_from_13_uv,
        sum(is_from_24_uv) as is_from_24_uv,
        sum(is_from_25_uv) as is_from_25_uv,
        sum(is_from_26_uv) as is_from_26_uv,
        sum(is_from_37_uv) as is_from_37_uv,
        sum(is_from_38_uv) as is_from_38_uv
from (
    select  max(if(user_from_v1 = 1, 1, 0)) as is_from_11_uv,
            max(if(user_from_v1 = 2, 1, 0)) as is_from_12_uv,
            max(if(user_from_v1 = 3, 1, 0)) as is_from_13_uv,
            max(if(user_from_v2 = 4, 1, 0)) as is_from_24_uv,
            max(if(user_from_v2 = 5, 1, 0)) as is_from_25_uv,
            max(if(user_from_v3 = 6, 1, 0)) as is_from_26_uv,
            max(if(user_from_v3 = 7, 1, 0)) as is_from_37_uv,
            max(if(user_from_v3 = 8, 1, 0)) as is_from_38_uv
    from table_a
    where p_date = '${p_date}'
    group by user_id
) a
```

---

## 6. FULL OUTER JOIN 的优化

### 场景说明

在企业数据处理中，full outer join 场景较为常见。例如需整合多个数据源的信息（如不同渠道的广告数据），或单个表的核心指标需从不同数据源汇总时，会涉及多表全外连接操作。

### 常规写法

```sql
select  customer_id,
        sum(ad_show) as ad_show,
        sum(ad_click) as ad_click
from (
    select  coalesce(t1.customer_id, t2.customer_id) as customer_id,
            ad_click
    from    A_product_data_source as t1
    full join B_product_data_source as t2 on t2.t_date = '${p_date}'
    and     t1.customer_id = t2.customer_id
    and     t1.date = '${p_date}'
) a
group by customer_id
```

### 优化代码（用 union all 避免 Shuffle 过程）

```sql
select  customer_id,
        sum(ad_show) as ad_show,
        sum(ad_click) as ad_click
from (
    select  customer_id,
            ad_show,
            ad_click
    from    A_product_data_source as t1
    where   t1.date = '${p_date}'

    union all

    select  customer_id,
            ad_show,
            ad_click
    from    B_product_data_source as t2
    where   t2.t_date = '${p_date}'
) a
group by customer_id
```

当然实际场景中，可能是 A 数据源要去计算指标 A，B 数据源要去计算指标 B，最终要输出同一个表的同时能看到指标 A 和指标 B，这种情况下也要涉及到数据源的合并操作，如果两个数据源粒度合适，可以进行合并操作，此时输出的字段是两个数据源的并集，数据源中没有的字段置空即可。

```sql
select  customer_id,
        sum(ad_show) as ad_show,
        sum(ad_click) as ad_click
from (
    select  customer_id,
            ad_show,
            null as ad_click
    from    A_product_data_source as t1
    where   t1.date = '${p_date}'

    union all

    select  customer_id,
            null as ad_show,
            ad_click
    from    B_product_data_source as t2
    where   t2.date = '${p_date}'
) a
group by customer_id
```

---

## 7. UDF 函数的优化

当某个 UDF 函数执行本身耗时较长，且 UDF 处理结果会被重复使用的时候，建议先把 UDF 执行结果做成子查询，再对查询结果进行使用，而不是在代码中反复调用此 UDF 函数

**源代码**：

```sql
-- 1. get_true_us_info(user_id)是一个UDF函数，通过该函数的规则对user_id格式和内容进行转换，使其可以正常进行关联
select  t1.get_true_us_info(t1.user_id) as user_id,
        xxxx
from    table_a as t1
left join dim_table_b as t2 on t1.get_true_us_info(t1.user_id) = t2.user_id
left join dim_table_c as t3 on t1.get_true_us_info(t1.user_id) = t3.user_id
left join dim_table_d as t4 on t1.get_true_us_info(t1.user_id) = t4.user_id
where   t1.p_date = '20220401'
```

**优化后的代码**：

```sql
catch table t1(
 select  p_date,
            get_true_us_info(user_id) as user_id
    from    table_a
    where   p_date = '20220401'
)
select  t1.user_id,
        xxxx
from  t1
left join dim_table_b as t2 on t1.user_id = t2.user_id
left join dim_table_c as t3 on t1.user_id = t3.user_id
left join dim_table_d as t4 on t1.user_id = t4.user_id
```

---

## 8. 版本升级优化

如果是历史代码（如 Spark 2.4），最好的优化方式是 **升级版本**。

### Spark 3.x 相比 2.x 的性能提升

| 特性 | 说明 |
|-----|------|
| **AQE (Adaptive Query Execution)** | 运行时自适应优化，动态调整执行策略 |
| **DPP (Dynamic Partition Pruning)** | 动态分区裁剪，自动识别并过滤无关分区 |
| **自动广播 Join 优化** | 更智能的广播阈值判断 |
| **文件列表加速** | 小文件场景显著提升 |
| **向量化执行** | 利用 CPU SIMD 指令加速计算 |
| **Dynamic Allocation 自动开启** | 动态资源分配默认开启，资源利用更高效 |

**建议**：优先升级版本，再进行代码层面的优化。升级后观察 SQL 性能，大部分场景会自动提升。

---

## 9. 小文件问题优化

### 问题描述

上游表如果逻辑复杂且有大表，但是最终得到的结果较小，往往会产生小文件问题。

**影响**：下游读取文件时，主要的耗时都在启动 Task 中（而非数据读取），导致整体效率低下。

### 解决方案

使用 `DISTRIBUTE BY` 重新分配数据：

```sql
INSERT OVERWRITE TABLE target_table
SELECT *
FROM source_table
DISTRIBUTE BY ceil(rand() * 1)
```

**说明**：`ceil(rand() * 1)` 将数据打散成 1 个分区，避免产生过多小文件。

---

## 10. Dynamic Allocation 动态资源分配

`dynamicAllocation` 是 Spark 的动态资源伸缩机制：根据作业负载，自动增减 Executor 数量。

### Spark 版本差异

- **Spark 2.4**：需要手动开启，否则即使集群资源空闲，也无法充分利用资源
- **Spark 3.2**：自动开启，效率更好

### 核心参数

```sql
-- 基础配置
spark.dynamicAllocation.enabled=true           -- 开启动态分配 executor
spark.shuffle.service.enabled=true             -- 开启外部 shuffle 服务，支持 executor 被回收后 shuffle 文件仍可用

-- 资源范围
spark.dynamicAllocation.initialExecutors=1     -- 启动先给 1 个 executor
spark.dynamicAllocation.minExecutors=1         -- 最少保留 1 个
spark.dynamicAllocation.maxExecutors=25        -- 最多扩到 25 个

-- 扩容触发条件
spark.dynamicAllocation.schedulerBacklogTimeout=2s        -- 任务排队超过 2 秒就开始申请更多 executor
spark.dynamicAllocation.sustainedSchedulerBacklogTimeout=2s  -- 排队持续时每 2 秒继续评估扩容
spark.dynamicAllocation.executorAllocationRatio=0.5       -- 扩容时按"需求的一半"来申请，避免一下子冲太猛

-- 缩容触发条件
spark.dynamicAllocation.executorIdleTimeout=60s          -- executor 空闲 60 秒就回收（缩容偏积极）
spark.dynamicAllocation.cachedExecutorIdleTimeout=30min  -- 如果 executor 上有缓存数据（cache），空闲也会等 30 分钟才回收（保护缓存）
```

---

## 11. 大文件读取优化

### spark.sql.files.maxPartitionBytes

```sql
spark.sql.files.maxPartitionBytes = 268435456  -- 默认 128MB，建议调整到 256MB
```

### 说明

- 控制每个 Task 读取的数据量
- **太小**：Task 数量过多，无法充分利用资源，启动 Task 开销大
- **太大**：Task 数量过少，并行度不够
- 需要根据集群性能合理调整

---

## 12. 推测执行优化

### spark.speculation

```sql
spark.speculation = false  -- 关闭推测执行
```

### 原理

推测执行本来是为了解决慢节点/拖尾 Task：对疑似慢 Task 再启动一个副本，谁先完成用谁。

### 为何建议关闭

- 占用额外资源
- 产生大量 killed task
- 遇到过开启之后数据丢失的情况

---

## 13. AQE (Adaptive Query Execution) 优化

### 核心参数

```sql
-- 基础开关
spark.sql.adaptive.enabled = true                          -- AQE 总开关

-- 分区合并相关
spark.sql.adaptive.coalescePartitions.enabled = true       -- 允许 AQE 合并小分区
spark.sql.adaptive.coalescePartitions.initialPartitionNum = 200  -- 合并前的初始 shuffle 分区数（AQE 起点）
spark.sql.adaptive.advisoryPartitionSizeInBytes = 67108864  -- 合并时的目标分区大小（默认 64MB）

-- Join 优化相关
spark.sql.adaptive.autoBroadcastJoinThreshold = 134217728   -- AQE 运行时改广播 join 的阈值（默认 128MB）
spark.sql.adaptive.skewJoin.enabled = true                  -- 是否做倾斜 join 优化

-- Shuffle 读取优化
spark.sql.adaptive.localShuffleReader.enabled = true        -- 是否优先本地读取 shuffle
```

### 参数详解

| 参数 | 说明 |
|-----|------|
| `coalescePartitions.initialPartitionNum` | AQE 起点：先按这个分区数执行早期 shuffle，再根据实际数据决定是否合并。可以理解为"先切多细，再动态并回来"的起始粒度 |
| `advisoryPartitionSizeInBytes` | AQE 合并时的目标分区大小 |
| `autoBroadcastJoinThreshold` | AQE 运行时动态调整广播 join 的阈值，比静态的 `autoBroadcastJoinThreshold` 更灵活 |
| `skewJoin.enabled` | 自动识别并处理 Join 倾斜 |
| `localShuffleReader.enabled` | 优先本地读取 shuffle 数据，减少网络 IO |

### 经验总结

```
initialPartitionNum 偏大 + advisoryPartitionSizeInBytes 合理
= 前期并行度足够，后期 AQE 再收敛分区，通常更稳
```

**注意**：系统默认的 `initialPartitionNum` 往往比较大，最好根据实际的数据量手动设置一下。

---

*文档来源：Confluence - SQL 优化总结*