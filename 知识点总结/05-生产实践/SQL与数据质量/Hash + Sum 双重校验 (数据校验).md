- 通过 Hash + Sum 双重校验，验证优化后表与基准表是否一致。

> 双表行是否存在 + 校验列 MD5 是否相同 的差异检测 SQL；
> 只列出有问题的主键，用于优化或同步后的 数据一致性核对。


```sql
SELECT 
    org_id,
    user_id,
    time_type,
    CASE 
        WHEN table_count = 1 AND table_name = 't1' THEN '仅表1存在'
        WHEN table_count = 1 AND table_name = 't2' THEN '仅表2存在'
        WHEN hash_count = 1 THEN '数据一致'
        ELSE '数据不一致'
    END AS result
FROM (
    SELECT 
        org_id,  -- 主键
        user_id,
        time_type,
        COUNT(DISTINCT table_flag) AS table_count,  -- 为1  说明两表字段数量不同
        COUNT(DISTINCT row_hash) AS hash_count,     -- 为2 说明校验字段结果不同
        MAX(table_flag) AS table_name
    FROM (
        SELECT 
            org_id,
            user_id,
            time_type,
            MD5(CONCAT_WS('|',
                COALESCE(app_uid, ''),         
                COALESCE(industry_type, ''),    
                COALESCE(design_render_cnt, 0), 
                COALESCE(render_room_cnt, 0),  
                COALESCE(share_panovisit_cnt, 0) 
            )) AS row_hash,  --主键 + 核心指标
            't1' AS table_flag
        FROM 表1
        UNION ALL
        SELECT 
            org_id,
            user_id,
            time_type,
            MD5(CONCAT_WS('|',
                COALESCE(app_uid, ''),
                COALESCE(industry_type, ''),
                COALESCE(design_render_cnt, 0),
                COALESCE(render_room_cnt, 0),
                COALESCE(share_panovisit_cnt, 0)
            )) AS row_hash,
            't2' AS table_flag
        FROM 表1
    ) t
    GROUP BY org_id, user_id, time_type
) t2
WHERE table_count = 1 OR hash_count > 1
```


> 无法精确到某行，只能按照分区进行判断，但效率比上面的更高

```sql
SELECT 
    COALESCE(t1.hash_area, 0) - COALESCE(t2.hash_area, 0) as diff_hash_area,
    COALESCE(t1.hash_deleted, 0) - COALESCE(t2.hash_deleted, 0) as diff_hash_deleted,
    COALESCE(t1.hash_created, 0) - COALESCE(t2.hash_created, 0) as diff_hash_created,
    COALESCE(t1.hash_modified_time, 0) - COALESCE(t2.hash_modified_time, 0) as diff_hash_modified_time,
    COALESCE(t1.hash_design_name, 0) - COALESCE(t2.hash_design_name, 0) as diff_hash_design_name
FROM (
    -- 表1
    SELECT 
        ds,
        SUM(hash(COALESCE(area, 0))) as hash_area,
        SUM(hash(COALESCE(deleted, FALSE))) as hash_deleted,
        SUM(hash(COALESCE(created, ''))) as hash_created,
        SUM(hash(COALESCE(modified_time, ''))) as hash_modified_time,
        SUM(hash(COALESCE(design_name, ''))) as hash_design_name
    FROM kdw_dw.dwd_cntnt_project_design_s_d
    WHERE ds = '20251217'
    GROUP BY ds
) t1
FULL OUTER JOIN (
    -- 表2
    SELECT 
        ds,
        SUM(hash(COALESCE(area, 0))) as hash_area,
        SUM(hash(COALESCE(deleted, FALSE))) as hash_deleted,
        SUM(hash(COALESCE(created, ''))) as hash_created,
        SUM(hash(COALESCE(modified_time, ''))) as hash_modified_time,
        SUM(hash(COALESCE(design_name, ''))) as hash_design_name
    FROM kdw_dw.dwd_cntnt_project_design_s_d
    WHERE ds = '20251217'
    GROUP BY ds
) t2
ON t1.ds = t2.ds
```

## 1、一些注意事项

* 数据验证中 如果有字符串的拼接 随机数 需要手动接入
	* 比如：collect_set等不确定顺序的UDF 或者本身存在类似 UUID，rand()等 UDF，则结果中的字符串肯定会是不同的，需要人工介入

## 2、odps和hive中的迁移表时的处理
1. 一些需要注意的地方
![[images/企业微信截图_17663704578080.png]]

2. odps具体下云时的步骤
![[images/企业微信截图_17663714752870.png]]