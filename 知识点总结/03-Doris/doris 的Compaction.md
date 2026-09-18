
```
Doris 中的 Compaction。
Doris 是列式存储 + 类 LSM 的 segment/version 追加模型
Doris 每次导入都会新增 Rowset（数据文件），文件越来越多后，后台会自动把多个小文件合并成大文件，这个过程就叫 Compaction。
每次写入 = 生成一个 Rowset（新版本）
逻辑结构：
Table
 └── Partition
      └── Tablet
           └── Rowset（版本）
                └── Segment（列式数据文件）
```