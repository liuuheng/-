
```
enable_unique_key_merge_on_write= "true"
解决的问题：查询时需要对同一主键的多条记录进行实时聚合（Merge-on-Read），导致计算开销过大。
行为：在数据导入（写入）阶段直接完成数据合并，并将最终结果落盘。
```

```
enable_unique_key_skip_bitmap_column= "true"
解决的问题：执行部分列更新时，必须先读取整行旧数据再与新数据拼接，引发大量不必要的 IO。
行为：在底层记录列级的变更标记（Bitmap），仅写入发生变化的列，跳过未变更列的读取与拼接操作
```