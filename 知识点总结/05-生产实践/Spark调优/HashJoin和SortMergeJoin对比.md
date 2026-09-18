
![[99-images/企业微信截图_17783075985758.png]]


>这里有一个知识点，spark-job对两个dataset进行join的算子中，一般有BroadcastHashJoin、ShufflehashJoin和ShuffleSortMergeJoin三种，另外还有两种自然join，效率低暂不考虑。BroadcastHashJoin比较好理解，效率最高，不展开。HashJoin和SortMergeJoin两种是对两张“同等数量表”进行join的时候使用，一般情况下，HashJoin的计算复杂度肯定更小，因为SortMergeJoin需要有排序过程，而排序的复杂度至少是nlog2n，但是spark的shuffle比较特殊，它是内置了排序过程，也就是说HashJoin做了hash之后还要排序，这样一来SortMergeJoin的速度就是最快的。所以spark默认使用都是SortMergeJoin，比如上面的例子：40亿的renderpic表 join 20亿的designsnapshot表，肯定是使用SortMergeJoin。控制优先选择SortMergeJoin的参数是：spark.sql.join.prefersortmergeJoin，默认为true


![[99-images/企业微信截图_17783078086403.png|697]]


 > shufflehashjoin 在spark中 并不依赖排序，他是 build hash table+probe（查找、拿另一张表的数据，去 hash 表里查找匹配项）。但是spark的shuffle 都是sort shuffle  因此即使已经是排序好的，但是对于shufflehashjoin 没有太大的价值，排序成本已经付出，但是在join中没有吃到具体的收益