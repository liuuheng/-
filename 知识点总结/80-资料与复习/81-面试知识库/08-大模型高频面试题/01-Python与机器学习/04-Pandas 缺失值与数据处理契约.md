---
tags:
  - 大模型
  - 面试
source: "尚硅谷大模型技术之高频面试题-V2.1.6.docx"
source_blocks: "159-168"
reviewed: 2026-10-21
---

# Pandas 缺失值与数据处理契约

缺失值处理首先是业务语义，再是 fillna 或 dropna 的选择。

前篇：[[知识点总结/80-资料与复习/81-面试知识库/08-大模型高频面试题/01-Python与机器学习/03-数据结构、复杂度与常见算法|数据结构、复杂度与常见算法]] ｜ 后篇：[[知识点总结/80-资料与复习/81-面试知识库/08-大模型高频面试题/01-Python与机器学习/05-机器学习特征工程与数据泄漏|机器学习特征工程与数据泄漏]]

## 深入理解（新增）

空字符串、0、None、NaN 和 pd.NA 不必具有相同语义。先定义每列的类型、合法范围和缺失含义，再调用 isna 检查；把所有缺失值统一填 0，会把“未知价格”变成“免费”，改变模型学习目标。

均值、中位数、类别字典都属于从数据拟合得到的状态。必须先划分训练集，再用训练集统计量处理验证集和测试集。按用户或时间分组的数据还要避免同一实体跨集合产生隐式泄漏。

缺失本身可能带有信号。例如设备离线导致整组特征缺失，可以同时保留缺失指示列和填充值；但上线时缺失机制变化会使这条信号失效。应监测缺失率及其与标签的关系，而不只是保证程序不报错。



## 核心代码（新增）

以下是可在 CPU 运行的机制或业务契约示例，完整源码与测试见 [[知识点总结/80-资料与复习/81-面试知识库/08-大模型高频面试题/06-代码实验/01-核心代码实验与验证范围|核心代码实验与验证范围]]。示例不包含完整模型训练或外部平台部署。

```python
import numpy as np

def standardize_from_train(train, test):
    mean = train.mean(axis=0)
    scale = train.std(axis=0)
    scale = np.where(scale == 0, 1.0, scale)
    return (train - mean) / scale, (test - mean) / scale

train = np.array([[1., 2.], [3., 2.]])
test = np.array([[100., 2.]])
scaled_train, scaled_test = standardize_from_train(train, test)
# 第二列训练方差为零；测试集不参与统计量拟合。
```

## 追问与验证（新增）

把测试集整体平移 100，检查训练集的处理结果是否变化；若变化，说明预处理偷看了测试分布。

## 官方资料与论文

- [Pandas 缺失值处理](https://pandas.pydata.org/docs/user_guide/missing_data.html)
- [scikit-learn：常见陷阱与数据泄漏](https://scikit-learn.org/stable/common_pitfalls.html)

核验日期：2026-10-21。链接中的 `stable`、`main` 或平台文档会继续变化；新增纯 Python/NumPy 实验与原文旧版框架代码已分开标注。

## 原文保留

> [!warning] 原文批注
> 本节代码用标准化展示“训练拟合、测试变换”的契约；填补缺失值也必须遵守这一契约。标准化不保证数据变成正态分布。

<!-- source-block:159 -->
### 数据分析

<!-- source-block:160 -->
#### Pandas相关

<!-- source-block:161 -->
##### Pandas 的主要数据结构是什么？

<!-- source-block:162 -->
其主要数据结构是 Series 和 DataFrame。其中 Series 是带标签索引的一维数组。DataFrame 为二维结构，类似电子表格或 SQL 表，含行列信息，不同列可存不同数据类型。

<!-- source-block:163 -->
##### 怎样处理 DataFrame 中的缺失值？

<!-- source-block:164 -->
常用 dropna 或 fillna 方法。dropna 可删除含缺失值的行或列，fillna 能用指定值或统计量（均值、中位数等）填充缺失值，像 df.fillna(df.mean()) 可用列均值填充。

<!-- source-block:165 -->
##### groupby 函数的用法是什么？

<!-- source-block:166 -->
groupby 按特定标准分组数据，再针对各分组单独执行函数。如按 “category” 列分组求其他列和，可写成 df.groupby('category').sum()，也能传入自定义聚合函数（如sum\\mean\\count等）达成复杂计算。

<!-- source-block:167 -->
##### iloc 和 loc 的区别是什么？

<!-- source-block:168 -->
loc 依据标签选取数据，iloc 基于整数索引选取数据。比如 df.loc\['row\_label', 'col\_label'\] 用行列标签取值，df.iloc\[0, 1\] 选取第 1 行第 2 列的数据。
