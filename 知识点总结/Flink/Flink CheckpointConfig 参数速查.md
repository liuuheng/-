# Flink CheckpointConfig 参数速查

> 适用版本：Flink 1.17

## StreamExecutionEnvironment 入口方法

```java
// 获取检查点配置对象
CheckpointConfig getCheckpointConfig();

// 使用默认参数开启检查点
StreamExecutionEnvironment enableCheckpointing();

// 开启检查点并设置触发间隔
StreamExecutionEnvironment enableCheckpointing(long interval);

// 开启检查点并设置触发间隔和检查点模式
StreamExecutionEnvironment enableCheckpointing(
        long interval,
        CheckpointingMode mode
);

// 开启检查点并设置触发间隔、检查点模式和强制检查点标志
StreamExecutionEnvironment enableCheckpointing(
        long interval,
        CheckpointingMode mode,
        boolean force
);

// 获取检查点触发间隔
long getCheckpointInterval();

// 获取检查点模式
CheckpointingMode getCheckpointingMode();

// 设置状态后端
StreamExecutionEnvironment setStateBackend(StateBackend stateBackend);

// 获取状态后端
StateBackend getStateBackend();
```

## CheckpointConfig 构造方法

```java
// 创建默认检查点配置对象
CheckpointConfig();

// 深拷贝一个检查点配置对象
CheckpointConfig(CheckpointConfig checkpointConfig);
```

## 开启、关闭与状态查询

```java
// 关闭检查点
void disableCheckpointing();

// 查询是否已经开启检查点
boolean isCheckpointingEnabled();
```

## 检查点模式

```java
// 设置检查点模式
void setCheckpointingMode(CheckpointingMode checkpointingMode);

// 获取检查点模式
CheckpointingMode getCheckpointingMode();
```

## 检查点间隔与超时

```java
// 设置检查点触发间隔，单位为毫秒
void setCheckpointInterval(long checkpointInterval);

// 获取检查点触发间隔
long getCheckpointInterval();

// 设置单次检查点超时时间，单位为毫秒
void setCheckpointTimeout(long checkpointTimeout);

// 获取单次检查点超时时间
long getCheckpointTimeout();

// 设置两次检查点之间的最小暂停时间，单位为毫秒
void setMinPauseBetweenCheckpoints(long minPauseBetweenCheckpoints);

// 获取两次检查点之间的最小暂停时间
long getMinPauseBetweenCheckpoints();
```

## 并发检查点

```java
// 设置同时执行的最大检查点数量
void setMaxConcurrentCheckpoints(int maxConcurrentCheckpoints);

// 获取同时执行的最大检查点数量
int getMaxConcurrentCheckpoints();
```

## 检查点失败容忍

```java
// 设置可容忍的连续检查点失败次数
void setTolerableCheckpointFailureNumber(
        int tolerableCheckpointFailureNumber
);

// 获取可容忍的连续检查点失败次数
int getTolerableCheckpointFailureNumber();

// 设置检查点出错时是否使作业失败，已弃用
@Deprecated
void setFailOnCheckpointingErrors(boolean failOnCheckpointingErrors);

// 获取检查点出错时是否使作业失败，已弃用
@Deprecated
boolean isFailOnCheckpointingErrors();
```

## 外部化检查点

```java
// 设置外部化检查点的清理方式
void setExternalizedCheckpointCleanup(
        CheckpointConfig.ExternalizedCheckpointCleanup cleanupMode
);

// 开启外部化检查点并设置清理方式，已弃用
@Deprecated
void enableExternalizedCheckpoints(
        CheckpointConfig.ExternalizedCheckpointCleanup cleanupMode
);

// 查询是否已经开启外部化检查点
boolean isExternalizedCheckpointsEnabled();

// 获取外部化检查点的清理方式
CheckpointConfig.ExternalizedCheckpointCleanup
        getExternalizedCheckpointCleanup();
```

## ExternalizedCheckpointCleanup 枚举

```java
// 作业取消时删除外部化检查点
ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION;

// 作业取消时保留外部化检查点
ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

// 不启用外部化检查点
ExternalizedCheckpointCleanup.NO_EXTERNALIZED_CHECKPOINTS;

// 查询取消作业时是否删除检查点
boolean deleteOnCancellation();
```

## 非对齐检查点

```java
// 开启非对齐检查点
void enableUnalignedCheckpoints();

// 设置是否开启非对齐检查点
void enableUnalignedCheckpoints(boolean enabled);

// 查询是否已经开启非对齐检查点
boolean isUnalignedCheckpointsEnabled();

// 设置是否强制使用非对齐检查点
void setForceUnalignedCheckpoints(boolean forceUnalignedCheckpoints);

// 查询是否强制使用非对齐检查点
boolean isForceUnalignedCheckpoints();

// 设置对齐检查点超时时间
void setAlignedCheckpointTimeout(Duration alignedCheckpointTimeout);

// 获取对齐检查点超时时间
Duration getAlignedCheckpointTimeout();

// 设置 Barrier 对齐超时时间，已弃用
@Deprecated
void setAlignmentTimeout(Duration alignmentTimeout);

// 获取 Barrier 对齐超时时间，已弃用
@Deprecated
Duration getAlignmentTimeout();
```

## CheckpointStorage

```java
// 使用 CheckpointStorage 对象设置检查点存储
void setCheckpointStorage(CheckpointStorage storage);

// 使用字符串路径设置检查点存储目录
void setCheckpointStorage(String checkpointDirectory);

// 使用 URI 设置检查点存储目录
void setCheckpointStorage(URI checkpointDirectory);

// 使用 Flink Path 设置检查点存储目录
void setCheckpointStorage(Path checkpointDirectory);

// 获取检查点存储配置
CheckpointStorage getCheckpointStorage();
```

## Channel State

```java
// 设置共享同一个 Channel State 文件的最大 Subtask 数量
void setMaxSubtasksPerChannelStateFile(
        int maxSubtasksPerChannelStateFile
);

// 获取共享同一个 Channel State 文件的最大 Subtask 数量
int getMaxSubtasksPerChannelStateFile();
```

## Approximate Local Recovery

```java
// 设置是否开启近似本地恢复
void enableApproximateLocalRecovery(boolean enabled);

// 查询是否已经开启近似本地恢复
boolean isApproximateLocalRecoveryEnabled();
```

## 忽略在途数据

```java
// 设置恢复时忽略在途数据的 Checkpoint ID
void setCheckpointIdOfIgnoredInFlightData(
        long checkpointIdOfIgnoredInFlightData
);

// 获取恢复时忽略在途数据的 Checkpoint ID
long getCheckpointIdOfIgnoredInFlightData();
```

## 强制检查点

```java
// 设置是否强制执行检查点，已弃用
@Deprecated
void setForceCheckpointing(boolean forceCheckpointing);

// 查询是否强制执行检查点，已弃用
@Deprecated
boolean isForceCheckpointing();
```

## Configuration 转换

```java
// 使用 ReadableConfig 中的相关选项更新检查点配置
void configure(ReadableConfig configuration);

// 将检查点配置转换为 Configuration
Configuration toConfiguration();
```

