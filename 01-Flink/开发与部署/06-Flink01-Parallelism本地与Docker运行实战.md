# Flink01 Parallelism 本地与 Docker 运行实战

## 1. 示例目标

`Flink01_Parallelism` 用于观察不同算子的并行度。

核心流程：

```text
Socket 输入
→ FlatMap 拆分单词
→ KeyBy 按单词分组
→ Sum 累加计数
→ Print 输出
```

## 2. 当前代码中的双运行模式

核心逻辑：

```java
String socketHost = args.length > 0 ? args[0] : "localhost";
int socketPort = args.length > 1 ? Integer.parseInt(args[1]) : 8888;

Configuration conf = new Configuration();
if (args.length == 0) {
    conf.setInteger("rest.port", 5678);
}

StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(conf);
```

含义：

| 运行模式 | 是否传参数 | Socket 地址 | Web UI |
|---|---|---|---|
| IDEA 本地运行 | 否 | `localhost:8888` | `http://localhost:5678` |
| Docker 集群提交 | 是 | 默认传入 `host.docker.internal:8888` | `http://localhost:8081` |

## 3. 本地 IDEA 运行

### 3.1 启动输入源

Mac 终端执行：

```bash
nc -lk 8888
```

### 3.2 IDEA 运行入口

运行：

```text
com.atguigu.flink.architecture.Flink01_Parallelism.main()
```

不传参数。

### 3.3 查看 Web UI

```text
http://localhost:5678
```

### 3.4 输入数据

在 `nc` 终端输入：

```text
hello flink
hello java
hello flink
```

### 3.5 输出位置

本地运行时，`print()` 通常显示在 IDEA Run Console。

## 4. Docker 集群提交

### 4.1 确认 Docker 容器

```bash
docker ps --format 'table {{.Names}}\t{{.Ports}}'
```

应能看到：

```text
jobmanager
 taskmanager1
 taskmanager2
```

### 4.2 启动 Mac 输入源

```bash
nc -lk 8888
```

### 4.3 使用脚本提交

```bash
./Flink/submit-parallelism-to-docker.sh
```

脚本内容分解如下。

#### 加载 SDKMAN Java

```bash
if [[ -s "${HOME}/.sdkman/bin/sdkman-init.sh" ]]; then
  source "${HOME}/.sdkman/bin/sdkman-init.sh"
fi
```

确保 Maven 使用 SDKMAN 管理的 Java，而不是 Homebrew Java 25。

#### 严格 Shell 模式

```bash
set -euo pipefail
```

| 参数 | 含义 |
|---|---|
| `-e` | 任一命令失败立即停止 |
| `-u` | 使用未定义变量时报错 |
| `pipefail` | 管道中任一命令失败即视为失败 |

#### 计算目录

```bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
```

无论从哪个目录执行脚本，都能定位项目目录。

#### 定义提交参数

```bash
JAR_NAME="Flink-thin.jar"
MAIN_CLASS="com.atguigu.flink.architecture.Flink01_Parallelism"
SOCKET_HOST="${1:-host.docker.internal}"
SOCKET_PORT="${2:-8888}"
```

默认：

```text
host.docker.internal:8888
```

也可以手动覆盖：

```bash
./Flink/submit-parallelism-to-docker.sh 192.168.1.10 9999
```

#### 干净编译

```bash
mvn -pl Flink -am clean compile -DskipTests
```

`clean` 很重要，避免旧 Java 17 class 混入 Java 11 目标产物。

#### 生成 thin JAR

```bash
jar --create \
  --file "${SCRIPT_DIR}/target/${JAR_NAME}" \
  -C "${SCRIPT_DIR}/target/classes" .
```

只将项目编译结果打包，不执行全量 Maven Shade。

#### 复制到 JobManager

```bash
docker cp \
  "${SCRIPT_DIR}/target/${JAR_NAME}" \
  "jobmanager:/tmp/${JAR_NAME}"
```

#### 提交

```bash
docker exec jobmanager flink run -d \
  -c "${MAIN_CLASS}" \
  "/tmp/${JAR_NAME}" \
  "${SOCKET_HOST}" \
  "${SOCKET_PORT}"
```

最后两个值会作为 `args` 传给 Java `main()`。

## 5. 手动提交，不使用脚本

### 5.1 构建 thin JAR

```bash
source "$HOME/.sdkman/bin/sdkman-init.sh"

mvn -pl Flink -am clean compile -DskipTests

jar --create \
  --file Flink/target/Flink-thin.jar \
  -C Flink/target/classes .
```

### 5.2 复制到容器

```bash
docker cp \
  Flink/target/Flink-thin.jar \
  jobmanager:/tmp/Flink-thin.jar
```

### 5.3 提交

```bash
docker exec jobmanager flink run -d \
  -c com.atguigu.flink.architecture.Flink01_Parallelism \
  /tmp/Flink-thin.jar \
  host.docker.internal \
  8888
```

## 6. 使用 Web UI 上传提交

先构建：

```text
Flink/target/Flink-thin.jar
```

打开：

```text
http://localhost:8081
```

进入：

```text
Submit New Job
→ Add New
→ 上传 Flink-thin.jar
```

填写：

```text
Entry Class:
com.atguigu.flink.architecture.Flink01_Parallelism
```

```text
Program Arguments:
host.docker.internal 8888
```

点击 Submit。

## 7. 查看运行状态

Web UI：

```text
http://localhost:8081
```

命令行：

```bash
docker exec jobmanager flink list
```

REST API：

```bash
curl http://localhost:8081/jobs/overview
```

## 8. 查看输出

因为 `print()` 算子在 TaskManager 中执行：

```bash
docker logs -f taskmanager1
docker logs -f taskmanager2
```

## 9. 停止作业

```bash
docker exec jobmanager flink cancel <JobID>
```

示例：

```bash
docker exec jobmanager flink cancel \
  09ce3818e824cff040eed86b03dae021
```

## 10. 切换到其他入口类

统一 thin JAR 中也包含其他示例，例如：

```text
com.atguigu.flink.architecture.Flink02_OperatorChain
```

可以修改 `-c`：

```bash
docker exec jobmanager flink run -d \
  -c com.atguigu.flink.architecture.Flink02_OperatorChain \
  /tmp/Flink-thin.jar
```

但是该类如果仍写死 `hadoop102`，需要先改成可传入的 socket 地址，或改为 `host.docker.internal`。
