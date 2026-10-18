# DB-GPT 本地启动与公司数仓接入准备

更新时间：2026-10-11

## 一、当前已经验证的环境

- 项目目录：`/Users/nanan/Documents/DB-GPT`
- DB-GPT 版本：`0.8.2`
- 当前代码提交：`361eb344`
- Python：由 `uv` 管理，当前使用 Python 3.11
- `uv`：`0.13.0`
- Web 地址：`http://localhost:5670`
- 大模型平台：硅基流动 SiliconFlow
- 对话模型：`deepseek-ai/DeepSeek-V4-Flash`
- Embedding 模型：`BAAI/bge-large-zh-v1.5`
- Reranker：`BAAI/bge-reranker-v2-m3`
- 元数据库：本地 SQLite，文件为 `pilot/meta_data/dbgpt.db`
- 向量库：本地 Chroma，目录为 `pilot/data`
- 当前配置文件：`configs/dbgpt-proxy-siliconflow-deepseek-v4-flash.toml`

已验证的范围：DB-GPT 可以启动，SiliconFlow Key 鉴权返回 HTTP 200，普通对话接口最终返回“测试成功”。公司数仓连接、Text2SQL 准确率和知识库/RAG 还没有验证。

## 二、日常启动流程

### 1. 进入项目目录   -- 直接跳到 步骤4启动 apikey已经配置完成

```bash
cd /Users/nanan/Documents/DB-GPT
```

### 2. 在当前终端加载 SiliconFlow API Key

不要把完整 Key 写进 Git、配置文件、Shell 历史或 Obsidian 笔记。每次新开终端，可以用静默输入：

```bash
unset SILICONFLOW_API_KEY

read -rs "SILICONFLOW_API_KEY?请输入完整的硅基流动 API Key: "
echo
export SILICONFLOW_API_KEY

printf '已加载 Key：长度=%s，开头=%s\n' \
  "${#SILICONFLOW_API_KEY}" \
  "${SILICONFLOW_API_KEY:0:3}"
```

这只显示长度和前三个字符，不会打印完整 Key。环境变量只对当前终端及其子进程有效，换一个终端后要重新加载。

### 3. 验证 Key

```bash
curl -sS -o /dev/null \
  -w "SiliconFlow HTTP %{http_code}\n" \
  https://api.siliconflow.cn/v1/models \
  -H "Authorization: Bearer $SILICONFLOW_API_KEY"
```

预期结果：

```text
SiliconFlow HTTP 200
```

如果是 `401`，不要继续启动。通常表示当前终端没有正确加载 Key、Key 复制不完整、包含空格，或者 Key 已失效。

### 4. 启动 DB-GPT

```bash
uv run --no-sync dbgpt start webserver \
  --config configs/dbgpt-proxy-siliconflow-deepseek-v4-flash.toml
```

保持这个终端窗口运行。启动完成后打开：

```text
http://localhost:5670
```

首次确认时，新建对话并发送一句简单问题，例如“请只回复：测试成功”。

### 5. 检查运行状态

另开一个终端：

```bash
lsof -nP -iTCP:5670 -sTCP:LISTEN
```

查看实时日志：

```bash
cd /Users/nanan/Documents/DB-GPT
tail -f logs/dbgpt_webserver.log
```

浏览器仍显示上一次的错误时，先新建会话；仍未恢复再用 `Command + Shift + R` 强制刷新。

### 6. 停止服务

回到运行 DB-GPT 的终端，按：

```text
Control + C
```

然后确认端口已经释放：

```bash
lsof -nP -iTCP:5670 -sTCP:LISTEN
```

没有输出表示 Web 服务已经停止。

## 三、首次安装或重建环境

当前电脑已经完成以下操作，日常启动不需要重复执行。只有删除虚拟环境、重新克隆项目或依赖损坏时才使用。

安装 `uv`：

```bash
brew install uv
```

安装本项目需要的依赖：

```bash
cd /Users/nanan/Documents/DB-GPT

uv sync \
  --all-packages \
  --no-default-groups \
  --extra base \
  --extra proxy_openai \
  --extra rag \
  --extra storage_chromadb \
  --extra dbgpts
```

验证版本：

```bash
uv run --no-sync dbgpt --version
```

当前输出应为：

```text
dbgpt, version 0.8.2
```

这个 checkout 还包含两处本地修改：

1. `packages/dbgpt-core/pyproject.toml` 增加空的 `dependency-groups.dev`，用于解决当前 `uv sync` 的依赖组错误。
2. `packages/dbgpt-app/src/dbgpt_app/openapi/api_v1/agentic_data_api.py` 给普通聊天路径初始化 `wiki_available = False`，解决未选择知识库时的 `UnboundLocalError`。

升级或重新拉取代码后，要检查上游是否已经修复；不要不加判断地重复应用补丁。

## 四、接入公司数仓前要准备什么

### 1. 先确认技术连接信息

需要向平台或 DBA 收集：

- 数仓查询引擎及准确版本，例如 Hive、Spark SQL、Doris、StarRocks、ClickHouse、MySQL、PostgreSQL 或 MaxCompute。
- 连接方式：JDBC、HTTP、Thrift、网关、代理或企业统一数据服务。
- 测试环境的主机名、端口、Catalog、Database、Schema。
- 是否必须连接公司 VPN、堡垒机或零信任客户端。
- 本机 IP 是否需要加入白名单。
- 是否要求 TLS，以及 CA 证书、客户端证书的管理方式。
- 数据库驱动和认证方式：账号密码、LDAP、Kerberos、Token、云账号等。
- 一条经过确认、可以在测试环境运行的最小查询 SQL。

当前 DB-GPT 代码中可以看到 MySQL、PostgreSQL、Hive、Spark、Doris、StarRocks、ClickHouse、MaxCompute、Oracle、SQL Server 等连接器，但“存在连接器”不等于能直接适配公司的版本、认证和网络环境，必须实际做连接测试。

### 2. 单独申请只读测试账号

第一阶段不要连接生产写账号，也不要让 DB-GPT 拥有建表、改表、删除或写入权限。建议账号满足：

- 只允许 `SELECT` 和读取必要元数据。
- 只开放一个测试 Catalog/Schema 或少量白名单表。
- 不开放原始敏感明细表。
- 限制查询超时、并发、扫描量和最大返回行数。
- 使用独立资源组、队列或查询集群，避免影响调度任务和 BI 查询。
- 账号可以单独审计和随时吊销。

仅在 Prompt 中告诉模型“不要执行危险 SQL”不是权限控制。最终边界必须由数据库权限、SQL 校验和执行侧资源限制共同保证。

### 3. 准备业务元数据和语义资料

只连数据库通常只能得到表名、字段名和类型，不足以让模型理解公司的业务。还要准备：

- 表中文名、用途、负责人、更新周期和数据截止时间。
- 数仓分层与主题域，例如 ODS、DWD、DWS、ADS。
- 字段中文注释、枚举值和脱敏说明。
- 主键、外键、常用 Join 键、基数和时间有效性规则。
- 指标名称、定义、公式、粒度、时间口径、适用组织和来源表。
- 常用问题与经过人工确认的标准 SQL。
- 应优先使用的汇总表，以及禁止直接查询的明细表。

建议第一阶段只选择一个主题域、5～20 张结构清晰的表和 20～50 个真实问题，不要一开始把整个数仓全部交给模型。

### 4. 做外部模型的数据安全评审

当前模型、Embedding 和 Reranker 都调用 SiliconFlow。可能被发送到外部服务的内容包括：

- 用户自然语言问题。
- 表名、字段名、字段注释和部分元数据。
- 指标口径、知识库片段和示例 SQL。
- 工具执行结果、错误信息或被拼入上下文的数据样例。

因此，在连接公司数据前必须确认：

- 哪些元数据允许离开公司网络。
- 是否允许将业务问题和 SQL 发送给外部模型。
- 是否需要字段映射、脱敏或完全私有化模型。
- 模型服务商的数据保存、训练使用、日志和合规条款是否通过公司审查。
- DB-GPT 本地日志中是否可能保存敏感 Prompt、SQL 和结果。

在没有完成安全评审前，只使用虚构数据、脱敏数据或专门的测试库。

### 5. 准备验证集，而不是只看“SQL 能不能跑”

每个测试问题至少保存：

- 原始业务问题。
- 明确的指标口径和时间范围。
- 期望使用的表和字段。
- 人工确认的标准 SQL。
- 期望结果或结果校验规则。
- 错误类型：选表、Join、过滤、口径、方言、权限或执行错误。

验收需要分别检查：

1. Schema 召回是否选对表。
2. SQL 中的表和字段是否真实存在。
3. SQL 是否符合目标引擎方言。
4. SQL 是否安全、能裁剪分区、不会超量扫描。
5. 执行结果是否符合业务口径。

SQL 可以执行只是必要条件，不代表结果正确。

## 五、推荐的接入顺序

### 阶段 1：连通性验证

1. 连接 VPN 或公司网络。
2. 使用数据库原生客户端和只读账号执行最小 SQL。
3. 确认本机到目标地址、端口、TLS 和认证都正常。
4. 根据实际引擎确认 DB-GPT 连接器及 Python 驱动。
5. 在 DB-GPT 中只连接测试库，并读取少量表的 Schema。

这一阶段的完成标准是“能够稳定读取元数据并执行受限查询”，不是页面显示连接成功。

### 阶段 2：单主题域问数验证

1. 选择一个低敏感、口径相对稳定的主题域。
2. 补齐表、字段、Join 和指标口径。
3. 导入少量人工确认的问答与 SQL 示例。
4. 用固定问题集测试 Text2SQL。
5. 对错误做分类并逐项修正。

### 阶段 3：安全和工程化

1. 增加 SQL AST 解析与只读校验，拒绝 DDL、DML 和多语句。
2. 增加表、列、行级权限校验。
3. 设置查询超时、扫描量、并发和结果行数上限。
4. 记录用户、问题、召回元数据、生成 SQL、改写和执行结果。
5. 分离开发、测试和生产配置，并使用公司密钥管理系统。
6. 建立固定回归集、监控、发布和回滚流程。

## 六、下一步需要确认的信息

准备实际接入时，先回答以下问题：

1. 公司主要查询引擎是什么，准确版本是多少？
2. 本机通过什么网络方式访问，是否需要 VPN、白名单或堡垒机？
3. 是否能申请只读测试账号和独立测试库？
4. 计划先接哪个主题域、哪些表？
5. 表字段注释、指标口径和标准 SQL 目前存在哪里？
6. 公司是否允许把 Schema、业务问题和 SQL 发送给 SiliconFlow？

其中第 6 项决定能否继续使用当前外部模型方案；如果不允许，需要改为公司批准的模型网关或内网私有模型。

## 七、相关本地文件

- 启动配置：`/Users/nanan/Documents/DB-GPT/configs/dbgpt-proxy-siliconflow-deepseek-v4-flash.toml`
- 运行日志：`/Users/nanan/Documents/DB-GPT/logs/dbgpt_webserver.log`
- 架构图：`/Users/nanan/Documents/DB-GPT/DB-GPT-整体架构.drawio`
- 本地元数据库：`/Users/nanan/Documents/DB-GPT/pilot/meta_data/dbgpt.db`
- 本地向量数据：`/Users/nanan/Documents/DB-GPT/pilot/data`

注意：元数据库、向量库和日志可能逐渐包含连接信息、业务元数据、问题、SQL 或检索片段。备份、同步和提交 Git 前要先检查敏感内容。
