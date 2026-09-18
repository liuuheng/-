# OpenClaw、Hermes 与 LM Studio 使用手册

更新时间：2026-08-17

## 一、当前安装情况

### 本地大模型

- 运行软件：LM Studio
- 主模型：Qwen3.6-35B-A3B 4-bit MLX
- 模型标识：`qwen3.6-35b-a3b`
- 模型大小：约 20.43 GB
- 支持：文字对话、视觉输入、工具调用
- 模型原生最大上下文：262,144 tokens
- Hermes 和 OpenClaw 当前按约 64K 上下文使用
- API 地址：`http://127.0.0.1:1234/v1`

本地嵌入模型：

- 模型：Nomic Embed Text v1.5
- 标识：`text-embedding-nomic-embed-text-v1.5`
- 用途：供 OpenClaw 做本地语义记忆检索

### Hermes Agent

- 版本：0.20.1
- 安装目录：`~/.hermes/hermes-agent`
- 配置文件：`~/.hermes/config.yaml`
- 环境变量：`~/.hermes/.env`
- 记忆目录：`~/.hermes/memories`
- 默认模型：`qwen3.6-35b-a3b`
- 模型提供方：LM Studio
- 上下文：65,536 tokens

### OpenClaw

- App 版本：2026.7.1
- CLI/Gateway 版本：2026.7.1-2
- App 位置：`/Applications/OpenClaw.app`
- 配置文件：`~/.openclaw/openclaw.json`
- 工作区：`~/.openclaw/workspace`
- 会话目录：`~/.openclaw/agents/main/sessions`
- 默认模型：`lmstudio/qwen3.6-35b-a3b`
- Gateway：`127.0.0.1:18789`
- Gateway 仅允许本机访问
- 记忆嵌入使用本机 LM Studio，不回退到云端

## 二、三者之间的关系

```text
                       ┌──────────────────────┐
                       │      LM Studio       │
                       │ Qwen 本地模型 + API   │
                       │ 127.0.0.1:1234       │
                       └──────────┬───────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    │                           │
          ┌─────────▼─────────┐       ┌────────▼────────┐
          │      Hermes       │       │    OpenClaw     │
          │ 对话、工具、记忆    │       │ App、Agent、记忆 │
          └───────────────────┘       └────────┬────────┘
                                               │
                                      Gateway 127.0.0.1:18789
```

LM Studio 负责真正运行模型。

Hermes 和 OpenClaw 是 Agent 层，负责：

- 组织提示词
- 保存会话和记忆
- 调用工具
- 操作文件或执行命令
- 连接聊天界面、定时任务或其他服务

如果 LM Studio 服务没有运行，Hermes 和 OpenClaw 都无法调用当前本地模型。

## 三、日常使用：推荐启动顺序

### 方式一：使用 OpenClaw.app

1. 打开 LM Studio：

```bash
open -a "LM Studio"
```

2. 启动 LM Studio API：

```bash
lms server start
```

3. 加载 Qwen 模型：

```bash
lms load qwen3.6-35b-a3b --context-length 65536 --gpu max
```

4. 打开 OpenClaw：

```bash
open -a OpenClaw
```

也可以直接点击“应用程序”中的 OpenClaw。

OpenClaw.app 会运行本机 Gateway，并显示在 macOS 菜单栏中。

### 方式二：使用 Hermes

先启动 LM Studio 服务并加载模型，然后运行：

```bash
hermes
```

一次性提问：

```bash
hermes -z "请分析这个问题，并给出结构化结论"
```

或者：

```bash
hermes chat -q "请分析这个问题，并给出结构化结论"
```

Hermes CLI 不要求 OpenClaw Gateway 运行。

### 方式三：使用 OpenClaw 终端界面

```bash
openclaw chat
```

或者：

```bash
openclaw
```

运行一次本地 Agent 任务：

```bash
openclaw agent \
  --local \
  --thinking off \
  --message "请分析这个问题，并给出结构化结论"
```

当前 Qwen 模型在 OpenClaw 中只接受：

```text
thinking = off
```

这不等于模型完全不推理。测试中模型仍产生了内部 reasoning tokens，只是 OpenClaw 不能单独选择低、中、高思考等级。

## 四、停止与释放资源

### 只结束当前对话

在 Hermes 或 OpenClaw 终端界面中按：

```text
Control + C
```

也可以输入界面提供的退出命令。

### 停止 OpenClaw

正常方式：

- 点击 macOS 菜单栏中的 OpenClaw 图标
- 选择退出

停止后台 Gateway：

```bash
openclaw gateway stop
```

重新启动 Gateway：

```bash
openclaw gateway restart
```

检查 Gateway：

```bash
openclaw gateway status
```

只退出 OpenClaw.app 不一定会停止后台 Gateway，需要以 `openclaw gateway status` 的结果为准。

### 卸载模型，释放约 20 GB 内存

```bash
lms unload qwen3.6-35b-a3b
```

卸载全部已加载模型：

```bash
lms unload --all
```

### 停止 LM Studio API

```bash
lms server stop
```

检查服务状态：

```bash
lms server status
```

关闭 LM Studio App 后，如果模型或服务仍占用资源，可使用上面的 `unload` 和 `server stop` 命令。

### 推荐的完整停止顺序

```bash
openclaw gateway stop
lms unload --all
lms server stop
```

Hermes 没有在后台运行时，不需要额外停止。

## 五、常用状态检查

### LM Studio

检查 API：

```bash
lms server status
```

查看已加载模型：

```bash
lms ps
```

查看本机已有模型：

```bash
lms ls
```

通过 API 查看模型：

```bash
curl http://127.0.0.1:1234/api/v1/models
```

### Hermes

```bash
hermes status
hermes status --deep
hermes config get model
hermes memory status
hermes sessions
hermes version
```

当前模型设置应为：

```text
default: qwen3.6-35b-a3b
provider: lmstudio
base_url: http://127.0.0.1:1234/v1
context_length: 65536
```

### OpenClaw

```bash
openclaw status
openclaw gateway status
openclaw models status
openclaw memory status --deep
openclaw config validate
```

重新建立记忆索引：

```bash
openclaw memory index --force
```

## 六、LM Studio API 使用方法

### API 地址

OpenAI 兼容接口：

```text
http://127.0.0.1:1234/v1
```

LM Studio 原生接口：

```text
http://127.0.0.1:1234/api/v1
```

目前服务只在本机使用，没有必要开放到局域网。

### 1. 查看模型

LM Studio 原生接口：

```bash
curl http://127.0.0.1:1234/api/v1/models
```

OpenAI 兼容接口：

```bash
curl http://127.0.0.1:1234/v1/models
```

### 2. 普通对话 API

```bash
curl http://127.0.0.1:1234/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "qwen3.6-35b-a3b",
    "messages": [
      {
        "role": "system",
        "content": "你是一个严谨的中文助手。"
      },
      {
        "role": "user",
        "content": "请解释什么是本地大模型。"
      }
    ],
    "temperature": 0.7,
    "max_tokens": 1024
  }'
```

从返回结果中读取：

```text
choices[0].message.content
```

### 3. Responses API

```bash
curl http://127.0.0.1:1234/v1/responses \
  -H "Content-Type: application/json" \
  -d '{
    "model": "qwen3.6-35b-a3b",
    "input": "请用中文解释量化模型。"
  }'
```

Responses API 支持有状态对话、工具和 MCP，适合新的程序。

### 4. Embeddings API

```bash
curl http://127.0.0.1:1234/v1/embeddings \
  -H "Content-Type: application/json" \
  -d '{
    "model": "text-embedding-nomic-embed-text-v1.5",
    "input": "这是一段需要转换成向量的文字。"
  }'
```

Embeddings 的用途包括：

- 语义搜索
- 本地知识库
- 文档相似度
- 长期记忆检索
- RAG

它不会直接回答问题，只会把文字转换成向量。

### 5. Python 调用

安装 OpenAI Python SDK：

```bash
python3 -m pip install openai
```

示例：

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://127.0.0.1:1234/v1",
    api_key="lm-studio"
)

response = client.chat.completions.create(
    model="qwen3.6-35b-a3b",
    messages=[
        {"role": "system", "content": "你是一个严谨的中文助手。"},
        {"role": "user", "content": "请解释本地大模型的优缺点。"}
    ],
    temperature=0.7,
    max_tokens=1024,
)

print(response.choices[0].message.content)
```

LM Studio 当前没有开启 API 认证，因此 `api_key` 只是兼容 SDK 所需的占位值。

如果以后把接口开放到局域网，必须启用 API Token，并配置防火墙。

## 七、Hermes API

Hermes 可以提供一个 OpenAI 兼容的 Agent API。它与 LM Studio API 的区别是：

- LM Studio API：直接调用模型
- Hermes API：调用完整 Agent，可以使用记忆、文件、终端和其他工具

Hermes Agent API 当前没有启用。

如以后需要启用，应先在 `~/.hermes/.env` 中设置：

```text
API_SERVER_ENABLED=true
API_SERVER_KEY=一段足够长的随机密码
```

然后启动：

```bash
hermes gateway
```

默认 API 地址：

```text
http://127.0.0.1:8642/v1
```

调用示例：

```bash
curl http://127.0.0.1:8642/v1/chat/completions \
  -H "Authorization: Bearer 你的API_SERVER_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "hermes-agent",
    "messages": [
      {
        "role": "user",
        "content": "请检查当前目录并总结内容。"
      }
    ]
  }'
```

注意：Hermes API 调用的是具有工具权限的 Agent，不只是普通聊天模型。不要使用简单密码，也不要随意开放到局域网或互联网。

## 八、OpenClaw Gateway 的用途

OpenClaw Gateway 地址：

```text
http://127.0.0.1:18789/
```

它主要用于：

- OpenClaw.app
- Web 控制界面
- WebSocket 控制
- 会话管理
- 定时任务
- 聊天渠道
- Agent 和节点通信

它不是 LM Studio 的 OpenAI 模型 API。

不要把 OpenAI SDK 的 `base_url` 设置为：

```text
http://127.0.0.1:18789
```

程序要直接调用模型时，应使用：

```text
http://127.0.0.1:1234/v1
```

检查 Gateway：

```bash
openclaw gateway status
```

启动：

```bash
openclaw gateway start
```

停止：

```bash
openclaw gateway stop
```

重启：

```bash
openclaw gateway restart
```

## 九、记忆机制

### Hermes 记忆

Hermes 内置记忆始终可用，主要文件：

```text
~/.hermes/memories/MEMORY.md
~/.hermes/memories/USER.md
```

- `MEMORY.md`：环境、项目、经验和约定
- `USER.md`：用户偏好、沟通方式和期望

Hermes 默认可以自行写入记忆。

如果希望每次写入前都需要确认：

```bash
hermes config set memory.write_approval true
```

检查外部记忆：

```bash
hermes memory status
```

Hermes 的内置记忆容量有限，应该保存稳定且重要的信息，不适合把所有聊天记录都塞进去。

### OpenClaw 记忆

OpenClaw 工作区：

```text
~/.openclaw/workspace
```

主要文件和目录包括：

```text
~/.openclaw/workspace/USER.md
~/.openclaw/workspace/memory/
```

当前设置：

- 记忆后端：内置 SQLite
- 搜索方式：关键词 + 向量混合检索
- 嵌入提供方：本机 LM Studio
- 嵌入模型：Nomic Embed Text v1.5
- 云端回退：关闭

检查：

```bash
openclaw memory status --deep
```

重新索引：

```bash
openclaw memory index --force
```

记忆系统已经可以使用，但只有实际写入记忆文件后才会产生可检索内容。

## 十、需要特别注意的安全问题

### 1. Agent 和普通聊天模型不同

LM Studio 直接对话通常只生成文字。

Hermes 和 OpenClaw 可能拥有：

- 读取文件权限
- 写入或修改文件权限
- 执行终端命令权限
- 网络访问权限
- 定时任务权限
- 浏览器或电脑控制权限

不要把网页、邮件、陌生文档中的指令直接交给 Agent 自动执行。

### 2. OpenClaw 当前不是沙箱运行

验收时检测到 OpenClaw Agent 的沙箱状态为关闭，并启用了 coding 工具配置。

这意味着 Agent 可能直接访问当前用户有权限访问的文件。

涉及以下操作时应人工确认：

- 删除文件
- 安装软件
- 修改系统设置
- 发送消息
- 上传文件
- 使用密码或 API Key
- 执行从网页复制的命令

### 3. macOS 权限按需开启

OpenClaw.app 可能申请：

- 麦克风
- 通知
- 辅助功能
- 屏幕录制
- 自动化控制

只开启确实需要的权限。普通文字聊天通常不需要辅助功能和屏幕录制权限。

### 4. 本地并不代表绝对不会联网

模型推理可以完全在本机完成，但以下功能可能联网：

- 网页搜索
- 打开网站
- 在线技能和插件
- 外部 MCP
- 聊天渠道
- 软件更新
- 云端记忆服务
- 图片、视频或语音生成服务

使用这些功能前应确认数据会被发送到哪里。

### 5. 不要把本地端口暴露到公网

当前推荐保持：

```text
LM Studio：127.0.0.1:1234
OpenClaw：127.0.0.1:18789
Hermes API（如果启用）：127.0.0.1:8642
```

除非已经配置身份验证、防火墙或安全隧道，否则不要改成 `0.0.0.0`。

## 十一、性能注意事项

Qwen3.6-35B-A3B 加载后占用约 20 GB 统一内存。

建议：

- 不使用时卸载模型
- 不要同时加载多个大型模型
- 上下文优先保持 64K
- 上下文越大，内存占用和首字延迟越高
- 长对话变慢时开启新会话
- 进行简单问答时不必要求超长输出
- 同时运行 OpenClaw、Hermes 和 LM Studio 会增加内存与后台进程数量

卸载模型：

```bash
lms unload --all
```

再次加载：

```bash
lms load qwen3.6-35b-a3b --context-length 65536 --gpu max
```

## 十二、常见故障排查

### Hermes 或 OpenClaw 提示无法连接模型

检查：

```bash
lms server status
lms ps
curl http://127.0.0.1:1234/v1/models
```

如果服务没有运行：

```bash
lms server start
```

如果模型没有加载：

```bash
lms load qwen3.6-35b-a3b --context-length 65536 --gpu max
```

### OpenClaw.app 无法连接

```bash
openclaw gateway status
openclaw gateway restart
```

确认配置：

```bash
openclaw config validate
openclaw models status
```

### OpenClaw 记忆不可用

```bash
openclaw memory status --deep
openclaw memory index --force
```

确认 LM Studio 中存在嵌入模型：

```bash
curl http://127.0.0.1:1234/api/v1/models
```

### 终端提示找不到命令

重新打开终端，或者运行：

```bash
source ~/.zshrc
```

然后检查：

```bash
command -v lms
command -v hermes
command -v openclaw
```

### 模型占用内存但没有使用

```bash
lms ps
lms unload --all
```

### OpenClaw 日志

```bash
openclaw logs
```

本机日志通常位于：

```text
/tmp/openclaw/openclaw-日期.log
```

### Hermes 日志

```text
~/.hermes/logs
```

## 十三、更新建议

更新前先确认当前配置和功能正常，不要同时更新所有组件。

### OpenClaw

优先通过 OpenClaw.app 的更新界面更新。App 会协调更新 Gateway。

检查版本：

```bash
openclaw --version
```

### Hermes

只检查更新：

```bash
hermes update --check
```

执行更新：

```bash
hermes update
```

### LM Studio

通过 LM Studio App 内置更新功能更新。

模型文件和应用程序是分开的。更新 LM Studio 通常不会删除已经下载的模型，但重要配置仍建议备份。

## 十四、最简操作速查

### 启动

```bash
lms server start
lms load qwen3.6-35b-a3b --context-length 65536 --gpu max
open -a OpenClaw
```

使用 Hermes：

```bash
hermes
```

### 检查

```bash
lms server status
lms ps
openclaw gateway status
openclaw models status
hermes status
```

### 停止并释放内存

```bash
openclaw gateway stop
lms unload --all
lms server stop
```

## 十五、官方资料

- [LM Studio API](https://lmstudio.ai/docs/developer/rest)
- [Hermes LM Studio 配置](https://hermes-agent.nousresearch.com/docs/integrations/providers)
- [Hermes 命令参考](https://hermes-agent.nousresearch.com/docs/reference/cli-commands)
- [Hermes 记忆](https://hermes-agent.nousresearch.com/docs/user-guide/features/memory/)
- [Hermes API Server](https://hermes-agent.nousresearch.com/docs/user-guide/features/api-server/)
- [OpenClaw macOS App](https://docs.openclaw.ai/platforms/macos)
- [OpenClaw LM Studio](https://docs.openclaw.ai/providers/lmstudio)
- [OpenClaw 记忆配置](https://docs.openclaw.ai/reference/memory-config)
- [OpenClaw Gateway](https://docs.openclaw.ai/cli/gateway)
