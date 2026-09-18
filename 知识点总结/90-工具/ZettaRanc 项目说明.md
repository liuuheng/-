---
aliases:
  - ZettaRanc
  - 万千股票分析项目
tags:
  - 工具
  - 股票
  - Tushare
  - Codex
status: active
---

# ZettaRanc 项目说明

## 一、项目是什么

ZettaRanc（万千）是一个面向 A 股研究的 Python 项目，主要提供：

- 股票技术指标分析
- 战法和交易信号识别
- 股票综合评分
- 条件选股
- 自选股监控
- 持仓诊断
- 策略回测
- 端到端交易模拟
- 可选的飞书预警和大模型回答生成

项目原本主要面向 Claude Code、Cursor 等宿主设计，但核心能力由 Python CLI 提供，不依赖 Claude，因此也可以由 Codex 或普通终端调用。

## 二、本地项目位置

项目目录：

`/Users/nanan/Documents/ChatGPT/z哥/zettaranc-skill`

Python 虚拟环境：

`/Users/nanan/Documents/ChatGPT/z哥/zettaranc-skill/.venv`

环境变量：

`/Users/nanan/Documents/ChatGPT/z哥/zettaranc-skill/.env`

本地数据库：

`/Users/nanan/Documents/ChatGPT/z哥/zettaranc-skill/data/stock_data.db`

## 三、核心组件

### Python CLI

项目安装后提供 `zt` 命令，是目前最直接、稳定的使用入口。

主要子命令包括：

- `zt analyze`：股票分析
- `zt score`：股票评分
- `zt diagnose`：持仓诊断
- `zt screen`：条件选股
- `zt watchlist`：观察池管理
- `zt backtest`：策略回测
- `zt simulate`：端到端模拟
- `zt sync`：行情数据同步

### SQLite 数据库

项目把行情、指标和股票基础信息保存在：

`data/stock_data.db`

分析和回测时会优先使用已经同步到本地的数据。

### Tushare

当前项目使用 Tushare 获取股票基础信息和行情数据。

当前接口地址：

```text
https://api.waditu.com/dataapi
```

`TUSHARE_TOKEN` 属于敏感信息，不能写入笔记、截图或 Git 仓库。

### 免费数据源

项目还支持 `a-stock-data` 等免费数据源。

当 Tushare 的积分、频率或复权因子接口受限时，可以用免费数据源补充单只股票的 K 线。

## 四、是否需要大模型

大模型不是必需项。

不配置大模型，也可以正常使用：

- 行情同步
- 技术指标计算
- 股票评分
- 战法识别
- 条件选股
- 策略回测
- 端到端模拟

只有需要自然语言回答生成时，才需要配置：

```dotenv
LLM_API_KEY=自己的密钥
LLM_BASE_URL=https://兼容OpenAI接口的地址/v1
LLM_MODEL=模型名称
```

项目也预留了 Anthropic Claude API 配置，但不是运行核心功能的必要条件。

## 五、是否需要飞书

飞书不是必需项。

飞书 Webhook 只用于：

- 观察池自动扫描
- 条件触发后的消息提醒
- 主动监控结果推送

未配置飞书时，将下面的配置保留为空即可：

```dotenv
IM_PUSH_WEBHOOK=
```

## 六、是否需要 Rust

Rust 扩展属于可选的性能优化组件，不是项目运行的必要条件。

当前环境使用 Python 回测实现：

```dotenv
ZETTARANC_BACKTEST_IMPL=python
```

因此目前不需要额外安装 Rust 工具链。

## 七、当前初始化状态

截至 2026-08-22：

- Python 3.12 虚拟环境已创建。
- Python 项目依赖已安装。
- Tushare Token 已验证可用。
- Tushare 接口地址已验证可用。
- SQLite 数据库已初始化。
- 股票基础信息已同步，共约 5549 条。
- `600519.SH` 已同步 154 条日 K 线。
- `600519.SH` 已生成 120 条指标缓存。
- 项目质量检查通过 12/12。
- Rust 扩展未安装，当前使用 Python 实现。
- Web 看板尚未初始化。
- 飞书推送尚未配置。
- Codex Skill 尚未链接到工作区 Skill 目录。

## 八、Tushare 使用限制

当前 Tushare 账号调用 `adj_factor` 等接口时可能受到积分或访问频率限制。

这可能影响：

- 前复权和后复权行情
- 大批量股票同步
- 全市场选股
- 长周期批量回测

推荐策略：

1. 先同步股票基础信息。
2. 只同步准备分析或观察的股票。
3. 使用观察池管理重点股票。
4. 不要频繁进行全市场同步。
5. Tushare 受限时，用免费数据源补充 K 线。

## 九、与 Codex 的适配情况

项目的 `SKILL.md` 结构可以被 Codex 理解，但它目前位于项目根目录。

Codex 工作区通常从下面的位置发现 Skill：

```text
/Users/nanan/Documents/ChatGPT/z哥/.agents/skills/
```

如果希望 Codex 自动把 ZettaRanc 作为工作区 Skill 加载，需要把项目链接或安装到类似位置：

```text
/Users/nanan/Documents/ChatGPT/z哥/.agents/skills/zettaranc-perspective
```

即使不安装为 Skill，Codex 仍然可以直接调用项目的 `zt` 命令。

因此适配结论是：

- Python CLI：可以直接适配。
- `SKILL.md`：格式基本兼容。
- 自动发现：尚未完成，需要建立 Skill 链接。
- Claude API：不是必需项。
- Codex 调用项目：没有核心障碍。

## 十、尚未完成的可选事项

后续可以按需要处理：

- 将项目正式接入 Codex Skill 目录。
- 初始化并启动 Web 看板。
- 配置飞书机器人 Webhook。
- 配置可选的大模型接口。
- 根据 Tushare 权限完善数据同步策略。
- 为观察池建立每日自动同步和扫描任务。

## 十一、风险说明

ZettaRanc 是股票研究和策略验证工具，不是自动获利系统。

使用时应注意：

- 历史回测不代表未来收益。
- 技术指标不能单独作为买卖依据。
- 免费数据源与 Tushare 可能存在复权口径差异。
- 数据缺失会影响指标、评分和战法识别。
- 实盘决策前应核对行情日期、停牌状态及数据完整性。
- Token、API Key 和 Webhook 不应写入知识库。
