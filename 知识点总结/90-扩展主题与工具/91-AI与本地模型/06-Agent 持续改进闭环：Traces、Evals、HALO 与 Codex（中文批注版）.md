---
title: "Agent 持续改进闭环：Traces、Evals、HALO 与 Codex（中文批注版）"
aliases: ["Agent 持续改进闭环"]
author: "Wesley Pasfield (OpenAI)"
source_url: "https://developers.openai.com/cookbook/examples/agents_sdk/agent_improvement_loop"
published_at: "2026-05-12"
organized_at: "2026-11-11"
type: "文章批注"
language: "zh-CN"
tags: [Agent, 评测, Agents-SDK, Promptfoo, HALO, Codex, 回归测试]
---

# Agent 持续改进闭环：Traces、Evals、HALO 与 Codex（中文批注版）

来源：[OpenAI Cookbook 原文](https://developers.openai.com/cookbook/examples/agents_sdk/agent_improvement_loop)，Wesley Pasfield，2026-05-12。整理日期：2026-11-11。代码和图示根据 MIT 许可的官方示例翻译整理，版权与许可文字见附件。

整理说明：正文、图中文字、代码注释、提示词、示例材料和运行结果均用中文整理，不附英文原文或英文输出。保留 API、变量、字段、文件名、模型名与协议常量等必要标识符。完整技术实现放在中文代码附录；其中提示词、问题库和示例文本已经翻译，中文路由与报告标题同步调整。这是学习用的中文改写，未执行，不能据原文结果宣称中文版已通过评测。“批注”为补充解释，“核对说明”标记原文问题。

[[知识点总结/90-扩展主题与工具/91-AI与本地模型/00-AI学习导航|返回 AI 学习导航]] · [[知识点总结/90-扩展主题与工具/91-AI与本地模型/05-Agent 评测白皮书（一）：Agent 评测全览（文章批注版）|前置阅读：Agent 评测全览]] · [[知识点总结/99-images/openai-agent-improvement-loop-20260512/来源与覆盖核对|来源与覆盖核对]]

Agent 改进闭环把一次运行的经验保存成下一次可以执行的约束：运行轨迹记录发生了什么，人工与模型反馈说明哪些行为需要关注，评测固定这些要求，HALO 根据证据排列修改建议，再由 Codex 或开发者修改系统并重新验证。

这里的 `harness` 指模型外部的执行约束与实现，包括系统提示词、工具、任务路由、输出要求和校验逻辑。后文统一称为“执行框架”。它不等同于 Agents SDK 这个软件包，也不只是一份提示词。改进可以落在 Python 工具、计算方法、产物结构或流程控制上。

贯穿案例是一名财务尽调 Agent：它只读一家虚构公司的资料，回答收购尽调问题，并留下其他人可以复核的证据与文件。案例数字只用于理解评测机制。

## 将得到什么

![[知识点总结/99-images/openai-agent-improvement-loop-20260512/images/agent-improvement-loop-flywheel.svg]]

图示说明：SDK 运行轨迹记录实际行为；人工与 LLM 反馈解释轨迹；Promptfoo 生成并验证评测；HALO 诊断并排列修改；Codex 交接将证据转成代码任务；执行框架更新经过 PR 与验证，进入下一轮。图中“自动化与心跳”表示检查是否出现新交接文件并触发后续任务。

原文构建六项产物：用 Agents SDK 编写的财务分析 Agent、五次带轨迹的运行、人工与模型反馈、自动生成的 Promptfoo 评测集、对当前行为的评测门禁，以及 HALO 诊断后交给 Codex 的实现说明。Agent 同时读取财务导出、客户数据、合同、安全说明、董事会材料和管理层叙述。

贯穿后续工作的文件是 `ARTIFACT_DIR/codex_handoff.md`。它应包含完整诊断、排序后的建议、证据与实现指导。开发者可以先采用“系统提出修改、人工审查差异后合并”的方式；可信的评测门禁建立后，也可以把交接接入自动创建 PR、合并和部署的流程。

批注：本篇 Notebook 的实际执行终点是生成交接文件。末尾介绍的自动修改、合并、部署和周期触发是可扩展架构，不是已经由这个 Notebook 实现并验证的后半程。

## 运行前提与版本

原文在 Cookbook 仓库根目录运行 Notebook，安装 `openai`、`openai-agents`、`halo-engine`，并要求已安装带 `npx` 的 Node.js。API 凭据通过 `OPENAI_API_KEY` 环境变量提供。原文命令适用于 Bash：

```bash
python -m venv .venv
source .venv/bin/activate
pip install openai openai-agents halo-engine
```

原文另用 `export OPENAI_API_KEY=...` 设置环境变量。Windows PowerShell 的虚拟环境激活方式是 `.\.venv\Scripts\Activate.ps1`，环境变量写法是 `$env:OPENAI_API_KEY='你的密钥'`；这些只是环境语法差异，不能据此认定下面的 Unix 本地执行环境可直接在 Windows 使用。

Notebook 中的安装单元使用 `%%capture` 和 `%pip`，属于 IPython/Jupyter 语法，不能直接复制到普通 `.py` 文件运行。所有分析、反馈、生成评测、评分和 HALO 诊断都使用实时模型调用。原文估计默认五条轨迹约需二十分钟，实际耗时受模型和网络影响；这不是本次整理的实测。

五个模型角色集中配置：`AGENT_MODEL` 执行尽调，`ANALYSIS_MODEL` 生成反馈，`EVAL_GENERATION_MODEL` 生成评测，`JUDGE_MODEL` 评分，`HALO_MODEL` 诊断。原文默认使用 `gpt-5.5`，后面三个角色默认继承分析模型；可分别通过 `OPENAI_AGENT_MODEL`、`OPENAI_ANALYSIS_MODEL`、`OPENAI_EVAL_GENERATION_MODEL`、`OPENAI_JUDGE_MODEL`、`OPENAI_HALO_MODEL` 覆盖。Promptfoo 默认版本是 `0.121.9`，由 `PROMPTFOO_VERSION` 覆盖。这里保留原文版本，不将其宣称为当前推荐版本。

初始化通过上级目录中的 `registry.yaml` 查找项目根目录；找不到就使用当前目录。`ARTIFACT_DIR` 指向 `examples/agents_sdk/agent_improvement_loop_artifacts`，`TRACE_DIR` 保存逐次产物，`HALO_TRACE_PATH` 指向 `halo_traces/traces.jsonl`。

核对说明：初始化代码会用 `shutil.rmtree(ARTIFACT_DIR)` 清空已有实验产物，再建立新目录。重复运行前应另存上一轮轨迹、评测与交接文件，才能比较版本。这里没有执行该初始化代码。

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果#代码单元 03：安装 示例依赖|中文代码：安装 Notebook 依赖]]

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果#代码单元 04：初始化模型与产物目录|中文代码：初始化模型与产物目录]]

## 第一步：构造带有冲突证据的公司资料

示例公司 `FictionalCorp XYZ` 销售收入智能 SaaS，包括年度订阅、用量附加项和上线阶段承诺。数据有意混合 CSV 结构化导出与 Markdown 叙述：Agent 需要判断证据权重、暴露冲突，并在材料不足时保留未知。

原文的十二份叙述材料如下。表中的事实是案例设定，不是外部公司的事实。

| 文件 | 内容与用途 |
| --- | --- |
| `overview.md` | 管理层称 FY2025 ARR 为 4300 万美元、增长 71%，按其法人客户与 booked ARR 口径称没有客户超过 15%，并称法律问题不影响估值。 |
| `product_strategy.md` | 产品包括 Forecast Assist、Pipeline Quality Monitor、Renewal Risk Workbench；强调企业工作流与用量扩张，引用未获财务确认的 122% NRR。 |
| `go_to_market.md` | 面向 CRO、RevOps 直接销售，计划提高企业合同金额并增加伙伴商机；需要用 `sales/pipeline.csv` 检验转化假设。 |
| `board_deck.md` | 2025 年 12 月董事会材料列出 ARR 4300 万美元、增长 71%、毛利率 69%、月度现金消耗 290 万美元、可支撑 11 个月，并承认包含未上线承诺及用量调整。 |
| `financials/revenue_recognition_notes.md` | 明确财务控制口径为 `arr_bridge.csv`；董事会口径含 280 万美元未上线承诺及 110 万美元非经常性用量调整；RevOps 另有 4080 万美元的 bookings 调整口径。 |
| `legal/contracts_summary.md` | 年度 SaaS 合同含安全与数据处理条款，前五大客户 ARR 2510 万美元；管理层概述尚未与完整条款清单对齐。 |
| `legal/open_issues.md` | DataHarbor 请求 320 万美元损害赔偿及提前支付佣金，律师认为损失可能发生但非很可能；还存在非标准控制权变更通知与无上限保密赔偿条款。 |
| `security/security_overview.md` | SOC 2 Type I 已完成，Type II 现场工作进行中，尚未出具 Type II 报告。 |
| `sales/security_faq.md` | 销售说法为“SOC 2 已完成”，安全团队说明只涉及 Type I，不能据此宣称 Type II 已完成。原文此处出现 Aurora 名称，与主案例名称不一致，保留为来源瑕疵。 |
| `hr/org_chart.md` | CEO、CFO、销售副总裁、产品副总裁、安全负责人；2026 年计划净增 14 名市场与销售人员。 |
| `sales/pipeline_notes.md` | 提到 160 万美元 DataHarbor 来源商机受争议影响；Northstar 扩张依赖 Type II，未计入财务控制 ARR。 |
| `notes/qa_log.md` | 记录未确认 NRR、未提供 CAC 回收期、Northstar 母账户 1240 万美元及约 34% 暴露，并要求保留 ARR 口径冲突。 |

批注：ARR 是年度经常性收入，NRR 是净收入留存率，CAC payback 是客户获取成本回收期。原文要求在这个资料室中优先使用受控财务导出；“CSV 比文字可靠”不是适用于所有系统的格式定律，来源负责人、口径、时间和校验情况仍决定能否采信。

### 数据文件、字段与可复算样例

`WORKSPACE_FILES` 保存所有原始文本，运行时才写出资料室。以下十一份结构化文件与上面的十二份叙述文件共同构成原始业务资料；完整字段与数据在代码附录中保留；叙述文本、备注和提示词已译为中文。

| 文件 | 字段与原文数据 |
| --- | --- |
| `financials/arr_bridge.csv` | `metric,value_m`；期初 21.58、新增 8.1、扩张 3.2、收缩 1.1、流失 2.7、期末 36.9、bookings 调整口径 40.8，单位为百万美元。 |
| `financials/monthly_kpis.csv` | `month,ending_arr_m,new_arr_m,expansion_arr_m,churn_arr_m,gross_margin`；2025 年 1、2、3、4、5、6、7、9、10、12 月共十行；期末 ARR 从 21.58 增至 36.90，毛利率均为 0.69。缺少 8、11 月。 |
| `financials/p_and_l.csv` | `period,revenue_m,gross_margin,opex_m,cash_burn_m,runway_months`；FY2025 分别为 30.26、0.69、47.71、2.9、11。按董事会与样例解释，2.9 是月度现金消耗。 |
| `financials/retention_extract.csv` | `metric,value,status,notes`；NRR 122% 为 `management_estimate_unvalidated`；收入毛留存率 84%、客户数量留存率 91% 均为 `finance_partial`；CAC 回收期为空且 `not_provided`。 |
| `customers/top_customers.csv` | `customer,parent_account,arr_m,arr_share,segment,renewal_date,inclusion_basis`；五客户 ARR 为 7.8、4.6、6.9、3.6、2.2；控制口径一致，续约日分别为 2026-02-15、04-01、05-15、06-30、02-28。 |
| `customers/account_hierarchy.csv` | `legal_entity,parent_account,parent_arr_m,note`；Northstar 两个法人映射同一母账户，每行均重复展示母账户 ARR 12.4；Helio 为 6.9，BluePeak 为 3.6。 |
| `customers/renewal_calendar.csv` | `customer,renewal_date,renewal_risk,notes`；Northstar 两客户及 Helio 风险为中，BluePeak 为高；原因含 Type II、共同采购委员会、采用不足与 CRM 同步错误。 |
| `customers/customer_health.csv` | `customer,health,primary_risk,signal_date,caveat`；Northstar Bank 绿、Capital Markets 黄，均为 2025-10-31 的法人级记录；Helio 黄、BluePeak 红、Summit 黄，为 2025-12-15。 |
| `legal/clause_inventory.csv` | `customer,issue,exposure,confidence`；Northstar 控制权变更后十日内可能要求过渡计划，Helio 存在无上限保密赔偿，BluePeak 连续两月 SLA 未达标时服务抵扣可能超一个月费用。 |
| `sales/pipeline.csv` | `stage,pipeline_m,historical_close_rate,quality_note`；commit 为 6.1 与 0.39，best_case 为 9.7 与 0.28，early 为 18.2 与 0.08；分别含安全依赖、争议渠道、低转化率提示。 |
| `support/escalations.csv` | `customer,severity,issue,status`；Northstar Capital Markets 的预测延迟为中等且监控中，BluePeak CRM 同步为高且未关闭，Northstar Bank 安全问卷因等待 Type II 报告而阻塞。 |

沿客户集中度问题走一遍：`top_customers.csv` 中 Northstar Bank 为 780 万美元，Northstar Capital Markets 为 460 万美元；`account_hierarchy.csv` 说明它们受同一母账户 Northstar Holdings 控制，因此合并暴露为 1240 万美元。用财务控制的 3690 万美元作分母，`12.4 / 36.9 ≈ 33.6%`。输出应点明母账户依赖，不能只说两个法人分别约 21.1% 和 12.5%。

批注：层级文件的 `parent_arr_m=12.4` 在两个法人行重复出现，不能直接求和成 24.8。应汇总法人 `arr_m` 或对母账户指标去重，并检查两种结果是否一致。客户健康为绿色也不能替代母账户集中度计算，且两个 Northstar 健康记录早于其他客户。

核对说明：原始合成数据有多处无法闭合。ARR 桥按常规加减为 `21.58+8.1+3.2-1.1-2.7=29.08`，不等于文件列出的 36.9；董事会 43.0 与财务 36.9 相差 6.1，已说明的 2.8 与 1.1 合计仅 3.9，仍有 2.2 未解释。问答日志称“前两大客户共 12.4”，但按 ARR 排序实际前两名是 7.8 与 6.9，合计 14.7；12.4 对应的是 Northstar 两个法人。正文保留文件值及其权威口径设定，同时标明这些算术与文字冲突，不能伪造对账过程。

核对说明：叙述文件将 160 万美元 DataHarbor 来源商机放在 commit 阶段，结构化 pipeline 的备注却将其关联到 best_case。此处也只能保留冲突并请求明细，不能静默统一。

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果#代码单元 07：定义全部公司源文件|中文代码：定义全部公司源文件]]

### 将资料写入工作目录

`generate_acquisition_diligence_workspace()` 为每个相对路径创建父目录，将去缩进后的字符串写入 `ARTIFACT_DIR/synthetic_dataroom/`，再写 `manifest.json`，记录名称、用途及文件列表。Notebook 会检查生成结果；后续运行时，副本才挂载为 Agent 可见的 `data/`。

批注：`manifest.json` 帮助确认资料室包含哪些文件，后面用于描述“未提供哪些材料”。文件清单能说明当前可用文件范围，不能单独证明所有文件正文都不存在某个事实；对“未提供现金余额”等判断，还需要检查相关内容。

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果#代码单元 09：写出资料与 manifest|中文代码：写出资料与 manifest]]

## 第二步：定义 Agent 执行框架

Agent 面向投资团队，只使用挂载资料室中的信息回答问题。材料有一致、冲突和仅部分支持的内容，因此框架必须规定证据选择、未知处理与可复核产物。Agents SDK 提供运行器、模型设置、隔离执行环境和轨迹钩子；具体业务约束由本例的提示词与代码实现。

| 产物 | 用途 |
| --- | --- |
| `summary_answer.md` | 给用户的简短答案。 |
| `investment_memo.md` | 供尽调人员复核的完整备忘录。 |
| `risk_register.json` | 可由其他程序读取的风险、严重程度与证据。 |
| `open_questions.md` | 缺失材料和未解决的问题。 |
| `citations.json` | 结论到来源文件的机器可读关联。 |
| `evidence_table.csv` | 按行展示结论和来源的审计表。 |

需要防止的行为包括：把管理层叙述当成正式指标；把未经确认的 NRR 当成财务核准值；忽略母账户集中度；将只有 Type I 的状态说成全部 SOC 2 已完成；答案写得完整但引用、风险文件或证据产物缺失。

### 用结构化配置表达约束

`ModelSettings` 保存 `agent_model` 和 `reasoning_effort`。`AgentConfig` 保存 `version`、`system_prompt`、模型设置、`tool_policy`、`eval_metadata` 和配置路径；`required_artifacts` 从工具策略读取六份必需产物。

`build_instructions()` 将系统提示词、格式化工具策略和运行版本拼成最终指令；要求当前运行使用已选定配置，运行中不能修改。`format_policy_value()` 递归展开字典与列表，避免策略只停留在 Python 对象里而没有进入模型上下文。

批注：将配置显式序列化，HALO 才能判断某项要求是否已经存在。否则它可能把一次执行没有遵守的规则，误判为提示词从未写过，再提出重复追加同一规则的建议。

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果#代码单元 12：定义配置结构|中文代码：定义配置结构]]

### 指令、工具策略与版本元数据

系统提示词规定只使用 `data/` 下的材料，不补外部知识；冲突时优先结构化导出；每条实质结论引用准确的资料室相对路径。缺证据就报告未知，缺数字就不编造，存在矛盾就显式说明。

工具约束要求在提交答案前运行 `data/tools/check_evidence_coverage.py`，输入含 `claim`、`claim_type`、`citations` 的结论列表，生成 `outputs/evidence_coverage.json`；写完产物后运行 `data/tools/validate_output_contract.py`，生成 `outputs/output_contract_validation.json`。出现缺引用、缺文件、格式错误或空产物时，模型应先修订。

`TOOL_POLICY` 设定可读根为 `data`、输出根为 `outputs`，列出六份产物、两个运行时工具，以及引用、未知和写入规则：不从相邻指标推断缺失值，分开事实、推断与待确认问题，不修改资料室输入或运行配置。执行命令使用工作区相对路径，省略 `workdir`，不传入宿主临时目录绝对路径。

元数据中版本为 `v001`、状态为 `promoted`，创建来源是人工基线，晋升门禁为人工审查；推理强度为 `medium`。这是示例配置的声明，不表示它在本次整理时被部署或重新审查。

批注：提示词写了“只能写 outputs”，并不自动等同于文件系统级只读隔离。源码为 SDK 准备了可写数据副本，保护原始资料室靠复制；对运行时副本的写入限制仍需结合实际执行环境与权限判断。

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果#代码单元 14：配置提示词与策略|中文代码：配置提示词与策略]]

### 检查生效配置

原文显示的配置摘要为：版本 `v001`，模型 `gpt-5.5`，推理强度 `medium`；六份必需产物与前表一致；两个工具分别负责引用路径检查和输出契约检查。摘要用于在开始采样前确认真正传入 Agent 的版本与工具，完整打印代码见附录。

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果#代码单元 16：显示生效配置|中文代码：显示生效配置]]

### 校验器实际检查了什么

`check_evidence_coverage()` 逐条读取结论，将 `data/` 前缀移除后查找文件。没有引用时加入 `missing_citations`，引用文件不存在时加入 `unsupported_claims`，所有引用文件存在时加入 `supported_claims`。两个问题列表都为空才返回 `passed=True`。

核对说明：这里的 `supported_claims` 名称比实际能力强。实现没有读取来源正文，也没有验证算术、口径或文本蕴含关系。给“Type II 已完成”引用真实存在但写着“尚未完成”的文件，仍可能通过这个检查。应把它理解成“引用路径齐全”，不能理解成“事实已获支持”。

`validate_output_contract()` 检查六份文件存在且字节数不为零；检查风险与引用 JSON 顶层为列表；风险条目有 `evidence`、引用条目有 `sources`，且相应文件存在；CSV 有行时检查列包含 `claim_id`、`claim`、`sources`。工具代码由 `write_runtime_tools()` 写入挂载副本的 `data/tools/`。

核对说明：源码仍会允许空 JSON 列表通过，也可能允许只有表头、没有数据行的 CSV 通过；它没有完整的风险字段类型约束，也没有把 Markdown 内全部结论与证据逐项对照。两个脚本把 `passed=False` 写进结果，未主动以非零退出码阻断。因此本例主要依赖 Agent 读取报告并修复，尚未形成宿主侧强制拒绝提交的硬门禁。

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果#代码单元 18：生成两个本地校验工具|中文代码：生成两个本地校验工具]]

### 根据问题补充当前轮提示词

`build_user_prompt()` 加入原始问题、当前配置、六份产物要求及路径规则；仅在命中相应问法时追加任务约束：提到 memo、IC-style 或 investment committee 时，最终回复需内联备忘录内容；同时包含 financial、legal、customer 和 risk 时，财务、法律与客户集中度风险分节写；提到 NRR 或 net revenue retention 时，没有直接支持就报告未知，除非用户明确要求估算，否则不从 ARR 桥推算 NRR。

批注：原文路由使用英文关键词匹配，直接换成中文问题会漏触发分支。本次中文代码附录已为备忘录、风险分类和净收入留存率补充中文关键词，并同步翻译问题库与反馈键；用于实际产品时仍需验证覆盖范围和字面断言。

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果#代码单元 20：构造当前问题提示词|中文代码：构造当前问题提示词]]

### 导出轨迹供 HALO 分析

本地导出器把 SDK 事件转换成类似 OpenTelemetry 的 JSONL，每一行保存一个 span。Trace 是一次问题运行的完整轨迹，span 是其中的一段活动，例如模型调用、工具执行或产物读取。一个 Trace 通常包含多个 span，JSONL 行数不能当成 Agent 运行次数。

#### 配置导出器

`HaloExportContext` 保存项目、服务名称、版本、环境和附加资源属性。`setup_halo_tracing()` 使用 `set_trace_processors([processor])`，将处理器设置为本地 `HaloJsonlTraceProcessor`；本例不依赖托管轨迹接收服务。

`on_trace_start()` 记录工作流、组和元数据；`on_span_end()` 转换一个完成的 span，用锁保护追加写入；`force_flush()` 刷新文件，`shutdown()` 刷新并关闭。`on_trace_end()` 移除对应元数据。

批注：设置处理器会替换该 Python 进程的处理器列表，不是只给一个局部对象附加输出。如果与其他应用共用进程，需要考虑原有轨迹处理器是否被替换。

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果#代码单元 23：配置本地轨迹导出器|中文代码：配置本地轨迹导出器]]

#### 将 SDK span 映射成 HALO 字段

顶层字段包括 `trace_id`、`span_id`、`parent_span_id`、名称、类型、开始与结束时间、状态、资源和属性。导出时分别去掉 SDK 的 `trace_`、`span_` 前缀；父子 ID 仍用于恢复调用层级。资源带 `service.name` 和版本，属性带 `inference.project_id`、`inference.export.schema_version`、工作流名与配置元数据。

映射中，agent 对应 `AGENT`；generation、response 对应 `LLM`；function、mcp_tools 对应 `TOOL`；handoff 对应 `CHAIN`；guardrail 对应 `GUARDRAIL`。其他自定义活动保留类型与原始数据。LLM span 尽可能保存模型、调用参数、输入输出消息、输入输出 token；工具 span 保存工具名与输入输出，guardrail 保存是否触发。

核对说明：字段是否实际有值取决于 SDK 的 span 导出。比如 `_response_attrs()` 只提取响应 ID 和用量，没有补入完整响应正文或模型名；`inference.llm.cost.total` 虽有字段位置，但这里的投影函数没有计算费用。不能因为 schema 出现字段，就断言本例已经完整记录模型成本与所有内容。

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果#代码单元 25：映射所有 span 字段|中文代码：映射所有 span 字段]]

#### 统一 ID、时间与序列化

辅助函数去除 ID 前缀，将时间归一化，给 span 命名与分类，把消息展开为可检索属性，处理 JSON 可序列化值、整数转换和空值过滤，并写入 SDK 版本。这样反馈、评测和运行证据才能使用相同 ID 与数据类型关联。

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果#代码单元 27：归一化轨迹辅助字段|中文代码：归一化轨迹辅助字段]]

### 执行一次 Agent 调用

`run_sdk_agent()` 先把原始资料复制到临时挂载目录，写入运行工具与 manifest；再创建 `SandboxAgent`，将副本挂载为 `data`，创建输出目录 `outputs`，设置模型、推理强度和拼好的指令。随后创建 `UnixLocalSandboxClient` 会话，在 trace 上记录工作区与配置，调用 `Runner.run()`，最多运行 30 轮。

调用结束后，代码逐个从会话读取六份要求的产物，复制到该问题的本地产物目录，并返回 `result.final_output`。清理逻辑尝试删除会话、结束 trace、刷新并关闭导出器。原始数据靠 `staged_dataset_mount()` 的临时副本机制保护。

核对说明：这段代码使用 `agents.sandbox` 和 Unix 本地执行实现。本文只根据官方 Notebook 解释，并未在当前机器验证 SDK 安装版本、操作系统兼容性或实际执行。产物读取失败被 `except Exception: continue` 忽略，因而“Runner 返回答案”不保证本地六份文件全部保存成功；将其用于稳定闭环时应检查复制结果。

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果#代码单元 29：运行 Agent 并取回产物|中文代码：运行 Agent 并取回产物]]

## 第三步：生成五次带轨迹的运行

原文的问题库有十个问题：现金支撑期与消耗对应什么融资风险；收入质量与 ARR 采用口径；母账户集中度；收购方应优先调查什么法律暴露；企业安全审查准备情况；董事会、财务与管理层叙述有哪些冲突；哪些不受支持的指标应拒绝推断；投委会前应问管理层什么；前三大尽调风险如何排序；哪些说法仅有方向性价值、还缺强证据。

默认通过 `DEFAULT_TRACE_INDICES=[0,1,2,4,6]` 选择其中五个，按顺序 `await` 执行。使用异步 API 不等于五个问题并发运行，源码是在循环中等上一问结束后再跑下一问。

`TraceRecord` 保存 `trace_id`、`sdk_trace_id`、`trace_label`、`question`、`answer`、`output_dir`、`mode`。SDK ID 为 `trace_` 加标签 SHA256 前 32 个十六进制字符；导出 ID 去掉前缀；标签如 `trace-01` 仅供阅读。反馈与评测都必须使用导出的 `trace_id`，不能混用 SDK ID 或标签。

| 默认轨迹 | 问题与原文答案摘要 |
| --- | --- |
| `trace-01` | 月度消耗 290 万美元、现金可支撑 11 个月，对应较高的近期融资风险。 |
| `trace-02` | 收入质量并非没有疑点；财务控制 ARR 为 3690 万美元，要保留不同 ARR 定义及留存、续约方面的限制。 |
| `trace-03` | Northstar 两法人合为母账户 1240 万美元，占控制 ARR 约 33.6%，集中度风险较高。 |
| `trace-04` | 已完成 Type I，但 Type II 仍在进行，没有 Type II 报告，企业安全审查仍有阻力。 |
| `trace-05` | CAC 回收期未提供，官方 NRR 缺乏支持，不能把 122% 管理层估计提升成正式指标。 |

原文保存的采样耗时为 7 分 59 秒。法律暴露等其余五题没有进入默认采样；它们存在于问题库，不等于已经被本轮五个评测覆盖。

批注：标签固定时生成的 ID 也固定。本例每次初始化清空产物，因此没有保留多轮历史；若改成跨轮追加日志，应在标签中加入 run ID 或版本，防止多轮数据误归到同一 Trace。

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果#代码单元 31：生成五条真实运行轨迹|中文代码：生成五条真实运行轨迹]]

### 检查第一次运行的六份产物

原文 `show_trace_artifacts(traces[0])` 将首轮全部产物打印出来。下面用中文保留其结论、字段、证据和未决问题；不重复收录英文输出。

`summary_answer.md` 给出近期融资风险较高的判断，引用 `financials/p_and_l.csv` 与 `board_deck.md` 中每月 290 万美元消耗、11 个月支撑期；解释在不足一年内，需要减耗、改善现金回收或取得资金。它还指出财务 ARR 为 3690 万美元、董事会为 4300 万美元，并承认资料未给现金余额、债务清单、未用额度、契约条款和融资计划。

`investment_memo.md` 分结论、证据、解读和尽调意见。证据列出 FY2025 收入 3026 万美元、毛利率 69%、运营支出 4771 万美元、月度消耗 290 万美元、11 个月支撑期、财务控制 ARR 3690 万美元，以及董事会的 4300 万美元和 71% 增长。解读要求融资论证以财务控制口径为基础；未获支持的现金余额、额度、契约余量和融资时间保持未知。尽调意见把融资风险列为高，要求管理层给出延长支撑期的计划。

`risk_register.json` 包含三条风险：`R-001`“支撑期不足十二个月”，严重程度高，引用 P&L 与董事会，追问不受限现金及跌破六个月前的融资动作；`R-002`“ARR 质量削弱融资叙事”，严重程度中，引用 ARR 桥、收入确认说明和董事会，追问融资采用哪种 ARR 及未上线承诺后来落地多少；`R-003`“缺乏流动性结构证据”，严重程度中，引用 P&L 和 manifest，追问未动用循环额度、风险债务、契约与最低现金要求。字段为 `id,risk,severity,rationale,evidence,open_questions`。

`open_questions.md` 保留五组问题：不受限现金如何对应 11 个月支撑期；是否有债务、循环额度、契约及最低现金约束；融资时间、规模和市场不可用时的预案；哪些减耗措施各能增加多少支撑月份；融资谈判究竟使用 3690 万还是 4300 万美元 ARR。

`citations.json` 按 `claim_id,claim,sources` 保存六条结论：`C-001` 为 P&L 中的消耗与支撑期；`C-002` 为董事会确认同样两值；`C-003` 为财务 ARR；`C-004` 为董事会 ARR 与增长率；`C-005` 为未上线承诺与非经常性用量调整；`C-006` 为资料室缺少现金、债务、额度、契约及融资计划。`evidence_table.csv` 将这六条映射成相同三列，多个来源用分号连接。

核对说明：首轮原始答案把 4300 万与 3690 万的差异叙述为由 280 万和 110 万构成，但这些数字不能完全对账。该输出的中文含义在本节保留；阅读时应应用第一步的算术核对，指出未解释的 220 万美元。原文评测全过，仍不能证明这个细节正确。

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果#代码单元 33：展示首轮六份产物|中文代码：展示首轮六份产物]]

## 第四步：生成模拟人工反馈与模型反馈

原文用 `generate_mock_human_feedback()` 模拟专家对五条轨迹的审阅。真实应用中，反馈可以来自财务负责人或熟悉案件的专家；本例中反馈是按问题预先写好的规则，不是五名真人在本次运行后产生的评语，也不是全部由模型生成。

每条人工反馈含 `feedback_id,trace_id,trace_label,question,source_type,theme,summary,required_observations,prohibited_claims`。要求与禁止项分别约束必须说清什么、不能误导成什么。

| 主题 | 必须观察到 | 禁止出现 |
| --- | --- | --- |
| 融资风险 | 明确 11 个月支撑期，并把现金消耗与近期融资压力连接起来。 | 暗示可支撑超过十二个月。 |
| 收入质量 | 以控制 ARR 为准，保留董事会与财务口径冲突。 | 静默抹平 ARR 差异。 |
| 客户集中度 | 汇总到 Northstar Holdings，使用 `account_hierarchy.csv`。 | 只分析法人实体。 |
| 安全准备 | 分清 Type I 完成与 Type II 进行中，将销售 FAQ 视为较弱证据。 | 笼统称 SOC 2 全部完成。 |
| 无支持指标 | 标明官方 NRR 与 CAC 回收期缺乏支持。 | 将管理层 NRR 估计当正式指标。 |

首条模拟评语的意思是：应同时讲明 11 个月支撑期与现金消耗带来的融资风险，不能只写一句泛泛的“风险提示”。

`generate_llm_feedback()` 则调用 `ANALYSIS_MODEL`，给它由 `TraceRecord` 转成的 JSON，要求返回 `insight_id,trace_id,question,source_type,observations`，其中 `source_type=llm_insight`。模型需要提取适合以后生成评测的行为观察，不复述全部答案，也不编造材料。代码检查列表类型，并核对每条 ID 确实能对应现有轨迹。

原文首条模型反馈指出：答案识别了不足十二个月的融资窗口；优先财务控制 ARR；指出缺少现金、债务、契约和融资计划；对数字加引用并关注产物与校验。原文各生成五条人工与模型反馈，耗时 13 秒。

核对说明：人工模板使用了“现金消耗上升”的说法，但原始 P&L 与董事会只给 2.9 的月度消耗值，没有历史消耗序列。应保留“消耗较高或构成融资压力”这一有据判断，将“上升”标为待补证，而非从模板反向当成已知事实。这里的 LLM 反馈输入主要是问答和路径记录，完整工具 span 是后面单独交给 HALO 的证据，不能把该反馈函数描述成逐 span 审计。

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果#代码单元 35：生成模拟人工与模型反馈|中文代码：生成模拟人工与模型反馈]]

## 第五步：将反馈转换成 Promptfoo 评测

`generate_feedback_derived_evals()` 把轨迹记录、人工反馈与模型反馈交给 `EVAL_GENERATION_MODEL`。默认五条轨迹时，要求生成五至七项长期可复用的评测，原文保存的这次输出为五项。评测应固定应有行为，而不是简单复述一条历史答案。

每项定义包含 `eval_id,title,scoring_method,expected_behavior,source_trace_id,rubric,deterministic_assertions,suggested_pass_example,suggested_fail_example`。`source_trace_id` 必须匹配导出 ID；`scoring_method` 可标为 `deterministic`、`llm_judge` 或 `hybrid`。字面断言仅允许 `contains`、`icontains`、`not-contains`，没有适合的字面条件就返回空列表。`rubric` 描述语义上的通过与失败标准。

下面保留原文五项生成结果及其正反例的中文含义，五项均标为 `hybrid`。原文生成耗时 52 秒。

| 评测 | 预期行为与通过例 | 失败例 |
| --- | --- | --- |
| 从支撑期与消耗识别融资压力 | 明确只有 11 个月，月度消耗造成不足一年内减耗、改善回款或融资的压力。 | 说有充足资金支撑下一整年，不需要近期融资安排。 |
| 选财务 ARR 并保留冲突 | 收入质量仍有疑点；承保或投资判断采用 3690 万美元，4300 万董事会口径与 4080 万 bookings 口径不等价，不能当作同一经常性 ARR。 | 直接采用 4300 万，并称经正常调整已与财务桥完全对齐。 |
| 按母账户计算集中度 | Northstar 两法人合并到 Holdings 后约 1240 万，占控制 ARR 约三分之一；说明只看法人低估依赖。 | 因单个法人未越阈值就宣布集中度可接受。 |
| 精确区分安全状态 | Type I 完成、Type II 仍在现场工作且没有报告；销售 FAQ 证据弱，Type II 缺失会影响企业采购。 | 依据 FAQ 宣布 SOC 2 完成、企业审查无安全障碍。 |
| 拒绝无证据指标 | CAC 回收期未提供；122% NRR 是未确认估计，不能用作正式留存指标，部分数据也不能冒充完备指标。 | 宣布官方 NRR 为 122%，并根据增长推断 CAC 回收期有吸引力。 |

批注：自动生成只能提出评测草案。领域人员仍需检查口径、代表性、误判和遗漏。此处应修正“现金消耗上升”的证据要求，并增加 ARR 算术闭合测试；不能把有瑕疵的反馈直接固化成长期门禁。

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果#代码单元 37：根据反馈生成评测|中文代码：根据反馈生成评测]]

## 第六步：用 Promptfoo 评分当前答案

本例 Promptfoo 对现有轨迹保存的 `answer` 评分：字面断言检查特定文本，`llm-rubric` 判断是否满足语义要求。评测结果随后作为 HALO 的输入。

批注：这里不会重新调用尽调 Agent。Provider 从本地 `trace_outputs.json` 取出旧答案返回；因此这是一轮“对当前已采样行为的评分”。修改框架后必须重跑候选 Agent，并让相同测试消费新答案，才构成版本回归验证。反复评分同一份旧答案不能证明改动有效。

### 构造 Provider 和测试配置

`call_api()` 从 `options.config.trace_outputs_path` 读取所有轨迹，用 `context.vars.trace_id` 找到一条并返回 `trace['answer']`，同时附带问题与 ID 元数据。`trace_for_eval()` 保证评测关联的轨迹存在。

`promptfoo_test_from_eval()` 过滤不受支持或空值的字面断言，再无条件追加一个 `llm-rubric`，评分 Provider 为 `openai:{JUDGE_MODEL}`，阈值 `0.8`。测试变量包含问题、导出 ID 和标签，元数据保存评测 ID 与评分方式。

核对说明：`scoring_method` 在该实现里是元数据，并不决定是否调用 LLM。即使生成项标为 `deterministic`，当前构造器仍追加 LLM 评分，不能仅凭这个字段估算调用次数或认定该测试完全确定性。

`write_promptfoo_artifacts()` 写出 `trace_output_provider.py`、`trace_outputs.json`、`promptfooconfig.yaml`，评测结果写为 `promptfoo_results.json`。配置虽然后缀为 YAML，写入内容是 JSON；JSON 是 YAML 的可接受子集，避免额外序列化依赖。

`promptfoo_summary()` 兼容两种结果列表结构，提取每行 `eval_id,title,trace_id,trace_label,passed,score,explanation`，并汇总总数、通过数和失败数。

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果#代码单元 40：构造 Promptfoo Provider|中文代码：构造 Promptfoo Provider]]

### 执行门禁并解释结果

原文通过 `npx --yes promptfoo@0.121.9 eval --no-cache --no-table`，传入配置与输出路径运行；为子进程指定当前 Python 解释器，将 Promptfoo 配置目录放到本实验产物内，并关闭 WAL 模式。返回结果包含命令、退出码、结果路径和日志尾部，结果文件未生成时抛错。

原文保存的五项测试全部通过：`total=5,passed=5,failed=0`，每项 `score=1`、说明为全部断言通过；耗时 9 秒。它说明这五份已保存答案满足这次生成的规则，不证明整个资料室的每项计算、全部文件和全部未来问法都正确。

核对说明：运行函数使用 `check=False` 收集子进程结果，后续 HALO 会继续接收失败结果做诊断；这里没有“失败即阻止合并或部署”的实现。真实发布门禁要显式检查进程退出码、预期用例数、错误数与失败数，并绑定候选版本。

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果#代码单元 42：运行 Promptfoo 并汇总|中文代码：运行 Promptfoo 并汇总]]

## 第七步：HALO 汇总证据并生成交接

HALO 的全称为 Hierarchical Agent Loop Optimization，原文将其作为基于轨迹改进执行框架的方法与 Python 包。它在这里读取当前框架、SDK 轨迹、人工与模型反馈、生成的评测和 Promptfoo 评分，提出有优先级的修改建议。

它的任务是把“哪里需要改”变成能交给编码人员的诊断。它不会因为输出了建议就自动修改当前 `AgentConfig`，也没有在本单元训练模型权重。

### 收集七类输入

`build_halo_context()` 将 `traces,human_feedback,llm_feedback,eval_suite,gate_result,agent_config` 放入同一个上下文。配置包含完整系统提示词、模型设置、工具策略与评测元数据。

| 输入信号 | 原文数量 | 存放方式 | 内容 |
| --- | --- | --- | --- |
| 当前执行框架配置 | 1 | 全局配置 span | 系统提示词、模型、工具策略、版本元数据。 |
| SDK 执行轨迹 | 5 | 原始运行轨迹 | Agent 步骤、工具调用与输出。 |
| 模拟人工反馈 | 5 | 附到原 Trace | 评语、必须观察到的行为、禁止结论。 |
| LLM 反馈 | 5 | 附到原 Trace | 模型行为观察。 |
| 生成的评测定义 | 5 | 附到原 Trace | 预期行为、正反例；rubric 保留在评测定义中。 |
| Promptfoo 单项结果 | 5 | 附到原 Trace | 通过与否、原因。 |
| Promptfoo 门禁汇总 | 1 | 全局汇总 span | 总数、通过数、失败数。 |

批注：这里的“七类输入”不是七次业务 Agent 调用。运行轨迹始终只有五次，后续会额外构造配置和汇总两个全局 Trace。

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果#代码单元 45：组合 HALO 上下文|中文代码：组合 HALO 上下文]]

### 将反馈挂回对应 Trace

`write_halo_optimization_context()` 先复制原始 `HALO_TRACE_PATH` 的 span 行，再追加合成 span。人工反馈使用 `HUMAN_FEEDBACK`，模型反馈使用 `LLM_FEEDBACK`，评测定义使用 `EVAL`，评分结果使用 `EVAL_RESULT`。通过 `optimizer.signal_source` 区分信号来源。

针对某个答案的反馈、评测及评分复用其导出 `trace_id`，但获得新的 `span_id`；全局配置和门禁总结果各用独立合成 Trace。各条合成 span 使用当前时间表示上下文构造时间，不能当成原始执行步骤的耗时。输出写入 `halo_optimization_context.jsonl`。

核对说明：追加的评测 span 保存 ID、标题、评分方式、预期行为及正反例，但未完整复制 `rubric` 和 `deterministic_assertions`。这些字段仍在 Python 的 `eval_suite` 与生成的 Promptfoo 配置中。不能把这份 JSONL 描述成包含所有评分配置的无损副本。

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果#代码单元 47：写出 HALO JSONL|中文代码：写出 HALO JSONL]]

### 约束诊断的内容与格式

提示词要求 HALO 将人工反馈作为一类正式证据；提出修改前，先与当前配置比较，区分三种情况：框架缺少要求；已有要求但执行不稳定；实现或可观测性存在缺陷。这个区分决定应该补规则、强化控制流，还是修工具与记录逻辑。

交接文件须按以下七节输出，原文标题受解析函数约束；中文代码附录已将提示词与解析器同步改为以下中文标题：

1. `执行摘要`：当前做得如何、剩余缺口、门禁是否通过。
2. `优先实施的三项修改`：优先实施的三项与理由。
3. `修改建议排序表`：排名、建议、影响、置信度、实施成本、证据、验证。
4. `诊断依据与证据`：重复出现的问题及所属缺陷类别，标明证据来源。
5. `详细修改建议`：行为契约下的提示词、技能；运行实现下的工具、控制流、路由；输出契约下的产物结构；可观测性与评测下的可观测性、评测。
6. `各反馈来源的洞察`：分别说明轨迹、人工、模型、评测、评分和配置贡献的判断。
7. `机器可读摘要`：包含 `top_priorities` 的 JSON 块。

原始提示词还约束详细建议各子标题的固定顺序，不使用“负责人”一词，并禁止增加其他顶级章节。它用于得到结构稳定、可交接的报告；标题齐全仍不等于报告内容都正确。

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果#代码单元 49：定义 HALO 报告提示词|中文代码：定义 HALO 报告提示词]]

### 运行 HALO 并保存报告

`EngineConfig` 使用 `HALO_MODEL` 作为根 Agent、子 Agent、综合和压缩模型；每个 Agent 最多 20 轮，最大深度 1，并行子 Agent 上限 2。这里描述原文 HALO 的配置，本次整理没有启动 HALO 或这些子 Agent。

原文通过 Python API 调用 `stream_engine_async()`：注释说明当时 CLI 的压缩温度 0.0 与所选模型不兼容，改用配置了 `temperature=1.0` 的压缩模型；这是来源中的特定版本处理，不是适用于所有当前模型的温度建议。HALO 分析阶段把 SDK 轨迹处理器清空，避免再向托管服务导入分析活动。

为了只用轨迹分析工具，不依赖可选的 Deno/Pyodide `run_code` 环境，代码临时替换 `Sandbox.get` 为返回 `None`，在 `finally` 中恢复。进度任务每 30 秒打印已耗时；同时收集流式文本或最终输出，若没有报告就抛错。

`clean_halo_handoff()` 从最后出现的 `## 执行摘要` 开始截取，并检查七个必需标题是否存在；`write_halo_handoff()` 保存 `codex_handoff.md`。Notebook 只显示“前三项修改”和“各反馈来源洞察”的摘录。

核对说明：HALO 的数据中有五个业务 Trace，加配置和门禁汇总两个全局 Trace，总计七个不同 Trace ID。反馈与评测 span 不会把业务调用变成更多次。报告校验只检查标题存在，未严格验证顺序、JSON 合法性或证据充分性。

原文保存的 HALO 耗时为 7 分 15 秒，写出 `halo_optimization_context.jsonl` 与 `codex_handoff.md`。日志中的 `Gate result passed into optimization context: True` 来自检查 `'gate_result' in halo_context`，表达的是“上下文包含评分结果”，不是一个独立的门禁通过判断；本例门禁确实五项通过，要看前一步的计数。

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果#代码单元 51：运行 HALO 并清理交接报告|中文代码：运行 HALO 并清理交接报告]]

## 第八步：把完整报告交给 Codex

HALO 负责诊断和排序，实际修改仍由 Codex、其他编码 Agent 或开发者完成。原文 Notebook 打印交接文件位置，并显示以下三项优先建议。

1. 增加确定性的尽调事实台账与领域检查表，明确 ARR 权威口径、现金支撑期与消耗、母账户集中度、无支持指标和 SOC 2 状态。让业务规则进入可计算、可检查的实现，减少只靠通用引用指令的情况。
2. 让校验器审计真正写出的产物。解析 Markdown、JSON、CSV，提取实质结论，核验支持材料，对未经审计或无支持的结论判失败。原文报告观察到“校验通过后仍发生修复”，因此现有检查不足以代表最终文件正确。
3. 将生成的五项评测保存进版本管理的回归集。它们当前通过，仍应长期保留，后续提示词或运行实现变化时重新执行。

| 证据来源 | 原文报告给出的洞察 |
| --- | --- |
| 运行轨迹 | 多数运行遵循产物与校验流程，但做法偏通用、有时集中在大段执行中；部分修复晚于校验通过。母账户计算提供了可推广的确定性计算样例。 |
| 人工反馈 | 领域判断集中于 11 个月融资窗口、ARR 财务口径、母账户汇总、Type I/II 区别，以及拒绝无支持的官方 NRR 与 CAC。模板中的消耗趋势仍须按第四步核对。 |
| 模型反馈 | 与人工主题基本一致，还强调 ARR 标题值、留存和商机的限制，避免夸大 Type II 完成状态。 |
| 生成评测 | 五个针对性测试承接了这些问题，应保存为回归集。 |
| 评分结果 | 五项全过，但还需要覆盖校验器、产物解析和计算正确性。 |
| 当前配置 | 已有通用证据、引用、产物与校验要求，缺的是明确领域不变量及相应确定性运行检查。 |

批注：正文提供的是官方 Notebook 中公开展示的报告摘录。原文完整 `codex_handoff.md` 由运行时生成，Notebook 没有嵌入其全部正文，本次没有重新生成。附录保留代码的中文改写及已展示运行结果的中文整理，不把摘录冒充完整 HALO 报告。

把交接落实为一次可比较的修改时，应保留基线配置、固定评测与输入，实施候选改动，重新运行候选 Agent，再比较相同规则下的新旧结果。只改提示词、只看门禁变绿或只保留一份覆盖式交接，都无法解释改动具体改善了哪种行为。

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果#代码单元 53：展示 Codex 交接摘录|中文代码：展示 Codex 交接摘录]]

## 第九步：连接下一轮运行

![[知识点总结/99-images/openai-agent-improvement-loop-20260512/images/agent-improvement-loop-flywheel.svg]]

第一张图回到同一循环：新轨迹与反馈形成新评测，HALO 诊断、Codex 修改、验证与部署后的行为成为下一轮输入。原文设想把交接文件写到共享存储，用周期检查唤醒编码流程。

![[知识点总结/99-images/openai-agent-improvement-loop-20260512/images/agent-improvement-loop-human-gates.svg]]

第二张图说明：可在四处设置人工门禁。轨迹审查补充领域反馈；评测审查收紧测试规则；PR 审查检查代码差异；合并审批按既定规则进入部署。也可以在足够有把握时，让这些节点按策略自动执行。

原文允许完整自动执行、只审部分节点、每步都审三种参与方式。人工判断仍通过反馈决定系统接下来学什么、改什么；是否每次都需要批准，是流程设计选择。

批注：周期检查、自动开 PR、合并和部署都需要另行实现。要自动接受修改，至少应让门禁消费候选版本的新运行输出，并覆盖确定性计算、产物结构和业务证据；本例的旧答案评分不能直接充当这条发布链路。

## 原文结论与适用边界

原文将持续改进的对象扩展到整个执行框架：轨迹保存行为，人工反馈给出判断，评测固定要求，HALO 将证据组织成修改建议，编码环节落实下一版。这个过程不要求每次都改模型权重，也不限定只能调提示词。

它是一份可运行示例及架构说明，组件与接口可能随版本变化。本文从公开代码确认了资料生成、采样、反馈、旧答案评分、上下文组织及报告交接的关系；没有执行 API 调用，也没有验证某个修改版比基线更好。原始合成数据中的算术矛盾、人工模板中的未证趋势和较弱的校验器，都需要在实际采用前修正。

## 按原文继续实验

1. 根据任务与预算分别配置五个模型角色，核对当前 SDK、HALO、Promptfoo 及执行环境兼容性，保留本轮配置快照。
2. 替换为自己的资料与问题，运行并保存真实轨迹；人工审阅需要从样例模板改为基于实际业务证据的反馈。
3. 审查自动生成的评测，修正错误要求，补足计算、来源冲突和产物测试，并将固定评测集纳入版本管理。
4. 将生成的 `codex_handoff.md` 交给编码人员实施，按需要选择审查与自动化节点。
5. 为候选版本重新生成答案，让同一套评测检查新输出，再决定是否接受修改并进入下一轮。

[[知识点总结/99-images/openai-agent-improvement-loop-20260512/中文代码与运行结果|查看中文代码与运行结果]] · [[知识点总结/99-images/openai-agent-improvement-loop-20260512/来源与覆盖核对|查看逐项覆盖清单与来源文件]]

