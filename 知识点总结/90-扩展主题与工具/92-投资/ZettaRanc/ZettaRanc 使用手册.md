---
aliases:
  - ZettaRanc 使用方法
  - 万千使用手册
tags:
  - 工具
  - 股票
  - Tushare
status: active
---

# ZettaRanc 使用手册

项目背景、依赖和当前初始化状态参见：

[[ZettaRanc 项目说明]]

## 一、进入项目

打开终端：

```bash
cd "/Users/nanan/Documents/ChatGPT/z哥/zettaranc-skill"
source .venv/bin/activate
```

检查命令：

```bash
zt --help
zt sync status
```

如果不激活虚拟环境，可以直接使用：

```bash
.venv/bin/zt --help
```

## 二、环境变量

配置文件位置：

`/Users/nanan/Documents/ChatGPT/z哥/zettaranc-skill/.env`

核心配置：

```dotenv
DATA_MODE=jnb

TUSHARE_TOKEN=自己的_Tushare_Token
TUSHARE_API_URL=https://api.waditu.com/dataapi

DATA_DIR=data
DB_PATH=data/stock_data.db

ZETTARANC_BACKTEST_IMPL=python
```

不要将真实的 Token 写入笔记或提交到 Git。

## 三、股票代码格式

项目建议使用 Tushare 完整股票代码。

| 股票代码开头 | 交易所 | 示例 |
|---|---|---|
| 600、601、603、605、688 | 上海证券交易所 | `600519.SH` |
| 000、001、002、003、300、301 | 深圳证券交易所 | `000001.SZ` |
| 4、8、92 等 | 北京证券交易所 | `920xxx.BJ` |

后缀含义：

- `.SH`：上海证券交易所
- `.SZ`：深圳证券交易所
- `.BJ`：北京证券交易所

不建议省略后缀，因为不同市场可能存在容易混淆的代码。例如：

- `000001.SH`：上证指数
- `000001.SZ`：平安银行
- `399001.SZ`：深证成指

不确定代码时，可以查询本地股票基础信息：

```bash
.venv/bin/python -c "
import sqlite3

code = '600519'
db = sqlite3.connect('data/stock_data.db')

rows = db.execute(
    'SELECT ts_code, name, exchange, market '
    'FROM stock_basic WHERE ts_code LIKE ?',
    (code + '.%',)
).fetchall()

print(rows)
"
```

## 四、查看数据状态

```bash
zt sync status
```

主要用于确认：

- 数据库是否已经初始化
- 股票基础信息是否存在
- K 线数据是否已经同步
- 指标缓存是否存在

## 五、股票分析

普通输出：

```bash
zt analyze 600519.SH
```

JSON 输出：

```bash
zt analyze 600519.SH --json
```

指定分析天数：

```bash
zt analyze 600519.SH --days 100
```

分析结果可能包含：

- 技术指标
- 趋势判断
- 战法识别
- 主力阶段
- 风险提示
- 综合结论

## 六、股票评分

```bash
zt score 600519.SH
zt score 600519.SH --json
```

评分适合用于不同股票之间的初步比较，但不能直接替代投资决策。

## 七、持仓诊断

```bash
zt diagnose 600519.SH
zt diagnose 600519.SH --days 100
zt diagnose 600519.SH --json
```

诊断结果适合检查：

- 当前趋势
- 持仓风险
- 技术形态
- 止损和退出条件

## 八、条件选股

B1 买点：

```bash
zt screen --strategy B1 --limit 20
```

其他示例：

```bash
zt screen --strategy 超级B1 --limit 10
zt screen --strategy 完美图形 --limit 10
zt screen --strategy 长安 --limit 10
zt screen --strategy 建仓波 --limit 20
zt screen --strategy 吸筹 --limit 20
zt screen --strategy 突破 --limit 20
zt screen --strategy 安全 --limit 20
```

全市场选股依赖本地数据完整度。如果只同步了少量股票，筛选范围也会受到限制。

## 九、观察池

添加股票：

```bash
zt watchlist add 600519.SH --tags 长线,白酒
```

查看观察池：

```bash
zt watchlist list
```

批量扫描：

```bash
zt watchlist scan --json
```

移除股票：

```bash
zt watchlist remove 600519.SH
```

推荐把准备长期跟踪的股票放入观察池，然后只同步和扫描这些股票。

## 十、策略回测

少妇战法回测：

```bash
zt backtest shaofu 600519.SH --days 250
```

JSON 输出：

```bash
zt backtest shaofu 600519.SH --days 250 --json
```

多策略融合：

```bash
zt backtest multi 600519.SH --strategy b1,b2
```

组合回测：

```bash
zt backtest portfolio 600519.SH,601318.SH
```

回测前需要保证本地存在足够长的历史行情。

## 十一、端到端模拟

基础模拟：

```bash
zt simulate 600519.SH --days 250 --json
```

启用 ATR 仓位控制：

```bash
zt simulate 600519.SH --days 250 --atr-sizing --json
```

模拟器可以考虑：

- A 股 T+1
- 涨跌停限制
- 交易成本
- 滑点
- 仓位控制
- 止损和止盈
- 策略信号共振

## 十二、同步数据

初始化数据库：

```bash
zt sync init
```

同步单只股票和指标：

```bash
zt sync sync --ts_code 600519.SH --days 120 --indicators
```

检查同步结果：

```bash
zt sync status
zt analyze 600519.SH
```

当前 Tushare 账号可能受到 `adj_factor` 接口权限或访问频率限制，因此推荐按观察池逐只同步，不建议直接高频同步全市场。

## 十三、使用免费数据源补充行情

Tushare 复权因子接口受限时，可以临时使用免费数据源：

```bash
.venv/bin/python - <<'PY'
from modules.data_sync import DataSyncer
from modules.datasource import get_datasource

syncer = DataSyncer(datasource=get_datasource("a-stock-data"))

syncer.sync_daily_kline(
    "600519.SH",
    start_date="20260101",
    end_date="20260822",
)

syncer.sync_indicator_cache(
    "600519.SH",
    days=120,
)
PY
```

完成后验证：

```bash
zt sync status
zt analyze 600519.SH
```

使用免费数据源时，需要注意它与 Tushare 可能存在复权口径差异。

## 十四、推荐的日常工作流

### 首次使用

```bash
cd "/Users/nanan/Documents/ChatGPT/z哥/zettaranc-skill"
source .venv/bin/activate

zt sync status
zt analyze 600519.SH
```

### 添加准备跟踪的股票

```bash
zt watchlist add 600519.SH --tags 长线,白酒
zt watchlist list
```

### 每日检查

```bash
zt watchlist scan --json
zt diagnose 600519.SH
```

### 需要验证策略时

```bash
zt backtest shaofu 600519.SH --days 250
zt simulate 600519.SH --days 250 --atr-sizing --json
```

## 十五、常见问题

### 提示数据不足

先同步该股票：

```bash
zt sync sync --ts_code 600519.SH --days 250 --indicators
```

### Tushare 请求失败

依次检查：

1. `.env` 中的 `TUSHARE_TOKEN` 是否正确。
2. `TUSHARE_API_URL` 是否为 `https://api.waditu.com/dataapi`。
3. 当前接口是否需要更高积分。
4. 是否触发访问频率限制。
5. 是否可以使用免费数据源补充 K 线。

### 找不到 `zt` 命令

确认已经激活虚拟环境：

```bash
source .venv/bin/activate
```

或者直接使用：

```bash
.venv/bin/zt --help
```

### 是否必须配置大模型

不需要。

股票分析、指标计算、选股和回测都能在没有大模型的情况下运行。

### 是否必须配置飞书

不需要。

不使用主动消息推送时，让 `IM_PUSH_WEBHOOK` 保持为空即可。

## 十六、安全和使用边界

- 不要把 Token、API Key 或 Webhook 写入笔记。
- 分析前确认本地行情是否更新。
- 数据不足时不要直接采信指标结果。
- 回测结果不能代表未来收益。
- 输出内容只能作为研究参考，不构成投资建议。
