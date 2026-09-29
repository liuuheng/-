# API 接口调用基础：请求结构、鉴权、Postman 与排错

## 一、核心认识

调用 API 的本质是：客户端按照服务端规定的协议发送请求，服务端完成身份认证、权限检查和业务处理后返回结果。

一条完整调用链可以概括为：

`接口地址 + 请求方法 + 请求参数 + 鉴权信息 → API 网关 → 身份认证 → 权限检查 → 业务处理 → 响应结果`

实际接入一个官方 API，至少需要明确四类信息：

1. **接口契约**：Endpoint、Method、参数位置、Body 格式和返回结构。
2. **身份凭证**：如 `appKey + appSecret`、Access Token。
3. **鉴权规则**：凭证放在哪里、签名如何计算、时间戳有效多久。
4. **调用权限**：应用是否被授权使用该接口、Scope 或对应产品能力。

## 二、HTTP 请求的组成

以下是一条完整请求的结构示例：

```http
POST /v2/bigdata/topic/query?appkey=xxx&timestamp=123&sign=xxx HTTP/1.1
Host: openapi.kujiale.com
Content-Type: application/json
Accept: application/json

{
  "topic": "example",
  "pageIndex": 1,
  "pageSize": 100
}
```

上面的业务字段仅用于展示结构，不代表目标接口的真实参数。

| 名称 | 示例 | 含义 |
|---|---|---|
| Scheme | `https` | 网络协议 |
| Host/Domain | `openapi.kujiale.com` | 目标服务器域名 |
| Base URL | `https://openapi.kujiale.com` | API 服务基础地址 |
| Path | `/v2/bigdata/topic/query` | 具体接口路径 |
| Endpoint | Base URL 加 Path | 请求实际发送到的接口地址 |
| Method | `POST` | 请求的操作方式 |
| Query Params | `appkey=xxx` | URL 中 `?` 后面的参数 |
| Headers | `Content-Type` | 请求的格式、认证和控制信息 |
| Body/Payload | JSON | 真正提交的业务数据 |
| Response | JSON | 服务端返回的处理结果 |

简化记忆：

- URL 决定请求发到哪里。
- Method 决定如何调用。
- Params 是 URL 上携带的参数。
- Headers 告诉服务器如何解析和处理请求。
- Body 携带具体业务数据。
- Response 是服务器的处理结果。

## 三、HTTP Method

| Method | 常见用途 |
|---|---|
| `GET` | 查询资源 |
| `POST` | 提交数据、创建资源或执行复杂查询 |
| `PUT` | 完整更新资源 |
| `PATCH` | 部分更新资源 |
| `DELETE` | 删除资源 |

不能只根据接口名称猜 Method。例如名称中有 `query` 的查询接口也可能要求 `POST`，因为它可能需要复杂 JSON Body、统一签名或平台统一网关。最终以官方文档为准。

浏览器地址栏通常只能方便地发起 GET，不能完整配置 POST Body、自定义 Header 和动态签名。因此复杂服务端 API 应使用 Postman、官方 SDK、`curl` 或后端 HTTP 客户端调用。

## 四、参数的位置

同一个参数放错位置，服务端就可能认为没有收到。

| 参数类型 | 示例 | Postman 中的位置 |
|---|---|---|
| Path Parameter | `/users/{userId}` | URL 路径 |
| Query Parameter | `?page=1&pageSize=20` | Params |
| Header Parameter | `Authorization: Bearer xxx` | Headers |
| Body Parameter | `{ "username": "nanan" }` | Body |

### Params

Postman 的 Params 对应 URL 中 `?` 后面的 Query 参数。直接修改 URL 和修改 Params 表格通常会相互同步。

### Headers

Headers 是请求头，携带请求的格式、认证、版本和追踪信息。常见请求头包括：

| Header | 作用 |
|---|---|
| `Content-Type` | 声明请求体格式 |
| `Accept` | 声明期望的响应格式 |
| `Authorization` | 携带 Token 等认证信息 |
| `X-Request-Id` | 标识一次请求，便于排查 |
| `x-acs-action` | 阿里云 API 操作名称 |
| `x-acs-version` | 阿里云 API 版本 |

`Content-Type: application/json` 只是告诉服务器 Body 是 JSON，不包含业务数据本身。

### Body

Body 是实际提交的业务数据。文档要求 JSON 时，在 Postman 中通常选择 `Body → raw → JSON`，并使用 `Content-Type: application/json`。

## 五、Postman 各区域的作用

| 区域 | 作用 |
|---|---|
| Method | 选择 GET、POST 等请求方法 |
| URL | 填写完整 Endpoint |
| Params | 管理 Query 参数 |
| Authorization | 配置 Bearer Token、OAuth、API Key 等标准认证 |
| Headers | 设置内容类型、认证头、版本等 |
| Body | 填写 JSON、表单或文件 |
| Pre-request Script | 请求前生成时间戳、随机数和签名 |
| Tests | 响应后校验 HTTP 状态码和业务码 |
| Settings | 设置超时、重定向等行为 |

Postman 中显示的 hidden headers 通常是自动生成的 `Host`、`User-Agent`、`Content-Length` 等请求头，一般不需要手工填写。

## 六、身份认证与权限授权

### 身份认证 Authentication

回答“调用者是谁”。常见方式包括：

- API Key
- `appKey + appSecret` 签名
- Bearer Token
- OAuth 2.0
- JWT
- Cookie 会话

### 权限授权 Authorization

回答“调用者可以做什么”。即使签名正确，仍可能因为以下原因失败：

- 应用没有开通对应接口；
- 缺少所需 Scope；
- 没有绑定正确企业或用户；
- 测试凭证调用了生产环境；
- 产品未购买或应用不在白名单。

正常校验顺序是：

`身份认证 → 权限检查 → 参数校验 → 业务执行`

## 七、appKey、appSecret、timestamp 与 sign

| 字段 | 作用 |
|---|---|
| `appKey` | 标识调用方应用，类似应用账号 |
| `appSecret` | 应用密钥，类似应用密码，只保存在调用方服务端 |
| `timestamp` | 请求生成时间，用于限制有效期和防止重放 |
| `nonce` | 一次性随机数，防止同一请求被重复使用 |
| `sign` | 使用密钥对请求信息计算得到的签名 |

签名可抽象为：

`sign = 签名算法(Method、Path、Params、Body、timestamp、nonce、appSecret)`

哪些字段参与签名、如何排序和编码、使用 MD5 还是 HMAC-SHA256，必须以对应平台和接口版本的官方文档为准。不能凭经验猜测。

修改 `timestamp` 后通常必须重新计算 `sign`；旧签名不能直接复用。调用机器的系统时间也必须准确。

## 八、REST/ROA 与 RPC/Action

### REST/ROA 风格

REST 强调“操作资源”，由 Method 和 Path 共同表达语义：

- `GET /instances`：查询实例
- `POST /instances`：创建实例
- `DELETE /instances/123`：删除实例

### RPC 风格

RPC 强调“执行远程方法”，通过 Action 区分操作：

- `DescribeInstances`
- `CreateInstance`
- `StartInstance`
- `StopInstance`

阿里云中的 `Action` 或 `x-acs-action` 就是要调用的 API 操作名称。简单区分：

- REST：操作哪个资源。
- RPC：执行哪个方法。

## 九、响应、HTTP 状态码和业务码

HTTP 状态码描述协议或网关层结果：

| 状态码 | 常见含义 |
|---|---|
| `200` | HTTP 层处理正常，不代表业务一定成功 |
| `400` | 请求格式或参数错误 |
| `401` | 身份认证失败 |
| `403` | 身份已识别，但没有权限 |
| `404` | 地址、Path 或路由不匹配 |
| `405` | Method 不允许 |
| `429` | 请求过于频繁 |
| `500` | 服务端内部错误 |
| `502/503` | 网关或上游服务异常 |

很多平台还会在 JSON 中返回自己的业务码。例如：

```json
{
  "c": "100004",
  "m": "request time out.",
  "d": null
}
```

即使 HTTP 状态是 `200`，业务码仍然可能表示失败。调用程序必须同时判断 HTTP 状态码和业务状态码。

## 十、阅读 API 文档的固定顺序

拿到一份 API 文档后，按以下顺序检查：

1. 接口用途和适用场景。
2. 环境、Base URL、Endpoint、Path。
3. HTTP Method。
4. Content-Type 和请求格式。
5. 鉴权方式及凭证获取方法。
6. 所需 Scope、企业授权或白名单。
7. 参数名称、类型、是否必填及所在位置。
8. 完整请求示例。
9. 成功码、返回字段和分页结构。
10. 错误码、频率限制、超时和重试规则。
11. 接口是否异步，是否需要回调或轮询。
12. 是否提供官方 SDK。

可以为每个接口填写下面的检查表：

- 接口用途：
- 环境：
- Method：
- Base URL/Endpoint：
- Path：
- Params：
- Headers：
- Body：
- 鉴权方式：
- 所需权限：
- 成功码：
- 错误码：
- 分页或异步规则：
- 超时和重试规则：
- 请求示例：
- 返回示例：

## 十一、从 Postman 调通到代码接入

标准调用步骤：

1. 确认开发、测试或生产环境，避免域名和凭证混用。
2. 根据文档填写 Method、Endpoint 和 Path。
3. 将参数放入正确的 Params、Headers 或 Body。
4. 配置认证信息；动态签名放在 Pre-request Script 中生成。
5. 发送请求，记录 HTTP 状态码、业务码、响应和 request ID。
6. 按网络、HTTP、鉴权、权限、参数、业务六层定位错误。
7. Postman 调通后，优先使用官方 SDK；SDK 不支持时再使用后端 HTTP 客户端。

Java 常见 HTTP 客户端包括 Spring `WebClient`、`RestTemplate`、JDK `HttpClient` 和 OkHttp。

只要接口需要 `appSecret`，就不应由浏览器前端直接调用。正确链路通常是：

`浏览器前端 → 自己的后端 → 第三方服务端 API`

## 十二、安全、重试与排错注意事项

### 密钥安全

`appSecret`、Token、私钥和密码不能放在前端代码、URL、Git 仓库、截图或普通日志中。应保存到服务端环境变量或密钥管理系统。

URL 可能被浏览器历史、代理和访问日志记录，因此敏感凭证不应长期放在 URL 中。短期签名也应脱敏展示。

### 重试与幂等

不要看到失败就盲目重试 POST。网络超时可能表示服务端已经执行成功，只是响应没有返回。

只有以下情况才适合自动重试：

- 文档明确说明接口幂等；
- 使用了幂等键；
- 接口是无副作用查询；
- 能先查询任务状态再决定是否重试。

### 分层排错

1. 没有收到响应：检查 DNS、网络、TLS 和超时。
2. `404/405`：检查域名、Path 和 Method。
3. `401`：检查凭证、签名、时间戳和系统时间。
4. `403`：检查 Scope、企业绑定、产品权限和白名单。
5. `400`：检查参数位置、类型、必填项和 JSON 格式。
6. `429`：降低频率并按文档退避重试。
7. HTTP `200` 但业务失败：检查业务码和错误信息。
8. 平台无法定位：提供脱敏请求摘要、时间、request ID 或 trace ID。

## 十三、酷家乐 Topic 查询接口的当前结论

目标接口：`/v2/bigdata/topic/query`

已经验证：

- 使用旧平台域名 `https://openapi.kujiale.com`。
- 接口要求 `POST`，浏览器直接打开产生的 GET 无法命中接口。
- URL 中的 `appkey`、`timestamp`、`sign` 属于 Query Params。
- `Content-Type: application/json` 属于 Header。
- 原时间戳过期时，接口返回业务码 `100004` 和 `request time out.`。
- 修改时间戳后必须配套重新计算签名。

仍需从官方文档确认：

- 完整 POST Body 和必填字段；
- Topic 名称或 ID 的来源；
- 具体签名算法；
- 哪些参数参与签名及排序、编码规则；
- 时间戳允许误差；
- 当前 appKey 所需的接口权限；
- 成功业务码和完整响应结构；
- 分页、频率限制、超时和重试规则；
- 是否存在覆盖该接口的官方 SDK。

在这些信息补齐前，只能证明接口路由和部分鉴权流程存在，不能认为已经完成可靠接入。

## 十四、推荐学习顺序

1. URL、Host、Path、Endpoint。
2. GET、POST 等 Method。
3. Params、Headers、Body。
4. JSON 和常见参数类型。
5. HTTP 状态码与业务码。
6. Authentication 与 Authorization。
7. appKey、appSecret、Token、timestamp 和签名。
8. 在 Postman 中调通完整请求。
9. 使用 Pre-request Script 动态生成鉴权参数。
10. 转换成官方 SDK 或后端 HTTP 客户端代码。

最终应形成一个习惯：**先读懂接口契约和鉴权文档，再在 Postman 建立最小可验证请求，最后进入代码开发。**
