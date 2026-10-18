# HTTP 通信与 Java 客户端基础

## 1. HTTP 核心流程

HTTP 通信的核心流程是：**构造地址和数据 → 添加请求头 → 发送请求 → 服务端处理 → 返回状态码和结果 → 客户端判断成功或失败**。请求地址（URL）决定请求发给哪个服务；请求体决定发送什么内容，Java 对象通常需要先序列化为 JSON。请求头负责补充通信信息，例如 `Content-Type` 表示数据格式，`Authorization` 携带身份凭证，`Idempotency-Key` 用于帮助服务端识别重复请求。客户端使用 `GET`、`POST` 等方法发送请求；服务端收到请求后验证身份、解析参数、执行业务逻辑并可能读写数据库，最后返回由状态码、响应头和响应体组成的 HTTP 响应。

客户端通常根据状态码决定下一步：`2xx` 表示请求成功；`4xx` 通常表示请求参数、认证、权限或地址存在问题，直接重试往往不能解决；`5xx` 通常表示服务端异常，部分情况可以等待后重试。常见状态码包括：`200/201` 表示成功，`400` 表示参数或格式错误，`401/403` 表示认证失败或无权限，`404` 表示接口或资源不存在，`429` 表示请求过于频繁，`500/503` 表示服务端异常或暂时不可用。

## 2. Java 代码中的对应流程

`HttpInternalApiClient.sendBatch()` 首先使用 `JSON.toJSONString(...)` 将 Java 数据转换为 JSON 请求体；随后通过 `HttpRequest.newBuilder(URI.create(config.endpoint))` 设置请求地址，并使用 `timeout()`、`header()`、`POST()` 设置超时、请求头和请求体，最后调用 `build()` 创建请求对象。需要注意，`build()` 只是完成请求的构造，并没有真正发送请求。

```java
String body = JSON.toJSONString(
    Collections.singletonMap("events", records)
);

HttpRequest request = HttpRequest.newBuilder(URI.create(config.endpoint))
    .timeout(Duration.ofMillis(config.requestTimeoutMs))
    .header("Content-Type", "application/json; charset=UTF-8")
    .header("Authorization", "Bearer " + config.token)
    .header("Idempotency-Key", idempotencyKey)
    .POST(HttpRequest.BodyPublishers.ofString(body))
    .build();
```

真正发送请求的是 `client.send()`。这是同步调用，当前线程会等待服务端返回响应、发生异常或请求超时。收到响应后，代码通过 `response.statusCode()` 获取状态码，将 `200～299` 判断为成功；其他状态码会转换成 `ApiException`，交给上层逻辑决定是否重试。

```java
HttpResponse<String> response = client.send(
    request,
    HttpResponse.BodyHandlers.ofString()
);

int status = response.statusCode();
if (status < 200 || status >= 300) {
    throw new ApiException(...);
}
```

代码流程可以简记为：**JSON 数据 → `HttpRequest` → `client.send()` → `HttpResponse` → `statusCode()`**。

## 3. `InternalApiClient` 与 `AutoCloseable`

`AutoCloseable` 是 Java 提供的资源关闭约定，实现该接口表示对象在使用结束后可以调用 `close()`，释放网络连接、线程池或其他运行期资源。`InternalApiClient` 是项目自定义的通信接口，它继承 `AutoCloseable`，并通过 `sendBatch()` 约定批量发送能力；调用方只依赖这份接口，不需要知道底层使用 HTTP、RPC、gRPC 还是公司 SDK。

```java
interface InternalApiClient extends AutoCloseable {
    void sendBatch(List<String> records, String idempotencyKey)
        throws Exception;

    default void close() throws Exception { }
}
```

`HttpInternalApiClient` 是 `InternalApiClient` 的 HTTP 实现，负责构造请求、发送请求以及检查响应状态。接口抽象的主要价值是隔离具体通信技术：以后如果改用 RPC 或公司 SDK，可以替换客户端实现，而上层调用逻辑通常不需要整体重写。当前接口提供了空的默认 `close()`，适用于没有显式资源需要释放的实现；如果某个 SDK 持有连接或线程池，其实现类应覆盖 `close()` 并执行实际释放操作。

## 4. 判断原则

理解这段代码时，可以先区分三层职责：HTTP 负责客户端与服务端之间的请求和响应；`InternalApiClient` 负责抽象“发送数据”这一能力；`HttpInternalApiClient` 负责使用 HTTP 完成该能力。遇到失败时，先看状态码属于 `4xx` 还是 `5xx`：前者优先检查请求本身，后者再考虑超时、退避和重试。
