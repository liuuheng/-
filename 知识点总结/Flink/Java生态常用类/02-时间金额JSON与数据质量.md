# 时间、金额、JSON 与数据质量

> 数据处理代码最常见的隐性错误不是语法错误，而是时间语义不清、浮点精度丢失、JSON 宽松解析掩盖上游变更，以及脏数据处理策略不完整。本篇给出适用于 Flink 1.17 的生产实现骨架。

## 1. 时间类应该如何选择

| 业务含义 | 推荐类型 | 示例 | 不应替代为 |
| --- | --- | --- | --- |
| 时间线上的确定瞬间 | `Instant` 或 epoch millis `long` | 支付发生时刻 | `LocalDateTime` |
| 某地区看到的日期时间 | `ZonedDateTime` | 上海当地 09:00 | 无时区的字符串 |
| 只有日期 | `LocalDate` | 账期、自然日 | 00:00 的时间戳 |
| 本地日期时间但尚无时区 | `LocalDateTime` | 用户输入“2026-08-31 09:00” | 直接当 UTC |
| 时长/超时 | `Duration` | 30 秒请求超时 | 无单位的 `30000L` |
| 日历周期 | `Period` | 一个月后 | 固定 30 天 |

### 核心判断

`LocalDateTime` 不表示全球时间线上的唯一时刻。必须结合 `ZoneId` 才能解析成 `Instant`：

```java
private static final ZoneId BUSINESS_ZONE = ZoneId.of("Asia/Shanghai");
private static final DateTimeFormatter LOCAL_FORMATTER =
        DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")
                .withResolverStyle(ResolverStyle.STRICT);

LocalDateTime local = LocalDateTime.parse("2026-08-31 09:30:00", LOCAL_FORMATTER);
Instant instant = local.atZone(BUSINESS_ZONE).toInstant();
long epochMillis = instant.toEpochMilli();
```

`DateTimeFormatter` 是不可变且线程安全的，可以定义为 `static final`；`SimpleDateFormat` 不是线程安全的，不要把它作为共享静态对象放入算子。

### Flink 数据模型中的建议

- 流记录和 State 中优先保存 `long eventTimeMillis`，跨版本、跨语言和序列化行为最清晰。
- 在输入边界把字符串转换成毫秒，在展示/分区边界再结合明确 `ZoneId` 格式化。
- 不要依赖 TaskManager 操作系统默认时区；所有 `ZoneId.systemDefault()` 都应经过审查。
- Flink watermark 使用的是 epoch timestamp；业务日期分区则通常需要业务时区，两者不能混为一谈。

## 2. 用 `Clock` 让时间逻辑可测试

直接调用 `Instant.now()` 会导致测试不可重复。普通业务服务可以注入 `Clock`：

```java
public final class ExpiryPolicy {
    private final Clock clock;
    private final Duration ttl;

    public ExpiryPolicy(Clock clock, Duration ttl) {
        this.clock = Objects.requireNonNull(clock);
        this.ttl = Objects.requireNonNull(ttl);
    }

    public boolean isExpired(Instant createdAt) {
        return !createdAt.plus(ttl).isAfter(clock.instant());
    }
}
```

测试：

```java
Clock fixed = Clock.fixed(
        Instant.parse("2026-08-31T00:00:00Z"), ZoneOffset.UTC);
ExpiryPolicy policy = new ExpiryPolicy(fixed, Duration.ofMinutes(10));

assertThat(policy.isExpired(
        Instant.parse("2026-08-30T23:49:59Z"))).isTrue();
assertThat(policy.isExpired(
        Instant.parse("2026-08-30T23:55:00Z"))).isFalse();
```

注意：Flink 的定时器语义应通过 `TimerService` 测试，不要用 `Clock` 模拟事件时间或处理时间定时器。`Clock` 适用于从 Flink 运行时中剥离出来的纯业务逻辑。

## 3. 金额必须使用 `BigDecimal`

### 3.1 构造

推荐：

```java
BigDecimal price = new BigDecimal("19.90");
BigDecimal quantity = BigDecimal.valueOf(3L);
BigDecimal total = price.multiply(quantity);
```

不推荐：

```java
BigDecimal wrong = new BigDecimal(0.1); // 把 double 的二进制误差带入
```

如果上游 JSON 金额是数字字面量，Jackson 的 `decimalValue()` 通常可得到 `BigDecimal`，但生产协议更推荐金额用字符串或最小货币单位整数表示，避免中间系统先转成 `double`。

### 3.2 舍入只能发生在定义好的边界

```java
private static final int MONEY_SCALE = 2;
private static final RoundingMode MONEY_ROUNDING = RoundingMode.HALF_UP;

BigDecimal payable = unitPrice
        .multiply(quantity)
        .subtract(discount)
        .setScale(MONEY_SCALE, MONEY_ROUNDING);
```

- 中间计算不要每一步都舍入，否则累计误差会扩大。
- 除法必须指定精度或舍入方式，否则无限循环小数会抛 `ArithmeticException`。

```java
BigDecimal rate = successCount.divide(totalCount, 6, RoundingMode.HALF_UP);
```

### 3.3 比较

```java
new BigDecimal("1.0").equals(new BigDecimal("1.00"));   // false，scale 不同
new BigDecimal("1.0").compareTo(new BigDecimal("1.00")) == 0; // true
```

业务数值相等通常使用 `compareTo`；如果 scale 本身也是协议的一部分，才使用 `equals`。

## 4. Jackson 的生命周期和配置

`ObjectMapper` 在所有读写发生之前完成配置后可以安全复用；不要每条数据 `new ObjectMapper()`。在 Flink Rich Function 中把 Jackson 对象声明为 `transient`，并在 `open()` 初始化，避免把具体 Jackson 实现状态随闭包序列化。

```java
private transient ObjectReader jsonReader;

@Override
public void open(Configuration parameters) {
    ObjectMapper mapper = JsonMapper.builder()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
            .build();
    this.jsonReader = mapper.reader();
}
```

如果直接绑定 POJO，应明确处理未知字段：

- 强协议事件：开启 `FAIL_ON_UNKNOWN_PROPERTIES`，上游变更立刻暴露；
- 兼容演进事件：允许未知字段，但必须有 Schema 版本、契约测试和监控；
- 不能为了“先跑起来”全局宽松解析，然后完全不观测未知字段。

## 5. 生产级事件模型

Flink 1.17 POJO 使用无参构造器和标准 getter/setter。金额保留 `BigDecimal`，事件时间保存毫秒。

```java
package com.example.flink.model;

import java.math.BigDecimal;

public class OrderEvent {
    private String eventId;
    private String orderId;
    private String currency;
    private BigDecimal amount;
    private long eventTimeMillis;

    public OrderEvent() {}

    public OrderEvent(
            String eventId,
            String orderId,
            String currency,
            BigDecimal amount,
            long eventTimeMillis) {
        this.eventId = eventId;
        this.orderId = orderId;
        this.currency = currency;
        this.amount = amount;
        this.eventTimeMillis = eventTimeMillis;
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }
    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public long getEventTimeMillis() { return eventTimeMillis; }
    public void setEventTimeMillis(long eventTimeMillis) {
        this.eventTimeMillis = eventTimeMillis;
    }
}
```

脏数据模型：

```java
package com.example.flink.model;

public class DeadLetter {
    private String reasonCode;
    private String reasonMessage;
    private String payloadPreview;
    private long observedAtMillis;

    public DeadLetter() {}

    public DeadLetter(
            String reasonCode,
            String reasonMessage,
            String payloadPreview,
            long observedAtMillis) {
        this.reasonCode = reasonCode;
        this.reasonMessage = reasonMessage;
        this.payloadPreview = payloadPreview;
        this.observedAtMillis = observedAtMillis;
    }

    public String getReasonCode() { return reasonCode; }
    public void setReasonCode(String reasonCode) { this.reasonCode = reasonCode; }
    public String getReasonMessage() { return reasonMessage; }
    public void setReasonMessage(String reasonMessage) {
        this.reasonMessage = reasonMessage;
    }
    public String getPayloadPreview() { return payloadPreview; }
    public void setPayloadPreview(String payloadPreview) {
        this.payloadPreview = payloadPreview;
    }
    public long getObservedAtMillis() { return observedAtMillis; }
    public void setObservedAtMillis(long observedAtMillis) {
        this.observedAtMillis = observedAtMillis;
    }
}
```

## 6. 严格解析并侧输出脏数据

```java
package com.example.flink.function;

import com.example.flink.model.DeadLetter;
import com.example.flink.model.OrderEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.Set;

public final class OrderEventParseFunction
        extends ProcessFunction<String, OrderEvent> {

    public static final OutputTag<DeadLetter> DEAD_LETTER_TAG =
            new OutputTag<DeadLetter>("invalid-order-events") {};

    private static final int MAX_PAYLOAD_PREVIEW = 4096;
    private static final Set<String> SUPPORTED_CURRENCIES =
            Set.of("CNY", "USD", "EUR");

    private transient ObjectReader reader;

    @Override
    public void open(Configuration parameters) {
        ObjectMapper mapper = JsonMapper.builder().build();
        this.reader = mapper.reader();
    }

    @Override
    public void processElement(
            String raw,
            Context context,
            Collector<OrderEvent> out) {
        try {
            out.collect(parse(raw));
        } catch (EventValidationException e) {
            context.output(DEAD_LETTER_TAG, new DeadLetter(
                    e.getCode(),
                    truncate(e.getMessage(), 512),
                    truncate(raw, MAX_PAYLOAD_PREVIEW),
                    System.currentTimeMillis()));
        } catch (Exception e) {
            // 不把完整异常或原始消息拼进错误原因，避免泄露和超大死信。
            context.output(DEAD_LETTER_TAG, new DeadLetter(
                    "MALFORMED_JSON",
                    truncate(e.getClass().getSimpleName() + ": " + e.getMessage(), 512),
                    truncate(raw, MAX_PAYLOAD_PREVIEW),
                    System.currentTimeMillis()));
        }
    }

    private OrderEvent parse(String raw) throws Exception {
        if (raw == null || raw.trim().isEmpty()) {
            throw new EventValidationException("EMPTY_PAYLOAD", "payload is blank");
        }

        JsonNode root = reader.readTree(raw);
        if (root == null || !root.isObject()) {
            throw new EventValidationException(
                    "INVALID_ROOT", "JSON root must be an object");
        }

        String eventId = requiredText(root, "eventId", 128);
        String orderId = requiredText(root, "orderId", 128);
        String currency = requiredText(root, "currency", 3)
                .toUpperCase(Locale.ROOT);
        if (!SUPPORTED_CURRENCIES.contains(currency)) {
            throw new EventValidationException(
                    "UNSUPPORTED_CURRENCY", "unsupported currency");
        }

        BigDecimal amount = requiredDecimal(root, "amount")
                .setScale(2, RoundingMode.UNNECESSARY);
        if (amount.signum() < 0 || amount.compareTo(new BigDecimal("9999999999.99")) > 0) {
            throw new EventValidationException(
                    "INVALID_AMOUNT", "amount is outside the allowed range");
        }

        long eventTimeMillis = requiredTimestamp(root, "eventTime");
        return new OrderEvent(eventId, orderId, currency, amount, eventTimeMillis);
    }

    private static String requiredText(JsonNode root, String field, int maxLength) {
        JsonNode node = root.get(field);
        if (node == null || !node.isTextual() || node.textValue().trim().isEmpty()) {
            throw new EventValidationException(
                    "INVALID_" + field.toUpperCase(Locale.ROOT),
                    field + " must be a non-blank string");
        }
        String value = node.textValue().trim();
        if (value.length() > maxLength) {
            throw new EventValidationException(
                    "INVALID_" + field.toUpperCase(Locale.ROOT),
                    field + " exceeds maximum length");
        }
        return value;
    }

    private static BigDecimal requiredDecimal(JsonNode root, String field) {
        JsonNode node = root.get(field);
        if (node == null || (!node.isNumber() && !node.isTextual())) {
            throw new EventValidationException(
                    "INVALID_AMOUNT", field + " must be a decimal");
        }
        try {
            return new BigDecimal(node.asText());
        } catch (NumberFormatException e) {
            throw new EventValidationException(
                    "INVALID_AMOUNT", field + " is not a valid decimal", e);
        }
    }

    private static long requiredTimestamp(JsonNode root, String field) {
        JsonNode node = root.get(field);
        if (node == null) {
            throw new EventValidationException(
                    "INVALID_EVENT_TIME", field + " is required");
        }
        if (node.isIntegralNumber()) {
            long millis = node.longValue();
            // 2000-01-01 到 2100-01-01，防止秒/毫秒混淆和明显脏值。
            if (millis < 946684800000L || millis >= 4102444800000L) {
                throw new EventValidationException(
                        "INVALID_EVENT_TIME", "epoch millis is outside the accepted range");
            }
            return millis;
        }
        if (node.isTextual()) {
            try {
                return Instant.parse(node.textValue()).toEpochMilli();
            } catch (DateTimeParseException e) {
                throw new EventValidationException(
                        "INVALID_EVENT_TIME", "eventTime must be ISO-8601 instant", e);
            }
        }
        throw new EventValidationException(
                "INVALID_EVENT_TIME", "eventTime must be epoch millis or ISO-8601 instant");
    }

    private static String truncate(String value, int maxLength) {
        if (value == null) {
            return null;
        }
        return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }

    private static final class EventValidationException extends RuntimeException {
        private final String code;

        private EventValidationException(String code, String message) {
            super(message);
            this.code = code;
        }

        private EventValidationException(String code, String message, Throwable cause) {
            super(message, cause);
            this.code = code;
        }

        private String getCode() { return code; }
    }
}
```

连接到数据流：

```java
SingleOutputStreamOperator<OrderEvent> validEvents = rawEvents
        .process(new OrderEventParseFunction())
        .name("parse-and-validate-order-event")
        .uid("parse-and-validate-order-event-v1");

DataStream<DeadLetter> invalidEvents = validEvents.getSideOutput(
        OrderEventParseFunction.DEAD_LETTER_TAG);
```

### 生产上还必须补齐

- `validEvents` 和 `invalidEvents` 都要有可靠 Sink；不能只 `print()`。
- 死信中是否允许保留原文取决于数据分类。包含身份证、手机号、Token 时应脱敏、加密并设置较短保留期。
- 每个 `reasonCode` 注册 Counter；脏数据比例异常时告警。
- “格式错误可以进死信”不等于“所有异常都吞掉”。内存错误、客户端初始化错误、代码 Bug 等系统异常通常应该让作业失败并由重启策略处理。

## 7. Jackson 与 Fastjson 的项目现状

当前项目显式依赖 `fastjson:1.2.83`。本篇示例选择 Jackson，原因不是声称所有场景下 Jackson 都绝对更好，而是：

- Jackson 的 Streaming/Tree/Data Binding 模型成熟，Flink 生态中也很常见；
- Fastjson 1.x 已属于老版本线，生产使用需要额外进行安全与历史漏洞审计；
- 解析库切换会改变数字、日期、未知字段和异常行为，不能只替换 import 后直接上线。

迁移步骤应是：冻结输入样本 → 契约测试旧/新解析结果 → 比较异常分类 → 灰度双解析指标 → 再切换。不要在没有回归测试时批量替换。

## 8. 推荐测试边界

至少覆盖：

- `null`、空串、非对象 JSON；
- 必填字段缺失、空白、类型错误、超长；
- `amount` 为 `0`、负数、上限、超过两位小数、科学计数法；
- 时间为毫秒、ISO-8601、秒级误传、极端时间和非法字符串；
- 未知币种、大小写；
- 超长原文是否被截断；
- 合法输出和 Side Output 的数量及内容；
- POJO 是否被 Flink 识别为预期类型。

## 9. 官方参考

- [Java 17 `BigDecimal`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/math/BigDecimal.html)
- [Java 17 `java.time`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/package-summary.html)
- [Jackson `ObjectMapper`](https://fasterxml.github.io/jackson-databind/javadoc/)
- [Flink 1.17 Side Outputs](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/process_function/#side-outputs)

