# Java record 用法笔记

## 1. record 是什么

`record` 是 Java 中用来简化“数据类”的语法。

它适合用来表示：

- 配置对象
- DTO
- 方法返回结果
- 只承载数据、不强调行为的对象

例如：

```java
public record SqlPlan(
        String catalogDdl,
        String databaseDdl,
        String sourceDdl,
        List<String> tableDdls,
        List<String> inserts) {}
```

这表示定义了一个叫 `SqlPlan` 的数据对象，里面有 5 个字段。

---

## 2. record 自动生成哪些东西

对于下面这个 record：

```java
public record User(String name, int age) {}
```

Java 编译器会自动生成：

- 构造方法
- 私有 final 字段
- 字段访问方法
- `equals()`
- `hashCode()`
- `toString()`

大致等价于：

```java
public final class User {
    private final String name;
    private final int age;

    public User(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public String name() {
        return name;
    }

    public int age() {
        return age;
    }

    // 自动生成 equals()
    // 自动生成 hashCode()
    // 自动生成 toString()
}
```

---

## 3. record 如何创建对象

record 和普通类一样，用 `new` 创建对象：

```java
User user = new User("Tom", 18);
```

项目中的例子：

```java
SqlPlan plan = new SqlPlan(
        catalogSql,
        databaseSql,
        sourceSql,
        tableDdls,
        inserts
);
```

---

## 4. record 如何读取字段

record 的访问方法不是 `getXxx()`，而是字段名加括号。

例如：

```java
user.name();
user.age();
```

项目中的例子：

```java
plan.catalogDdl();
plan.databaseDdl();
plan.sourceDdl();
plan.tableDdls();
plan.inserts();
```

注意，不是：

```java
plan.getCatalogDdl();
```

---

## 5. record 默认是不可变的吗

record 的字段引用默认是不可变的，因为字段会被编译成 `private final`。

例如：

```java
public record User(String name, int age) {}
```

创建之后不能再给字段重新赋值：

```java
User user = new User("Tom", 18);

// 不允许
// user.name = "Jerry";
```

但要注意：record 不是“深度不可变”。

如果字段本身是可变对象，比如 `List`：

```java
public record SqlPlan(List<String> inserts) {}
```

那么下面这种修改仍然可能发生：

```java
plan.inserts().add("some sql");
```

因为 `final` 限制的是字段引用不能换，不代表 List 里面的内容不能变。

如果想让 List 也不可变，可以在构造时复制：

```java
public record SqlPlan(List<String> inserts) {
    public SqlPlan {
        inserts = List.copyOf(inserts);
    }
}
```

---

## 6. record 的构造方法

record 默认会生成完整构造方法：

```java
public record User(String name, int age) {}
```

可以直接这样创建：

```java
User user = new User("Tom", 18);
```

如果需要校验参数，可以写紧凑构造方法：

```java
public record User(String name, int age) {
    public User {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name is required");
        }
        if (age < 0) {
            throw new IllegalArgumentException("age must not be negative");
        }
    }
}
```

这里不需要写：

```java
this.name = name;
this.age = age;
```

Java 会自动赋值。

---

## 7. record 自动生成 toString()

例如：

```java
User user = new User("Tom", 18);
System.out.println(user);
```

输出类似：

```text
User[name=Tom, age=18]
```

普通 class 如果不重写 `toString()`，通常会输出类似：

```text
com.example.User@5a07e868
```

record 对调试更友好。

---

## 8. record 自动生成 equals() 和 hashCode()

record 的 `equals()` 默认按字段值比较。

例如：

```java
User a = new User("Tom", 18);
User b = new User("Tom", 18);

System.out.println(a.equals(b));
```

结果是：

```java
true
```

如果是普通 class，除非自己重写 `equals()`，否则默认比较对象地址。

---

## 9. 项目中的 JobConfig record

项目里的 `JobConfig` 是一个典型 record：

```java
public record JobConfig(
        Kafka kafka,
        Paimon paimon,
        Checkpoint checkpoint,
        int parallelism,
        List<Event> events) {
```

它表示整个 YAML 配置文件对应的 Java 数据结构。

里面还定义了嵌套 record：

```java
public record Kafka(
        String bootstrapServers,
        String topic,
        String groupId,
        String startupMode) {}

public record Paimon(
        String warehouse,
        String database,
        String s3Endpoint,
        String s3AccessKey,
        String s3SecretKey,
        boolean pathStyleAccess) {}

public record Checkpoint(
        long intervalMs,
        long timeoutMs,
        int tolerableFailures,
        String storageUri,
        String savepointUri) {}

public record Event(
        String eventId,
        String table,
        List<String> primaryKey,
        Map<String, String> options,
        List<Field> fields) {}

public record Field(
        String name,
        String type,
        String jsonPath,
        boolean required) {}
```

这些 record 共同描述了 YAML 的结构。

例如 YAML 中：

```yaml
kafka:
  bootstrapServers: "kafka:29092"
  topic: "tracking-events"
  groupId: "paimon-ods-v1"
  startupMode: "group-offsets"
```

会被映射成：

```java
config.kafka().bootstrapServers();
config.kafka().topic();
config.kafka().groupId();
config.kafka().startupMode();
```

---

## 10. 项目中的 SqlPlan record

项目里的 `SqlPlan`：

```java
public record SqlPlan(
        String catalogDdl,
        String databaseDdl,
        String sourceDdl,
        List<String> tableDdls,
        List<String> inserts) {}
```

它表示一组已经生成好的 SQL 语句。

其中：

- `catalogDdl`：创建 Paimon catalog 的 SQL
- `databaseDdl`：创建 database 的 SQL
- `sourceDdl`：创建 Kafka source table 的 SQL
- `tableDdls`：创建 Paimon 目标表的 SQL 列表
- `inserts`：写入目标表的 INSERT SQL 列表

使用方式：

```java
SqlPlan plan = new SqlPlanBuilder().build(config);

tableEnv.executeSql(plan.catalogDdl());
tableEnv.executeSql(plan.databaseDdl());
tableEnv.executeSql(plan.sourceDdl());
plan.tableDdls().forEach(tableEnv::executeSql);
plan.inserts().forEach(statementSet::addInsertSql);
```

---

## 11. record 适合什么场景

适合：

- 只保存数据的对象
- 配置结构
- 查询结果
- 方法返回多个字段
- DTO
- 不需要 setter 的对象

例如：

```java
public record JobArguments(Path registry, String restorePath) {}
```

这种很适合。

---

## 12. record 不适合什么场景

不太适合：

- 对象状态会频繁变化
- 需要很多 setter
- 有复杂生命周期
- 需要继承其他 class
- 主要职责不是承载数据，而是封装复杂行为

例如下面这种就不适合：

```java
public record Order(...) {
    public void pay() {}
    public void cancel() {}
    public void refund() {}
}
```

如果对象有大量业务状态变化，普通 class 通常更合适。

---

## 13. record 的一句话总结

`record` 是 Java 用来写数据类的简洁语法。

它帮我们自动生成：

- 构造方法
- 字段访问方法
- `equals()`
- `hashCode()`
- `toString()`

在当前项目中：

- `JobConfig` 用 record 表示配置文件结构
- `SqlPlan` 用 record 表示生成好的 SQL 执行计划
- `JobArguments` 用 record 表示命令行参数解析结果

核心记法：

```java
public record Xxx(字段类型 字段名, 字段类型 字段名) {}
```

读取字段：

```java
对象.字段名()
```
