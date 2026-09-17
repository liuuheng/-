# Flink + Fluss + Paimon + Doris 架构补齐方案

> 背景画像：中等团队（6–15 人）/ 混合部署（部分自建 + 部分云）/ 日增 100GB–1TB / 场景覆盖实时大屏、BI 报表、对外数据服务、实时数仓 ETL。

---

## 一、核心架构分工

- **Flink**：所有计算（CDC、ETL、join、聚合、维表 lookup）
- **Fluss**：实时热数据层（秒级新鲜度、主键表做点查/lookup、短窗口保留）
- **Paimon**：长期全量底座（权威数据源，离线 + 准实时统一一份）
- **Doris**：查询层
  - **外表查 Paimon** 为主（明细、大范围查询、BI 报表）
  - **内表加速** 高并发看板/对外 API 的汇总层（DWS）

---

## 二、按优先级的补齐清单（P0 → P2）

### P0：先上这些，才能"跑起来、稳得住"

1. **统一 Catalog**
   - 推荐 **Hive Metastore**（最成熟，Flink/Doris/Paimon 都原生支持）
   - 更现代可选 **Gravitino** 或 **Nessie**
   - 解决 Flink 和 Doris 看同一张 Paimon 表的问题

2. **Flink 作业平台**
   - 推荐 **StreamPark** 或 **Dinky**（都是开源，社区活跃，支持 SQL 开发 + 提交 + 版本）
   - 避免 Flink SQL 散落在各自 jar 包/脚本里

3. **对象存储 / HDFS 底座**
   - 混合部署通常：自建 → HDFS/MinIO；云上 → S3/OSS
   - Paimon 和 Fluss Remote Storage **共用同一个底座**最省心

4. **监控 + 告警（最容易被忽略但最重要）**
   - **Prometheus + Grafana** 收集 Flink / Fluss / Doris 指标
   - 至少监控：**作业失败、Checkpoint 失败、消费 lag、Paimon compaction 落后、Doris 慢查询**
   - 告警推到企业微信/飞书/钉钉

5. **冷热分层 & TTL 策略（一开始就定好）**
   - Fluss：`table.log.ttl` 建议 **3–7 天**（足够回放和容灾恢复）
   - Paimon：按日分区，**历史分区长期保留**；配好 **compaction / snapshot expire**
   - Doris 内表：**只放 DWS 汇总层**，保留 30–90 天滚动

---

### P1：3–6 个月内补齐（治理和效率）

6. **数据质量 (DQ)**
   - 不一定上 Great Expectations 这种重框架
   - 先用 **Flink SQL + 一张 DQ 规则表** 做基础校验（空值率、主键唯一、延迟阈值）也够用

7. **血缘与数据资产（DataHub）**
   - 中等团队强烈推荐上 **DataHub**，Flink / Paimon / Doris 都有现成 connector
   - 能回答"这个指标从哪来"是中等团队的刚需

8. **批调度**
   - 推荐 **DolphinScheduler**（中文社区好、国产生态友好）或 **Airflow**
   - 跑离线回补、历史重刷、Paimon compaction 维护任务

9. **对外数据服务网关**
   - 别让业务直连 Doris
   - 简单场景：用 **Nginx + 自研 HTTP 服务** 包一层
   - 复杂场景：上 **Kong / APISIX** 做网关（限流、鉴权、缓存）
   - 高 QPS 指标查询前面加 **Redis 缓存**

10. **权限体系**
    - 至少库/表级权限
    - Doris 自带 RBAC 够用，Paimon 侧靠 Catalog 层权限控制

---

### P2：随业务增长再上（非立刻需要）

11. **Kafka/Pulsar 前置缓冲**：只有在上游写入压力非常大、多消费者共享原始流时才需要
12. **Feast 特征平台**：将来做 AI 特征再考虑
13. **OpenLineage 精细血缘**：DataHub 够用后再精细化
14. **跨机房灾备**：数据量更大、SLA 要求更高时再做

---

## 三、实施路线图（6 个月可落地）

| 时间 | 重点任务 |
| --- | --- |
| 第 1 个月 | Catalog 统一 + StreamPark/Dinky + 监控告警（P0 的 1/2/4） |
| 第 2 个月 | Fluss/Paimon/Doris TTL 分层策略 + Doris 外表对接（P0 的 5） |
| 第 3 个月 | 数据质量规则 + 对外 API 网关 + Redis 缓存（P1 的 6/9） |
| 第 4–5 个月 | DataHub 血缘 + DolphinScheduler 批调度（P1 的 7/8） |
| 第 6 个月 | 权限体系收口 + 容量复盘 + 成本优化（P1 的 10） |

---

## 四、三条关键建议

1. **Paimon 是底座，别让 Doris 变成"数据副本库"**
   - Doris 内表只放"对外服务的 DWS 汇总层"，其它查询走外表

2. **Fluss 不要当长期存储用**
   - 热窗口 3–7 天就够；历史全部交给 Paimon

3. **监控先行**
   - 这套链路跨 4 个组件，**端到端延迟监控**是你能否稳住生产的关键

---

## 五、数据流向参考

```
上游业务库 (MySQL/PG)
        │
        │  Flink CDC
        ▼
      Fluss  ────────── 秒级热数据层（3-7 天）
        │
        │  datalake tiering（分钟级）
        ▼
     Paimon  ────────── 长期全量底座（权威数据源）
        │
        ├── Doris 外表查询（明细 / BI 报表 / 大范围查询）
        │
        └── Flink 批作业 → Doris 内表（DWS 汇总层）
                                  │
                                  ▼
                        对外 API 网关 + Redis 缓存
                                  │
                                  ▼
                          业务系统 / 大屏 / BI
```
