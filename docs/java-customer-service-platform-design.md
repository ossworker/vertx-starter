# Java 生产级客服平台完整方案（IM + 工单/SLA）

> 基线：Java 21 · Vert.x 5.1.7 · Spring Boot 3.5.14 · PostgreSQL · Zenoh（事件面）· GoRules ZEN
> 落地位置：本仓库 `/Users/apple/IDE/ideaProjects/vertx-starter` 新增 `customer-service/` 多模块
> 交付物：本方案文档。不含业务代码、不含骨架、不改动现有模块。
> 编写日期：2026-09-06

---

## 0. 阅读指引

本文吸收三份源设计：

| 源文档 | 采纳方式 |
|---|---|
| `production-customer-service-im-architecture.md` | **全量采纳**为主干（IM、传输分级、派单、消息协议、Zenoh topic、DDL），但传输优先级按 Java 现实重排 |
| `rust-customer-service-helpdesk-complete-design.md` | 采纳 **Conversation/Ticket 分离、Ticket 状态机、SLA 独立域、Queue+Routing 打分、Outbox、多租户/权限** |
| `rust-customer-agent-runtime-design.md` | **不落设计**，仅保留扩展接缝（第 13.6 节），Phase 1 不做 |

文中每条技术结论都来自实测代码或已核实的外部事实，第 2 章与附录 A 给出可追溯清单。**凡 Java 生态不具备的能力，本文直接写"不可行"并给出替代路径，不做能力承诺。**

---

## 1. 目标、非目标与 Rust→Java 等价替换

### 1.1 目标

构建可水平扩展的生产级客服平台，覆盖：

- 消费者发起咨询 → 自动创建会话 → 规则决策 → 自动/手动派单 → 实时双向通信 → 转接/结束
- 多类型消息（文本/富文本/图片/视频/音频/文件/卡片/动作卡/位置/系统通知/撤回/表情回应）
- Conversation 与 Ticket 双聚合，工单闭环、SLA 计时与超时升级
- 坐席、技能、队列、排班、容量、Presence
- 多租户、RBAC、审计、限流、可观测性
- 服务端多节点，Zenoh 作为事件面，PostgreSQL 作为唯一真相源

### 1.2 非目标（Phase 1 明确不做）

1. **AI Agent Runtime / RAG / Tool Calling / WASI 插件沙箱**——仅留接缝。
2. **实时音视频媒体流**——视频/音频消息只承载对象存储元数据，不做 WebRTC/MoQ。
3. **WebTransport 生产落地**——见 1.4，JVM 上不可行，仅留协商位与 SPI。
4. **全文检索（OpenSearch）**——Phase 1 用 Postgres 全文索引兜底。
5. **Zenoh 承载强一致状态**——见第 4 章原则。

### 1.3 Rust→Java 等价替换表

| 源文档能力 | Rust 技术 | Java 落地 | 结论 |
|---|---|---|---|
| HTTP 服务 | Axum | Vert.x 5 `HttpServer` + `Router` | 等价 |
| 异步运行时 | Tokio | Vert.x event loop + Java 21 | 等价 |
| DB 访问 | SQLx | `vertx-pg-client` 5.1.x + MyBatis（异步签名） | 等价 |
| 规则/决策 | Zen Engine (Rust) | **`io.gorules:zen-engine:2.0.2`** 官方 Java 绑定（UniFFI over Rust，jar 内含 linux-x86-64 / linux-aarch64 / darwin / win 原生库，支持 JDM 求值与 `ZenLoader` 热加载） | **可用**；兜底 Drools 10.2 / Kogito DMN |
| 事件网格 | Zenoh (Rust) | 本仓库自研 `zenoh-jni`，或官方 `org.eclipse.zenoh:zenoh-java:1.10.0` + `zenoh-flat-jni-jvm:1.10.0` | 可用，但有原生库约束（第 10 章） |
| LLM Brain | Rig | —— | 超范围，仅接缝 |
| 插件沙箱 | WASI Component Model | —— | 超范围（JVM 侧 Wasm 宿主 chicory/wasmtime-java 均不满足 Component Model + WIT 生产要求） |
| WebTransport 服务端 | `wtransport` | **无可生产落地的 JVM 实现** | **不可行**，见 1.4 |
| 对象存储 | S3 SDK | AWS SDK v2 异步客户端 / MinIO SDK | 等价 |

### 1.4 关于 WebTransport 的现实结论（必须先讲清）

源文档 1 把 WebTransport 设为首选传输。**在 Java 上这个前提不成立**：

- Maven Central 上没有可用于生产的 JVM WebTransport 服务端实现；Netty 的 extended CONNECT（RFC 9220）支持在 2025-11 才合入主干，尚未随稳定版发布；`webtransport4j` 未发布正式版本。
- 常见入口层 **nginx / Envoy / HAProxy / Caddy 均不终结 WebTransport**，无法用网关兜住。
- Vert.x 5.1.7 的 HTTP/3 是 GA，但那是 HTTP/3 请求响应与 QUIC，**不等于 WebTransport 会话**。

因此本方案：

1. **WebSocket 是生产主力传输**；
2. **SSE + HTTP Command 是保底传输**（企业代理/老浏览器/受限网络）；
3. **WebTransport 只保留能力协商位与 `ImTransport` SPI 实现桩**，注明唯一可行落地是 Rust `wtransport` 或 Go `webtransport-go` 边车，边车与 Java 之间用统一 Envelope 桥接。上层业务代码因此无需改动。

### 1.5 关于 HTTP/3 的现实结论

- Vert.x 5.1.7 的 HTTP/3 与 QUIC **是 GA 且在 `vertx-core` 内**：`HttpServerConfig.setVersions(HttpVersion.HTTP_3)`、`Http3ServerConfig`、`QuicServer` / `Vertx.createQuicServer`。
- **`HttpServerOptions` 上没有 H3 开关**——只能走 `HttpServerConfig` 新配置模型。
- 需额外引入可选依赖 `io.netty:netty-codec-native-quic`，且**必须带 classifier**（`linux-x86_64` / `linux-aarch_64` / `osx-aarch_64` …），镜像构建要按目标架构固定。
- **RFC 8441（WebSocket over HTTP/2）Vert.x 不支持**，所以 WebSocket 长连接继续走 HTTP/1.1 Upgrade。

结论：**HTTP/3 先只用于控制面 REST API**（提升移动端弱网首屏与 API 延迟），**不用于 IM 长连接**。

---

## 2. 现有仓库能力盘点与缺口清单

### 2.1 可复用（直接依赖）

| 模块 | 复用点 |
|---|---|
| `vertx-spring-boot-autoconfigure` | `Vertx` Bean 生命周期、`VertxProperties`、Spring Boot 自动装配约定 |
| `vertx-spring-boot-cluster-zenoh` | Zenoh `Session` 管理、`KeyPrefixStrategy`（根 `vertx/{app}/{env}`）、`ZenohClusterProperties` 配置前缀 |
| `zenoh-jni` | Session/Publisher/Subscriber/Queryable/**Querier**/KeyExpr/ZBytes/Config/EmbeddedRouter 句柄式 API |
| `vertx-spring-boot-mybatis` | `VertxExecutor`、`VertxMapperProxy`、`SqlResolver`、`VertxSqlTemplate`（`#{name}` 命名参数） |
| `vertx-spring-boot-web-server` | **仅控制面 REST**（WebFlux `@RestController`） |
| `data-dict` 的 Caffeine + Zenoh 两级缓存 | 作为字典/配置缓存的通用件 |

### 2.2 必须绕过（不修，Phase 1 不动）

**`vertx-spring-boot-web-server` 不能承载 IM 数据面**，实测原因：

1. `VertxServerHttpResponse.writeWithInternal()` 执行 `Flux.from(body).collectList()` 后单次 `response.end(...)` → **SSE/流式响应不可用**：无限 Flux 永远挂住；`writeAndFlushWithInternal()` 是 `Flux.from(body).flatMap(this::writeWithInternal).then()`，**首个元素就把响应 end 掉**。
2. `VertxWebServer` 只注册 `requestHandler(handler)`，**从未注册 `webSocketHandler`** → 没有 WebSocket 能力。
3. `HttpServer` / `HttpServerOptions` 均为 private 字段，**没有对外定制点**，无法注入 H2/H3/WS/背压配置。
4. TLS 只有 `options.setSsl(true)`，**证书链路未接线**。

修它等于重写整条写路径，且即便修完仍拿不到 WebSocket 帧级 API 与逐连接背压。**决策：IM 数据面另起独立端口的原生 Vert.x `HttpServer` + `Router`。**

### 2.3 必须补齐（缺口）

| 缺口 | 现状 | 归属 |
|---|---|---|
| `io.vertx.sqlclient.Pool` Bean | **全仓不存在**，也没有数据源配置 → mybatis 模块的依赖未被满足 | `cs-infra-persistence` 提供，顺带补上 mybatis 模块缺的 Bean |
| 事务 | `VertxDataSourceTransactionManager` thread-bound + `.join()` → `@Transactional` 在事件循环上不可用 | 统一改用 `pool.withTransaction`，**禁用 `@Transactional`** |
| 流式游标 | `VertxCursor` 非流式（预加载全部结果） | 大结果集走 `RowStream`/分页，不用 Cursor |
| Redis | 全仓无 | `cs-infra-cache` 引入 `vertx-redis-client` |
| 对象存储 | 全仓无 S3/MinIO | `cs-infra-storage` |
| 认证鉴权 | 全仓无 JWT/OAuth/Security | `cs-common` + 网关过滤器 |
| Zenoh 生产缺口 | 见第 10 章 4 项必修 | `vertx-spring-boot-cluster-zenoh` 的 `support` 子包 |

### 2.4 Vert.x 集群模式的处置

实测 `ZenohClusterManager` 存在三个硬问题：

- `registrations` 是纯本地 `ConcurrentHashMap`，**无人订阅** → 跨节点 `eventBus.send()` 实质失效；
- EventBus 相关类是旁路通道，**reply 写死 dummy**；
- 节点发现是 5s `setPeriodic` 的 `session.put(nodes/{nodeId})`，**无 TTL/租约** → 崩溃节点永不淘汰；
- `getSyncMap` 通过 `.await()` 阻塞；`ZenohMapStore` 的 CAS 本地生效后广播 → **跨节点 last-writer-wins**；`ZenohLock` 自述 best-effort；所有 Zenoh 接收路径是阻塞 `recv()` 守护线程，**无 `runOnContext` 回跳**；`ZenohAsyncMap` 用 **JDK 序列化**。

**决策：Phase 1 设 `vertx.cluster.enabled=false`**，跨节点通信全部走自研 `EventMesh`（第 9 章），把上述问题移出关键路径。Zenoh ClusterManager 的补齐作为独立技术债排期（第 10 章）。

---

## 3. 总体架构与分层原则

```text
                     Customer Client            Agent Console
                            |                         |
                    GET /im/capabilities  (能力协商)
                            |                         |
        +-------------------+------------+------------+-------------------+
        |                   |            |            |                   |
        v                   v            v            v                   v
   WebSocket           SSE + POST    WebSocket    WebTransport(桩)   REST/HTTP3
   (生产主力)           /im/command    (主力)       仅协商位          (控制面)
        |                   |            |            |                   |
        +-------------------+------------+------------+                   |
                            |                                            |
              ┌─────────────▼──────────────┐              ┌──────────────▼─────────────┐
              │  IM Data Plane :9443       │              │ Control Plane :8080        │
              │  原生 Vert.x HttpServer     │              │ WebFlux (现有 web-server)   │
              │  + Router (绕过 WebFlux)    │              │ 管理/配置/报表/工单 CRUD     │
              └─────────────┬──────────────┘              └──────────────┬─────────────┘
                            |                                            |
                            +---------------------+----------------------+
                                                  |
                                      Unified ImEnvelope
                                                  |
                        ┌─────────────────────────▼─────────────────────────┐
                        │              Application (用例编排)                │
                        │  Conversation / Message / Dispatch / Ticket / SLA  │
                        └─────────────────────────┬─────────────────────────┘
                                                  |
                     ┌────────────────────────────┼────────────────────────────┐
                     v                            v                            v
             ┌───────────────┐          ┌──────────────────┐         ┌──────────────────┐
             │ Domain (纯 Java)│          │  RuleEngine SPI  │         │  EventMesh SPI   │
             │ 聚合/状态机/不变式│          │  ZEN / Java 决策表│         │  Zenoh 实现       │
             └───────┬───────┘          └──────────────────┘         └────────┬─────────┘
                     v                                                        v
        ┌────────────────────────┐                                  ┌──────────────────┐
        │  PostgreSQL 唯一真相源  │                                  │  Zenoh 事件面     │
        │  seq / 幂等 / CAS / 锁  │  ──── Outbox Relay ──────────────▶│  Pub/Sub·Querier │
        │  状态迁移 / SLA due 表  │                                  └──────────────────┘
        └────────────┬───────────┘
                     |
          ┌──────────┴──────────┐
          v                     v
       Redis              Object Storage
    (presence/限流/路由表)   (S3/MinIO 附件)
```

### 3.1 三条不可违反的分层原则

**原则一：Transport ≠ 协议。** 业务代码里不允许出现 `if (websocket)` / `if (sse)` / `if (webtransport)`。统一抽象：

```text
ImTransport
├── type(): TransportType
├── send(connectionId, ImEnvelope): Future<Void>
├── supportsClientPush(): boolean     // SSE 为 false
└── close(connectionId, CloseReason)
```

实现：`WebSocketTransport` / `SseTransport` / `WebTransportTransport`(桩)。业务层只操作 `ImEnvelope`。

**原则二：Zenoh ≠ 数据库。** Zenoh 只做事件面：事件 fan-out、定向节点投递、presence 提示、缓存失效、`Querier`+`Queryable` 请求应答。**所有需要线性一致的语义一律落 Postgres**：会话序列号、幂等、坐席容量 CAS、分布式锁、聚合状态迁移。核实依据：Zenoh **没有 CAS / 线性一致原语**，liveliness token **没有租约自动过期**。

**原则三：规则引擎 ≠ 坐席选择。** `RuleEngine` 只产出无副作用的 `RoutingDecision`（该找什么样的坐席）；`AgentSelector` 才接触实时状态并执行容量 CAS（找到具体哪个坐席）。规则引擎绝不持有实时连接、绝不写库。

**补充原则：** Domain 不依赖 Vert.x / MyBatis / Zenoh / Spring；Infrastructure 实现 Repository；Application 编排 Domain；Transport 只做协议转换。

---

## 4. 模块划分与依赖方向

```text
vertx-starter/
└── customer-service/                    (pom 聚合)
    ├── cs-common                        工具、错误码、ID 生成、租户上下文、脱敏
    ├── cs-contract                      Envelope/Command/Event/DTO/枚举（对外契约，零框架依赖）
    ├── cs-domain                        聚合、值对象、状态机、领域事件、Repository 接口
    ├── cs-application                   用例编排、事务边界、Outbox 写入、SLA 编排
    ├── cs-infra-persistence             Pool Bean、Repository 实现、Outbox Relay、DDL/Flyway
    ├── cs-infra-cache                   Redis：presence、限流、连接路由表
    ├── cs-infra-storage                 S3/MinIO：预签名上传、缩略图元数据
    ├── cs-infra-mesh                    EventMesh SPI 的 Zenoh 实现、CBOR codec
    ├── cs-transport-core                ImTransport SPI、ConnectionRegistry、背压、协商
    ├── cs-transport-ws                  WebSocket 实现
    ├── cs-transport-sse                 SSE + HTTP Command 实现
    ├── cs-transport-webtransport        桩：仅能力位 + 边车桥接说明
    ├── cs-rule-engine                   RuleEngine SPI、RoutingDecision、灰度分桶、shadow 对账
    ├── cs-rule-zen                      GoRules ZEN 实现（可选依赖）
    ├── cs-bootstrap                     Spring Boot 应用、两端口装配、健康检查
    └── cs-testkit                       Testcontainers、协议一致性测试、ArchUnit 规则
```

### 4.1 artifactId 与坐标

groupId 沿用仓库现有 `com.workoss.boot`，版本由根 pom 统一管理。

| artifactId | 允许依赖 |
|---|---|
| `cs-common` | 无（仅 JDK + slf4j） |
| `cs-contract` | `cs-common` |
| `cs-domain` | `cs-common`, `cs-contract` |
| `cs-application` | `cs-domain`, `cs-rule-engine` |
| `cs-infra-*` | `cs-application`（实现其接口）+ 各自技术栈 |
| `cs-transport-*` | `cs-transport-core`, `cs-contract` |
| `cs-transport-core` | `cs-contract`, Vert.x core |
| `cs-rule-zen` | `cs-rule-engine` + `io.gorules:zen-engine`（optional） |
| `cs-bootstrap` | 全部 |

### 4.2 依赖方向（严格单向，ArchUnit 强制）

```text
cs-bootstrap
    ├──▶ cs-transport-ws ──┐
    ├──▶ cs-transport-sse ─┼──▶ cs-transport-core ──▶ cs-contract
    ├──▶ cs-transport-webtransport ─┘
    ├──▶ cs-infra-persistence ─┐
    ├──▶ cs-infra-cache ───────┤
    ├──▶ cs-infra-storage ─────┼──▶ cs-application ──▶ cs-domain ──▶ cs-contract ──▶ cs-common
    ├──▶ cs-infra-mesh ────────┘                 └──▶ cs-rule-engine
    └──▶ cs-rule-zen ──────────────────────────────────▶ cs-rule-engine
```

禁止边（ArchUnit 断言）：
- `cs-domain` 不得引用 `io.vertx..`、`org.apache.ibatis..`、`com.workoss.zenoh..`、`org.springframework..`
- `cs-transport-*` 不得引用 `cs-infra-persistence`
- `cs-application` 不得引用任何 `cs-infra-*` 与 `cs-transport-*` 的实现类

### 4.3 Zenoh 补齐件的归属

Zenoh 的 4 项必修补齐**放进已有 `vertx-spring-boot-cluster-zenoh` 的 `support` 子包**，不在 `customer-service/` 里另建一套 Zenoh 封装。理由：避免两处各自拼 key 前缀导致 `KeyPrefixStrategy` 语义分叉。`cs-infra-mesh` 只消费该模块暴露的 `Session` 与前缀策略。

---

## 5. 包结构与核心类型清单

### 5.1 cs-contract

```text
com.workoss.cs.contract
├── envelope
│   ├── ImEnvelope            version/id/type/timestamp/tenantId/conversationId/traceId/payload
│   ├── EnvelopeType          COMMAND | EVENT | ACK | ERROR | HEARTBEAT
│   └── Ack                   requestId/status/seq/serverTime/error
├── command                   *Command 记录类（见 6.2）
├── event                     *Event 记录类（见 6.3）
├── message
│   ├── MessageType           TEXT|MARKDOWN|IMAGE|VIDEO|AUDIO|FILE|RICH_TEXT|CARD|
│   │                         ACTION_CARD|PRODUCT_CARD|ORDER_CARD|LOCATION|LINK|
│   │                         SYSTEM|NOTICE|RECALL|REACTION
│   └── content               TextContent / ImageContent / FileContent / CardContent ...
└── error                     ErrorCode（分域：AUTH_/PROTO_/BIZ_/RATE_/SYS_）
```

**约定：** `cs-contract` 全部使用 Java 21 `record`，序列化统一 `io.vertx.core.json.JsonObject`（遵循仓库既有约定：不引 Jackson `ObjectMapper`）；跨节点二进制走 CBOR（第 9.4 节）。

### 5.2 cs-domain

```text
com.workoss.cs.domain
├── conversation   Conversation(聚合根) ConversationStatus ConversationMember
├── message        Message MessageId Sequence
├── customer       Customer CustomerTag CustomerNote
├── agent          Agent AgentStatus AgentSkill AgentCapacity
├── queue          Queue QueueMember
├── assignment     Assignment AssignmentStatus
├── ticket         Ticket(聚合根) TicketStatus TicketArticle TicketTask TicketRelation
├── sla            SlaPolicy SlaInstance SlaEvent BusinessCalendar
├── shared         TenantId Priority Language Channel AggregateVersion
└── event          DomainEvent + 各领域事件（见 6.3）
```

**聚合边界：** `Conversation` 与 `Ticket` 是**两个独立聚合**，互相只持有对方 ID，跨聚合副作用只经 Outbox（第 12.2 节）。

### 5.3 cs-transport-core

```text
com.workoss.cs.transport
├── ImTransport / TransportType / CloseReason
├── Connection            connectionId/tenantId/principalId/deviceId/transport/subscriptions/lastAckSeq
├── ConnectionRegistry    本地 Map + Redis 路由表 + Zenoh presence 广播
├── EnvelopeCodec         JSON(文本帧) / CBOR(二进制帧)
├── Backpressure          WriteQueueGuard / DropPolicy / OverflowStrategy
├── Negotiation           CapabilitiesResponse / TransportPreference
└── dispatch              CommandRouter / EventFanout
```

### 5.4 cs-application 用例（部分）

```text
CreateConversationUseCase        SendMessageUseCase        RecallMessageUseCase
AcceptAssignmentUseCase          RejectAssignmentUseCase   TransferConversationUseCase
CloseConversationUseCase         SyncMessagesUseCase       ChangeAgentStatusUseCase
CreateTicketUseCase              UpdateTicketUseCase       EscalateTicketUseCase
ResolveTicketUseCase             StartSlaUseCase           CheckSlaBreachUseCase
DispatchConversationUseCase      IssueUploadUrlUseCase
```

---

## 6. 统一协议

### 6.1 Envelope

```json
{
  "version": 1,
  "id": "req_10001",
  "type": "MESSAGE_SEND",
  "kind": "COMMAND",
  "timestamp": 1788600000000,
  "tenantId": "t1",
  "conversationId": "c100",
  "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
  "payload": {}
}
```

规则：
- `id` 即幂等 `requestId`，客户端生成，同一命令重试必须复用；
- `version` 为协议大版本，服务端在 `/im/capabilities` 声明支持区间；
- `traceId` 采用 W3C trace-id 格式，贯穿传输→应用→DB→Zenoh→对端（第 18 章）；
- 未知字段必须被接收方忽略（前向兼容）；未知 `type` 回 `ERROR` 而非断连。

### 6.2 Command（客户端→服务端）

```text
MESSAGE_SEND          MESSAGE_RECALL          MESSAGE_REACTION
CONVERSATION_CREATE   CONVERSATION_CLOSE      CONVERSATION_TRANSFER
CONVERSATION_SYNC     (按 seq 拉取补齐)
AGENT_ACCEPT          AGENT_REJECT            AGENT_STATUS_SET
TYPING_START          TYPING_STOP
READ_RECEIPT          HEARTBEAT
TICKET_CREATE         TICKET_UPDATE           TICKET_COMMENT
```

每个 Command 必须回 `ACK`，`ACK` 携带服务端分配的 `seq` 与 `serverTime`。

### 6.3 Event（服务端→客户端）与领域事件

```text
MESSAGE_CREATED  MESSAGE_UPDATED  MESSAGE_RECALLED  MESSAGE_REACTION_CREATED
CONVERSATION_CREATED  CONVERSATION_ASSIGNED  CONVERSATION_TRANSFERRED  CONVERSATION_CLOSED
ASSIGNMENT_OFFERED  ASSIGNMENT_ACCEPTED  ASSIGNMENT_REJECTED  ASSIGNMENT_TIMEOUT
AGENT_STATUS_CHANGED  AGENT_PRESENCE_CHANGED
DISPATCH_STARTED  DISPATCH_COMPLETED  DISPATCH_FAILED
TICKET_CREATED  TICKET_ASSIGNED  TICKET_PRIORITY_CHANGED  TICKET_ESCALATED
TICKET_RESOLVED  TICKET_CLOSED  TICKET_REOPENED
SLA_STARTED  SLA_WARNING  SLA_BREACH
SYSTEM_NOTICE  TYPING  READ_RECEIPT_UPDATED
```

### 6.4 消息类型与内容体

```json
// TEXT
{"type":"TEXT","content":{"text":"你好"}}

// IMAGE
{"type":"IMAGE","content":{"fileId":"f100","url":"https://obj/...","thumbnail":"...",
 "width":1200,"height":800,"size":204800,"mimeType":"image/jpeg"}}

// FILE
{"type":"FILE","content":{"fileId":"f200","name":"invoice.pdf","size":1024000,
 "mimeType":"application/pdf","url":"..."}}

// ACTION_CARD
{"type":"ACTION_CARD","content":{"title":"订单异常","description":"订单配送异常",
 "actions":[{"id":"view","label":"查看订单","action":"OPEN_ORDER","params":{"orderId":"10001"}},
            {"id":"refund","label":"申请退款","action":"CREATE_REFUND","params":{"orderId":"10001"}}]}}
```

约束：
- 媒体类消息**只存元数据 + 对象存储地址**，字节流永不进 IM 通道；
- 单条消息 `content_json` 硬上限 32 KiB，超限拒绝（`PROTO_PAYLOAD_TOO_LARGE`）；
- `ACTION_CARD` 的 `action` 必须在服务端白名单内，`params` 不可携带任何权限信息（服务端重新鉴权）。

### 6.5 协议版本演进

| 变更类型 | 允许 | 做法 |
|---|---|---|
| 新增可选字段 | ✅ | 不升版本 |
| 新增 Command/Event 类型 | ✅ | 不升版本，旧端忽略未知 type |
| 字段语义变更/删除/必填化 | ❌ | 升 `version`，双版本并行 ≥2 个发布周期 |

服务端同时支持 `version` 的一个闭区间，`/im/capabilities` 返回 `{"minVersion":1,"maxVersion":1}`。

---

## 7. 传输层

### 7.1 能力协商

```http
GET /im/capabilities
```

```json
{
  "protocolVersion": {"min": 1, "max": 1},
  "transports": [
    {"type": "WEBSOCKET",    "enabled": true,  "url": "wss://host:9443/im/ws",
     "preference": 100},
    {"type": "SSE",          "enabled": true,  "url": "https://host:9443/im/events",
     "commandUrl": "https://host:9443/im/command", "preference": 50},
    {"type": "WEBTRANSPORT", "enabled": false, "reason": "NOT_AVAILABLE_ON_THIS_DEPLOYMENT",
     "preference": 0}
  ],
  "limits": {"maxFrameBytes": 65536, "maxContentBytes": 32768,
             "heartbeatIntervalMs": 25000, "maxSubscriptions": 200}
}
```

客户端降级链：**WebSocket → SSE + HTTP Command**。`WEBTRANSPORT` 在部署开启边车时才 `enabled: true`。

### 7.2 WebSocket（生产主力）

基于 Vert.x 5 服务端 API（已核实）：

- 握手：`webSocketHandshakeHandler` 中完成鉴权与租户解析，然后 `accept()` / `reject(status)`；或在 Router 中 `request.toWebSocket()`。**握手阶段就要拒绝**未认证连接，不要连上再踢。
- 背压：`setWriteQueueMaxSize` + `writeQueueFull()` + `drainHandler`，逐连接生效（见 8.4）。
- 心跳：Vert.x **不会自动发 ping**，必须自己调度 `writePing` 并处理 `pongHandler`；服务端每 25s 一次 ping，连续 2 次无 pong 判定连接死亡。
- 帧：文本帧承载 JSON Envelope；二进制帧预留给 CBOR（移动端流量优化）。**不使用分片帧**，单帧上限 64 KiB。
- **RFC 8441（WS over HTTP/2）Vert.x 不支持** → WebSocket 走 HTTP/1.1 Upgrade，端口与控制面分离。

### 7.3 SSE + HTTP Command（保底）

```http
GET  /im/events        # Server → Client 事件流
POST /im/command       # Client → Server 命令
```

事件流格式：

```text
event: message
id: 10001
data: {...}

event: assignment
id: 10002
data: {...}
```

关键点：
- `id:` 必须写会话内 `seq`；重连时读 `Last-Event-ID` 请求头，与 `seq` 结合做断线恢复（第 8.5 节）；
- SSE 是单向的，`supportsClientPush()` 返回 `false`，命令一律走 `POST /im/command`，请求体就是 Envelope；
- 每 15s 发一次注释行 `: keepalive` 防中间代理超时断流；
- **SSE 必须实现在独立端口的原生 Vert.x `HttpServer` 上**，因为现有 WebFlux 适配层的写路径会把首元素当成响应结束（第 2.2 节）。

### 7.4 WebTransport 接缝

`cs-transport-webtransport` 只包含：

1. `WebTransportTransport implements ImTransport`，构造时检测边车可达性，不可达则 `enabled=false`；
2. 与边车之间的桥接协议说明：边车终结 QUIC/WebTransport，把每条 datagram/stream 消息转成同一个 `ImEnvelope`，通过本机 Unix Domain Socket 或 gRPC 送进 Java 进程；
3. 逻辑通道规划（沿用源文档，供边车实现者参照）：Control / Message / Event / File Stream 走可靠流；`typing`、`presence hint` 走 datagram。**`MESSAGE_SEND`、`ASSIGNMENT`、订单/退款类命令禁止走 datagram。**

Phase 1 该模块不参与部署，只保证编译与协商位正确。

### 7.5 端口/协议矩阵

| 端口 | 协议 | 用途 | 承载方式 |
|---|---|---|---|
| 8080 | HTTP/1.1 + H2 | 控制面 REST、管理后台、Webhook | 现有 `VertxReactiveWebServerFactory`（WebFlux） |
| 8443 | HTTP/3 (QUIC) | 控制面 REST（移动端弱网优化，可选） | `HttpServerConfig.setVersions(HTTP_3)` + `netty-codec-native-quic` |
| 9443 | HTTP/1.1 + WS + SSE | **IM 数据面** | 原生 Vert.x `HttpServer` + `Router`（绕过 WebFlux） |
| 4443/udp | WebTransport | 仅在部署边车时开启 | 非 JVM 边车 |
| 7447 | Zenoh TCP | 节点间事件面 | Zenoh router/peer |

---

## 8. 连接注册表、Presence、背压与恢复

### 8.1 连接注册表（三层）

```text
L1 本地      Map<String, Connection> localConnections     真实连接对象只在所属节点
L2 Redis     im:route:{tenantId}:{principalId} → Set<connectionId@nodeId>  TTL 60s
L3 Zenoh     presence 事件广播（提示性，非真相）
```

**铁律：WebSocket / SSE 的连接对象绝不放进 Redis 或 Zenoh。** 只传播：

```json
{"userId":"u100","nodeId":"node-01","connectionId":"conn-123",
 "transport":"WEBSOCKET","status":"ONLINE","lastSeen":1788600000000}
```

### 8.2 Presence 与心跳

- 客户端心跳：WebSocket 用 ping/pong（服务端主动 ping）；SSE 用 `POST /im/command` 的 `HEARTBEAT`。
- 节点侧每 20s 刷新 L2 Redis TTL（60s）；连接关闭立即删除 L2 条目并广播 `AGENT_PRESENCE_CHANGED`。
- 坐席 Presence 与 Agent 业务状态**分离**：`AgentPresence`（ONLINE/OFFLINE，由连接推导）vs `AgentStatus`（IDLE/BUSY/AWAY/BREAK，由人工或系统设置）。**只有 `AgentStatus == IDLE && availableCapacity > 0 && presence == ONLINE` 才允许自动派单。**

### 8.3 跨节点投递

```text
Customer ──WebSocket──▶ Node-01
                          │ 查 L2 路由表得知目标在 node-03
                          ▼
                    Zenoh: {root}/eventbus/inbox/node-03
                          ▼
                       Node-03 ──WebSocket──▶ Agent
```

Customer 与 Agent 可用不同传输，业务层无感知。若 L2 查不到路由，说明对端离线 → 走离线通知（第 17.5 节）。

### 8.4 背压与丢弃分级

Vert.x WebSocket 的写队列是逐连接的，用 `writeQueueFull()` 判定。当队列满：

| 消息级别 | 类型 | 策略 |
|---|---|---|
| P0 不可丢 | `MESSAGE_CREATED`、`ASSIGNMENT_*`、`TICKET_*`、`SLA_BREACH`、`ACK` | 入连接级有界缓冲（512 条）；缓冲也满 → **主动关连接**，让客户端重连后按 seq 补齐 |
| P1 可合并 | `AGENT_STATUS_CHANGED`、`READ_RECEIPT_UPDATED` | 同 key 覆盖式合并，只保留最新 |
| P2 可丢 | `TYPING`、`presence hint` | 直接丢弃，不计错误 |

`drainHandler` 触发后先冲 P0 缓冲，再恢复正常写入。**关连接优于无界堆积**——这是防 OOM 的底线。

### 8.5 按 seq 恢复

会话内 `sequence` 单调无空洞（第 15.1 节），因此：

```text
客户端记录 lastSeq = 10002
     │ 断线重连
     ▼
CONVERSATION_SYNC {conversationId, fromSeq: 10003, limit: 200}
     ▼
服务端返回 [10003..10202] + hasMore
```

SSE 走 `Last-Event-ID` 请求头等价处理。补齐上限 200 条/次，超过 2000 条差距则返回 `SYNC_TOO_FAR`，客户端改走 REST 历史消息分页接口。

---

## 9. Zenoh 事件面设计

### 9.1 Key Expression 规划

沿用现有 `KeyPrefixStrategy` 的根前缀 `vertx/{app}/{env}`（记为 `{root}`），避免与已有 key 空间冲突。客服业务 key 一律挂在 `{root}/cs/` 下：

```text
{root}/cs/{tenantId}/conversation/{conversationId}/message
{root}/cs/{tenantId}/conversation/{conversationId}/event
{root}/cs/{tenantId}/conversation/{conversationId}/typing
{root}/cs/{tenantId}/agent/{agentId}/status
{root}/cs/{tenantId}/agent/{agentId}/assignment
{root}/cs/{tenantId}/queue/{queueId}/event
{root}/cs/{tenantId}/ticket/{ticketId}/event
{root}/cs/{tenantId}/sla/event
{root}/cs/{tenantId}/cache/invalidate/{namespace}
{root}/cs/{tenantId}/system/event
```

已有 key（不动）：`{root}/nodes/{id}`、`{root}/nodeinfo/{id}`、`{root}/registrations/{address}/{nodeId}`、`{root}/cluster/maps/{map}/{key}`、`{root}/cluster/counters/{name}`、`{root}/cluster/locks/{name}`、`{root}/eventbus/inbox/{nodeId}`、`{root}/eventbus/pub/{address}`。

订阅粒度：节点按**在线连接的租户+会话集合动态声明 Subscriber**，不做 `**` 全量订阅（否则每节点收全网流量）。会话订阅数上限 5000/节点，超限拒绝新连接落到该节点。

### 9.2 Pub/Sub 与 Querier/Queryable 分工

| 场景 | 机制 |
|---|---|
| 消息/事件 fan-out 到多节点 | `put` + Subscriber |
| 定向单节点投递 | `put` 到 `{root}/eventbus/inbox/{nodeId}` |
| Typing / presence 提示 | `put`（可丢，无持久化） |
| 缓存失效广播 | `put` 到 `cache/invalidate/{namespace}` |
| **跨节点请求应答** | **`Querier.get()` / `getWithPayload(byte[])` + 对端 `Queryable`** |
| 新节点拉取全量共享态 | `session.get({root}/cs/**)` 向对端 `Queryable` 请求 |

注：`Querier` 在 `zenoh-jni` 中**已存在但当前未被使用**，本方案首次启用它做跨节点请求应答（例如"哪个节点持有连接 X"的兜底查询、坐席实时容量快照聚合）。

### 9.3 EventMesh SPI

```text
EventMesh
├── publish(KeyExpr, MeshMessage): Future<Void>
├── subscribe(KeyExpr, Handler<MeshMessage>): Future<Subscription>
├── request(KeyExpr, MeshMessage, Duration): Future<MeshMessage>   // Querier
└── serve(KeyExpr, Function<MeshMessage, Future<MeshMessage>>)     // Queryable
```

`cs-infra-mesh` 提供 `ZenohEventMesh` 实现。**Phase 1 因 `vertx.cluster.enabled=false`，所有跨节点通信走这个 SPI，不走 Vert.x EventBus。** 好处：Vert.x ClusterManager 的注册缺失、假 reply、best-effort Lock 全部与业务无关。

### 9.4 编解码（必须替换 JDK 序列化）

现状 `ZenohAsyncMap` 用 JDK 序列化 → 跨版本不兼容、有反序列化安全面。新 codec：

```text
[0]      magic  0xC5
[1]      codecVersion (u8)
[2..3]   flags (u16)  bit0=压缩(zstd) bit1=加密预留
[4..7]   payloadLen (u32, big-endian)
[8..]    CBOR payload
```

- CBOR 库：`com.upokecenter:cbor` 或 Jackson CBOR dataformat（仅在 mesh 模块内使用，不污染业务层的 `JsonObject` 约定）；
- payload > 4 KiB 才启用 zstd；
- 反序列化白名单：只允许 `cs-contract` 下的类型；
- 未知 `codecVersion` → 丢弃并计数告警，不抛异常打断订阅循环。

---

## 10. Zenoh 缺口补齐清单

### 10.1 必修（上线前，硬阻塞）

**Z-1 · Linux 原生库缺失（最高优先级）**

现状：`zenoh-jni/src/main/resources/` 下**只有 `darwin-arm64/libzenoh_ffi_jni.dylib`** → **无法部署到 Linux/K8s**。

两条路径：
- **路径 A（推荐）**：交叉编译出 `linux-x86_64/libzenoh_ffi_jni.so` 与 `linux-aarch64/libzenoh_ffi_jni.so`，随 jar 打包（沿用现有 classpath 自动加载机制）。注意 rustc 版本需与 zenoh 版本严格匹配（本仓库经验：zenoh 1.10.0 ↔ rustc 1.97.1，含 commit hash）。
- **路径 B（兜底）**：改用官方 `org.eclipse.zenoh:zenoh-java:1.10.0` + `zenoh-flat-jni-jvm:1.10.0`，其含 linux x64/aarch64。**约束：要求 glibc ≥ 2.28，无 musl 构建 → 不支持 Alpine 基础镜像**，必须用 debian/ubuntu slim 或 UBI。

验收：CI 在 `linux/amd64` 与 `linux/arm64` 两个 runner 上跑通 3 节点集群冒烟测试，容器镜像非 Alpine。

**Z-2 · 接收线程未回跳 Vert.x Context**

现状：所有 Zenoh 接收路径是阻塞 `recv()` 守护线程，**无 `runOnContext` 回跳** → 业务回调在非 Vert.x 线程执行，破坏线程模型，且一旦回调里做了 Vert.x 操作会出现难查的并发问题。

补齐：`ZenohEventMesh` 的每个 Subscriber 接收循环中，收到消息后一律
`context.runOnContext(v -> handler.handle(msg))`；接收线程只做 `recv` + 解码，绝不执行业务逻辑。为每个 Subscriber 绑定其声明时所在的 `Context`。

验收：单测断言 handler 执行线程名匹配 `vert.x-eventloop-*`；压测 1 万条/秒下无消息乱序（同一 key 保序由单 Context 保证）。

**Z-3 · 节点失效检测无租约**

现状：5s `setPeriodic` 的 `session.put({root}/nodes/{nodeId})`，**无 TTL/租约** → 崩溃节点的 key 永久残留，路由表指向死节点。

补齐：value 内写入 `expiresAt = now + 3 * heartbeatInterval`（沿用 `ZenohMapStore` 已有的「8 字节 big-endian expiresAt + payload」编码约定），读侧一律先判过期再使用；另起一个租约清理 timer，对已过期的 `nodes/*` 执行 `delete` 并广播 `NODE_LEFT`。**不依赖 Zenoh liveliness token**——已核实其无租约自动过期语义。

验收：`kill -9` 一个节点，≤15s 内其他节点的路由表不再包含该节点，且该节点上的会话在客户端重连后能正常落到新节点。

**Z-4 · 替换 JDK 序列化**

按 9.4 节的带版本头 CBOR codec 实施，覆盖 `ZenohAsyncMap` 与新的 `EventMesh`。

验收：滚动升级测试——新旧版本节点混跑，旧 codec 消息被正确识别并按兼容策略处理，无 `ClassNotFoundException`/`InvalidClassException`。

### 10.2 延后（技术债，不阻塞上线）

| 编号 | 项 | 为何可延后 |
|---|---|---|
| Z-5 | `registrations` 无订阅 → 跨节点 `eventBus.send()` 失效 | Phase 1 关闭 Vert.x 集群模式，业务不依赖它 |
| Z-6 | EventBus 旁路通道 reply 写死 dummy | 同上；业务请求应答改用 `Querier`/`Queryable` |
| Z-7 | `ZenohLock` best-effort、`ZenohCounter` 非强一致 | 业务锁与计数全部落 Postgres（第 15 章） |
| Z-8 | `ZenohMapStore` 跨节点 last-writer-wins | 只用于可容忍 LWW 的缓存态，不承载业务真相 |
| Z-9 | `getSyncMap` 用 `.await()` 阻塞 | 业务不调用 |

### 10.3 加分项

- `data-dict` 的 Caffeine + Zenoh 两级缓存目前**无 pub/sub 失效、L1 只本地驱逐** → 接上 `cache/invalidate/{namespace}` 主题，实现跨节点 L1 失效。这是个独立小改动，收益明确。

---

## 11. Conversation / Agent / Assignment 状态机

### 11.1 Conversation

```text
NEW
 │ 首条消息落库
 ▼
WAITING ◀──────────────┐
 │ 触发派单            │ 派单超时 / 坐席拒绝 / 转接
 ▼                     │
ASSIGNING ─────────────┤
 │ 命中坐席            │
 ▼                     │
ASSIGNED ──────────────┤
 │ 坐席接单(或自动接单) │
 ▼                     │
ACTIVE ────────────────┘
 │ 关闭（人工/超时/客户离开）
 ▼
CLOSED
```

状态集：`NEW | WAITING | ASSIGNING | ASSIGNED | ACTIVE | CLOSED`

**所有状态转换必须经领域服务 + 乐观锁 `version`，禁止 Controller/Handler 直接改状态。** 非法转换抛 `IllegalStateTransition`，返回 `BIZ_INVALID_STATE`。

### 11.2 Agent

```text
OFFLINE ──login──▶ ONLINE
                     ├──▶ IDLE    （可自动派单）
                     ├──▶ BUSY    （容量已满或人工置忙）
                     ├──▶ AWAY
                     └──▶ BREAK
```

容量模型：

```text
maxSessions        坐席可并发会话上限
activeCount        当前进行中会话数
availableCapacity  = maxSessions - activeCount
```

自动派单前置条件（三者同时成立）：

```text
presence == ONLINE  &&  status == IDLE  &&  availableCapacity > 0
```

`activeCount` 的增减**必须是 Postgres 原子 CAS**（第 15.4 节），不能读改写。

### 11.3 Assignment

```text
PENDING_OFFER ──accept──▶ ACCEPTED ──▶ (Conversation → ACTIVE)
      │
      ├──reject──▶ REJECTED  ──▶ 回 WAITING，该坐席进入本会话的冷却名单
      │
      └──timeout─▶ TIMEOUT   ──▶ 回 WAITING，释放容量，重新派单
```

自动接单模式下跳过 `PENDING_OFFER`，直接 `ACCEPTED`。

- `timeout_at` 落库（默认 30s），由 SLA/派单 worker 扫描（第 15.6 节的两级定时器同一套机制）；
- 拒绝/超时必须**回滚容量占用**，且把该坐席写入本会话的 `recent_reject`（TTL 5 分钟）避免立刻回弹；
- 转接（`CONVERSATION_TRANSFER`）= 关闭当前 Assignment + 会话回 `WAITING` + 携带 `transferHint`（指定坐席/指定队列）重新派单。

---

## 12. Ticket、Conversation↔Ticket 关系、SLA 域

### 12.1 Ticket 状态机

```text
NEW
 │
 ▼
OPEN ◀──────────────────────────┐
 ├──▶ PENDING ──────────────────┤
 ├──▶ WAITING_CUSTOMER ─────────┤
 ├──▶ WAITING_INTERNAL ─────────┤
 ├──▶ ESCALATED ────────────────┤
 │                              │
 ▼                              │
RESOLVED ──reopen──▶ REOPENED ──┘
 │
 ▼
CLOSED

任意非终态 ──cancel──▶ CANCELLED
```

状态集：`NEW | OPEN | PENDING | WAITING_CUSTOMER | WAITING_INTERNAL | ESCALATED | RESOLVED | CLOSED | REOPENED | CANCELLED`

`RESOLVED → CLOSED` 由「客户确认」或「N 天无响应自动关闭」触发。`CLOSED` 是终态，重开必须新建 Ticket 并用 `ticket_relation` 关联（避免终态被反复翻动导致 SLA 无法结算）。

### 12.2 Conversation 与 Ticket 的关系

**两个独立聚合，绝不合并为一个对象。**

```text
Customer
  ├── Conversation ──┬── Message
  │                  ├── Attachment
  │                  └── Assignment
  └── Ticket ────────┬── TicketArticle
                     ├── TicketTask
                     ├── SlaInstance
                     └── TicketRelation
```

- 一次 Conversation 可产生多张 Ticket（一次会话里客户问了退款和发票两件事）；
- 一张 Ticket 可关联多次 Conversation（跨天多次沟通同一问题）；
- 关联表 `conversation_ticket(tenant_id, conversation_id, ticket_id, link_type)`，`link_type ∈ {ORIGIN, RELATED}`；
- **跨聚合副作用只经 Outbox**：会话内触发建单 → 同事务写 `ConversationEscalated` 到 outbox → relay 投递 → Ticket 应用侧消费并建单。禁止在一个事务里同时改两个聚合。

### 12.3 SLA 作为独立域

SLA 不是 Ticket 的几个字段，而是独立聚合：

```text
SlaPolicy
├── priority            P1/P2/P3
├── businessCalendarId  工作日历
├── firstResponseTime   首响时限
├── resolutionTime      解决时限
├── warningThreshold    预警阈值（如 80%）
└── escalationRules     升级规则链

SlaInstance
├── targetType          CONVERSATION | TICKET
├── targetId
├── policyId
├── startedAt
├── pausedTotalMs       累计暂停时长
├── firstResponseDueAt  已按日历折算的绝对时刻
├── resolutionDueAt
├── firstRespondedAt
├── resolvedAt
└── status              RUNNING | PAUSED | MET | WARNED | BREACHED | CANCELLED
```

示例策略：

| 优先级 | 首响 | 解决 |
|---|---|---|
| P1 | 5 min | 1 h |
| P2 | 30 min | 4 h |
| P3 | 4 h | 2 d |

### 12.4 工作日历与暂停语义

```text
BusinessCalendar
├── timezone
├── weeklyRules        每周工作时段
├── holidays          节假日（排除）
└── specialWorkdays   调休（补入）
```

- **`dueAt` 在 SLA 启动时就按日历折算成绝对时刻并落库**，而不是每次检查时现算——否则日历改动会让历史工单的 SLA 结论漂移；
- 日历变更只影响之后新建的 `SlaInstance`；
- `WAITING_CUSTOMER` 状态**暂停** resolution 计时（累加 `pausedTotalMs` 并重算 `resolutionDueAt`），首响计时不暂停；
- `PAUSED → RUNNING` 时重算 due，写 `sla_event` 审计。

### 12.5 SLA 升级链

```text
到达 warningThreshold  ──▶ SLA_WARNING  ──▶ 通知坐席 + 主管
到达 dueAt 未达成      ──▶ SLA_BREACH   ──▶ Ticket → ESCALATED
                                          + 提优先级
                                          + 转升级队列
                                          + 通知主管
```

升级动作本身是**领域事件驱动**的，经 Outbox 投递，保证「判定 breach」与「执行升级」解耦、可重放。

---

## 13. 派单流水线与规则引擎

### 13.1 流水线

```text
Conversation Created / 回到 WAITING
        │
        ▼
Build DispatchContext          （只读快照，无副作用）
        │
        ▼
RuleEngine.evaluate()          （ZEN / Java 决策表）
        │
        ▼
RoutingDecision                （该找什么样的坐席）
        │
        ▼
AgentSelector.select()         （接触实时状态）
        │
        ▼
容量 CAS 抢占（Postgres 原子 UPDATE）
        │
        ├─ 成功 ──▶ 创建 Assignment ──▶ AUTO_ACCEPT / PENDING_OFFER
        │                                        │
        │                                        ▼
        │                              Conversation → ACTIVE
        │
        └─ 失败 ──▶ 换下一候选；候选耗尽 ──▶ 入队等待 + DISPATCH_FAILED
```

### 13.2 DispatchContext（规则引擎输入）

```json
{
  "tenant":       {"id": "t1"},
  "customer":     {"id": "u100", "vip": true, "language": "zh-CN",
                   "tags": ["VIP", "ORDER"], "historyTicketCount": 12},
  "conversation": {"id": "c100", "category": "ORDER", "priority": 80,
                   "channel": "WEB", "firstMessageIntent": "REFUND"},
  "queue":        {"waitingCount": 8, "avgWaitSeconds": 45},
  "time":         {"hour": 20, "dayOfWeek": 6, "inBusinessHours": false}
}
```

**输入是纯快照**，规则引擎不查库、不访问连接、不写任何状态。

### 13.3 RoutingDecision（规则引擎输出）

```json
{
  "queue": "vip-order",
  "priority": 100,
  "requiredSkills": ["ORDER"],
  "preferredSkills": ["REFUND"],
  "language": "zh-CN",
  "strategy": "LEAST_LOAD",
  "autoAccept": true,
  "slaPolicyId": "P1",
  "offerTimeoutSeconds": 30
}
```

### 13.4 AgentSelector

```text
1.  按 queue 取候选坐席
2.  过滤 requiredSkills
3.  过滤 language
4.  过滤 presence == ONLINE && status == IDLE
5.  过滤 availableCapacity > 0
6.  过滤排班（当前时段在班）
7.  排除本会话 recent_reject 冷却名单
8.  按 strategy 排序（下方打分）
9.  逐候选执行容量 CAS 抢占
10. 抢到即创建 Assignment
```

打分公式（`SKILL_MATCH` 策略，源自文档 2）：

```text
score = skillMatch        * 0.35
      + availability      * 0.25
      + (1 - workloadPct) * 0.20
      + customerAffinity  * 0.10
      + priorityFit       * 0.10
```

支持策略：`LEAST_LOAD | LONGEST_IDLE | ROUND_ROBIN | SKILL_MATCH | LANGUAGE_MATCH | VIP_FIRST`。

### 13.5 规则引擎接缝、版本化与灰度

```text
RuleEngine
├── evaluate(DispatchContext): Future<RoutingDecision>
└── reload(RuleBundle): Future<Void>
```

- **主实现 `cs-rule-zen`**：`io.gorules:zen-engine:2.0.2`，JDM 决策模型，`ZenLoader` 支持热加载。规则文件存 `rule_definition` / `rule_version` 表，节点通过 Zenoh `cache/invalidate/rules` 广播触发 reload。
- **兜底实现 `JavaDecisionTableRuleEngine`**：纯 Java 决策表，覆盖默认路由（按 category → queue 的静态映射 + VIP 提优先级）。当 ZEN 原生库不可用或规则求值异常时自动降级，并打 `rule_engine_fallback_total` 指标。
- **版本化 + 灰度分桶**：`RuleBundle` 带 `version`；灰度按 `hash(conversationId) % 100 < grayPercent` 决定用新版还是旧版。
- **Shadow 对账**：新版本上线前以 shadow 模式并行求值，只记录差异不生效，产出 `rule_shadow_diff` 报表；差异率低于阈值才允许放量。
- **Simulator**：用历史 `DispatchContext` 快照批量回放新规则，输出队列分布、预计等待时长、SLA 达成率变化。规则变更必须先过 Simulator。

### 13.6 AI Agent 扩展接缝（Phase 1 不实现）

只预留两个位置，不写实现：

1. `RoutingDecision` 增加可选字段 `assigneeType: HUMAN | BOT`，`AgentSelector` 见到 `BOT` 时走 `BotAssignmentStrategy`（Phase 1 该策略不存在，命中即降级为 HUMAN 并告警）。
2. 定义 `ConversationParticipantType ∈ {CUSTOMER, HUMAN_AGENT, BOT, SYSTEM}`，消息表的 `sender_type` 预留 `BOT` 取值，使未来接入 Agent Runtime 时**无需改表结构**。

文档 3 的 AgentState / AgentAction / Policy / AgentTool / WASI 等设计一律不在 Phase 1 落地。

---

## 14. PostgreSQL 数据模型

### 14.1 全局建模规范

所有业务表：

```text
id           VARCHAR(64) 或 UUID    主键
tenant_id    VARCHAR(64) NOT NULL   多租户隔离键
created_at   TIMESTAMPTZ NOT NULL
updated_at   TIMESTAMPTZ NOT NULL
version      BIGINT NOT NULL DEFAULT 0   （聚合根才有）
```

- **核心查询字段必须结构化，不得依赖 JSONB。** JSONB 只用于 `custom_attributes`、`metadata`、`channel_payload`、`content_json`、`reason_json`。
- 所有索引第一列是 `tenant_id`（除全局表）。
- 时间统一 `TIMESTAMPTZ`，应用侧统一 UTC，展示层做时区转换。

### 14.2 核心表清单

```text
tenant  tenant_setting
sys_user  role  permission  user_role
customer  customer_identity  customer_tag  customer_tag_relation  customer_note
agent  agent_skill  agent_queue  agent_schedule  agent_workload
queue  queue_member
conversation  conversation_member  conversation_seq(内联在 conversation)  assignment
message  message_attachment  message_reaction  message_read_receipt
ticket  ticket_article  ticket_task  ticket_tag  ticket_relation  ticket_custom_field
conversation_ticket
sla_policy  sla_instance  sla_event  business_calendar
rule_definition  rule_version  rule_shadow_diff
outbox_event
idempotency_record
timer_task
audit_log
notification  notification_template
```

### 14.3 关键 DDL

```sql
-- 会话：last_seq 内联，用于原子生成会话内序列号
CREATE TABLE conversation (
    id                VARCHAR(64) PRIMARY KEY,
    tenant_id         VARCHAR(64) NOT NULL,
    customer_id       VARCHAR(64) NOT NULL,
    queue_id          VARCHAR(64),
    agent_id          VARCHAR(64),
    status            VARCHAR(32) NOT NULL,
    priority          INTEGER     NOT NULL DEFAULT 0,
    category          VARCHAR(64),
    language          VARCHAR(32),
    channel           VARCHAR(32),
    last_seq          BIGINT      NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ NOT NULL,
    assigned_at       TIMESTAMPTZ,
    first_response_at TIMESTAMPTZ,
    closed_at         TIMESTAMPTZ,
    version           BIGINT      NOT NULL DEFAULT 0
);
CREATE INDEX idx_conv_customer ON conversation(tenant_id, customer_id, created_at DESC);
CREATE INDEX idx_conv_agent    ON conversation(tenant_id, agent_id, status);
CREATE INDEX idx_conv_queue    ON conversation(tenant_id, queue_id, status)
    WHERE status IN ('WAITING','ASSIGNING');
```

```sql
-- 消息：会话内 seq 唯一 + 幂等唯一
CREATE TABLE message (
    id              VARCHAR(64) PRIMARY KEY,
    tenant_id       VARCHAR(64) NOT NULL,
    conversation_id VARCHAR(64) NOT NULL,
    sender_id       VARCHAR(64) NOT NULL,
    sender_type     VARCHAR(32) NOT NULL,   -- CUSTOMER|HUMAN_AGENT|BOT|SYSTEM
    request_id      VARCHAR(128),
    sequence        BIGINT      NOT NULL,
    message_type    VARCHAR(64) NOT NULL,
    content_json    JSONB       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    recalled_at     TIMESTAMPTZ
) PARTITION BY RANGE (created_at);

CREATE UNIQUE INDEX uk_msg_sequence ON message(conversation_id, sequence);
CREATE UNIQUE INDEX uk_msg_request  ON message(tenant_id, sender_id, request_id)
    WHERE request_id IS NOT NULL;
CREATE INDEX idx_msg_conv_created   ON message(conversation_id, created_at);
```

分区策略：按 `created_at` 月分区，保留 13 个月热分区，更老的分区 detach 后归档。分区维护用 `pg_partman` 或定时 DDL 任务。

```sql
-- 派单
CREATE TABLE assignment (
    id              VARCHAR(64) PRIMARY KEY,
    tenant_id       VARCHAR(64) NOT NULL,
    conversation_id VARCHAR(64) NOT NULL,
    agent_id        VARCHAR(64),
    queue_id        VARCHAR(64),
    status          VARCHAR(32) NOT NULL,
    strategy        VARCHAR(64),
    rule_version    VARCHAR(64),
    reason_json     JSONB,
    assigned_at     TIMESTAMPTZ,
    accepted_at     TIMESTAMPTZ,
    rejected_at     TIMESTAMPTZ,
    timeout_at      TIMESTAMPTZ
);
CREATE INDEX idx_asg_agent   ON assignment(tenant_id, agent_id, status);
CREATE INDEX idx_asg_conv    ON assignment(conversation_id);
CREATE INDEX idx_asg_timeout ON assignment(timeout_at)
    WHERE status = 'PENDING_OFFER';
```

```sql
-- 坐席容量：CAS 抢占的目标表，与 agent 主表分离以降低热点行冲突面
CREATE TABLE agent_workload (
    agent_id     VARCHAR(64) PRIMARY KEY,
    tenant_id    VARCHAR(64) NOT NULL,
    capacity     INTEGER     NOT NULL,
    active_count INTEGER     NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ NOT NULL,
    CONSTRAINT ck_workload_range CHECK (active_count >= 0 AND active_count <= capacity)
);
```

```sql
-- 幂等
CREATE TABLE idempotency_record (
    tenant_id    VARCHAR(64)  NOT NULL,
    request_id   VARCHAR(128) NOT NULL,
    scope        VARCHAR(64)  NOT NULL,
    result_json  JSONB,
    created_at   TIMESTAMPTZ  NOT NULL,
    expires_at   TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (tenant_id, request_id, scope)
);
CREATE INDEX idx_idem_expires ON idempotency_record(expires_at);
```

```sql
-- 事务性 Outbox
CREATE TABLE outbox_event (
    id            BIGSERIAL PRIMARY KEY,
    tenant_id     VARCHAR(64)  NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id  VARCHAR(64)  NOT NULL,
    event_type    VARCHAR(64)  NOT NULL,
    key_expr      VARCHAR(512) NOT NULL,   -- 目标 Zenoh key
    payload       BYTEA        NOT NULL,   -- CBOR
    trace_id      VARCHAR(64),
    created_at    TIMESTAMPTZ  NOT NULL,
    published_at  TIMESTAMPTZ,
    attempt       INTEGER      NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ
);
CREATE INDEX idx_outbox_pending ON outbox_event(next_retry_at, id)
    WHERE published_at IS NULL;
```

```sql
-- SLA 实例（due 已按日历折算为绝对时刻）
CREATE TABLE sla_instance (
    id                   VARCHAR(64) PRIMARY KEY,
    tenant_id            VARCHAR(64) NOT NULL,
    policy_id            VARCHAR(64) NOT NULL,
    target_type          VARCHAR(32) NOT NULL,
    target_id            VARCHAR(64) NOT NULL,
    status               VARCHAR(32) NOT NULL,
    started_at           TIMESTAMPTZ NOT NULL,
    paused_total_ms      BIGINT      NOT NULL DEFAULT 0,
    first_response_due_at TIMESTAMPTZ,
    resolution_due_at    TIMESTAMPTZ,
    first_responded_at   TIMESTAMPTZ,
    resolved_at          TIMESTAMPTZ,
    version              BIGINT      NOT NULL DEFAULT 0
);
CREATE INDEX idx_sla_target ON sla_instance(tenant_id, target_type, target_id);
```

```sql
-- 统一定时任务表（SLA due / Assignment timeout / 自动关闭 共用）
CREATE TABLE timer_task (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   VARCHAR(64)  NOT NULL,
    task_type   VARCHAR(64)  NOT NULL,   -- SLA_WARNING|SLA_BREACH|ASSIGNMENT_TIMEOUT|AUTO_CLOSE
    target_id   VARCHAR(64)  NOT NULL,
    fire_at     TIMESTAMPTZ  NOT NULL,
    status      VARCHAR(32)  NOT NULL,   -- PENDING|CLAIMED|DONE|CANCELLED
    claimed_by  VARCHAR(64),
    claimed_at  TIMESTAMPTZ,
    payload     JSONB
);
CREATE INDEX idx_timer_due ON timer_task(fire_at, id) WHERE status = 'PENDING';
```

---

## 15. 一致性与可靠性机制

### 15.1 会话内单调无空洞 seq

```sql
UPDATE conversation
   SET last_seq = last_seq + 1
 WHERE id = $1
RETURNING last_seq;
```

- 在**同一个事务内**先取 seq 再插 message，`uk_msg_sequence` 做最终防线；
- **不用 Postgres SEQUENCE**：sequence 在回滚时会产生空洞，而客户端按 seq 补齐依赖无空洞；
- 该行是会话级热点，但会话并发写通常是单坐席+单客户，冲突可忽略；高并发群会话场景才需要考虑改造。

### 15.2 幂等

```sql
INSERT INTO idempotency_record (tenant_id, request_id, scope, result_json, created_at, expires_at)
VALUES ($1, $2, $3, $4, now(), now() + interval '24 hours')
ON CONFLICT (tenant_id, request_id, scope) DO NOTHING;
```

- 插入影响行数 0 → 是重放，读出 `result_json` 原样返回给客户端（**返回原结果，不是报错**）；
- `scope` 区分命令类型，避免不同命令误撞同一 requestId；
- 消息路径另有 `uk_msg_request` 兜底；
- 过期清理由定时任务批量删除。

### 15.3 persist-before-publish + 事务性 Outbox

**铁律：先持久化，再发布。** 不允许「DB 提交成功但事件发布失败」。

```text
Client ──▶ Gateway ──▶ 校验 ──▶ 幂等检查
                                   │
                         ┌─────────▼──────────┐  单个 pool.withTransaction
                         │ 1. 取 seq          │
                         │ 2. 插 message      │
                         │ 3. 写 outbox_event │
                         │ 4. 写 idempotency  │
                         └─────────┬──────────┘
                                   │ COMMIT
                                   ▼
                              回 ACK 给客户端
                                   │
                        Outbox Relay（异步）
                                   ▼
                            Zenoh publish
                                   ▼
                     目标 Node ──▶ Transport ──▶ 对端
```

Relay 取件：

```sql
SELECT * FROM outbox_event
 WHERE published_at IS NULL
   AND (next_retry_at IS NULL OR next_retry_at <= now())
 ORDER BY id
 LIMIT 200
 FOR UPDATE SKIP LOCked;
```

（`FOR UPDATE SKIP LOCKED` 与 advisory lock 在 `vertx-pg-client` 中**没有 API 封装，是裸 SQL**——已核实。）

- 发布成功 → `published_at = now()`；失败 → `attempt+1`，`next_retry_at = now() + backoff(attempt)`，上限 8 次后进死信并告警；
- 投递语义是**至少一次**，因此客户端与消费侧必须按 `messageId`/`eventId` 去重（seq 天然提供去重依据）；
- Relay 每节点都跑，靠 `SKIP LOCKED` 天然分片，无需选主。

### 15.4 坐席容量原子 CAS

```sql
UPDATE agent_workload
   SET active_count = active_count + 1,
       updated_at   = now()
 WHERE agent_id = $1
   AND active_count < capacity
RETURNING active_count;
```

- 返回空行 = 抢占失败，换下一候选，**绝不重试同一坐席**；
- 释放：`active_count = active_count - 1 WHERE agent_id = $1 AND active_count > 0`；
- `CHECK (active_count >= 0 AND active_count <= capacity)` 作为最后一道防线，一旦触发说明有代码路径绕过了 CAS；
- **坚决不用 Zenoh Counter**——已核实其非强一致，会导致超派。

### 15.5 队列取件与乐观锁

- 待派单会话取件：`SELECT ... WHERE status='WAITING' ORDER BY priority DESC, created_at LIMIT n FOR UPDATE SKIP LOCKED`；
- 聚合根更新一律带 `version`：`UPDATE ... SET ..., version = version + 1 WHERE id = $1 AND version = $2`，影响 0 行 → 抛 `OptimisticLockConflict`，应用层重试上限 3 次后返回 `BIZ_CONCURRENT_MODIFICATION`。

### 15.6 两级定时器（SLA / 派单超时 / 自动关闭）

单纯用 Vert.x timer 会在节点重启后丢失；单纯轮询 DB 会有秒级延迟与放大查询。方案：

```text
Level 1 · 持久层
  timer_task 表持有所有到期任务的绝对时刻（重启不丢）

Level 2 · 选主 Loader
  每节点尝试 pg_advisory_xact_lock(hash('cs:timer:loader'))
  拿到锁的节点每 10s 装载「未来 60s 内到期」的任务：
     UPDATE timer_task SET status='CLAIMED', claimed_by=$node, claimed_at=now()
      WHERE id IN (SELECT id FROM timer_task
                    WHERE status='PENDING' AND fire_at < now() + interval '60 seconds'
                    ORDER BY fire_at LIMIT 500 FOR UPDATE SKIP LOCKED)
   RETURNING *;

Level 3 · 近端执行
  装载到的任务放进本节点 Vert.x setTimer，精确到秒级触发
  执行完置 status='DONE'
```

- 用 `pg_advisory_xact_lock`（事务级，连接断开自动释放）而非会话级 advisory lock——会话级锁需要 `withConnection` 长连接持有，运维上更脆；
- 节点崩溃导致 `CLAIMED` 任务未执行：另有一个补偿扫描，把 `claimed_at < now() - 5 min` 且仍 `CLAIMED` 的任务重置为 `PENDING`；
- 任务执行必须幂等（例如 `SLA_BREACH` 先检查 `sla_instance.status` 是否已 BREACHED）。

### 15.7 一致性边界总表

| 语义 | 落在哪 | 机制 |
|---|---|---|
| 会话内 seq | Postgres | `UPDATE ... RETURNING` |
| 命令幂等 | Postgres | 主键 + `ON CONFLICT DO NOTHING` |
| 坐席容量 | Postgres | 条件 `UPDATE ... RETURNING` |
| 聚合状态迁移 | Postgres | 乐观锁 `version` |
| 分布式锁/选主 | Postgres | `pg_advisory_xact_lock` |
| 队列取件 | Postgres | `FOR UPDATE SKIP LOCKED` |
| 事件投递 | Postgres + Zenoh | Outbox + at-least-once |
| Presence 提示 | Redis + Zenoh | TTL + 广播（**允许不一致**） |
| 字典/配置缓存 | Caffeine + Zenoh | pub/sub 失效（**最终一致**） |

---

## 16. 数据访问与事务规范

### 16.1 Pool Bean

`cs-infra-persistence` 提供 `io.vertx.sqlclient.Pool` Bean（全仓当前不存在，同时也补上了 `vertx-spring-boot-mybatis` 未被满足的依赖）：

```text
cs.datasource.host / port / database / user / password
cs.datasource.pool.max-size            默认 = CPU核数 * 4，上限 32
cs.datasource.pool.max-wait-queue-size 默认 512
cs.datasource.pipelining-limit         默认 64
cs.datasource.prepared-statement-cache-max-size  默认 512
```

要点：
- 连接池上限必须与 Postgres `max_connections` 对齐（节点数 × pool 上限 + 运维预留 < max_connections）；
- 开启 pipelining 显著降低小查询往返开销（vertx-pg-client 5.1.x GA）；
- 预编译语句缓存必须开，且 SQL 必须参数化（同时防注入）。

### 16.2 SQL 分层选型

| 场景 | 用什么 |
|---|---|
| 热点路径（发消息、取 seq、容量 CAS、Outbox relay） | 原生 `SqlClient` + `preparedQuery`，手写 SQL |
| 中等复杂、参数命名可读性重要 | `VertxSqlTemplate`（`#{name}` 命名参数，仓库已有） |
| 复杂动态 SQL（工单列表多条件筛选、报表） | MyBatis，**Mapper 方法签名一律 `Future<T>`** |

**MyBatis 使用铁律：** 只有 `Future<T>` 返回类型的 Mapper 方法走异步；其他返回类型会退化为 `.join()` 阻塞（实测）。因此禁止在客服模块声明非 `Future` 的 Mapper 方法，由 ArchUnit 断言强制。

**不使用 `VertxCursor`**：实测它是预加载全部结果后惰性迭代，不是真流式。大结果集用 `RowStream` 或 keyset 分页。

### 16.3 事务

**只用 `pool.withTransaction`，全面禁用 `@Transactional`。** 原因：`VertxDataSourceTransactionManager` 是 thread-bound 且内部 `.join()`，在事件循环上不可用（实测）。

```text
pool.withTransaction(conn -> 
    nextSeq(conn, convId)
      .compose(seq -> insertMessage(conn, msg, seq))
      .compose(v   -> insertOutbox(conn, event))
      .compose(v   -> insertIdempotency(conn, req)))
```

规范：
- 一个用例一个事务，事务内**只做 DB 操作**，绝不发网络请求、绝不 publish；
- 事务时长目标 < 50ms，超过 200ms 打告警日志；
- 需要跨聚合 → 用 Outbox，不扩大事务；
- 会话级 advisory lock 必须 `withConnection` 包裹（否则锁会跟着连接归还池而语义错乱）；本方案统一用事务级 `pg_advisory_xact_lock` 规避这个坑。

### 16.4 防阻塞三道闸

事件循环上的阻塞调用是这个架构最容易犯、后果最重的错误。三道闸：

**闸一 · ArchUnit（编译期/CI）**

```text
禁止在 cs-* 包中出现：
  Future.join() / Future.await() / CompletableFuture.get()
  Thread.sleep / Object.wait
  java.sql.* 的任何类型
  同步 HTTP 客户端（HttpURLConnection、Apache HC 同步 API）
  非 Future 返回值的 @Mapper 方法
```

**闸二 · BlockedThreadChecker（运行期）**

```text
vertx.max-event-loop-execute-time = 2s（开发环境 500ms）
vertx.warning-exception-time      = 5s
```

事件循环阻塞会打出完整栈，CI 中把该日志视为测试失败。

**闸三 · EventLoopGuard（关键入口断言）**

在每个 `ImTransport` 入口与用例入口断言 `Context.isEventLoopContext()`，开发/测试 profile 下断言失败直接抛异常，生产 profile 下打指标 `event_loop_violation_total`。

必须走 `executeBlocking` 的场景（当前已知）：Zenoh JNI 的阻塞 `recv()`（由 mesh 模块内部消化，业务侧不可见）、对象存储 SDK 的同步调用（改用异步 SDK 后消除）。

---

## 17. 安全与多租户

### 17.1 认证

全仓当前无认证实现，需新建。

| 主体 | 方式 |
|---|---|
| 消费者（匿名/半匿名） | 短期 JWT（TTL 15 min），由业务方后端用 HMAC 签发，含 `tenantId`/`customerId`/`channel`；WebSocket 握手阶段验证 |
| 坐席 | OIDC 授权码流 + 平台自签 access token（TTL 30 min）+ refresh token |
| 服务间 | mTLS 或平台 API Key（`api_key` 表，带 scope） |

**WebSocket token 必须在握手阶段校验**（`webSocketHandshakeHandler` 内 `reject(401)`），不允许连上后再踢。token 过期时服务端主动发 `SYSTEM_NOTICE{code:TOKEN_EXPIRING}` 提示续期，宽限期 60s 后关连接。

**禁止把 token 放在 URL query 里**（会进 access log）——统一用 `Sec-WebSocket-Protocol` 子协议字段或首帧 `AUTH` 命令（首帧模式下未认证连接 5s 内不发 AUTH 即断开）。

### 17.2 授权（RBAC）

```text
Role ─▶ Permission ─▶ Resource:Action
```

权限粒度示例：`conversation:read:own` / `conversation:read:team` / `conversation:read:tenant` / `ticket:escalate` / `rule:publish` / `agent:impersonate`。

- 数据可见域三级：`own`（自己的会话）/ `team`（所在团队）/ `tenant`（全租户）；
- 每个查询在 Repository 层强制注入可见域条件，**不允许应用层自行拼**；
- 敏感操作（改规则、改 SLA 策略、导出数据、模拟坐席）必须写 `audit_log`。

### 17.3 租户隔离

- **每个 SQL 必须带 `tenant_id`**，由 Repository 基类从 `TenantContext` 自动注入；ArchUnit 断言禁止在 Repository 实现中出现不含 `tenant_id` 的 WHERE（白名单除外）；
- Zenoh key 天然含 `{tenantId}` 段，节点只订阅自己持有连接的租户前缀；
- Redis key 一律 `im:{tenantId}:...`；
- 对象存储路径 `s3://bucket/{tenantId}/{yyyy}/{MM}/{fileId}`；
- **不使用 Postgres RLS**（与连接池复用、`SET LOCAL` 的开销不匹配），改为应用层强制 + CI 静态检查双保险。

### 17.4 限流分层

| 层 | 维度 | 手段 |
|---|---|---|
| 连接层 | 每 IP 并发连接数、每租户并发连接数 | 握手阶段拒绝 |
| 命令层 | 每连接 QPS、每租户 QPS | Redis 滑动窗口 |
| 消息层 | 每会话每秒消息数、每客户端每分钟消息数 | Redis 令牌桶 |
| 上传层 | 每租户每日流量与文件数 | 预签名前校验配额 |
| DB 层 | 连接池 max-wait-queue-size | 队列满快速失败 `SYS_OVERLOADED` |

超限返回 `RATE_LIMITED` 并带 `retryAfterMs`，**不静默丢弃**。

### 17.5 附件安全

- 上传走**预签名 URL**，字节流不经过 Java 进程；
- 服务端签发前校验：租户配额、mimeType 白名单、大小上限；
- 下载同样预签名，TTL 5 min，URL 不落库不进日志；
- 图片/视频缩略图由独立 worker 异步生成（Phase 1 可先由客户端上传缩略图）；
- 恶意文件扫描留接缝（Phase 1 不做，但 mimeType 白名单 + 强制 `Content-Disposition: attachment` 必须做）。

### 17.6 日志脱敏

统一脱敏字段：`phone`、`email`、`idCard`、`bankCard`、`address`、消息 `content_json` 的文本正文、token、预签名 URL。实现为 logback 的 `MessageConverter` + `cs-common` 的 `Sensitive` 注解，**默认脱敏，显式声明才输出明文**。

---

## 18. 可观测性

### 18.1 指标清单（Prometheus）

传输层：
```text
im_connections_active{tenant,transport}                  gauge
im_connection_opened_total / im_connection_closed_total{reason}
im_frames_in_total / im_frames_out_total{type}
im_write_queue_full_total{transport}
im_dropped_events_total{level}                           P0/P1/P2 分级
im_heartbeat_timeout_total
```

业务层：
```text
cs_message_send_duration_seconds                         histogram
cs_conversation_wait_seconds                             histogram（进入 WAITING → ACTIVE）
cs_first_response_seconds                                histogram
cs_dispatch_duration_seconds{stage}                       rule/select/cas
cs_dispatch_failed_total{reason}                          no_agent/cas_conflict/rule_error
cs_assignment_timeout_total / cs_assignment_reject_total
cs_agent_utilization{agent}                              gauge = active/capacity
cs_queue_waiting_count{queue}                            gauge
cs_sla_warned_total / cs_sla_breached_total{policy}
cs_ticket_status_transition_total{from,to}
```

基础设施：
```text
cs_outbox_pending_count                                  gauge（**核心告警项**）
cs_outbox_publish_duration_seconds
cs_outbox_deadletter_total
cs_db_tx_duration_seconds / cs_db_pool_queue_size
cs_optimistic_lock_conflict_total{aggregate}
mesh_publish_total / mesh_receive_total / mesh_decode_error_total
mesh_request_duration_seconds                            Querier 请求应答
zenoh_node_alive_count                                   gauge
rule_engine_eval_duration_seconds / rule_engine_fallback_total
event_loop_violation_total
```

### 18.2 关键告警

| 告警 | 条件 | 含义 |
|---|---|---|
| Outbox 堆积 | `cs_outbox_pending_count > 5000` 持续 2 min | 事件面故障，客户端收不到消息 |
| Outbox 死信 | `increase(cs_outbox_deadletter_total[5m]) > 0` | 有事件永久丢失 |
| 事件循环阻塞 | `event_loop_violation_total > 0` | 代码引入了阻塞调用 |
| 容量约束触发 | DB `ck_workload_range` 违反 | 有路径绕过了 CAS |
| 规则降级 | `rule_engine_fallback_total > 0` | ZEN 不可用 |
| 节点数异常 | `zenoh_node_alive_count != 期望副本数` 持续 1 min | 节点失联或租约失效有 bug |

### 18.3 Trace 串联

`traceId` 必须贯穿：客户端 Envelope → 传输层 MDC → 用例 → SQL comment → Outbox 行 → Zenoh CBOR header → 对端节点 → 对端 Envelope。

- W3C `traceparent` 语义，OpenTelemetry SDK 埋点；
- 关键 span：`im.command.receive` / `usecase.execute` / `db.tx` / `outbox.publish` / `mesh.publish` / `mesh.receive` / `im.event.deliver`；
- **`outbox_event.trace_id` 落库**，让"消息发出去了但对端没收到"这类问题可以从 DB 直接追到 mesh。

### 18.4 日志规范

- 结构化 JSON，固定字段：`ts` `level` `logger` `traceId` `tenantId` `conversationId` `principalId` `nodeId` `msg`；
- 级别约定：客户端错误 = INFO/WARN（不是 ERROR，避免告警噪音）；服务端错误 = ERROR；协议违规 = WARN + 计数；
- **禁止在日志里打完整 Envelope**（含消息正文），只打 `type` + `id` + 尺寸。

---

## 19. 部署与运维

### 19.1 拓扑

```text
                        Internet
                            │
                    LB (L4 for WS / L7 for REST)
                            │
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
          Node-01       Node-02       Node-03
        (8080 REST)   (8080 REST)   (8080 REST)
        (9443 IM)     (9443 IM)     (9443 IM)
              │             │             │
              └─────────────┼─────────────┘
                            │
                     Zenoh 事件面 (7447)
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
   PostgreSQL           Redis            Object Storage
   (主 + 只读副本)     (presence/限流)      (S3/MinIO)
```

### 19.2 Zenoh 拓扑选择

沿用仓库现有经验：

- **单节点开发**：内置 `EmbeddedRouter`（监听 `tcp/127.0.0.1:7447`）；
- **多节点**：**只有一个实例配 `vertx.cluster.zenoh.mode: router`**（监听 7447），其余配 `client` 指向它——否则端口冲突（这是仓库已记录的坑）；
- **生产推荐**：独立部署 2~3 个 Zenoh router（StatefulSet），业务节点全部 `client` 模式连它们。业务节点无状态，可任意扩缩。

### 19.3 K8s 与镜像约束（硬约束）

| 约束 | 说明 |
|---|---|
| **基础镜像不能用 Alpine** | 官方 `zenoh-flat-jni-jvm` 无 musl 构建，要求 glibc ≥ 2.28 → 用 `debian-slim` / `ubuntu` / UBI |
| **原生库必须匹配架构** | zenoh JNI 与 `netty-codec-native-quic` 都要按 `linux-x86_64` / `linux-aarch_64` 打对 classifier；多架构镜像必须分别验证 |
| **rustc 版本严格匹配** | 若自行交叉编译 zenoh-jni，rustc 版本需与 zenoh 版本精确对应（含 commit hash），否则插件/库加载静默失效 |
| **WebSocket 需要 L4 或支持 Upgrade 的 L7** | Ingress 必须显式允许 HTTP Upgrade，且空闲超时 > 心跳间隔（建议 300s） |
| **SSE 需要禁用响应缓冲** | nginx `proxy_buffering off`、`X-Accel-Buffering: no` |
| **HTTP/3 需要 UDP** | Service 需 UDP 端口，LB 需支持 UDP 转发；不满足则不开 H3（不影响主功能） |

Pod 配置要点：
- `terminationGracePeriodSeconds: 60`，优雅停机流程见 19.4；
- readiness = DB 可达 + Zenoh session 建立 + Outbox relay 运行；liveness 只探进程存活（避免依赖故障导致全 Pod 重启雪崩）；
- HPA 按 `im_connections_active` 与 CPU 双指标；连接是长连接，缩容要配合 19.4 的优雅迁移。

### 19.4 优雅停机

```text
1. 收到 SIGTERM，readiness 立即置 false（LB 停止新连接与新请求）
2. 停止 Outbox relay 取新件，等待在途件发布完成（上限 10s）
3. 向本节点所有连接发 SYSTEM_NOTICE{code: NODE_DRAINING, reconnectAfterMs: 0}
4. 客户端立即重连到其他节点（依赖 8.5 的 seq 补齐保证不丢消息）
5. 等待连接数降至 0 或超时 30s，强制关闭剩余连接
6. 释放 Zenoh session（先 undeclare 所有 Subscriber/Queryable 再关 session——仓库已记录的安全停止顺序）
7. 关闭 Pool，退出
```

关闭过程中必须捕获并静默 `RejectedExecutionException`（仓库已记录的 Vert.x 资源关闭规范）。

### 19.5 数据库运维

- Flyway 管理 DDL，**所有变更必须向后兼容**（先加列再用、不删列只标废弃、不改列类型）；
- 消息表月分区，`pg_partman` 或定时任务预建未来 3 个月分区；
- 只读副本承载报表与历史消息查询；
- 备份：每日全量 + WAL 归档，RPO ≤ 5 min，季度演练恢复。

### 19.6 灰度与回滚

| 变更类型 | 灰度手段 | 回滚 |
|---|---|---|
| 应用版本 | 滚动 + 单 Pod 金丝雀，观察 15 min | 回滚 Deployment |
| 协议版本 | 双版本并行 ≥2 个发布周期 | 客户端降级到旧版本号 |
| 派单规则 | Simulator → shadow 对账 → 分桶放量 10%→50%→100% | 切回上一 `rule_version`（秒级） |
| SLA 策略 | 只对新建 SlaInstance 生效 | 改回旧策略，历史实例不受影响 |
| Zenoh codec | 双 codecVersion 并行识别 | 回滚应用版本 |

---

## 20. 风险登记与分阶段交付

### 20.1 风险登记

| ID | 风险 | 等级 | 缓解 |
|---|---|---|---|
| R-1 | **zenoh-jni 无 Linux 原生库** → 无法上 K8s | **致命** | Z-1 必修，或切官方 zenoh-java；Phase 0 必须先验证通 |
| R-2 | Zenoh 接收线程未回跳 Context → 隐蔽并发 bug | 高 | Z-2 必修 + 单测断言线程名 |
| R-3 | 节点崩溃后路由表指向死节点 | 高 | Z-3 租约 TTL + `kill -9` 演练 |
| R-4 | 现有 WebFlux 层不支持流式，误用会导致 SSE 永挂 | 高 | 数据面完全独立端口 + ArchUnit 禁止 `cs-transport-*` 依赖 web-server 模块 |
| R-5 | `@Transactional` 被误用 → 事件循环阻塞 | 高 | 防阻塞三道闸；`cs-*` 包内禁止 `@Transactional` |
| R-6 | GoRules ZEN 原生库在目标架构不可用 | 中 | Java 决策表兜底 + `rule_engine_fallback_total` 告警 |
| R-7 | 会话 `last_seq` 行成为热点 | 中 | 监控 `cs_optimistic_lock_conflict_total`；必要时按会话分片或改用 per-conversation advisory lock |
| R-8 | Outbox relay 落后 → 用户"发出去了但对方收不到" | 中 | `cs_outbox_pending_count` 核心告警 + relay 多节点 SKIP LOCKED 并行 |
| R-9 | 长连接节点缩容导致大规模重连风暴 | 中 | 优雅停机分批 drain + 客户端指数退避抖动重连 |
| R-10 | WebTransport 需求被重新提出 | 低 | 已留 SPI；实现路径明确为非 JVM 边车，不影响业务层 |
| R-11 | 消息表增长过快 | 低 | 月分区 + 13 月热数据 + 归档 |

### 20.2 Phase 1 明确不做

WebTransport 生产落地 · AI Agent Runtime / RAG / Tool Calling / WASI · WebRTC 音视频 · OpenSearch 全文检索 · Vert.x 集群模式（ClusterManager）· `ZenohLock`/`ZenohCounter` 作为业务强一致原语 · 多语言客户端 SDK · 工单自定义字段的可视化建模器 · CSAT/报表 BI。

### 20.3 分阶段交付与出场标准

**Phase 0 · 地基验证（1~2 周）**

内容：zenoh-jni Linux x64/aarch64 交叉编译（或切官方 zenoh-java）；`Pool` Bean + Flyway 基线；独立端口原生 `HttpServer` 骨架；ArchUnit + BlockedThreadChecker 接入。

出场标准：
- 3 节点在 `linux/amd64` 与 `linux/arm64` 容器中组网成功，非 Alpine 镜像；
- Zenoh 收发消息的 handler 执行线程名为 `vert.x-eventloop-*`；
- `kill -9` 一个节点，≤15s 内其余节点感知；
- CI 中 ArchUnit 全部规则绿灯。

**Phase 1 · IM 主干（4~6 周）**

内容：Envelope 协议 + WebSocket + SSE + 能力协商；ConnectionRegistry 三层；Conversation/Message/Assignment 领域与状态机；seq + 幂等 + Outbox + 容量 CAS；Zenoh EventMesh + CBOR codec；派单流水线（Java 决策表兜底实现）。

出场标准：
- 客户与坐席跨节点、跨传输（WebSocket ↔ SSE）双向通信正常；
- 断线重连按 seq 补齐，**消息零丢失零重复**（客户端去重后）；
- 并发派单压测下 `agent_workload.active_count` 从不超过 `capacity`，`ck_workload_range` 从未触发；
- 重复 `requestId` 返回同一结果，DB 中只有一条消息；
- 单节点 1 万并发 WebSocket 连接，P99 消息端到端延迟 < 200ms；
- 强制断开 Zenoh 后 Outbox 堆积，恢复后全部补投成功。

**Phase 2 · 工单与 SLA（3~4 周）**

内容：Ticket 聚合与状态机；Conversation↔Ticket 关联；SlaPolicy/SlaInstance/BusinessCalendar；两级定时器；升级链；控制面工单 REST。

出场标准：
- 一次会话可产生多张工单，一张工单可关联多次会话，关系查询正确；
- SLA 首响/解决计时在跨工作时段、节假日、`WAITING_CUSTOMER` 暂停场景下计算正确（用固定日历的黄金用例集断言）；
- 节点重启后 SLA due 不丢（timer_task 持久化验证）；
- breach 触发升级链，`sla_event` 审计完整；
- 定时器补偿扫描能回收崩溃节点 CLAIMED 的任务。

**Phase 3 · 规则引擎与运营化（3~4 周）**

内容：`cs-rule-zen` 接入 GoRules ZEN；规则版本化 + 灰度分桶 + shadow 对账 + Simulator；RBAC + 审计 + 限流分层；可观测性完整落地。

出场标准：
- ZEN 规则热更新生效无需重启，降级路径可用且有告警；
- Simulator 能对历史 1 万条 DispatchContext 回放并输出分布对比；
- shadow 模式差异率报表可用；
- 全部第 18 章指标可见，第 18.2 节告警全部配置并演练过一次；
- 越租户访问、越权访问的渗透用例全部拦截。

**Phase 4 · 优化与扩展（持续）**

Zenoh 延后项（Z-5~Z-9）· `data-dict` 跨节点 L1 失效 · 控制面 HTTP/3 · OpenSearch · 报表/CSAT · AI Agent 接缝落地。

---

## 附录 A · 技术结论可追溯清单

**本仓库实测代码事实**

| 结论 | 位置 |
|---|---|
| SSE/流式不可用（collectList 后单次 end；首元素结束响应） | `vertx-spring-boot-web-server` → `VertxServerHttpResponse.writeWithInternal` / `writeAndFlushWithInternal` |
| 无 WebSocket 能力 | `VertxWebServer`（只 `requestHandler`） |
| `HttpServer`/`HttpServerOptions` private，无定制点 | `VertxWebServer` |
| TLS 只 `setSsl(true)`，证书未接线 | `VertxReactiveWebServerFactory` |
| `registrations` 纯本地无订阅 → 跨节点 send 失效 | `ZenohClusterManager` |
| 节点心跳 5s `session.put(nodes/{id})` 无 TTL | `ZenohClusterManager` |
| `getSyncMap` 用 `.await()` 阻塞 | `ZenohClusterManager` |
| `ZenohMapStore` CAS 本地生效后广播 → 跨节点 LWW | `ZenohMapStore` |
| `ZenohLock` 自述 best-effort | `ZenohLock` |
| `ZenohAsyncMap` 用 JDK 序列化 | `ZenohAsyncMap` |
| Zenoh 接收路径为阻塞 `recv()` 守护线程，无 `runOnContext` | cluster-zenoh 各订阅实现 |
| key 前缀根为 `vertx/{app}/{env}` | `KeyPrefixStrategy` |
| 只有 `darwin-arm64/libzenoh_ffi_jni.dylib` | `zenoh-jni/src/main/resources/` |
| `Querier.get()/getWithPayload(byte[])` 存在但未被使用 | `zenoh-jni` → `Querier` |
| 仅 `Future<T>` 签名 Mapper 走异步，其余 `.join()` | `vertx-spring-boot-mybatis` → `VertxMapperProxy` |
| 全仓无 `io.vertx.sqlclient.Pool` Bean、无数据源配置 | 全仓检索 |
| `VertxDataSourceTransactionManager` thread-bound + `.join()` | `spring/VertxDataSourceTransactionManager` |
| `VertxCursor` 非流式（预加载全部） | `VertxCursor` |
| 全仓无 Redis / S3 / JWT / Security | 全仓检索 |
| `data-dict` 两级缓存无 pub/sub 失效，L1 只本地驱逐 | `data-dict` 缓存实现 |

**外部技术事实（2026-09 核实）**

| 结论 |
|---|
| Vert.x 5.1.7 HTTP/3 + QUIC 为 GA，在 `vertx-core` 内：`HttpServerConfig.setVersions(HttpVersion.HTTP_3)`、`Http3ServerConfig`、`QuicServer`、`Vertx.createQuicServer` |
| `HttpServerOptions` **没有** H3 开关；需可选依赖 `netty-codec-native-quic` 且必须带 classifier |
| JVM 上**无可生产落地的 WebTransport 服务端实现**；nginx/Envoy/HAProxy/Caddy 均不终结 WebTransport；Netty extended CONNECT 2025-11 才合入主干；`webtransport4j` 未发布 |
| Vert.x 5 服务端 WebSocket：`webSocketHandshakeHandler` + `accept()/reject()` 或 `request.toWebSocket()`；背压 `setWriteQueueMaxSize`/`writeQueueFull()`/`drainHandler`；ping/pong 需自行调度；**RFC 8441 不支持** |
| GoRules ZEN 有官方 Java 绑定 `io.gorules:zen-engine:2.0.2`（UniFFI over Rust），jar 内含 linux-x86-64/linux-aarch64/darwin/win 原生库，支持 JDM 求值与 `ZenLoader` 热加载 |
| 官方 `org.eclipse.zenoh:zenoh-java:1.10.0` 需配 `zenoh-flat-jni-jvm:1.10.0`，含 linux x64/aarch64，要求 glibc ≥ 2.28，**无 musl → 不支持 Alpine** |
| Zenoh **无 CAS/线性一致原语**；liveliness token **无租约自动过期** |
| `vertx-pg-client` 5.1.x：`Pool.withTransaction`、pipelining、预编译缓存、cursor/RowStream 均 GA；`FOR UPDATE SKIP LOCKED` 与 advisory lock **无 API 封装，是裸 SQL**；会话级 advisory lock 必须 `withConnection` |

**仓库已记录的运维经验（沿用）**

| 结论 |
|---|
| 多实例只能一个配 `mode: router`（占 7447），其余 `client`，否则端口冲突 |
| zenoh 插件/原生库要求 rustc 版本精确匹配（含 commit hash） |
| Zenoh 资源安全停止顺序：先 undeclare 所有 Subscriber/Queryable，再关 session |
| Vert.x 资源 `close()` 需捕获并静默 `RejectedExecutionException` |
| JSON 序列化统一用 `io.vertx.core.json.JsonObject`，不用 Jackson `ObjectMapper` |
| JNI 库从 classpath 按平台目录自动加载（`darwin-arm64/` 等） |
