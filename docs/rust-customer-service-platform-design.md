# Rust 生产级客服平台完整方案（Helpdesk + Workflow + AI Agent Runtime）

> 版本：2026-09
> 语言与路线：Rust stable / Tokio / Axum / PostgreSQL / Zenoh / wasmtime / rig
> 来源：合并《Rust 开源客服 + 工单系统完整方案》与《Rust Customer Agent Runtime 完整技术设计方案》
> 定位：可排期、可验收的落地方案，而非能力清单

---

## 0. 阅读指引

本文档合并两份 Rust 源文档并**收敛其中互相冲突的技术选择**。两份源文档的共同问题是：大量结论以「建议」形式给出，未标注版本、未验证 API 是否存在、未说明失败模式。本文档的第 2 章建立**已核实的技术事实基线**，后续所有结论都必须能追溯到那张表；附录 A 给出反向索引。

### 0.1 对两份源文档的取舍

| 源文档 | 取舍 |
|---|---|
| `rust-customer-service-helpdesk-complete-design.md` | **主体采纳**：产品定位、会话域模型、SLA 独立域、Queue+Routing 打分、Channel Adapter、多租户与权限、Outbox。**不采纳**：`services/` 微服务拆分（Phase 1 单二进制多角色，见 4.4）、OpenSearch 作为 Phase 1 依赖（见第 19 章）、**Ticket 作为平台内一等聚合**（工单是独立通用服务，见 1.3 与 5.3）、「Workflow = 自研 State Machine / DAG」（改为 zen-engine 单引擎，见第 13 章） |
| `rust-customer-agent-runtime-design.md` | **主体采纳**：Brain/Policy/Tool 三层分离、LLM 不直接持有业务权限、AgentAction → Policy → Dispatcher 执行链、Crash Recovery 快照、幂等键、Command/Event 分离、WASI Component Model + WIT、**工单只作为外部工具而非平台聚合**、**Workflow 交给 Zen Engine**（采纳但必须补全其缺失的持久化与挂起能力，见第 13 章）。**重写**：Agent 状态机（15 态压成两级，见第 14 章）、Zenoh topic 命名（与另一份文档冲突，见 9.1） |

### 0.2 两份源文档之间的 7 处冲突及裁决

| # | 冲突点 | 文档 A | 文档 B | 本文档裁决 | 依据 |
|---|---|---|---|---|---|
| 1 | Workspace 布局 | `customer-service/` + `cs-*` + `services/*` | `customer-agent/` + `agent-*` | 单 workspace，`cs-*` 与 `ca-*` 两个 crate 族，**`ca-*` 禁止依赖 `cs-*`** | 4.2 |
| 2 | Zenoh key 命名 | `cs/{tenant}/...` | `ca/{tenant}/{domain}/{resource}/{id}/{event}` | 统一为带版本段的单一方案，唯一构造入口 | 9.1 |
| 3 | Workflow 引擎 | 自研 State Machine / DAG | Zen Engine | 采纳 B：**zen-engine 是唯一引擎**，JDM 即 workflow 定义格式；不写第二套 DAG 模型，只加一个**不含业务语义的持久化驱动器**补齐 zen 缺失的挂起/定时/重试 | F-2 / F-14 / 第 13 章 |
| 4 | Agent 状态机 | 9 态 | 15 态 | 两级：平台态 9 个（可持久化可暂停）+ Brain 内核态由 `rig-agent` 拥有 | F-1 / 第 14 章 |
| 5 | Rig 定位 | 「Rig + 自研 Agent Runtime」 | 「Rig 只是 Brain Adapter」 | 采纳 B，并因 rig 为 0.x 追加版本隔离与快照弃用策略 | F-1 / 14.5 |
| 6 | 向量与检索 | OpenSearch | Qdrant / Milvus | Phase 1 单一 Postgres（pgvector + tsvector），独立检索引擎延后 | F-10 / 第 19 章 |
| 7 | Ticket 归属 | 平台内一等聚合 | 仅作为 `ticket-tool` 外部工具 | 采纳 B：**工单是独立通用服务**，不在本平台边界内。本平台只经 Outbox **单向外发**会话事件，不持有工单模型、不做反向查询 | 1.3 / 5.3 |

---

## 1. 目标、非目标与边界

### 1.1 目标

构建 Rust 原生、可自托管的客服平台，覆盖：

- 全渠道接入 → 会话创建 → 规则决策 → 派单 → 实时双向通信 → 转接/结束
- Conversation 单一会话侧聚合；**会话级** SLA 计时、预警与超时升级（工单外置，见 1.3）
- Queue / Skill / Team / 排班 / 容量 / Presence
- 长时 Workflow 编排（含人工审批、延时、重试、补偿）
- AI Agent Runtime：可暂停、可崩溃恢复、可审计，LLM 不持有业务权限
- WASI Tool 沙箱与插件版本化
- 多租户、RBAC、审计、限流、可观测性
- 多节点，Zenoh 作事件面，PostgreSQL 作唯一真相源

### 1.2 非目标（Phase 1 明确不做）

1. **HTTP/3 作为 IM 数据面**——`h3` crate 仍为 0.0.8 且自述 experimental（F-5），只在控制面做可选实验端口。
2. **WebTransport 生产落地**——`wtransport` 0.7.2 自述 not production-ready（F-6），仅留 SPI 与协商位。
3. **WebRTC / MoQ 音视频媒体流**——音视频消息只承载对象存储元数据，信令与媒体面解耦但媒体面不实现。
4. **独立检索引擎与独立向量库**——Phase 1 全部落 Postgres（F-10）。
5. **Multi-Agent / Supervisor 编排**——Runtime 留接缝，Phase 1 单 Agent。
6. **Zenoh 承载强一致状态**——Zenoh 无 CAS、liveliness 无租约（F-8），一致性全部落 Postgres。
7. **工单系统本体**——工单是独立的通用服务（不限客服场景），有自己的边界、库与生命周期。本平台只**单向外发**会话事件，不持有工单模型、不做反向查询（见 1.3、5.3）。

### 1.3 与既有 Java IM 方案的关系

本仓库另有 `docs/java-customer-service-platform-design.md`。两份方案在**工单边界上结论一致**：工单是独立的通用服务，两个平台都不持有工单模型，都只经 Outbox 单向外发会话领域事件。

因此两者不是分工，而是**同一职责的两种可选实现栈**：

```text
                     Channel（Web / App / 邮件 / IM 渠道）
                                    │
                 ┌──────────────────┴──────────────────┐
                 ▼                                     ▼
      Java IM 平面（Vert.x）                  Rust 平台（本文档）
      会话 / 消息 / 派单 / 会话级 SLA           同上 + Workflow + Knowledge
                                              + AI Agent Runtime + WASI Tool
                 │                                     │
                 └──────────────────┬──────────────────┘
                                    │  单向外发（at-least-once）
                                    │  ConversationEscalated / ConversationClosed / SlaBreached …
                                    ▼
                        通用工单服务（独立，非本文档范围）
                        自有边界 / 自有库 / 自有生命周期
                        按 conversationId 建单，本平台不反查
```

选型差异只在能力，不在边界：**Rust 方案多出 Workflow 编排、Knowledge 检索、AI Agent Runtime 与 WASI Tool 沙箱**（第 13～16、19 章），这些在 Java 方案里是明确的非目标。第 5、11 章的会话侧领域模型两边基本同构，可直接对照阅读；附录 B 给出逐项差异。

**外发契约由两个平台共用一份定义**，这样通用工单服务不需要区分上游是哪个栈。契约细节见 5.3。

---

## 2. 已核实的技术事实基线

**本章是全文的地基。** 所有版本号与 API 限制均于 2026-09-06 核实。带 ✗ 的是**必须绕过**的限制，它们直接决定了后续章节的设计。

### 2.1 传输与 Web 层

| # | 事实 | 影响 |
|---|---|---|
| F-3 | `axum` **0.8.9**，feature 表只有 `http1`/`http2`，**无 http3** | HTTP/3 必须另起 `quinn` + `h3` UDP 监听，不能复用 axum 监听器 |
| F-4 | ✗ **axum WebSocket 拿不到真实 TCP 背压**：写缓冲仅在底层写出错时才超过 `write_buffer_size`，`Sink::poll_ready` 几乎恒为 `Ready`；`max_write_buffer_size` **默认无限** | **逐连接背压必须自建**（bounded mpsc + 丢弃分级 + 踢连接），并下调 `max_message_size`（默认 64MB）防 OOM。见 8.4 |
| F-4b | ✗ 底层 `tungstenite` 0.30 **无 permessage-deflate** | 压缩只能做在应用层（Envelope payload 级），不能依赖 WS 扩展 |
| F-4c | ✗ axum 0.8.9 内部依赖 `tokio-tungstenite ^0.29`，直接混用其 `Message` 类型会冲突 | 不在业务层暴露 tungstenite 类型，一律转成自有 `Frame` |
| F-5 | `quinn` **0.11.11** 成熟；`h3` **0.0.8**，2025-05 后未更新，自述 "very experimental" | HTTP/3 不进 Phase 1 数据面（非目标 1） |
| F-6 | `wtransport` **0.7.2**，自述 not production-ready；自带 `wtransport-proto` 不依赖 `h3`，**无法在同端口同时服务普通 HTTP/3**；`Identity::self_signed` 强制有效期 ≤ 14 天 | WebTransport 只留 SPI；即便在 Rust 侧也不是生产选项（非目标 2） |
| F-7 | nginx ≥ 1.25.0 支持 `http_v3_module` 但**仅终结下游 H3，回源仍 HTTP/1.1 或 2**；nginx / Envoy **均无 WebTransport 代理能力** | WebTransport 若将来启用，必须直连或 L4 UDP 透传，不能走现有 L7 入口 |
| F-2t | `tokio` **1.53.1**，`tokio-tungstenite` **0.30.0** | — |

### 2.2 数据与一致性

| # | 事实 | 影响 |
|---|---|---|
| F-9 | `sqlx` **0.9.0**（MSRV 1.94）。✗ **不支持 PG pipelining**；✗ `FOR UPDATE SKIP LOCKED` **无 API，只能裸 SQL**；`query!` 要求 CI 备 `.sqlx` 缓存 + `SQLX_OFFLINE=true`；0.9 破坏性变更：查询参数改 `SqlSafeStr`，动态 SQL 需 `AssertSqlSafe` | 热点路径靠**批量与并发**而非 pipelining 提吞吐；队列取件与锁写裸 SQL 并集中在 repository 层；CI 增加 `cargo sqlx prepare --check` 关卡。见第 18 章 |
| F-9b | ✗ `PgListener` 有自动重连，但**重连窗口内通知丢失**（at-most-once），payload 上限 8KB | **LISTEN/NOTIFY 不能作事件骨干**，只能作为 Outbox relay 的「催醒」信号，正确性由轮询兜底。见 17.5 |
| F-9c | `sea-orm` 2.0.2 建在 sqlx 之上（多一层抽象无收益）；`diesel-async` 0.9.2 类型安全更强但异步生态更弱 | 选定 sqlx，不引入 ORM |
| F-10 | `pgvector` **0.8.6**：HNSW 自 0.5.0 起成熟，0.8.0+ 支持 iterative index scans。✗ `vector` 类型**维度上限 2000**；索引构建受 `maintenance_work_mem` 约束 | Phase 1 用 pgvector；embedding 维度选型必须 ≤ 2000。见 19.2 |
| F-10b | ✗ Postgres `tsvector` **无内置中文分词**，需 zhparser / pgroonga / pg_bigm；高写入下 GIN 更新放大明显 | 中文检索必须显式选定分词扩展并纳入镜像构建。见 19.1 |
| F-10c | `tantivy` **0.26.1** 是单机库，无分片/副本/集群（分布式需 Quickwit） | 不能直接等价替代 OpenSearch，延后 |
| F-11 | `qdrant-client` **1.19.0**，仅 gRPC/tonic | 延后；接口抽象为 `Knowledge` trait 以便切换 |

### 2.3 事件面（Zenoh）

| # | 事实 | 影响 |
|---|---|---|
| F-8a | `zenoh` **1.10.0**，`Queryable`/`Querier` 请求应答真实存在 | 命令/请求走 Query/Reply，事件走 Pub/Sub（第 9 章） |
| F-8b | ✗ `LivelinessToken` **无 TTL / 无租约**，builder 仅 `background()`，仅绑 Session 生命周期 | **进程假死但连接未断时不会过期** → 节点存活判定必须自建心跳 + Postgres 侧租约。见 10.2 |
| F-8c | ✗ 源码无 `compare_and_swap` 等**任何线性一致原语** | 所有仲裁（容量抢占、选主、序列号）落 Postgres。见第 17 章 |
| F-8d | storage/backend 插件为 `zenoh_plugin_*` 动态库，经 config `plugins` 段 + libloading 加载，需 `plugins`/`runtime_plugins` feature（官方标 **internal/unstable**），静态注册无公开 API | 不依赖 Zenoh storage 做持久化；插件能力不进关键路径。见 10.5 |

### 2.4 AI 与插件

| # | 事实 | 影响 |
|---|---|---|
| F-1 | `rig-core` **0.42.0**，**0.x 未到 1.0**，README 明确警告未来版本必然含破坏性变更（0.39–0.42 每个 minor 都带 `[breaking]`）；0.41 起拆为 `rig-core` / `rig-agent`。**`rig_agent::run::AgentRun` 是 sans-IO、`Serialize + Deserialize` 的步进状态机**（`CallModel` / `CallTools` / `Done`），**可挂起并在另一进程恢复**，但无内置持久化后端；`ModelHandle` 与 hook 栈刻意不可序列化。✗ **不支持 WASI**（仅 native 与 browser wasm32-unknown-unknown） | 决定了第 14 章的**两级状态机**：不重复实现 LLM 循环，直接持久化 `AgentRun` 快照。因 0.x 破坏性，快照必须带 schema 版本 + 弃用策略（14.5）。Agent Runtime 自身不能编译进 WASI |
| F-2 | `zen-engine` **2.0.1**（2026-08-22），已 1.0+。节点类型是**封闭枚举**：input/output/decisionTable/expression/function/switch/decision(子图)/custom。✗ **无 wait / timer / delay / human-task / signal / retry 节点**，✗ **不持久化任何 run 状态、无 suspend/resume**，✗ 图必须**无环**（`CyclicGraph` 校验错误），✗ `EvaluationOptions` 只有 `max_depth`(u8, 默认 10) / `trace`，**无求值级超时**，✗ 热加载**无 invalidate API**。三个关键扩展点（可用）：`CustomNodeAdapter::handle` 是 **async**（`Arc<dyn CustomNodeAdapter + Send + Sync>`，经 `with_adapter` 注册）；`DecisionLoader::load` 是 async 且**返回 Send future**（可直接查 Postgres，内置 `MemoryLoader`/`CachedLoader`/`ClosureLoader`）；`DecisionGraphTrace` 是 **`Serialize + Deserialize`** 且带 `order` 字段 | 裁决 #3 的直接依据。缺的四样（持久化/挂起/定时/重试）**与业务语义无关**，因此归入一个不含业务语义的驱动器（13.1）；三个扩展点分别成为副作用边界（13.4）、版本化定义加载（13.5）、memo 与补偿逆序依据（13.3/13.8）；无超时 → 外层自套 `timeout`（12.4/13.6）；无 invalidate → 不可变 key loader（12.5/13.5） |
| F-14 | zen-engine 的**执行模型与安全约束**（2.0.1 实测）：✗ `DecisionEngine::evaluate` 返回的 future 是 **`!Send`**，官方要求用 `tokio-util` 的 `LocalPoolHandle` 钉线程，**不能 `tokio::spawn`**（引擎句柄本身是 `Send + Sync + Clone`，只有求值 future 不是）；✗ `functionNode` 在进程内跑 QuickJS（rquickjs 0.10 + `swc_ts_fast_strip`），默认超时 `ZEN_CONFIG.function_timeout_millis = 5000ms`，**未设内存上限**（`set_memory_limit` 未调用）；✗ function 节点内置 `console` 与**带云厂商签名（reqsign）的 HTTP 模块**，规则作者可从规则里发外网请求；✗ `ZEN_CONFIG` 是**进程级全局**（`Lazy` + 原子），无法按引擎/按租户覆盖；✗ `TransformExecutionMode::Loop` **顺序执行**，图内**无并行 fan-out**；宿主侧可用 `with_http_handler` 拦截 function 节点的 HTTP | 决定 13.6 的 `LocalPoolHandle` 执行宿主与「customNode 内禁止 `block_on`」；决定 13.7 的 functionNode 治理（租户图禁用 function、`DenyAllHttpHandler`、启动期收紧全局超时）；决定并行分支必须由驱动器拆多 run（13.4）；QuickJS 无内存上限无法在库内解决 → 残留风险 R-13 |
| F-12 | `wasmtime` **48.0.1**（月度发布，24/36 为 LTS）。`component-model` 与 wasi-io/clocks/filesystem/random/sockets/**http 均 Tier 1 stable**；rustc `wasm32-wasip1/p2/p3` 均 Tier 2。WASI 0.3 已于 2026-06-11 发布（async 原生化），Wasmtime 46+ 支持，但 wasi:http 0.3 的 **Rust guest 工具链仍在补齐**。`wit-bindgen` **0.61.1** 仍 0.x、随组件模型频繁 breaking。沙箱：fuel（确定性、开销较大）、epoch interruption（约 10% 开销、基于 wall-time 故不确定）、`StoreLimitsBuilder`（memory_size / memories / tables / instances / trap_on_grow_failure）。✗ **fuel / epoch 只约束 guest，host 调用耗时不受限**；✗ 出网默认关闭且**无内置带宽 / 连接配额** | Tool 沙箱以 WASI **P2** 为目标（P3 观察），WIT 包名带版本号隔离 `wit-bindgen` 破坏性变更；host 侧必须自套超时与配额。见第 16 章 |
| F-13 | `rmcp` **3.2.0**（官方 rust-sdk），3.x 生产可用，实现 2026-07-28 规范。transport：stdio、child-process、Streamable HTTP（server 用 Tower / client 用 reqwest）、进程内 worker。✗ **不提供** 2024-11-05 legacy HTTP+SSE；✗ **无 WebSocket**；✗ 订阅状态**跨重连不恢复**，需重新 `listen` | MCP Tool 走 Streamable HTTP；重连后必须由 Runtime 主动重建订阅 |

### 2.5 本仓库现状

本仓库为 Java 工程，**无任何 Rust 源码**；`zenoh-jni/src/main/resources/` 下只有 `darwin-arm64/libzenoh_ffi_jni.dylib` 一个预编译产物。因此本方案对本仓库而言是**全新 workspace**，无可复用 Rust 代码，但团队已有 zenoh 1.10 + FFI 的实战经验可直接迁移（尤其是 10.1 的多平台交叉编译问题在 Java 侧已踩过一遍）。

---

## 3. 总体架构与分层原则

```text
                        Customer Channels
   WebChat    Email    WhatsApp    Telegram    WeChat    Voice(信令)
      |          |         |           |          |         |
      +----------+---------+-----------+----------+---------+
                                 |
                       ┌─────────▼──────────┐
                       │  Channel Adapter   │  归一化为 InboundMessage
                       └─────────┬──────────┘
                                 |
   ┌─────────────────────────────▼─────────────────────────────┐
   │                   cs-gateway  (无状态)                     │
   │   axum: REST + WebSocket(:8443)   |   quinn+h3(实验:8444)  │
   │   自建逐连接背压（F-4 强制）                                 │
   └─────────────────────────────┬─────────────────────────────┘
                                 |
                        Unified Envelope
                                 |
   ┌─────────────────────────────▼─────────────────────────────┐
   │                cs-application (用例编排)                    │
   │  Conversation · Message · Dispatch · SLA · Outbox          │
   └───────┬──────────────────┬──────────────────┬─────────────┘
           |                  |                  |
           v                  v                  v
   ┌───────────────┐  ┌────────────────┐  ┌────────────────────┐
   │ cs-domain     │  │ cs-workflow    │  │ ca-runtime         │
   │ 纯 Rust       │  │ 持久化驱动器    │  │ AI Agent Runtime   │
   │ 聚合/状态机    │  │  ├ memo 重放   │  │  ├ Brain(rig)      │
   │ 无 IO 依赖    │  │  └ timer_task  │  │  ├ Policy(Zen)     │
   └───────────────┘  └────────────────┘  │  ├ Tool(WASI/MCP)  │
                                          │  └ Handoff         │
                                          └────────────────────┘
           |                  |                  |
           +------------------+------------------+
                              |
        ┌─────────────────────┼─────────────────────┐
        v                     v                     v
  ┌───────────┐      ┌────────────────┐    ┌────────────────┐
  │ PostgreSQL│      │     Redis      │    │     Zenoh      │
  │ 唯一真相源 │      │ Presence/限流   │    │  事件面(only)   │
  │ +pgvector │      │ 路由表/缓存      │    │ Pub/Sub+Query  │
  └───────────┘      └────────────────┘    └────────────────┘
        |
  ┌───────────┐
  │ S3/MinIO  │  预签名 URL，二进制不过业务进程
  └───────────┘
```

### 3.1 五条分层铁律

1. **Domain 无 IO**：`cs-domain` / `ca-core` 不得依赖 axum、sqlx、redis、zenoh、rig、wasmtime。由 CI 的依赖断言强制（4.3）。
2. **Zenoh ≠ 数据库**：Zenoh 只做事件 fan-out、定向投递、presence 提示、请求应答。任何需要「读到的一定是最新值」或「只有一个赢家」的语义，全部落 Postgres（F-8b、F-8c）。
3. **Transport ≠ 协议**：Envelope 与业务语义和传输通道正交。同一 Envelope 必须能经 WebSocket、SSE、（将来）WebTransport 原样投递。
4. **LLM ≠ 权限持有者**：Brain 只能产出 `AgentAction` 提议，Policy 裁决后才由 Dispatcher 执行。LLM 永远不直接触达业务系统（源文档 B 原则 2，全文保留）。
5. **求值 ≠ 驱动**：zen-engine 是唯一的求值器，派单决策与 workflow 编排共用它；`cs-workflow` 只做持久化、重入、唤醒、补偿，**不含任何业务语义**——驱动器里出现业务分支即为设计违规（F-2、13.1）。

---

## 4. Cargo Workspace 与依赖方向

### 4.1 Workspace 布局

```text
support-platform/
├── Cargo.toml                 # [workspace] 单一 workspace
├── rust-toolchain.toml        # 锁定 stable 版本，与 CI 一致
├── crates/
│   ├── cs-core/               # TenantId/Id/时间/错误/Result 别名
│   ├── cs-domain/             # 聚合、状态机、不变式（无 IO）
│   ├── cs-contract/           # Envelope/Command/Event/DTO + 版本演进
│   ├── cs-application/        # 用例编排、事务边界
│   ├── cs-repo-pg/            # sqlx 实现，裸 SQL 集中于此
│   ├── cs-cache-redis/        # presence / 路由表 / 限流
│   ├── cs-storage-s3/         # 预签名 URL
│   ├── cs-mesh/              # EventMesh trait
│   ├── cs-mesh-zenoh/         # Zenoh 实现 + KeyExpr 唯一构造入口
│   ├── cs-transport/          # Connection/Registry/背压/协商（trait 层）
│   ├── cs-transport-ws/       # axum WebSocket + 自建背压
│   ├── cs-transport-sse/      # SSE + HTTP Command 保底通道
│   ├── cs-transport-wt/       # WebTransport 桩（feature-gated，默认关）
│   ├── cs-channel/            # ChannelAdapter trait + 各渠道实现
│   ├── cs-routing/            # 派单流水线
│   ├── cs-sla/                # SLA 域 + 工作日历
│   ├── cs-workflow/           # 持久化驱动器（无业务语义，13.1）
│   ├── cs-knowledge/          # KB + 检索（pgvector）
│   ├── cs-decision/           # zen 引擎宿主：LocalPool + loader + adapter
│   ├── cs-integration/        # 对外单向外发契约 + 出站 ACL（5.3）
│   ├── cs-gateway/            # axum 组装 + 路由
│   ├── cs-worker/             # Outbox relay / timer / SLA / 补偿扫描
│   ├── cs-observability/      # OTel + Prometheus
│   ├── cs-ai-bridge/          # ⚠ 唯一允许同时依赖 cs-* 与 ca-* 的 crate
│   │
│   ├── ca-core/               # Agent 领域契约与 trait（无 IO）
│   ├── ca-state/             # 平台级状态机 + 快照
│   ├── ca-runtime/            # Runtime loop / Planner / Dispatcher / Evaluator
│   ├── ca-brain/              # Brain trait + rig 适配（版本隔离）
│   ├── ca-policy/             # Policy trait + Zen 实现
│   ├── ca-tool/               # ToolRegistry / Manifest / Dispatcher
│   ├── ca-tool-wasi/          # wasmtime Component 宿主 + 权限
│   ├── ca-tool-mcp/           # rmcp 客户端
│   ├── ca-memory/             # 三层记忆
│   └── ca-handoff/            # 人工转接
├── wit/
│   ├── tool@1.0.0.wit
│   └── host@1.0.0.wit
├── plugins/                   # WASI Tool 源码（独立 crate，独立版本）
├── migrations/                # sqlx migrate
└── .sqlx/                     # 编译期查询缓存，必须提交（F-9）
```

### 4.2 `ca-*` 禁止依赖 `cs-*`

这是裁决 #1 的核心。源文档 B 明确希望 Agent Runtime 是可独立复用的产品，源文档 A 又把它当作平台内的一个 crate。折中方案：**同一 workspace（共享 CI、lockfile、toolchain），但依赖方向单向**。

```text
cs-gateway ──▶ cs-application ──▶ cs-domain ──▶ cs-core
                     │
                     └──▶ cs-ai-bridge ──▶ ca-runtime ──▶ ca-core
                                                 │
                              ┌──────────────────┼──────────────────┐
                              ▼                  ▼                  ▼
                          ca-brain           ca-policy          ca-tool
                              │                  │                  │
                            rig             zen-engine      ca-tool-wasi / -mcp
```

- `ca-*` **不得出现任何** `cs-*` 依赖。Agent Runtime 需要的会话上下文由 `cs-ai-bridge` 转成 `ca-core` 的输入类型传入。
- 收益：Runtime 可整体抽出发布为独立 crate；`rig` 的 0.x 破坏性变更（F-1）被挡在 `ca-brain` 内，不会传导到平台层。
- 代价：多一层类型转换。这是有意的——它同时是 F-1 版本隔离的落点。

### 4.3 依赖方向由 CI 强制

不依赖口头约定。CI 增加三个关卡：

| 关卡 | 工具 | 断言 |
|---|---|---|
| 分层 | `cargo-deny` + 自定义 `deny.toml` bans | `cs-domain` / `ca-core` 的依赖树中不得出现 axum / sqlx / redis / zenoh / rig / wasmtime |
| 反向依赖 | `cargo tree --invert` 脚本 | 任何 `ca-*` 的依赖树中不得出现 `cs-*` |
| 查询缓存 | `cargo sqlx prepare --check` | `.sqlx` 与源码一致，否则 CI 红（F-9） |

### 4.4 Phase 1 不拆微服务

源文档 A 的 `services/{gateway,conversation,ticket,worker,ai-agent,notification}` 是六个独立二进制。Phase 1 **不采纳**，改为**单二进制 + 角色开关**：

```bash
support-platform --role gateway,worker,agent   # 小规模单进程
support-platform --role gateway                # 中规模按角色横向扩
```

理由：跨进程边界会把大量本可以是函数调用的交互变成 Zenoh 往返，而在 Zenoh 无 CAS、无租约（F-8b/c）的前提下，每一次跨进程都要新增一致性设计。角色开关保留了后续拆分能力——因为编译期已由 4.2/4.3 强制了依赖方向，拆分时不需要重构领域层。

---

## 5. 核心领域模型与聚合边界

### 5.1 聚合清单

```text
Customer     (聚合根)  身份、标签、自定义属性、语言时区
Conversation (聚合根)  状态、优先级、归属、last_seq、awaiting_party
Message                隶属 Conversation，不独立演进
Agent        (聚合根)  状态、presence、技能、容量、排班
Queue        (聚合根)  成员、策略
Assignment   (聚合根)  一次派单尝试的生命周期
SlaInstance  (独立域)  只挂 Conversation，单 target
Workflow     (聚合根)  定义 + Run 实例
AgentExecution(聚合根) 一次 Agent 回合的可恢复执行体
OutboundEvent(投影)    经 Outbox 单向外发的会话领域事件（5.3）
```

**没有 Ticket 聚合。** 工单是独立的通用服务，本平台不持有工单模型、无 `ticket*` 表、无工单 CRUD、无工单 SLA（1.2 非目标 7）。

### 5.2 聚合边界铁律

- **一个事务只修改一个聚合**。跨聚合副作用一律经 Outbox（17.5）。
- 与工单服务之间**只有出站方向**：会话侧写 Outbox，工单侧自行消费；本平台不持有工单 ID、不做反向查询（5.3）。
- `AgentExecution` 独立于 `Conversation`：Agent 崩溃恢复不得回滚会话消息。
- 每个聚合根带 `version: i64` 乐观锁。

### 5.3 对外单向外发契约

会话中产生的升级/建单诉求，以领域事件形式**单向**离开本平台。下游是谁（通用工单服务、CRM、BI）对领域层不可见。

```text
会话内触发升级 / 结束 / SLA 违约
        │
        ▼
同事务写 outbox_event
        │  aggregate_type = CONVERSATION
        │  event_type     = ConversationEscalated | ConversationClosed | SlaBreached
        │  key_expr       = {root}/v1/{tenant}/intg/outbound/{conversation_id}/event
        ▼
Outbox Relay（cs-worker，at-least-once）
        ▼
通用工单服务订阅，自行决定是否建单、如何建单
```

契约由 `cs-integration` 持有，与 Java 平面共用同一份定义：

```rust
pub struct OutboundIntegrationEvent {
    pub event_id: Uuid,            // 去重键，下游按此幂等
    pub event_type: OutboundEventType,
    pub occurred_at: i64,
    pub tenant: TenantId,
    pub conversation: ConversationId,
    pub customer: CustomerId,
    pub channel: Channel,
    pub category: Option<String>,
    pub priority: i32,
    pub agent: Option<AgentId>,
    pub reason: EscalationReason, // SlaBreach | AgentHandoff | Manual | Closed
    pub transcript_ref: String,   // 拉取地址，不内联正文
    pub trace_id: Option<String>,
}
```

设计要点：

- **只给引用不给内容**：`transcript_ref` 是会话记录的拉取地址，事件体不塞消息正文——避免事件膨胀，也避免正文在外部系统留副本；
- **at-least-once**，下游必须按 `event_id` 去重（与 17.5 的 Outbox 语义一致）；
- 本平台**不等待外部响应**，外部失败不回滚会话状态。这是单向集成的核心取舍：可用性换一致性，代价是「已升级但未建单」的窗口，由下游重放消费兜住；
- 本平台**不提供** `conversation → ticket` 反查。工作台若要展示「本会话关联的工单」，由工单服务按 `conversation_id` 反查、前端聚合，**不落库到本平台**；
- 新增下游只在 `cs-integration` 增加订阅方，领域层零改动。

---

## 6. 统一协议

### 6.1 Envelope

```rust
pub struct Envelope {
    pub v: u16,                     // 协议版本，不兼容变更才递增
    pub id: EnvelopeId,             // 客户端生成，用于幂等与 ACK 关联
    pub kind: EnvelopeKind,         // Command | Event | Ack | Error
    pub tenant: TenantId,
    pub conversation: Option<ConversationId>,
    pub seq: Option<i64>,           // 仅 Event，会话内单调无空洞
    pub ts: i64,                    // 服务端时间，毫秒
    pub trace: Option<TraceContext>,// W3C traceparent
    pub payload: Payload,           // 按 kind 分派
}
```

### 6.2 Command（客户端 → 服务端）

```text
MESSAGE_SEND        MESSAGE_RECALL        MESSAGE_REACTION
CONVERSATION_CREATE CONVERSATION_CLOSE    CONVERSATION_TRANSFER
CONVERSATION_SYNC   (按 seq 拉取补齐)
AGENT_ACCEPT        AGENT_REJECT          AGENT_STATUS_SET
CONVERSATION_ESCALATE  (升级诉求，触发 5.3 外发)
TYPING_START        TYPING_STOP
READ_RECEIPT        HEARTBEAT
```

每个 Command 必须回 `Ack`，`Ack` 携带服务端分配的 `seq` 与 `ts`。**Command 一律带 `id` 作幂等键**（17.3）。

### 6.3 Event（服务端 → 客户端）

```text
MESSAGE_CREATED  MESSAGE_UPDATED  MESSAGE_RECALLED  MESSAGE_REACTION_CREATED
CONVERSATION_CREATED  CONVERSATION_ASSIGNED  CONVERSATION_TRANSFERRED  CONVERSATION_CLOSED
ASSIGNMENT_OFFERED  ASSIGNMENT_ACCEPTED  ASSIGNMENT_REJECTED  ASSIGNMENT_TIMEOUT
AGENT_STATUS_CHANGED  AGENT_PRESENCE_CHANGED
CONVERSATION_ESCALATED  (同时经 Outbox 外发，5.3)
SLA_STARTED  SLA_PAUSED  SLA_RESUMED  SLA_WARNING  SLA_BREACH
AGENT_STATE_CHANGED  AGENT_TOOL_STARTED  AGENT_TOOL_COMPLETED  AGENT_HANDOFF_REQUESTED
WORKFLOW_STARTED  WORKFLOW_TASK_PENDING  WORKFLOW_COMPLETED  WORKFLOW_FAILED
SYSTEM_NOTICE  TYPING  READ_RECEIPT_UPDATED
```

### 6.4 编解码与压缩

| 通道 | 编码 |
|---|---|
| WebSocket 文本帧 | JSON（serde_json），便于前端调试 |
| WebSocket 二进制帧 | MessagePack（rmp-serde），移动端默认 |
| Zenoh 节点间 | MessagePack + **带版本头**（10.4） |

因 F-4b（无 permessage-deflate），大 payload 压缩必须做在 Envelope 内：`payload` 字段支持 `zstd` 包装并由 `Envelope.flags` 标记。**不对小消息压缩**——阈值 4KB 以下直接透传。

### 6.5 协议版本演进

- `v` 只在**不兼容**变更时递增；新增可选字段不递增。
- 服务端同时支持 `v` 与 `v-1`，跨版本窗口至少两个发布周期。
- 未知 `kind` / 未知字段：**接收端必须忽略而非报错**（forward compatibility），由 `#[serde(other)]` 与 `deny_unknown_fields` 的显式取舍保证。

---

## 7. 传输层

### 7.1 传输优先级（按 Rust 生态现实排序）

| 优先级 | 传输 | 状态 | 依据 |
|---|---|---|---|
| **主力** | WebSocket（axum） | 生产 | F-3 |
| **保底** | SSE + HTTP Command | 生产。用于企业代理只放行 HTTP 的环境 | — |
| 实验 | HTTP/3（quinn + h3，独立端口，仅控制面 API） | **不进数据面** | F-5 |
| 桩 | WebTransport（wtransport，feature 默认关） | **不生产** | F-6、F-7 |

值得记录的对比：在 Java 方案中 WebTransport 的结论是「JVM 上不存在可生产落地的服务端实现，唯一路径是 Rust/Go 边车」。核实后 Rust 侧结论是**边车确实只能用 Rust 写，但 `wtransport` 0.7.2 自身仍是 draft**（F-6）。因此本方案不因为「换到 Rust 了」就把 WebTransport 升级为生产选项——它只是从「不可能」变成「可能但不成熟」。

### 7.2 能力协商

客户端连接后首帧交换能力，服务端返回：

```json
{
  "transports": ["websocket", "sse"],
  "encodings": ["json", "msgpack"],
  "compression": ["zstd"],
  "maxMessageSize": 1048576,
  "heartbeatIntervalMs": 20000,
  "protocolVersions": [1]
}
```

`maxMessageSize` 由服务端下发且**服务端强制**（F-4：axum 默认 64MB 必须下调），客户端上报的值只作参考。

### 7.3 WebSocket 参数基线

```text
read_buffer_size        64 KiB     （默认 128KiB，连接数多时按连接摊内存）
write_buffer_size       64 KiB
max_write_buffer_size   256 KiB    ← 必须显式设置，默认无限（F-4）
max_message_size        1 MiB      ← 必须显式下调，默认 64MB（F-4）
max_frame_size          256 KiB
```

超过 `maxMessageSize` 的内容一律走对象存储 + 预签名 URL，消息体只带元数据引用。

---

## 8. 连接层：注册表、Presence、背压与恢复

### 8.1 Gateway 无状态

Gateway 进程不持有任何**权威**状态。它持有的只是本地连接句柄；「用户在哪个节点」的路由信息落 Redis：

```text
本地：  HashMap<ConnectionId, ConnectionHandle>       进程内，非权威
Redis： route:{tenant}:{principal} -> Set<node_id>    TTL 60s，心跳续期
Zenoh： presence 提示广播                              仅优化，非真相
```

三层的关系是：**Redis 是路由真相，Zenoh 只是让其他节点更快知道，本地 Map 只是句柄表**。Zenoh presence 丢失不影响正确性，只影响首次投递是否需要查 Redis。

### 8.2 心跳与死连接

因 F-8b（Zenoh liveliness 无租约，进程假死不会过期），连接与节点存活都不能依赖 Zenoh：

- **连接级**：客户端每 20s 发 `HEARTBEAT`；服务端 45s 未收到即关连接。服务端侧另有 WS Ping（`tokio` 定时任务，非自动）。
- **节点级**：每个节点每 5s `UPDATE node_registry SET last_seen = now()`；判定死亡阈值 15s。**Postgres 是节点存活的唯一真相**。

### 8.3 自建逐连接背压（F-4 的直接产物）

这是本章最关键的设计，因为 **axum 的 WebSocket 不提供真实背压**（F-4）：`Sink::poll_ready` 几乎恒为 `Ready`，写缓冲只在底层写出错时才受 `write_buffer_size` 约束，而 `max_write_buffer_size` 默认无限。若直接 `sink.send().await`，高延迟客户端会让服务端内存无界增长直至 OOM。

强制结构：每个连接一个**有界** channel，业务侧永不直接触碰 sink。

```text
业务侧 ──try_send()──▶ mpsc::channel(capacity=512) ──▶ writer task ──▶ WS sink
                            │
                            └─ 满时按 8.4 分级处理，绝不 await 阻塞业务侧
```

- 业务侧只用 `try_send`，**禁止 `send().await`**——否则背压会反压到事件分发循环，一个慢客户端拖垮整个节点。
- `writer task` 是唯一持有 sink 的任务，串行写出。
- 由 clippy 自定义 lint + review 检查表禁止 `WebSocket` 类型泄漏到 `cs-application`。

### 8.4 丢弃分级

| 级别 | 类型 | 队列满时策略 |
|---|---|---|
| P0 不可丢 | `MESSAGE_CREATED`、`ASSIGNMENT_*`、`CONVERSATION_ESCALATED`、`SLA_BREACH`、`Ack` | 入连接级溢出缓冲（再 512 条）；仍满 → **主动关连接**，客户端重连后按 seq 补齐 |
| P1 可合并 | `AGENT_STATUS_CHANGED`、`READ_RECEIPT_UPDATED`、`AGENT_STATE_CHANGED` | 同 key 覆盖式合并，只保留最新 |
| P2 可丢 | `TYPING`、presence hint | 直接丢弃，不计错误 |

**关连接优于无界堆积**——这是防 OOM 的底线，也是 8.3 所有约束存在的理由。

### 8.5 按 seq 恢复

会话内 `sequence` 单调无空洞（17.2），因此：

- 客户端持久化 `last_seq`；重连时 `CONVERSATION_SYNC { since_seq }`。
- 服务端按 `(conversation_id, seq) > since_seq` 补齐，keyset 分页，单批 200 条。
- 客户端按 `seq` 去重与排序 → 端到端「零丢失、零重复」可断言（22.3 Phase 1 出场标准）。

---

## 9. Zenoh 事件面设计

### 9.1 统一 Key 方案（裁决 #2）

两份源文档一个用 `cs/{tenant}/...`，一个用 `ca/{tenant}/{domain}/{resource}/{id}/{event}`。统一为：

```text
{root}/v1/{tenant}/{plane}/{domain}/{id}/{signal}

root   部署级前缀，隔离环境（如 sp/prod）
v1     key 方案版本，允许将来整体演进而不破坏在网节点
plane  conv | agent | tool | wf | intg | sys
signal event | command | state | reply
```

示例：

```text
sp/prod/v1/acme/conv/conversation/cv_123/event
sp/prod/v1/acme/conv/conversation/cv_123/command
sp/prod/v1/acme/intg/outbound/cv_123/event    ← 单向外发（5.3）
sp/prod/v1/acme/agent/execution/ex_88/state
sp/prod/v1/acme/tool/order/execute            ← Queryable
sp/prod/v1/acme/wf/run/wf_run_9/event
sp/prod/v1/acme/sys/cache/invalidate/{ns}
sp/prod/v1/acme/sys/node/{node_id}/inbox
```

两条硬规则：

1. **Command 与 Event 分离**（源文档 B 第 23 章，保留）：`command` 与 `event` 永不共用 key，避免订阅者误消费命令。
2. **KeyExpr 唯一构造入口**：`cs-mesh-zenoh::keys` 模块是全仓库**唯一**允许拼接 key 字符串的地方，对外只暴露 `keys::conversation_event(tenant, id)` 这类函数。理由：Zenoh key 无 schema 校验，手拼字符串的漂移在运行时表现为「消息静默丢失」，极难排查。

### 9.2 Pub/Sub 与 Querier/Queryable 分工

采纳源文档 B 第 24 章的原则，并补上依据：

| 场景 | 机制 | 理由 |
|---|---|---|
| 领域事件 fan-out | Pub/Sub | 多订阅者、无需应答 |
| Tool 调用请求应答 | **Querier / Queryable**（F-8a） | 天然请求应答语义，无需自建关联表 |
| 节点状态查询、新节点冷启动拉取 | Querier / Queryable | 一次性拉取 |
| Tool 进度、metrics、trace | Pub/Sub | 遥测，可丢 |
| 大 payload | **不进 Zenoh**，走对象存储 + 引用 | 源文档 B 第 39 章，保留 |

### 9.3 订阅粒度

节点按**在线连接的租户 + 会话集合动态声明 Subscriber**，不做 `**` 全量订阅——否则每节点都收全网流量。单节点会话订阅上限 5000，超限拒绝新连接落到该节点（由 Gateway 前置的负载均衡感知）。

---

## 10. Zenoh 能力边界与补齐清单

Zenoh 在本方案中**只承担事件面**。以下是必须补齐的项，每项带验收标准，可直接排期。

### 10.1 Z-1 多平台构建（必修，K8s 硬阻塞）

`zenoh` 1.10.0 官方 crate 在 Rust 侧是纯 Rust 依赖，交叉编译不像 JNI 场景那样需要预编译动态库——**但若启用 storage 插件（F-8d）则重新引入动态库分发问题**。

- 决策：Phase 1 **不启用任何 Zenoh 插件**，从而回避动态库分发。
- 验收：`cargo build --release --target x86_64-unknown-linux-gnu` 与 `aarch64-unknown-linux-gnu` 均通过；产物在非 Alpine（glibc）镜像与 musl 镜像分别启动成功并组网。

### 10.2 Z-2 节点存活判定（必修）

**问题**：`LivelinessToken` 无 TTL、无租约，仅绑 Session 生命周期（F-8b）。进程假死（GC 长停顿式的 stall、死锁、磁盘 IO 挂住）但 TCP 未断时，token 依然存在，其他节点会持续向死节点投递。

**补齐设计**：双重判定，Postgres 为准。

```text
liveliness token           →  仅作「快速发现新节点」的提示
node_registry.last_seen    →  每 5s 心跳 UPDATE，权威
判定死亡                    →  last_seen < now() - 15s
死亡处理                    →  该节点持有的 Assignment 回收、路由表清理、
                              CLAIMED 状态的 timer_task 重置为 PENDING
```

**验收**：对某节点发 `SIGSTOP`（模拟假死，连接不断）15s 内其余节点判定其死亡并完成回收；`SIGKILL` 同样在 15s 内完成。

### 10.3 Z-3 无 CAS 的应对（必修）

**问题**：Zenoh 无任何线性一致原语（F-8c）。

**补齐设计**：不补。**明确禁止**在 Zenoh 上实现锁、计数器、CAS。所有仲裁落 Postgres（第 17 章）。由 review 检查表与 `cs-mesh` trait 的接口设计强制——`EventMesh` trait **只暴露** `publish` / `subscribe` / `query` / `declare_queryable`，不提供任何 `put` / `get` 形态的 KV 接口，从而在类型层面杜绝误用。

**验收**：`cs-mesh` 的 trait 定义中不存在任何返回「当前值」语义的方法。

### 10.4 Z-4 带版本头的编解码（必修）

**问题**：节点间 payload 若无版本头，滚动升级期间新旧节点互发会静默解析错误。

**补齐设计**：所有 Zenoh payload 前置 4 字节头：`[magic:u8, codec:u8, schema_ver:u16]`，之后是 MessagePack body。解不出或版本超出支持窗口 → **丢弃并计数告警**，绝不 panic。

**验收**：新旧两版本节点同时在网，互发消息，不兼容部分被计入 `mesh_decode_rejected_total` 且无进程崩溃。

### 10.5 Z-5 插件与 storage（延后）

Zenoh 的 `plugins` / `runtime_plugins` feature 官方标 **internal/unstable**，静态注册无公开 API（F-8d）。**Phase 1 不启用**。如将来需要边缘缓存，重新评估而非默认引入。

---

## 11. 状态机

### 11.1 Conversation

```text
        NEW
         │ 首条消息落库
         ▼
      WAITING ──────────▶ ASSIGNING ──────▶ ACTIVE
         ▲                    │  超时/拒绝      │
         └────────────────────┘                │
                                     ┌─────────┼──────────┐
                                     ▼         ▼          ▼
                              TRANSFERRING   CLOSED    ABANDONED
                                     │                (客户长时无响应)
                                     └──▶ ASSIGNING
```

`awaiting_party ∈ {AGENT, CUSTOMER}` 随每条消息的 `sender_type` 在**同一事务内**翻转，驱动 SLA 暂停（11.3）。

会话侧没有工单状态机。`CLOSED` 与升级动作只经 5.3 外发，工单的生命周期由外部通用工单服务自己管。

### 11.2 Assignment

```text
OFFERED ──┬──▶ ACCEPTED ──▶ COMPLETED
          ├──▶ REJECTED ──▶ (回队列重派)
          └──▶ TIMEOUT  ──▶ (回队列重派，坐席容量释放)
```

`OFFERED → ACCEPTED` 必须通过坐席容量 CAS（17.4），失败即转 `REJECTED`。

### 11.3 SLA

`SlaInstance` 是独立域（源文档 A 第 10 章，保留），但 `target_type` 只有 `CONVERSATION` 一种取值——源文档把 SLA 挂在工单上，本平台只保留会话自身的首响与解决时限：

```text
RUNNING ──┬──▶ PAUSED ──▶ RUNNING     （暂停条件见下）
          ├──▶ MET
          ├──▶ WARNED ──▶ BREACHED
          └──▶ CANCELLED
```

| target | 首响计时 | 解决计时暂停条件 |
|---|---|---|
| CONVERSATION | 永不暂停 | `awaiting_party = CUSTOMER` |

due 时刻**在创建时即按工作日历折算为绝对时刻**并落库，而不是每次比较时动态计算——否则日历变更会追溯性地改变历史会话的达标判定。日历变更只影响之后新建的实例。

---

## 12. Queue + Routing 派单流水线

### 12.1 两段分离

源文档 A 给了一个打分公式，但没说打分发生在哪一层。这很关键：**打分需要实时状态，而实时状态不能进规则引擎**（规则引擎必须可回放、可 shadow 对账）。因此严格两段：

```text
第一段：决策（无副作用，可回放）
  DispatchContext ──▶ cs-decision (Zen Engine) ──▶ RoutingDecision
                                                    ├ queue_id
                                                    ├ priority
                                                    ├ required_skills
                                                    ├ sla_policy_id
                                                    └ auto_accept: bool

第二段：选择与抢占（触碰实时状态，有副作用）
  RoutingDecision ──▶ cs-routing::AgentSelector ──▶ 打分排序 ──▶ 容量 CAS
                                                                  │
                                                            成功 ──┴──▶ Assignment OFFERED
                                                            失败 ────▶ 下一候选
```

### 12.2 DispatchContext（Zen Engine 输入）

```json
{
  "tenant":       {"id": "acme"},
  "customer":     {"id": "cu_100", "vip": true, "language": "zh-CN",
                   "tags": ["VIP", "ORDER"], "history_conversation_count": 12},
  "conversation": {"id": "cv_100", "category": "ORDER", "priority": 80,
                   "channel": "WEB", "first_message_intent": "REFUND"},
  "queue":        {"waiting_count": 8, "avg_wait_seconds": 45},
  "time":         {"hour": 20, "day_of_week": 6, "in_business_hours": false}
}
```

**输入必须是纯数据快照**。禁止在 Zen 的 function 节点里回调数据库——`EvaluationOptions` 无超时控制（F-2），一次慢查询会挂住求值。

### 12.3 打分

```text
score = skill_match      * 0.35
      + availability     * 0.25
      + workload_inverse * 0.20
      + affinity         * 0.10
      + priority_fit     * 0.10
```

权重可按租户配置。`workload_inverse = 1 - active_count / capacity`。同分时按 `last_assigned_at` 升序（最久未派单优先），保证公平性且结果确定。

### 12.4 zen-engine 调用约束（F-2、F-14）

```text
1. 必须外层套 tokio::time::timeout（派单默认 200ms）—— EvaluationOptions 无超时
2. function 节点内禁止 IO，禁止访问 DB / HTTP —— QuickJS 在进程内跑，无隔离
3. 输入输出 schema 由 Rust 侧类型约束，不信任决策图的自由输出
4. 求值失败（超时/异常/输出不合 schema）→ 回落到 Rust 原生兜底决策表，并告警
5. evaluate 返回 !Send future —— 必须经 LocalPoolHandle::spawn_pinned 执行，
   禁止 tokio::spawn，禁止在 CustomNodeAdapter 内 block_on（13.6）
6. 启动时注册 DenyAllHttpHandler，并设 ZEN_CONFIG.function_timeout_millis = 200（13.7）
```

第 4 条是硬要求：规则引擎故障绝不能让派单停摆。`cs-decision` 内同时存在 `ZenDecisionEngine` 与 `FallbackDecisionTable` 两个实现。第 5、6 条是宿主级约束，派单决策与第 13 章的 workflow **共用同一个引擎句柄与同一套约束**。

### 12.5 规则版本化与灰度

```text
rule_definition ──▶ rule_version (draft → published → archived)
                         │
                    灰度分桶：hash(conversation_id) % 100 < rollout_percent
                         │
                    shadow 模式：新旧版本同时求值，只用旧版结果，
                                 差异写 rule_shadow_diff 供对账
```

热加载：`CachedLoader` 无 invalidate API（F-2），因此自建 loader——以 `(rule_id, version)` 为不可变 key 缓存，**发布新版本即换 key**，从不失效旧 key。这样规避了缺失的 invalidate 能力，且天然支持灰度期间新旧共存。

---

## 13. Workflow 引擎（裁决 #3）

### 13.1 裁决：zen-engine 是唯一引擎，外层只加驱动器

两份源文档在这里分岔：A 要自研 State Machine / DAG，B 要交给 Zen Engine。本文档**采纳 B**——`zen-engine` 是唯一的 workflow 引擎，**JDM 就是 workflow 的定义格式**，不再存在第二套 DAG 模型、第二套节点类型、第二套版本化机制。

但必须诚实面对核实结论。`zen-engine` 2.0.1 缺四样东西（F-2）：

```text
zen-engine 提供                        zen-engine 不提供
─────────────────────────             ─────────────────────────
图的定义、校验、版本化                  ✗ run 状态持久化
分支求值（switch/表/表达式）             ✗ 挂起与跨进程恢复
子图递归（decision 节点）                ✗ 定时唤醒（无 timer/delay 节点）
副作用边界（custom 节点，async）          ✗ 重试与补偿
可反序列化的执行轨迹（trace.order）
运行时定义加载（loader 返回 Send future）
```

关键判断：**缺的这四样全都与业务语义无关**。它们是「如何可靠地跑一个长时过程」的通用机制，不是「客服业务该怎么流转」的知识。所以它们不构成再写一个引擎的理由，只构成写一个**驱动器**的理由：

```text
cs-workflow —— 持久化驱动器，不含任何业务语义
   职责恰好四条：持久化 run 状态 / 带 memo 重入求值 / 挂起与定时唤醒 / 重试与补偿
   禁止事项：不解析 JDM、不实现节点类型、不做条件判断、不持有业务规则

cs-decision —— zen 引擎宿主
   DecisionEngine 句柄、LocalPool、loader、CustomNodeAdapter、超时策略
   派单决策（第 12 章）与 workflow 编排共用同一个引擎与同一套约束
```

一句话：**zen 是引擎，驱动器是曲轴箱**。判断「下一步走哪」永远是 zen 的事；判断「这一步的结果要不要落库、崩了怎么续」永远是驱动器的事。任何在驱动器里出现 `if node.kind == ...` 的业务分支都是设计违规（R-5 的检查点）。

### 13.2 JDM 即 workflow 定义格式

所有 workflow 都是一张 JDM 图，节点只用 zen 的封闭枚举，不扩展：

- **纯逻辑**用 `expressionNode` / `decisionTableNode` / `switchNode` —— 确定性、可回放、无副作用。
- **一切副作用**只能出现在 `customNode` 里，由平台注册的 `CustomNodeAdapter` 执行。这是唯一的副作用边界。
- **子流程**用 `decisionNode` 引用另一张图，但受 `max_depth = 10` 限制（F-2），更深的嵌套必须拆成独立 run 由驱动器串联。
- **`functionNode` 在租户自助编辑的图里禁用**，理由见 13.7。

好处是明确的：定义格式、版本化、校验、可视化编辑器、trace 全部复用 zen 与 GoRules 生态，没有自研 DSL 的长期维护成本。

### 13.3 无状态求值器上怎么做长时挂起：memo 重放

这是本章的核心机制。zen 不能挂起，所以**不挂起**——挂起表达为「结束本次求值 + 持久化 + 下次从头重放，但已完成的节点不再产生副作用」。

```text
首次执行
  evaluate(graph@v3, ctx ∪ {__memo: {}})
      │
      ├─ expression / decisionTable / switch ── 纯节点，正常求值
      │
      ├─ customNode "effect.emit_outbound"
      │       memo 命中？ 否 → 写 outbox_event（带 idempotency_key，5.3）
      │                        → 落 workflow_task = SUCCEEDED
      │                        → 写 memo[node_id] = output
      │
      └─ customNode "suspend.human"
              无审批结果 → 返回 Suspend{ reason, wake_on }
              驱动器捕获 → workflow_run.status = SUSPENDED
                          → 持久化 memo + variables
                          → 需要定时唤醒则注册 timer_task（17.6）
                          → 本次求值结束，进程不驻留任何状态

              ⋯⋯ 三天后，审批事件到达 / timer_task 到期 ⋯⋯

恢复执行
  evaluate(graph@v3, ctx ∪ {__memo: 已完成节点输出})
      │
      ├─ 前序纯节点     ── 重新求值（确定性，走同一条边）
      ├─ effect.*       ── memo 命中 → 直接返回缓存 output，**不产生副作用**
      └─ suspend.human  ── memo 已含审批结果 → 返回结果，继续往下走
```

**三条硬前提**，缺一条这套机制就不成立：

1. **图无环且纯节点确定性**（F-2 保证无环）。同一输入必走同一条边，否则重放会岔到另一条分支上，memo 与实际路径错位。因此纯节点内禁止读取任何随时间变化的量（`now()`、随机数、外部状态）——需要时间就由驱动器在 `ctx.__now` 里冻结传入。
2. **副作用只在 `customNode` 内，且必须幂等**，带 `workflow_task.idempotency_key`（17.3）。这一条兜住的是最关键的崩溃窗口：副作用已执行、memo 还没落库时进程死掉，重放会重新调用该 customNode，此时靠幂等键把它变成一次空操作。
3. **memo 以 `node_id` 为键持久化**在 `workflow_run.memo`（JSONB），与 run 状态在同一事务内写入。

**代价必须写明**：每次恢复都要从 input 节点重新走一遍全部纯节点。对客服场景（图规模几十个节点、单个 run 恢复次数个位数）这个开销可以忽略。但它不是无限可扩展的——如果将来出现上千节点的图或高频恢复（例如每分钟唤醒一次的轮询流程），需要引入**检查点子图**：把已完成的前缀折叠成一个常量 input，代价是失去完整 trace。这是本机制的真实边界，不是可以糊过去的细节。

### 13.4 节点语义与驱动器动作

驱动器只认 `customNode` 的 `kind` 前缀，不认业务含义：

| JDM 表达 | 语义 | 驱动器动作 |
|---|---|---|
| `expressionNode` / `decisionTableNode` / `switchNode` | 纯逻辑、分支 | 无动作（zen 原生求值） |
| `decisionNode`（子图） | 子流程 | 无动作；深度受 `max_depth = 10` 约束 |
| `customNode` `effect.*` | 有副作用的业务动作（建单外发、发消息、派单、更新会话） | 查 memo；未命中则调 `cs-application` 用例，落 `workflow_task` + 幂等键 |
| `customNode` `suspend.human` | 人工审批 / 人工介入 | `workflow_task = PENDING_HUMAN`，run 挂起；审批事件写入 memo 后唤醒 |
| `customNode` `suspend.timer` | 延时等待 | 注册 `timer_task`，run 挂起，**不占用任何运行时资源** |
| `customNode` `suspend.signal` | 等待外部事件（如客户回复） | run 挂起；事件经 Outbox/Zenoh 投递后唤醒 |
| `customNode` `agent.invoke` | 调 AI Agent 回合 | 经 `cs-ai-bridge` 调 `ca-runtime`；Agent 自身可再挂起（第 14 章），此时本 run 也停在 `WaitingAgent` |
| `TransformExecutionMode::Loop` | 数组迭代 | 无动作；**注意 zen 内是顺序执行**（F-14） |

**并行 fan-out zen 不支持**（F-14：图内无 `join_all`，`switchNode` 的 `Collect` 命中多分支也是顺序求值）。真需要并行时由驱动器拆成多个子 run 并发起、用一个 `suspend.signal` 节点等待全部子 run 完成。这比在引擎里硬造并行更可控，因为每个子 run 各自可独立重试与恢复。

### 13.5 数据模型与定义加载

```text
workflow            定义（租户级）
workflow_version    不可变版本快照：JDM 原文（DecisionContent）+ 发布状态
workflow_run        status / memo(JSONB) / variables(JSONB) / suspend_reason / wake_on / version
workflow_task       每个已执行 customNode 的记录：status / attempt / next_retry_at / idempotency_key
workflow_trace      归档的 DecisionGraphTrace（F-2：可反序列化）→ 审计、回放对账、补偿逆序依据
```

`workflow_run.status ∈ {RUNNING, SUSPENDED, COMPENSATING, COMPLETED, FAILED, CANCELLED}`。**RUNNING 只在一次 `evaluate` 调用期间成立**——没有任何 run 状态长驻内存，进程重启后由 timer loader 与 `SUSPENDED` 扫描自然续跑。这与第 14 章 Agent 的恢复机制共用同一套设施（`timer_task` + memo/快照 + 幂等键）。

定义加载用**自建 `DecisionLoader`**：以 `(workflow_id, version)` 为**不可变 key** 从 `workflow_version` 读 JDM 原文，从不失效旧 key。这与 12.5 的手法完全一致，同样是为了规避 zen 缺失的 invalidate API（F-2）；副产品是**一个正在挂起的 run 永远绑定它启动时的图版本**——发布新版本不会改变在途 run 的行为，这在长时流程里是必须的语义。`DecisionLoader::load` 返回 Send future（F-2），所以直接查 Postgres 是被支持的用法。

### 13.6 执行宿主：`!Send` 的直接后果（F-14）

这是全章最容易在实现阶段翻车的一条。`evaluate` 返回的 future 是 `!Send`，不能 `tokio::spawn`：

```text
tokio 主 runtime（全是 Send 任务：axum / sqlx / zenoh / redis）
        │
        │  提交 (run_id, graph_key, ctx, memo)
        ▼
LocalPoolHandle（tokio-util，线程数 = CPU 核数）
        │  spawn_pinned：在固定线程上跑 !Send 的 evaluate
        ▼
DecisionEngine（句柄本身 Send + Sync + Clone，可跨线程共享）
        │
        ▼
CustomNodeAdapter::handle（async，同样跑在该 pinned 线程上）
        │  需要 DB / HTTP / Zenoh（都是 Send future）
        ▼
oneshot channel 回抛主 runtime 执行 ──▶ 等回值 ──▶ 返回 NodeResult
```

三条硬规则：

1. **禁止 `tokio::spawn(engine.evaluate(..))`**。编译期就会失败，这不可怕；可怕的是有人为了「绕过编译错误」在 customNode 里塞 `block_on`。
2. **customNode 内禁止 `block_on` 与任何同步阻塞**。local pool 的线程数等于核数，一个阻塞的 customNode 会占死一整条求值线程；若它等待的 future 又需要同一 pool，直接死锁。所有 Send 的 IO 一律经 oneshot 回抛主 runtime。
3. **每次 `evaluate` 外层必套 `tokio::time::timeout`**。zen 无求值级超时（F-2）。基线：派单决策 200ms，workflow 单次求值 5s。超时即当作本次求值失败，run 保持在上一次已持久化的 memo 上，按 13.8 重试——**因为 memo 已落库，超时不会丢失已完成的副作用**。

local pool 的并发度是有限资源，因此 `evaluate` 提交口必须有队列上限与拒绝策略，指标见 `cs_zen_localpool_queue_depth`（20.6）。

### 13.7 `functionNode` 治理（F-14 的安全后果）

这一节是核实过程中最意外的发现，也是多租户下最大的单点风险。`functionNode` 跑 QuickJS，而它：

- **没有内存上限**——`set_memory_limit` 未被调用，一个 `new Array(1e9)` 足以把整个进程 OOM。
- **内置带云厂商签名能力（reqsign）的 HTTP 模块**——规则作者可以从规则里直接发外网请求。在多租户下，这等于把 SSRF 与数据外泄能力交给了规则编辑者。
- **超时是进程级全局**（`ZEN_CONFIG.function_timeout_millis`，默认 5000ms），无法按租户或按图收紧。

Phase 1 的治理措施，五条全部必须落地：

1. **租户自助编辑的图禁用 `functionNode`**。发布前对 JDM 做静态校验，遇到 `functionNode` 直接拒绝发布并给出替代指引（逻辑用 expression / decisionTable，副作用用平台注册的 customNode）。这是最有效的一条。
2. 平台自研图允许 `functionNode`，但必须走代码评审，且 JDM 静态检查进 CI。
3. 引擎一律注册 `.with_http_handler(DenyAllHttpHandler)`，从宿主侧彻底断掉 function 节点出网。需要出网的场景走 customNode，那里有域名白名单与审计。
4. 启动期把 `ZEN_CONFIG.function_timeout_millis` 设为 **200ms**（远小于默认 5000ms）。它是全局值，所以取所有用途中最严的那个。
5. QuickJS 无内存上限这条**无法在库内解决**。缓解只能靠外层：求值进程纳入容器 memory limit 并允许 OOM 重启（run 状态全在库里，重启不丢），并把它明确记为残留风险 **R-13**——不假装已经解决。

### 13.8 重试与补偿

- **重试即「带 memo 重放」**，不是另一条代码路径。`workflow_task.attempt` / `next_retry_at` 指数退避 + 抖动，上限可配；到期由 `timer_task` 唤醒，走的仍是 13.3 的同一条重入逻辑。这是 memo 机制的额外收益：恢复、重试、超时重入三者共用一条路径，只有一处需要正确。
- **补偿**：每个 `effect.*` customNode 在 JDM 的 `config` 里声明自己的 `compensate` 节点 id。超过重试上限 → run 转 `COMPENSATING`，驱动器按 `workflow_trace` 中的 `order` 字段（F-2）**逆序**执行已成功节点的补偿节点。用 trace 的 order 而非 memo 的插入顺序，是因为 order 是引擎给出的权威执行序。
- 补偿本身也是 customNode，同样幂等、同样可重试。**没有「补偿失败就算了」这个分支**——补偿终态失败必须进死信并告警人工介入。

---

## 14. AI Agent Runtime

### 14.1 两级状态机（裁决 #4，本章的核心设计）

两份源文档分别给了 9 态与 15 态的 Agent 状态机，且都要求 Crash Recovery。但核实发现（F-1）：**`rig_agent::run::AgentRun` 本身就是一个 sans-IO、可序列化、可跨进程恢复的步进状态机**（`CallModel` / `CallTools` / `Done`）。

若再自己实现一遍 LLM 循环，等于把 rig 的核心能力重写一遍，且两套状态机的一致性会成为长期 bug 源。因此设计为两级：

```text
┌─ 平台级状态机（ca-state，我们拥有，落 Postgres，粗粒度、业务语义）────────┐
│                                                                        │
│  Idle ──▶ Running ──┬──▶ WaitingUser      （AskUser，等客户回话）        │
│                     ├──▶ WaitingTool      （长时 Tool，异步回调）        │
│                     ├──▶ WaitingHuman     （Handoff，人工接管中）        │
│                     ├──▶ WaitingApproval  （Policy 要求审批）            │
│                     ├──▶ Completed                                     │
│                     ├──▶ Failed                                        │
│                     └──▶ Cancelled                                     │
│                                                                        │
│   每次状态迁移 = 一次 Postgres 事务 = 一个可恢复点                        │
│                                                                        │
│   ┌─ Brain 内核态（rig-agent 拥有，作为不透明快照持久化）──────────────┐  │
│   │   AgentRun: CallModel ⇄ CallTools ⇄ Done                       │  │
│   │   我们不解释它的内部结构，只 serde 存取                            │  │
│   └────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
```

**为什么这样切**：平台级状态是业务要审计、要在工作台展示、要驱动 SLA 的（9 个态足够）；Brain 内核态是 LLM 循环的实现细节，会随 rig 版本变化，不该进我们的领域模型。这个切分只有在核实了 F-1 之后才成立——两份源文档都没有这个信息。

### 14.2 执行链（保留源文档 B 原则 2）

```text
Brain (rig) ──▶ AgentAction ──▶ Policy ──▶ Dispatcher ──▶ Tool / Workflow / Human
                （提议）        （裁决）      （执行）
                                  │
                            Deny / RequireHuman / RequireApproval
```

LLM **永远只能提议**。这是不可协商的：

```rust
pub enum AgentAction {
    Respond { message: Message },
    CallTool { tool: ToolId, input: serde_json::Value },
    Retrieve { query: String },
    ExecuteWorkflow { workflow: WorkflowId, input: serde_json::Value },
    TransferHuman { reason: HandoffReason },
    AskUser { question: String },
    Finish,
}
```

### 14.3 Runtime 对外接口

```rust
#[async_trait]
pub trait CustomerAgentRuntime {
    async fn receive(&self, req: AgentRequest) -> Result<AgentResponse, AgentError>;
    async fn resume(&self, id: ExecutionId) -> Result<AgentResponse, AgentError>;
    async fn cancel(&self, id: ExecutionId) -> Result<(), AgentError>;
    async fn handoff(&self, id: ExecutionId, req: HandoffRequest) -> Result<(), AgentError>;
}
```

`resume` 是崩溃恢复与等待唤醒的**同一个入口**——两者都是「从快照继续」，没有理由分成两条路径。

### 14.4 Crash Recovery

```text
每次平台级状态迁移：
  BEGIN
    UPDATE agent_execution
       SET state = $new, snapshot = $brain_snapshot,
           schema_ver = $ver, version = version + 1
     WHERE execution_id = $id AND version = $expected     ← 乐观锁
    INSERT INTO outbox_event (...)                        ← 事件与状态同事务
  COMMIT
```

- 崩溃后由 `cs-worker` 扫描 `state = Running AND updated_at < now() - 2min` 的执行体，调 `resume`。
- **恢复必须幂等**：所有有副作用的 Tool 调用带 `idempotency_key`（14.6），重放时命中已有结果直接返回。

### 14.5 rig 是 0.x：版本隔离与快照弃用策略

`rig-core` 0.42.0 仍是 0.x，每个 minor 都带破坏性变更（F-1）。三道防线：

1. **类型隔离**：`rig` 只出现在 `ca-brain` 的依赖中。`ca-core` 定义自己的 `Brain` trait：

   ```rust
   #[async_trait]
   pub trait Brain: Send + Sync {
       async fn step(&self, snapshot: BrainSnapshot, input: BrainInput)
           -> Result<BrainOutcome, BrainError>;
   }
   ```

2. **快照带 schema 版本**：`agent_execution.schema_ver` 记录写入时的 Brain 快照格式版本。

3. **弃用而非迁移**：升级 rig 导致快照格式不兼容时，**不写迁移代码**。`resume` 遇到 `schema_ver` 不受支持时：丢弃 Brain 快照 → 平台级状态回退到该回合起点 → **重跑该回合**。这是安全的，因为所有副作用都有幂等键（14.6），重跑不会重复退款、重复建单。代价是一次多余的 LLM 调用，远低于维护快照迁移的成本。

   > 这条策略是「因为 rig 是 0.x」的直接产物。若 rig 到 1.0 且承诺快照兼容，可改为真正的迁移。

### 14.6 幂等（保留源文档 B 第 28 章并强化）

有副作用的 Tool 调用必须携带：

```json
{
  "tenant_id": "acme",
  "conversation_id": "cv_123",
  "turn_id": "turn_8",
  "action_id": "act_123",
  "idempotency_key": "refund-cv123-turn8-act123"
}
```

`idempotency_key` 的构成**必须是确定性的**（由 tenant + conversation + turn + action 派生），不能用随机 UUID——否则重跑会生成新 key，幂等失效。这是源文档未说明但至关重要的一点。

### 14.7 三层记忆

| 层 | 存储 | 内容 | 淘汰 |
|---|---|---|---|
| Short | Redis | 当前 turn 最近 10~20 条消息 | TTL 1h |
| Session | PostgreSQL | customer_id / order_id / intent / goal / variables | 随会话关闭归档 |
| Long | PostgreSQL + pgvector | 客户偏好、历史问题摘要、历史交互 | 不淘汰，定期重新摘要 |

Long Memory 用 pgvector 而非独立向量库（F-10、第 19 章）。摘要生成是**异步 workflow**，不在回合关键路径上。

### 14.8 人工转接

```text
Policy: RequireHuman ──▶ ca-handoff ──▶ Zenoh: .../agent/handoff/{id}/command
                                              │
                                         坐席工作台接受
                                              │
        AgentState::WaitingHuman ◀────────────┘   ← Runtime 停止主动生成回复
                    │
              human.completed
                    │
                    ▼
              Running（继续，或直接 Completed）
```

`WaitingHuman` 期间 Runtime **绝不生成回复**。这需要在 Dispatcher 入口做状态断言，而不是靠调用方自觉。

---

## 15. Policy 与 Tool 抽象

### 15.1 Policy

```rust
#[async_trait]
pub trait Policy: Send + Sync {
    async fn evaluate(&self, action: &AgentAction, ctx: &AgentContext) -> PolicyDecision;
}

pub enum PolicyDecision {
    Allow,
    Deny { reason: String },
    RequireHuman { reason: String },
    RequireApproval { workflow: WorkflowId },
}
```

主实现基于 zen-engine（`ca-policy::zen`，经 `cs-decision` 宿主），遵守 12.4 的全部六条约束（超时、无 IO、schema 校验、原生兜底、LocalPool 执行、DenyAllHttpHandler）。

**必须经 Policy 的高风险类目**（源文档 A 第 31 章，保留并落为 Manifest 声明）：PII、财务、退款、删除、账户变更、安全、法务。

```text
LLM 提议 refund.create(amount=5000)
        │
        ▼
     Policy (Zen)
        ├─ amount <= 1000                    ──▶ Allow
        ├─ 1000 < amount <= 10000            ──▶ RequireApproval(refund_approval_wf)
        └─ amount > 10000 或 客户风险等级高   ──▶ RequireHuman
```

`RequireApproval` 的落点是第 13 章的 workflow（`suspend.human` 节点，13.4），Agent 转 `WaitingApproval`。**审批不是 Policy 的职责，Policy 只裁决「需不需要审批」。**

### 15.2 Tool 抽象

```rust
#[async_trait]
pub trait AgentTool: Send + Sync {
    fn manifest(&self) -> &ToolManifest;
    async fn execute(&self, ctx: ToolContext, input: serde_json::Value)
        -> Result<ToolOutput, ToolError>;
}

pub struct ToolManifest {
    pub id: ToolId,
    pub name: String,
    pub version: semver::Version,
    pub description: String,
    pub input_schema: serde_json::Value,   // JSON Schema
    pub output_schema: serde_json::Value,
    pub permissions: Vec<Permission>,      // Host 据此授权，Tool 不自证
    pub requires_approval: bool,
    pub side_effecting: bool,              // 决定是否强制 idempotency_key
    pub timeout: Duration,                 // Host 强制，见 16.4
    pub execution: ExecutionMode,
}

pub enum ExecutionMode {
    InProcess,
    WasiComponent { component: ComponentId },
    Remote { endpoint: String },
    Mcp { server: String },
}
```

`side_effecting = true` 的 Tool，Dispatcher 在调用前**强制**检查 `idempotency_key` 存在，否则直接拒绝执行。这把 14.6 的约定变成了类型系统之外的运行时不变式。

### 15.3 Rig Tool 的定位（保留源文档 B 第 18 章）

Rig 的 Tool 只用来**向 LLM 描述 schema**，不用来执行：

```text
ToolManifest.input_schema ──▶ rig tool schema ──▶ LLM
                                                   │
                                          LLM 产出 tool call
                                                   │
                                          转成 AgentAction::CallTool
                                                   │
                                                Policy ──▶ Dispatcher
```

`ca-brain` 内不注册任何真正会执行业务的 rig tool。这样即使 rig 内部的 agent loop 自行决定「调用工具」，它拿到的也只是 schema，无法产生副作用。

### 15.4 MCP Tool（F-13）

- transport 用 **Streamable HTTP**（rmcp 3.2.0 不提供 legacy HTTP+SSE，也无 WebSocket）。
- ✗ 订阅状态跨重连不恢复 → `ca-tool-mcp` 必须在重连后**主动重建订阅**，并把「重连中」暴露为 Tool 的 `Unavailable` 状态而非静默挂起。
- MCP server 视为**不受信外部依赖**：超时、重试上限、熔断三件套齐备，且其返回值同样要过 `output_schema` 校验。

---

## 16. WASI Tool 沙箱

### 16.1 基于 Component Model 而非自定义 ABI

采纳源文档 B 第 13 章。`wasmtime` 48.0.1 的 `component-model` 与 wasi-io/clocks/filesystem/random/sockets/http 均为 **Tier 1 stable**（F-12），因此不自造 ABI。

目标 target：**`wasm32-wasip2`**。WASI 0.3（P3）虽已发布且 wasmtime 46+ 支持，但 wasi:http 0.3 的 **Rust guest 工具链仍在补齐**（F-12）→ Phase 1 用 P2，P3 只做观察。

### 16.2 WIT 接口版本化

`wit-bindgen` 0.61.1 仍是 0.x 且随组件模型频繁 breaking（F-12）。应对：**WIT 包名内嵌版本**，宿主同时支持 N 与 N-1。

```wit
package customer.agent:tool@1.0.0;

interface tool {
    record tool-request {
        request-id: string,
        tool-id: string,
        idempotency-key: option<string>,
        input: string,                     // JSON
    }
    record tool-response {
        request-id: string,
        success: bool,
        output: string,
        error: option<string>,
    }
    execute: func(req: tool-request) -> tool-response;
}
```

### 16.3 Host 能力与权限（Tool 不拥有权限，Host 决定权限）

```wit
package customer.agent:host@1.0.0;

interface host {
    get-customer-context: func() -> customer-context;
    http-request: func(request: http-request) -> http-response;
    publish-event: func(topic: string, payload: string);
    get-secret: func(name: string) -> option<string>;
}
```

四条约束：

1. **出网默认全关**（F-12：wasmtime 出网默认关闭）。`http-request` 是宿主注入的唯一出口，宿主按 Manifest 的 `permissions` 做**域名白名单**校验。
2. `get-secret` 只返回 Manifest 声明过的 secret 名，且**审计每一次读取**。
3. `publish-event` 的 topic 由宿主重写为受控前缀，Tool 无法向任意 key 发布。
4. 不暴露 filesystem、不暴露 raw socket、不暴露 shell。

### 16.4 资源限制（F-12 的必须绕过项）

wasmtime 提供的与**必须自建的**：

| 维度 | wasmtime 提供 | 缺口与补齐 |
|---|---|---|
| 指令数 | `fuel`（确定性，开销较大，可 `fuel_async_yield_interval` 让出） | 用于防死循环 |
| 墙钟时间 | `epoch interruption`（约 10% 开销，基于 wall-time 故不确定） | 用于兜底超时 |
| 内存 | `StoreLimitsBuilder::memory_size` + `trap_on_grow_failure` | — |
| 实例/表 | `StoreLimits`（memories / tables / instances） | — |
| **host 调用耗时** | ✗ **不受 fuel/epoch 约束** | **必须**在每个 host function 内自套 `tokio::time::timeout`；整个 `execute` 外层再套 `Manifest.timeout` |
| **网络配额** | ✗ **无内置带宽/连接配额** | 宿主侧计数：单次 execute 的出网请求数上限、总字节上限，超限即 trap |

第 5、6 行是最容易被忽略的：只配 fuel 和 memory limit 会让人误以为沙箱是完备的，而一个 host 侧的慢 HTTP 调用可以无限期挂住整个执行。

### 16.5 插件版本与灰度

```text
order-tool v1.2.0  ──┐
order-tool v2.0.0  ──┴──▶ ToolRegistry 按 (tenant, tool_id) 解析版本
                             tenant A ──▶ v1.2.0
                             tenant B ──▶ v2.0.0
```

组件是不可变产物，按 `(tool_id, version)` 缓存已编译的 `Component`（wasmtime 编译开销大，必须缓存）。切版本不需要重启 Runtime。

### 16.6 Agent Runtime 自身不能跑在 WASI

`rig-core` **不支持 WASI**（F-1，仅 native 与 browser wasm32-unknown-unknown）。因此边界很清楚：**Tool 在沙箱内，Runtime 在沙箱外**。任何「把整个 Agent 编译成 wasm 部署到边缘」的想法在当前生态下不成立。

---

## 17. PostgreSQL 数据模型与一致性机制

Zenoh 无 CAS、无租约（F-8b/c），`PgListener` 会丢通知（F-9b）。因此**所有正确性都落在 Postgres**。本章是全文的一致性底座。

### 17.1 核心表清单

```text
tenant  tenant_setting
sys_user  role  permission  user_role
customer  customer_identity  customer_tag  customer_tag_relation  customer_note
agent  agent_skill  agent_team  agent_queue  agent_schedule  agent_workload
team  queue  queue_member
conversation  conversation_participant  assignment
message  message_attachment  message_reaction  message_read_receipt
sla_policy  sla_instance  sla_event  business_calendar
workflow  workflow_version  workflow_run  workflow_task  workflow_trace
knowledge_base  knowledge_document  knowledge_chunk
agent_execution  agent_tool_execution  agent_memory
rule_definition  rule_version  rule_shadow_diff
channel  channel_account
outbox_event
idempotency_record
timer_task
node_registry
audit_log
notification  notification_template
```

### 17.2 会话内单调无空洞序列号

不能用 Postgres `SEQUENCE`——它在事务回滚时留空洞，破坏 8.5 的按 seq 补齐。改为内联列 + 原子自增：

```sql
UPDATE conversation
   SET last_seq = last_seq + 1
 WHERE id = $1 AND tenant_id = $2
RETURNING last_seq;
```

在**同一事务**内用返回的 `last_seq` 插入 message。行级锁天然串行化同一会话的并发写入，跨会话无争用。

### 17.3 幂等

```sql
CREATE TABLE idempotency_record (
    tenant_id   TEXT        NOT NULL,
    request_id  TEXT        NOT NULL,
    scope       TEXT        NOT NULL,
    result      JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, scope, request_id)
);
```

写路径：`INSERT ... ON CONFLICT DO NOTHING`；影响行数为 0 即回放已存结果。`scope` 区分用途（message / tool / workflow_task），避免不同域的 key 空间碰撞。保留期 7 天，按 `created_at` 分区清理。

### 17.4 坐席容量原子抢占

不用分布式锁，用条件更新：

```sql
UPDATE agent_workload
   SET active_count = active_count + 1,
       last_assigned_at = now()
 WHERE tenant_id = $1 AND agent_id = $2
   AND active_count < capacity
RETURNING active_count;
```

返回空即抢占失败，换下一候选。配合表级约束兜底：

```sql
ALTER TABLE agent_workload
  ADD CONSTRAINT ck_workload_range
  CHECK (active_count >= 0 AND active_count <= capacity);
```

约束是**第二道防线**——它一旦触发就说明存在绕过 CAS 的写路径，属于必须修的 bug（22.3 Phase 1 出场标准断言它从未触发）。

### 17.5 事务性 Outbox（publish-after-commit）

```sql
CREATE TABLE outbox_event (
    id            BIGSERIAL PRIMARY KEY,
    tenant_id     TEXT        NOT NULL,
    aggregate     TEXT        NOT NULL,
    aggregate_id  TEXT        NOT NULL,
    event_type    TEXT        NOT NULL,
    payload       BYTEA       NOT NULL,   -- MessagePack + 版本头（10.4）
    key_expr      TEXT        NOT NULL,   -- 由 keys 模块生成（9.1）
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ,
    attempt       INT         NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_outbox_pending ON outbox_event (next_retry_at, id)
    WHERE published_at IS NULL;
```

relay 循环（`cs-worker`）：

```sql
UPDATE outbox_event SET attempt = attempt + 1
 WHERE id IN (
   SELECT id FROM outbox_event
    WHERE published_at IS NULL AND next_retry_at <= now()
    ORDER BY id
    LIMIT 200
    FOR UPDATE SKIP LOCKED          -- 裸 SQL，sqlx 无 API（F-9）
 )
RETURNING *;
```

- `SKIP LOCKED` 让多个 relay 实例无锁并行，且不会重复取件。
- 投递成功置 `published_at`；失败指数退避；超上限进死信并告警。
- **至少一次**语义 → 所有订阅方按 `event_id` 去重。
- `PgListener` 只用来「催醒」relay 循环以降低延迟；因它会丢通知（F-9b），**正确性完全由轮询保证**，NOTIFY 丢了只会慢一点，不会丢事件。这是 F-9b 的正确用法。

### 17.6 两级定时器

单一 `timer_task` 表服务 SLA due、Assignment 超时、Workflow Delay、Agent 恢复扫描：

```sql
CREATE TABLE timer_task (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   TEXT        NOT NULL,
    kind        TEXT        NOT NULL,   -- SLA_DUE | ASSIGN_TIMEOUT | WF_DELAY | ...
    target_id   TEXT        NOT NULL,
    fire_at     TIMESTAMPTZ NOT NULL,
    status      TEXT        NOT NULL,   -- PENDING | CLAIMED | DONE | CANCELLED
    claimed_by  TEXT,
    claimed_at  TIMESTAMPTZ,
    payload     JSONB
);
CREATE INDEX idx_timer_due ON timer_task (fire_at, id) WHERE status = 'PENDING';
```

```text
持久层：timer_task（真相，进程重启不丢）
   │
   │  loader 每 10s：pg_advisory_xact_lock 选主 → 取 fire_at < now()+30s 的任务
   │                 置 CLAIMED，装载进本节点
   ▼
近端层：tokio::time::sleep_until / DelayQueue（秒级精度，无 DB 轮询开销）
   │
   ▼
执行 → 置 DONE
```

- 用 `pg_advisory_xact_lock`（**事务级**，连接断开自动释放）而非会话级——会话级锁需要长连接持有，运维上更脆。
- 崩溃导致 `CLAIMED` 未执行：补偿扫描把 `claimed_at < now() - 5min` 且仍 `CLAIMED` 的重置为 `PENDING`（与 10.2 的死节点处理是同一个 worker）。
- 任务执行必须幂等（例如 `SLA_DUE` 先检查 `sla_instance.status` 是否已 `BREACHED`）。

### 17.7 一致性边界总表

| 语义 | 落在哪 | 机制 |
|---|---|---|
| 会话内 seq 单调无空洞 | Postgres | `UPDATE ... RETURNING`（17.2） |
| 命令幂等 | Postgres | 主键 + `ON CONFLICT DO NOTHING`（17.3） |
| 坐席容量不超卖 | Postgres | 条件 UPDATE + CHECK（17.4） |
| 队列取件不重复 | Postgres | `FOR UPDATE SKIP LOCKED`（17.5） |
| 聚合并发修改 | Postgres | `version` 乐观锁 |
| 定时任务选主 | Postgres | `pg_advisory_xact_lock`（17.6） |
| 节点存活 | Postgres | `node_registry.last_seen`（10.2） |
| 事件投递 | Postgres → Zenoh | Outbox，至少一次（17.5） |
| 事件 fan-out | Zenoh | Pub/Sub（第 9 章） |
| 请求应答 | Zenoh | Querier/Queryable（9.2） |
| Presence / 路由表 | Redis | TTL + 心跳续期（8.1） |
| **锁 / 计数器 / CAS** | **禁止用 Zenoh** | 见 10.3 |

### 17.8 建模原则

- 所有业务表带 `id` / `tenant_id` / `created_at` / `updated_at`；聚合根另带 `version`。
- 多租户索引**一律以 `tenant_id` 为最左前缀**。
- **核心查询字段必须结构化，不得依赖 JSONB**（源文档 A 第 17 章，保留）。JSONB 只用于 `custom_attributes` / `metadata` / `channel_payload` / `snapshot` / `variables`。
- `message` 按月分区，热数据 13 个月，之后归档。

---

## 18. 数据访问规范（sqlx 0.9 具体约束）

### 18.1 查询方式选择

| 场景 | 用什么 |
|---|---|
| 静态 SQL（绝大多数） | `sqlx::query!` / `query_as!`，**编译期校验** |
| 动态 SQL（多条件筛选、报表） | `QueryBuilder`；若必须拼字符串，用 `AssertSqlSafe` 显式标注（F-9） |
| 大结果集 | `fetch()` 流式 + keyset 分页，**禁止 `fetch_all` 无上限** |

### 18.2 必须遵守的四条（均源自 F-9）

1. **`.sqlx` 缓存必须提交**，CI 跑 `cargo sqlx prepare --check`。否则离线构建会失败或（更糟）编译期校验被静默跳过。
2. **不指望 pipelining**。sqlx 0.9 不支持 PG pipeline mode。热点路径提吞吐的手段是：批量语句（多行 `INSERT ... VALUES`）、并发连接、以及减少往返次数（用 `RETURNING` 合并读写）。
3. **裸 SQL 集中在 `cs-repo-pg`**。`FOR UPDATE SKIP LOCKED`、advisory lock、`ON CONFLICT` 都没有 API，只能裸写；集中放置便于审计与测试。
4. **`PgListener` 只做优化，不做正确性**（F-9b）。任何「靠 NOTIFY 触发」的逻辑都必须有轮询兜底。

### 18.3 事务

- 事务边界只在 `cs-application` 的用例层划定，`cs-repo-pg` 接受 `&mut PgConnection` 而非自己开事务。
- **一个事务只改一个聚合 + 写 Outbox**。跨聚合一律经 Outbox。
- 事务内**禁止**任何网络调用（HTTP、Zenoh、Redis、LLM）。由 review 检查表强制；`cs-repo-pg` 的函数签名不接受任何 client 类型，从结构上降低误用可能。
- 长事务告警阈值 500ms。

---

## 19. 知识库与检索

### 19.1 Phase 1 单一 Postgres（裁决 #6）

两份源文档分别要求 OpenSearch 与 Qdrant/Milvus。Phase 1 **都不引入**：

| 需求 | Phase 1 方案 | 依据 |
|---|---|---|
| 全文检索 | `tsvector` + GIN，模糊匹配用 `pg_trgm` | F-10b |
| 向量检索 | `pgvector` 0.8.6 + HNSW | F-10 |
| 结构化筛选 | 普通索引 | — |

理由：省掉与业务库的二次同步（这是检索类系统最主要的 bug 与运维来源），且可以在同一事务里做「结构化过滤 + 向量召回」。`tantivy` 是单机库无集群（F-10c），不构成 OpenSearch 的等价替代；引入 OpenSearch 则要同时承担同步管道。

**必须写明的三个坑**：

1. ✗ Postgres `tsvector` **无内置中文分词**（F-10b）→ 必须显式选定 `zhparser` / `pgroonga` / `pg_bigm` 之一并**纳入数据库镜像构建**，不能假设生产环境自带。
2. ✗ 高写入下 GIN 索引更新放大明显（F-10b）→ 消息表不建全文索引，检索走**异步建索引的独立投影表**。
3. ✗ `vector` 类型**维度上限 2000**（F-10）→ embedding 模型选型受此约束；索引构建受 `maintenance_work_mem` 约束，需在运维手册中给出建索引期间的参数调整。

### 19.2 Knowledge 抽象保留可切换性

```rust
#[async_trait]
pub trait Knowledge: Send + Sync {
    async fn search(&self, q: SearchQuery) -> Result<Vec<Document>, KnowledgeError>;
}
```

实现：`PgVectorKnowledge`（Phase 1）；`QdrantKnowledge`（延后，F-11 已核实 client 1.19.0 可用）。**Runtime 不绑定任何具体向量库**（源文档 B 第 19 章，保留）。

### 19.3 RAG 链路

```text
Question ──▶ Query Rewrite ──▶ Hybrid Search ──▶ Rerank ──▶ Context ──▶ LLM ──▶ Answer
                                (BM25/tsvector
                                 + vector)
```

Rerank Phase 1 用简单的 RRF（倒数排名融合）而非独立 reranker 模型——引入 cross-encoder 需要额外推理服务，与「Phase 1 不引入新中间件」一致。

---

## 20. 安全、多租户与可观测性

### 20.1 认证与授权

- 认证：JWT（短期 access + refresh token rotation），OIDC 对接企业 IdP；密码 Argon2id。
- 授权：`Tenant → Organization → Team → Role → Permission`（源文档 A 第 27 章，保留）。
- 权限粒度：`conversation:read:own|team|tenant` / `conversation:escalate` / `workflow:manage` / `ai:manage` / `tool:invoke:{tool_id}` / `admin:manage`。
- **可见域条件在 repository 层强制注入**，不允许应用层自行拼接。这是租户越权最常见的漏点。

### 20.2 多租户隔离

- 每张业务表带 `tenant_id`，**所有查询的 WHERE 必须含 `tenant_id`**——由 `cs-repo-pg` 的函数签名强制（每个查询函数第一个参数是 `TenantId`）。
- 连接池按租户分组限流，防单租户打满。
- Zenoh key 与 Redis key 均以 `tenant` 分段（9.1、8.1）。

### 20.3 限流分层

| 层 | 维度 | 位置 |
|---|---|---|
| 连接 | 每 IP / 每租户并发连接数 | Gateway |
| 消息 | 每连接 msg/s、每租户 msg/s | Gateway + Redis 令牌桶 |
| 命令 | 每类 Command 独立配额 | Application |
| Tool | 每租户 tool 调用 QPS、每 execute 出网配额 | ca-tool（16.4） |
| LLM | 每租户 token/min、并发回合数 | ca-brain |

LLM 层限流是源文档未提但必须有的——否则单租户可以用 LLM 成本打穿整个系统的预算。

### 20.4 审计

敏感操作必须写 `audit_log`：改规则、改 SLA 策略、改权限、导出数据、模拟坐席、**每一次 `get-secret`**（16.3）、每一次 `PolicyDecision::Deny` 与 `RequireHuman`。

Agent 相关审计要能回答：**这条回复为什么发出去了？** 因此 `agent_tool_execution` 与 `sla_event` 之外，还需记录每个回合的 `PolicyDecision` 序列。

### 20.5 日志脱敏

PII（手机号、邮箱、身份证、卡号）在**进入日志前**脱敏，用 `tracing` 的自定义 `Layer` 统一处理，不靠调用方自觉。Tool 的 input/output 落库前同样脱敏，且原文不落日志。

### 20.6 指标

业务：

```text
cs_active_connections{transport}
cs_messages_total{direction,type}
cs_conversation_wait_seconds                    histogram
cs_first_response_seconds                       histogram
cs_dispatch_duration_seconds{stage}             rule|select|cas
cs_dispatch_failed_total{reason}                no_agent|cas_conflict|rule_error
cs_agent_utilization{agent}                     gauge = active/capacity
cs_queue_waiting_count{queue}                   gauge
cs_conversation_escalated_total{reason}
cs_outbound_integration_total{event_type,result}  （5.3）
cs_sla_warned_total / cs_sla_breached_total{policy}
```

Agent 与 Tool：

```text
ca_turn_duration_seconds{outcome}               completed|handoff|failed
ca_brain_tokens_total{tenant,model,kind}        prompt|completion
ca_policy_decision_total{decision}              allow|deny|human|approval
ca_tool_duration_seconds{tool,mode}
ca_tool_failed_total{tool,reason}
ca_wasi_fuel_consumed{tool}                     histogram
ca_wasi_trap_total{tool,reason}                 oom|fuel|epoch|net_quota
ca_snapshot_discarded_total{reason}             schema_mismatch（14.5）
```

基础设施：

```text
cs_outbox_pending_count                         gauge（★核心告警项）
cs_outbox_deadletter_total
cs_timer_lag_seconds                            fire_at 与实际执行的差值
cs_mesh_decode_rejected_total{reason}            （10.4）
cs_db_tx_duration_seconds / cs_db_pool_size
cs_optimistic_lock_conflict_total{aggregate}
cs_node_declared_dead_total
cs_zen_localpool_queue_depth                    gauge（13.6，!Send 执行宿主）
cs_zen_evaluate_duration_seconds{purpose}       dispatch|workflow
cs_zen_evaluate_timeout_total{purpose}          超时即熔断，memo 已落库
cs_workflow_replay_total{trigger}               resume|retry|recover
cs_workflow_replay_nodes                        histogram（13.3 的代价监控项）
cs_workflow_compensation_total{result}
```

`cs_outbox_pending_count` 持续增长是**最重要的单一告警**——它同时覆盖 Zenoh 故障、relay 停摆、下游不可用三类问题。

### 20.7 Trace

OpenTelemetry，贯穿字段：`trace_id` / `span_id` / `tenant_id` / `conversation_id` / `turn_id` / `execution_id` / `tool_id` / `workflow_run_id`。

一个回合的期望 trace 形态（源文档 B 第 30 章，保留）：

```text
turn_123
 ├── understanding      80ms
 ├── brain.call_model  820ms
 ├── policy              3ms
 ├── tool.order.query   45ms
 ├── brain.call_model  700ms
 └── response           20ms
```

`trace_id` 必须能从客户端 Envelope 一路带到 WASI Tool 内部（经 `host` 接口注入），否则跨沙箱边界的问题无法定位。

---

## 21. 部署与运维

### 21.1 端口与协议矩阵

| 端口 | 协议 | 用途 | L7 可代理 |
|---|---|---|---|
| 8080 | HTTP/1.1 + H2 | 控制面 REST、Webhook、健康检查 | 是 |
| 8443 | HTTP/1.1 + WebSocket | **IM 数据面主力** | 是 |
| 8444/udp | QUIC + HTTP/3 | 控制面实验（F-5，默认关） | nginx≥1.25 仅终结下游（F-7） |
| 8445/udp | QUIC + WebTransport | 桩，默认关（F-6） | **否**，L7 均不支持（F-7），须 L4 UDP 透传 |
| 7447 | Zenoh | 节点间事件面，**仅集群内网** | 否 |
| 9090 | HTTP | Prometheus metrics，仅内网 | — |

### 21.2 镜像与目标平台

- 构建 `x86_64-unknown-linux-gnu` 与 `aarch64-unknown-linux-gnu` 双架构。
- 基础镜像用 glibc（debian-slim / distroless）。若要 musl/Alpine，需单独验证——Postgres 与向量扩展的依赖在 musl 下常有惊喜，不作为默认。
- 数据库镜像**必须自建**，因为要装 `pgvector` 与中文分词扩展（F-10b）。这是一个容易被忽略的交付物。

### 21.3 K8s 拓扑

```text
Ingress (nginx / Envoy)
   │  8080 REST      8443 WS（需 sticky 或无状态化，见下）
   ▼
Deployment: gateway   x N   （无状态，HPA 按连接数与 CPU）
Deployment: worker    x M   （Outbox relay / timer / 补偿扫描）
Deployment: agent     x K   （ca-runtime，按 LLM 并发与 wasmtime 内存定容）
StatefulSet: zenoh-router x 3（可选，peer 模式下可省）
   │
PostgreSQL（Primary + Replica + PgBouncer + PITR）
Redis（Sentinel / Cluster）
S3 / MinIO
```

- WebSocket **不需要 sticky session**：路由真相在 Redis（8.1），重连可落任意节点并按 seq 补齐（8.5）。这是 8.1/8.5 设计的直接收益，应在部署文档中明确，避免运维默认加上 sticky 反而降低可用性。
- `agent` 角色单独 Deployment：wasmtime 的组件缓存与 LLM 并发让它的内存画像与 gateway 完全不同，混部会导致 HPA 误判。

### 21.4 滚动升级

- 协议兼容窗口：服务端同时支持 `v` 与 `v-1`（6.5）；Zenoh payload 带版本头（10.4）。
- 升级顺序：**先 worker，再 gateway，最后 agent**。理由：worker 只消费 Outbox（幂等、可重放），风险最低；agent 最后升级是因为 Brain 快照可能被弃用（14.5），放在最后可最小化受影响回合数。
- 数据库迁移必须**向后兼容**（先加列、双写、再删列），不允许「停机改表」。

### 21.5 容量与压测

性能目标**必须压测验证，不能把设计值当实测值**（源文档 A 第 40 章，保留并强化为出场标准）：

| 阶段 | 目标 |
|---|---|
| Phase 1 | 单节点 10K 并发 WebSocket；1K msg/s；P99 消息端到端 < 200ms；P99 API < 200ms |
| Phase 4 | 多节点 100K 并发；10K msg/s |

压测必须覆盖：慢客户端（验证 8.3 的背压不 OOM）、Zenoh 断连（验证 Outbox 堆积与补投）、节点假死（验证 10.2）。

---

## 22. 风险登记与分阶段交付

### 22.1 风险登记

| # | 风险 | 概率 | 缓解 |
|---|---|---|---|
| R-1 | `rig-core` 0.x 破坏性升级打断 Agent 演进 | **高**（每 minor 都 breaking，F-1） | `Brain` trait 隔离（14.5）；快照弃用而非迁移；固定小版本并有计划升级窗口 |
| R-2 | axum WebSocket 无真实背压导致 OOM | **高**（F-4，默认配置即有风险） | 自建 bounded channel + 丢弃分级（8.3/8.4）；压测必须含慢客户端 |
| R-3 | 误把 Zenoh 当状态存储 | 中（两份源文档都有此倾向） | `EventMesh` trait 不暴露 KV 接口（10.3）；review 检查表 |
| R-4 | `wit-bindgen` / Component Model 破坏性变更 | 中（0.61.1 仍 0.x，F-12） | WIT 包名内嵌版本，宿主支持 N 与 N-1（16.2） |
| R-5 | 持久化驱动器里长出业务语义，退化成第二个引擎 | 中（最省事的做法就是在驱动器里加 `if`） | 13.1 划死禁止事项；`cs-workflow` 不依赖 `cs-domain` 的业务类型，只认 `customNode` 的 `kind` 前缀；review 检查表专列一条 |
| R-6 | WASI Tool 的 host 调用挂住整个执行 | 中（F-12，fuel/epoch 不管 host） | 每个 host function 自套超时 + execute 外层超时（16.4） |
| R-7 | 中文全文检索上线才发现无分词 | 中（F-10b） | 数据库镜像自建并纳入 Phase 1 出场标准（21.2） |
| R-8 | LLM 成本失控 | 中 | 租户级 token/min 与并发回合限流（20.3） |
| R-9 | 单租户打满连接池 | 中 | 按租户分组限流（20.2） |
| R-10 | `PgListener` 丢通知被当成事件丢失 | 低 | NOTIFY 只做催醒，轮询保正确性（17.5/18.2） |
| R-11 | 消息表增长过快 | 低 | 月分区 + 13 月热数据 + 归档（17.8） |
| R-12 | WebTransport 需求被重新提出 | 低 | 已留 SPI；结论是「可能但不成熟」（F-6），可随 wtransport 成熟度重估 |
| R-13 | `functionNode` 的 QuickJS **无内存上限**，且内置带签名的出网 HTTP 模块（F-14） | 中（多租户下是 SSRF + 外传通道） | 租户图禁用 `functionNode`（发布期静态校验）；`DenyAllHttpHandler`；`function_timeout_millis = 200`。**内存无库内解法**——只能靠容器内存上限 + OOM 重启兜底，这是本方案唯一未闭环的残余风险（13.7） |

### 22.2 Phase 1 明确不做

HTTP/3 数据面 · WebTransport 生产落地 · WebRTC/MoQ 媒体面 · OpenSearch / Qdrant / Tantivy · Multi-Agent 与 Supervisor · Zenoh storage 插件 · 微服务拆分 · Tool Marketplace · CSAT/报表 BI · SAML/MFA（先只做 OIDC）。

### 22.3 分阶段交付与出场标准

**Phase 0 · 地基（2~3 周）**

内容：workspace 骨架与 4.3 的三道 CI 关卡；`.sqlx` 缓存与迁移基线；自建 pgvector + 中文分词的数据库镜像；`cs-mesh-zenoh` 与 `keys` 模块；双架构构建。

出场标准：
- `x86_64` 与 `aarch64` 镜像均构建成功，3 节点在 K8s 内组网；
- `cargo deny` + 反向依赖脚本 + `sqlx prepare --check` 三关卡在 CI 全绿；
- 故意在 `cs-domain` 里加一行 `use sqlx::...` 会让 CI 变红（**负向验证**，证明关卡真的生效）；
- 数据库镜像中 `pgvector` 与中文分词扩展可用，`CREATE EXTENSION` 成功。

**Phase 1 · IM 主干（5~7 周）**

内容：Envelope 协议 + WebSocket + SSE + 能力协商；自建背压；连接注册表三层；Conversation/Message/Assignment 领域与状态机；seq + 幂等 + Outbox + 容量 CAS；派单流水线（原生兜底决策表）。

出场标准：
- 客户与坐席跨节点、跨传输（WS ↔ SSE）双向通信正常；
- 断线重连按 seq 补齐，**消息零丢失零重复**（客户端去重后）；
- **慢客户端压测**：500 个不读取的连接持续 10 分钟，进程 RSS 稳定、无 OOM、慢连接被按 8.4 降级或关闭（**这是 R-2 的直接验收**）；
- 并发派单压测下 `active_count` 从不超过 `capacity`，`ck_workload_range` **从未触发**；
- 重复 `Envelope.id` 返回同一结果，DB 中只有一条消息；
- 单节点 10K 并发 WS，P99 消息端到端 < 200ms；
- 强制断开 Zenoh 后 Outbox 堆积，恢复后全部补投；
- `SIGSTOP` 某节点 15s 内被判定死亡并回收其 Assignment（**R-3/10.2 验收**）。

**Phase 2 · 会话级 SLA、外发契约与 Workflow 驱动器（4~5 周）**

内容：SlaPolicy/SlaInstance/BusinessCalendar 与 `awaiting_party` 驱动的暂停语义；两级定时器；`cs-integration` 的单向外发契约（5.3）；`cs-decision` 的 zen 宿主（LocalPool + 自建 loader + CustomNodeAdapter）；`cs-workflow` 持久化驱动器（memo 重放、挂起唤醒、重试、逆序补偿）。

出场标准：
- SLA 计时在跨工作时段、节假日、`awaiting_party=CUSTOMER` 暂停场景下正确（**固定日历的黄金用例集**）；
- 日历变更**不改变**历史实例的达标判定（11.3 的 due 预折算验收）；
- 节点重启后 SLA due 与 workflow 的 `suspend.timer` 均不丢；
- **在 `effect.*` 节点副作用已执行、memo 未落库的窗口内 `SIGKILL`**，重启后重放不产生第二次副作用（**13.3 前提 2 的直接验收，靠幂等键**）；
- 人为把 `evaluate` 超时压到 1ms 强制触发超时：已完成节点的 memo 不丢，下次重放从中断处继续（**13.6 验收**）；
- 补偿链在中途失败后按 `workflow_trace.order` 逆序执行完成，终态失败进死信并告警；
- 租户图中含 `functionNode` 时**发布被拒**（**13.7 验收，负向用例**）；
- `ConversationEscalated` 在 relay 重复投递下，下游按 `event_id` 去重后只建一单（**5.3 at-least-once 验收**）；
- 定时器补偿扫描能回收崩溃节点 `CLAIMED` 的任务。

**Phase 3 · AI Agent Runtime（5~6 周）**

内容：两级状态机与 `agent_execution` 快照；`Brain` trait + rig 适配；Policy（Zen + 原生兜底）；ToolRegistry；WASI 沙箱与资源限制；MCP 客户端；三层记忆；人工转接。

出场标准：
- 回合执行中 `SIGKILL`，`resume` 后**从快照继续**且不重复副作用（幂等键验证）；
- 人为把 `schema_ver` 改为不受支持值，`resume` **弃用快照并重跑回合**，结果正确、无重复退款/建单，`ca_snapshot_discarded_total` 计数（**14.5 验收**）；
- 死循环 Tool 被 fuel 中止；超内存 Tool 被 `StoreLimits` 中止；**host 侧慢 HTTP 的 Tool 被 host 超时中止**（**R-6 验收，三者缺一不可**）；
- WASI Tool 尝试访问未声明域名 / filesystem / raw socket 全部被拒并审计；
- 同一 Tool 的 v1/v2 按租户并行生效，不重启 Runtime；
- Policy 对高风险类目的 `Deny` / `RequireHuman` / `RequireApproval` 三分支均有端到端用例；
- `WaitingHuman` 期间 Runtime 不产生任何回复（断言级）；
- 一个回合的完整 trace 可从客户端 Envelope 一路查到 WASI Tool 内部。

**Phase 4 · 渠道、运营化与规模（4~5 周）**

内容：ChannelAdapter 多渠道；规则版本化 + 灰度 + shadow 对账；RBAC 完整落地 + 审计 + 限流分层；可观测性完整；多节点扩容与压测。

出场标准：
- 新增一个渠道**不修改** `cs-domain` 任何代码（依赖方向验收）；
- shadow 模式下新旧规则差异全部落 `rule_shadow_diff` 且可解释；
- 跨租户越权测试全部失败（正向即失败为通过）；
- 多节点 100K 并发连接、10K msg/s 达标；
- 全部 20.6 指标可见，`cs_outbox_pending_count` 告警链路演练通过。

---

## 附录 A · 技术结论可追溯清单

| 事实 | 核实结论 | 文档中依赖它的决策 |
|---|---|---|
| F-1 rig-core 0.42.0，0.x 每 minor breaking；`AgentRun` 是可序列化 sans-IO 状态机；不支持 WASI | 已核实 | 两级状态机（14.1）；Brain trait 隔离与快照弃用（14.5）；Runtime 不能跑在 WASI（16.6）；R-1 |
| F-2 zen-engine 2.0.1 是无状态决策求值器，无定时/等待/人审/持久化；无 invalidate；无求值超时；但 `CustomNodeAdapter` 是 async、`DecisionLoader::load` 返回 Send future、`DecisionGraphTrace` 可反序列化且带 `order` | 已核实 | 裁决 #3；zen 为唯一引擎 + 持久化驱动器（13.1）；memo 重放（13.3）；调用六约束（12.4）；自建不可变 key loader（12.5、13.5）；R-5 |
| F-3 axum 0.8.9 无 http3 feature | 已核实 | HTTP/3 需独立 quinn+h3 监听（7.1、21.1） |
| F-4 axum WS 无真实背压，`max_write_buffer_size` 默认无限，`max_message_size` 默认 64MB；无 permessage-deflate；内部依赖 tokio-tungstenite ^0.29 | 已核实 | 自建逐连接背压（8.3）；丢弃分级（8.4）；参数基线（7.3）；应用层压缩（6.4）；R-2 与其压测出场标准 |
| F-5 quinn 0.11.11 成熟；h3 0.0.8 experimental | 已核实 | HTTP/3 不进数据面（非目标 1、7.1） |
| F-6 wtransport 0.7.2 not production-ready；不能与 HTTP/3 同端口；自签证书 ≤14 天 | 已核实 | WebTransport 仅桩（非目标 2、7.1）；修正「换到 Rust 就可用」的直觉；R-12 |
| F-7 nginx≥1.25 仅终结下游 H3；nginx/Envoy 均不代理 WebTransport | 已核实 | 端口矩阵中 WT 需 L4 透传（21.1） |
| F-8 zenoh 1.10.0：Queryable/Querier 存在；liveliness 无 TTL；无 CAS；插件 internal/unstable | 已核实 | Pub/Sub 与 Query 分工（9.2）；节点存活落 Postgres（10.2）；禁止 Zenoh 做锁/计数（10.3）；不启用插件（10.5、21.2） |
| F-9 sqlx 0.9.0：无 pipelining；SKIP LOCKED 无 API；`PgListener` at-most-once；`query!` 需 `.sqlx`；`AssertSqlSafe` | 已核实 | 数据访问四条（18.2）；Outbox relay 裸 SQL（17.5）；NOTIFY 只做催醒（17.5）；CI 关卡（4.3）；R-10 |
| F-10 pgvector 0.8.6 HNSW 成熟，维度上限 2000；tsvector 无中文分词；tantivy 无集群 | 已核实 | 裁决 #6；Phase 1 单一 Postgres（19.1）；三个坑（19.1）；自建数据库镜像（21.2）；R-7 |
| F-11 qdrant-client 1.19.0 仅 gRPC | 已核实 | Knowledge trait 保留可切换（19.2） |
| F-12 wasmtime 48.0.1，Component Model 与 wasi:http Tier 1；WASI 0.3 已发布但 Rust guest 工具链在补齐；wit-bindgen 0.61.1；fuel/epoch/StoreLimits；host 调用不受限；无网络配额 | 已核实 | 目标 wasm32-wasip2（16.1）；WIT 版本化（16.2）；出网白名单（16.3）；资源限制表与自建项（16.4）；R-4、R-6 |
| F-13 rmcp 3.2.0 生产可用；无 legacy SSE；无 WebSocket；订阅跨重连不恢复 | 已核实 | MCP 走 Streamable HTTP、重连主动重建订阅（15.4） |
| F-14 zen-engine `evaluate` 返回 **`!Send`** future；`functionNode` 的 QuickJS 默认超时 5000ms 且**无内存上限**，内置 `console` 与 reqsign 签名的出网 HTTP 模块；`ZEN_CONFIG` 进程级全局；`TransformExecutionMode::Loop` 顺序执行、图内无并行 fan-out；提供 `with_http_handler` | 已核实 | LocalPool 执行宿主（13.6）；`functionNode` 治理五条（13.7）；并行拆子 run（13.4）；调用约束第 5/6 条（12.4）；R-13 |
| 本仓库无 Rust 源码，仅 darwin-arm64 预编译 dylib | 已核实 | 全新 workspace，无可复用 Rust 代码（2.5） |
| qdrant client/server 官方兼容矩阵；wasi:http 0.3 是否进入 Phase 5 | **未能核实** | 不作为任何决策依据 |

---

## 附录 B · 与 Java 方案的对照

| 维度 | Java 方案 | 本 Rust 方案 | 差异根因 |
|---|---|---|---|
| 业务范围 | IM + 会话级 SLA，工单外置 | 同上，另加 Workflow / Knowledge / AI Agent / WASI Tool，工单同样外置 | **工单边界结论一致**；Rust 侧范围更宽（1.3） |
| 数据面框架 | 绕过 WebFlux，用原生 Vert.x HttpServer | axum（无需绕过） | Java 侧 WebFlux 适配层写路径不支持流式 |
| WebSocket 背压 | Vert.x 提供 `writeQueueFull()` 真实背压 | **axum 不提供，必须自建**（F-4） | 框架能力差异，Rust 侧反而更需要手工建设 |
| HTTP/3 | Vert.x 5.1.7 内置 GA | 需 quinn + h3（0.0.8 experimental） | **Java 侧此项更成熟** |
| WebTransport | JVM 上无实现，需非 JVM 边车 | 可实现但 0.7.2 draft（F-6） | Rust 是唯一可行语言，但仍不成熟 |
| 规则引擎 | `io.gorules:zen-engine`（UniFFI over Rust） | `zen-engine` crate 原生 | 同一引擎，Rust 侧无 FFI 层 |
| Workflow | 未在范围内 | zen-engine 单引擎 + 持久化驱动器（JDM 即定义格式） | 范围差异；且 Java 侧的 UniFFI 绑定不暴露 `!Send` 问题（F-14 只对 Rust 侧成立） |
| AI Agent | 明确非目标 | 一等公民，两级状态机 | 范围差异；且 Java 无 rig 等价物 |
| Tool 沙箱 | 明确非目标 | wasmtime Component Model | JVM 侧无同等成熟的 wasm 沙箱 |
| 一致性机制 | seq / 幂等 / Outbox / CAS / SKIP LOCKED / advisory lock / 两级定时器 | **完全相同** | 都因 Zenoh 无 CAS 无租约而落 Postgres |
| Zenoh 缺口 | 需交叉编译 JNI、runOnContext 回跳、节点租约、替换 JDK 序列化 | 无 JNI 问题；节点租约与版本化编解码同样必修 | Rust 侧少两项，因无 FFI 与无 JDK 序列化 |

一个值得记录的结论：**换成 Rust 并没有消除这套系统的主要复杂度**。一致性机制完全一样（都落 Postgres），Zenoh 的语义缺口完全一样，SLA/派单/外发契约的领域复杂度完全一样，工单在两套栈里都是外部服务。Rust 带来的实质收益集中在三处——WASI Tool 沙箱、无 GC 的长连接内存画像、以及 Agent Runtime 的生态（rig/zen-engine/wasmtime 原生）；实质代价则是 **axum 的 WebSocket 背压需要自建**（Vert.x 免费提供）、**HTTP/3 反而更不成熟**、rig 仍是 0.x 带来的持续升级成本，以及 Rust 侧独有的两笔 zen 账：`!Send` 求值必须配 LocalPool 执行宿主（13.6），`functionNode` 的 QuickJS 出网与内存需要额外治理（13.7、R-13）。
