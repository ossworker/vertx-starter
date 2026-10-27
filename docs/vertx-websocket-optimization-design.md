# Vert.x WebSocket 优化方案（对标 socketio4j / netty-socketio 4.0.1）

> 基线：Java 21 · Vert.x 5.2.0 · Spring Boot 3.5.16 · Spring Framework 6.2.19
> 落地模块：`vertx-spring-boot-web-server`（SPI 与实现）、`vertx-spring-boot-cluster-zenoh`（仅跨节点扇出的可选装配）
> 前置文档：`docs/vertx-websocket-support-design.md`（Phase 1 已落地并验证）
> 交付物：本方案文档。**不含代码改动**。
> 编写日期：2026-09-24
> 参考对象：`https://www.socketio4j.org`（48 页 GitBook，实为 netty-socketio 4.0.1 的社区分叉）
> 文中所有 Vert.x / Spring / socketio4j 结论均来自本地 jar 源码与官方文档核对，附录 A 给出可追溯清单。

---

## 0. 结论摘要（TL;DR）

1. **不引入 socketio4j 作为依赖**。它自己 `bind` 端口、自己建 Netty pipeline（`config.setPort(9092)` → `new SocketIOServer(config)` → `server.start()`），与本项目「HTTP / WebSocket 共用一个端口」的硬要求直接冲突；它说的是 Socket.IO / Engine.IO 协议而非原生 WebSocket（`setContext("/socket.io")` + `Transport.POLLING`），引入后浏览器 `new WebSocket(...)` 就连不上了——而这正是 Phase 1 已验证可用的路径。
2. **借语义，不借库**。socketio4j 真正值得学的有五件事：房间（Rooms）+ 组播、心跳三件套（`pingInterval` / `pingTimeout` / `firstDataTimeout`）、`ExceptionListener` 式异常钩子、`EventStore` 式跨节点扇出、以及「handler 里不许阻塞」的工程纪律。其中**房间是最大的能力缺口**——今天用户要自己维护 `CopyOnWriteArraySet<WebSocketSession>` 并自己解决跨节点。
3. **一个关键架构判断：房间需要跨节点分发，Ack 不需要。** WebSocket 会话与它的 TCP 连接绑定在同一个 JVM，所以请求-响应/ack 永远是节点内的；只有房间广播需要扇出。这一刀砍掉了整类复杂度——而且它也是被迫的：Zenoh 层的跨节点 `request/reply` 目前是**死代码**（`ZenohMessageCodec.buildReplyPayload:187`、`KeyPrefixStrategy.eventBusReplyKey:112` 零调用方），今天根本建不起来。
4. **三个自有缺陷必须一并修，且都已解除疑点**：
   - 出站 `send()` 是严格串行的（`VertxWebSocketSession.java:84-88`，每条消息一个 `CompletionStage`），改成 Vert.x 惯用的「只在写队列满时才等」流水线。
   - **R12 现在可以做了**：核对 `VertxConnection.shutdown` 后确认二次关闭有守卫（`:200-218` 的 `shutdown != null` 分支 + `:228-241` 的 `terminateClose` 自有 `closeSent` 守卫），`WebSocketConnectionImpl.writeClose:100` 的 `assert !closeSent` **不可能被触发**。
   - **R6 的机理和修法都变了**：`idleTimeout` 映射到 Netty `IdleStateHandler` 的**第三个参数（all-idle）**（`NetServerImpl.java:478`），意味着**任何一次写操作都会重置计时器**——服务端周期 PING 单独就足以救活合法空闲连接。但这也带来一个反直觉的后果：**开启 PING 却不做 PONG 超时，会把原来 60 秒的上界变成无上界**（死对端靠 TCP 重传超时才暴露，可能十几分钟）。所以 `ping-timeout` 不是可选项，而是 `ping-interval` 的强制配套。
5. **依赖方向：SPI 定义在 `vertx-spring-boot-web-server`，Zenoh 传输实现也放在 web-server，但依赖 `vertx-spring-boot-cluster-zenoh` 声明为 `<optional>true</optional>`**。两个模块目前互不依赖（都只依赖 `vertx-spring-boot-autoconfigure`），因此不会产生 Maven 环；用 `@ConditionalOnClass` 的嵌套配置类装配，只装了 web-server 的用户拿到本地房间，两个都在的用户自动获得分布式房间。**cluster-zenoh 零改动**。
6. **明确不借的三样**：Socket.IO 线协议、HTTP 长轮询回退、Namespace。Namespace 是因为 Socket.IO 所有客户端共用一个 `/socket.io/` 端点才必须存在的逻辑分区手段；我们基于路径的 `HandlerMapping` 路由已经天然提供了同等隔离。
7. 分三期：P0 语义与性能补齐（约 1.5 人日）→ P1 会话注册表 + 本地房间（约 1.5 人日）→ P2 跨节点扇出 + 可观测性（约 2 人日）。

---

## 1. socketio4j 是什么，能学什么

### 1.1 项目定位（核实结论）

`socketio4j.org` 是 **netty-socketio 4.0.1** 的社区维护分叉，GitBook 文档共 48 页。它是一个**独立的服务端运行时**，不是库级别的扩展点：

```java
Configuration config = new Configuration();
config.setHostname("localhost");
config.setPort(9092);                 // ← 自己 bind 端口
config.setContext("/socket.io");      // ← Socket.IO 协议端点
config.setTransports(Transport.POLLING, Transport.WEBSOCKET);
config.setStoreFactory(new RedissonStoreFactory(...));

SocketIOServer server = new SocketIOServer(config);   // ← 自己建 Netty pipeline
server.addConnectListener(client -> ...);
server.addEventListener("chat", ChatMessage.class, (client, data, ackRequest) -> ...);
server.start();
```

它自己的性能基线（v4.0.0，2026-05-11 官方报告）：

| 场景 | 规模 | 吞吐 | 平均延迟 | P99 | 错误率 | 丢失率 | 堆峰值 | GC 后残留 |
|---|---|---|---|---|---|---|---|---|
| 单机 | 10000 客户端 × 10000 消息 | 9,999.69 msg/s | 66.21 ms | 116 ms | 0% | 0% | 3414 MB | 37 MB |
| 分布式 | 同上 | 8,571.24 msg/s | 672.94 ms | 775 ms | 0% | 0% | 2998 MB | 102 MB |

**分布式比单机吞吐低 14%、延迟高 10 倍**——这是跨节点扇出的固有代价，也是本方案把分布式能力放到 P2、并允许用户完全不启用它的原因。

### 1.2 为什么不能直接引入（四条）

| # | 冲突 | 证据 | 后果 |
|---|---|---|---|
| C1 | **自有端口 + 自有 Netty pipeline** | `config.setPort(9092)`、`new SocketIOServer(config)`、`server.start()`；另有 `bossThreads` / `workerThreads`（默认 0 → CPU×2）与 `transportType` AUTO（IO_URING→EPOLL→KQUEUE→NIO） | 直接违反用户在需求 1 中的硬要求「支持 http 和 websocket 共用端口」。并且会在 Vert.x 旁边复制一套 event loop、accept 队列、TLS 配置、原生传输探测 |
| C2 | **说的是 Socket.IO / Engine.IO，不是原生 WebSocket** | `setContext("/socket.io")`、`setTransports(Transport.POLLING, Transport.WEBSOCKET)`、polling→WS 的 `upgradeTimeout` 10000 ms | 只有 socket.io 客户端能连。Phase 1 已验证的浏览器 `new WebSocket(...)`、`wscat`、Node 全局 `WebSocket` 全部失效 |
| C3 | **回调式 API，与 Reactor 栈不同构** | `DataListener` 返回 `void`；`client.sendEvent(...)` 是 fire-and-forget；官方最佳实践明写「no blocking or DB access in event handlers」「offload business logic」 | 业务代码被迫在自己的 handler 里手工搭线程池/桥接 Reactor，正是我们要消除的样板 |
| C4 | **违反项目既有约束** | 「架构优化必须继续使用 Spring WebFlux 框架，所有改进需在现有适配层内实施，禁止引入架构变更（如混合路由、绕过 WebFlux 管道等），以影响最小为最高」 | 引入后 `WebFilter` / CORS / 鉴权 / `ServerWebExchange` 属性 / 统一异常处理对 WS 全部失效 |

> C4 是决定性的：即使 C1-C3 都能忍，这条也不允许。

### 1.3 能力矩阵：借 / 不借 / 已有

| socketio4j 能力 | 决策 | 理由 |
|---|---|---|
| **Rooms**（`joinRoom` / `leaveRoom` / `getRoomOperations` / `room.sendEvent` / `room.getClients` / 断连自动离房） | ✅ **借（P1，最大缺口）** | 今天用户必须自己维护 `CopyOnWriteArraySet<WebSocketSession>`、自己处理断连清理、自己解决跨节点。这是从「能用」到「好用」最值钱的一步 |
| **心跳三件套**（`pingInterval` 25000 / `pingTimeout` 60000 / `firstDataTimeout` 5000） | ✅ **借前两个（P0）** | 修 R6。详见 4.3；`firstDataTimeout` 不借，见下行 |
| `firstDataTimeout`（「prevents silent channel attacks」） | ❌ **不借** | 这是 **Engine.IO 协议要求**——Socket.IO 客户端升级后必须发一个握手包，所以「升级后静默」一定是异常。**原生 WebSocket 没有这个约束**：纯服务端推送通道（订阅行情、进度条）升级后长时间不发数据是完全合法的。照搬会误杀合法连接。它想防的「静默通道攻击」在原生 WS 语境下由 `ping-interval` + `ping-timeout` + `max-sessions` 覆盖 |
| **`ExceptionListener`**（`onEventException(Exception, List<Object>, SocketIOClient)` / `onDisconnectException`） | ✅ **借（P0，扩展现有 R11 分类）** | Phase 1 已在 `VertxRequestUpgradeStrategy` 里做了「对端走了 vs 真错误」分级日志；再开放一个可插拔钩子让用户接到 Sentry，成本极低 |
| **`EventStore` / `StoreFactory`**（跨节点事件同步，backend：NATS / Redis / Hazelcast / Kafka / Memory） | ✅ **借架构，不借实现（P2）** | 我们已经有 Zenoh EventBus。关键学习点是它**把「存储」和「事件传播」解耦成 SPI**（`StoreFactory.createStore/createMap/eventStore`），以及它诚实地写明「The EventStore interface does not define delivery guarantees」——我们的 Zenoh `publish` 同样是 fire-and-forget `void`，必须在文档里同等诚实 |
| `EventStore` 的 `nodeId` 自过滤 | ✅ **借（P2，但走 payload 而非 header）** | 详见 4.6：Zenoh 桥的本地 `eventBus.publish` 是注释掉的（`ZenohEventBusBridge.java:47`），本地投递靠 Zenoh 回环重注入，所以**发起节点一定会收到自己的消息**，必须过滤 |
| **`Store`**（per-session KV，`set/get/has/del`，「lives for exactly one client session and is destroyed when the session ends」） | 🟢 **已有，无需新增** | `AbstractWebSocketSession.java:58` 每个会话持有一个 `ConcurrentHashMap`，`:78-80` 从 `HandshakeInfo` 拷入初始属性（过滤 null），`:112-113` 由 `getAttributes()` 暴露。**生命周期保证与 socketio4j 的 `Store` 完全一致且免费**。这是本次盘点最重要的「不用做」结论 |
| `WebSession` → WS 会话属性透传 | ✅ **借（P0，一行配置）** | `HandshakeWebSocketService.java:153` 有官方钩子 `setSessionAttributePredicate(Predicate<String>)`；默认 null 时走 `:77` 的 `EMPTY_ATTRIBUTES`（`Collections.emptyMap()`）。我们的 `VertxWebFluxConfigurer` 现在没暴露它。补一个 `vertx.websocket.session-attribute-names` 即可 |
| `AckMode`（AUTO / AUTO_SUCCESS_ONLY / MANUAL）+ `ackSender.sendAckResponse` | ❌ **不借** | 原生 WebSocket **没有 ack 帧**。Socket.IO 的 ack 是它自己线协议的一部分；移植过来就必须给每条消息套信封，浏览器 `new WebSocket(...)` 立刻失效（同 C2）。而且 ack 天然是节点内的（见 0.3），WebFlux 已经能表达：handler 收到消息 → `session.send(...)` 回一条带 correlation id 的消息。缺的只是「关联 id + 超时」这点脚手架，属于应用层约定，不是传输层职责。**列为不做，并在用法文档里给出 15 行示例** |
| **Namespace**（`server.addNamespace("/chat")`，默认 `""`，单物理连接多逻辑分区） | ❌ **不借** | Socket.IO 需要 namespace 是因为**所有客户端共用一个 `/socket.io/` 端点**，只能在协议内再分层。我们基于路径的 `HandlerMapping` 路由（`/ws/chat`、`/ws/notify`）已经提供了同等的逻辑隔离，且每个路径可以有独立的 `WebFilter` 链、独立的子协议、独立的鉴权——比 namespace 更强 |
| `AuthorizationListener`（`data -> AuthorizationResult.SUCCESS`，`data.getSingleUrlParam("token")`） | ❌ **不借** | 握手发生在 WebFlux 管道内，`WebFilter` 严格更优：能拿到完整 `ServerWebExchange`、能返回标准 401/403、能与 Spring Security 组合。Phase 1 的 T11 已验证 |
| HTTP 长轮询回退（`Transport.POLLING`） | ❌ **不借** | 与 Phase 1 的非目标 2（SockJS）同理，投入产出比低 |
| `httpCompression` / `websocketCompression` | 🟢 **已有** | `VertxWebSocketProperties.java:47-58` 的 `perMessageCompression` / `perFrameCompression` / `compressionLevel` |
| `transportType` AUTO（原生传输探测） | 🟢 **已有** | `vertx.prefer-native-transport`，见示例 `application.yml` |
| `maxHttpContentLength` 64 KB / `maxFramePayloadLength` 64 KB | 🟢 **已有，需对齐文档** | `VertxWebSocketProperties.java:30,36` → Vert.x `DEFAULT_MAX_WEBSOCKET_FRAME_SIZE = 65536`（`HttpServerOptions.java:66`）、`DEFAULT_MAX_WEBSOCKET_MESSAGE_SIZE = 65536 * 4`（`:72`）。与 socketio4j 同量级，无需改默认值 |
| `bossThreads` / `workerThreads` / `upgradeTimeout` | 🟢 **已有或不适用** | event loop 池由 `vertx.event-loop-pool-size` 管；我们是就地升级，没有 polling→WS 的升级窗口 |
| `randomSession` / `needClientAuth` / `SocketSslConfig` | 🟢 **已有** | 复用 Spring Boot 标准 `server.ssl.*`（项目既有约束） |

---

## 2. 现状差距盘点（已核实代码事实）

| 位置 | 事实 | 差距 |
|---|---|---|
| `VertxWebSocketSession.java:84-88` | `send()` = `Flux.from(messages).concatMap(m -> Mono.fromCompletionStage(() -> write(m).toCompletionStage())).then()` | ⚠️ **严格串行**：`concatMap` 要等上一条 inner 终止才订阅下一条，而 `writeTextMessage` 的 `Future` 在帧真正写出去后才完成。N 条消息 = N 次完整 flush 往返。**这是本次最高价值的性能项** |
| `VertxWebSocketSession.java:146-148` | `toDataBuffer(buffer)` = `nettyBufferFactory.wrap(buffer.getBytes())`，每帧一次全量字节拷贝 | 🟡 已知取舍。javadoc 已说明为什么不能零拷贝（`receive()` 会排队，Vert.x 可能提前释放帧，且 `WebSocketHandler` 契约不保证用户会 release）。**保留拷贝**，改由 `max-message-size` 兜住单次拷贝上界 |
| `VertxWebSocketSession.java:56-80` | `receive()` 用 `Flux.create(..., OverflowStrategy.BUFFER)` | ⚠️ **入站积压无上界**。慢消费者 + 快生产者 = 堆无界增长，且每条排队消息都持有一份拷贝后的 `byte[]`。socketio4j 靠 `maxFramePayloadLength` 兜底，但那只管单帧大小，不管队列深度 |
| `VertxWebSocketSession.java:47-51` | 只在 `closeHandler` 里置 `closed` + 完成 sink | ⚠️ 没有对外的会话生命周期事件源，房间/指标/注册表都无处挂载 |
| `VertxWebSocketSession.java:96-103` | `close()` 先 `if (webSocket.isClosed()) return Mono.empty()` 再 `webSocket.close(code, reason)` | ✅ 已有二次关闭短路。配合 `VertxConnection` 的守卫（见 4.2），**R12 可安全实现** |
| `VertxRequestUpgradeStrategy.java:67-79` | `handler.handle(session)` → `checkpoint` → `onErrorResume`（分级日志） | ⚠️ **正常完成后不发 CLOSE 帧**（R12）。`HandshakeWebSocketService.handleRequest:231-234` 也是原样返回 `upgrade(...)`，Spring 层同样不补 |
| `VertxWebSocketProperties.java:19-74` | 10 个属性：`enabled` / `maxFrameSize` / `maxMessageSize` / `subProtocols` / 4 个压缩项 / `closingTimeout` | ⚠️ **没有任何心跳属性**（Phase 1 文档 §5.7 已把 `server-heartbeat-interval` 标为 Phase 2） |
| `VertxHttpServerProperties.java:23` | `idleTimeout` 默认 **60 秒** | ⚠️ **R6 已确认为真实缺陷**：`NetServerImpl.java:478` 把它装进 `IdleStateHandler` 的 all-idle 槽；`WebSocketConnectionImpl.java:37` `extends VertxConnection` 且**只覆写 `handleShutdown:92` 和 `writeClose:99`，没有覆写 `handleIdle`**；`VertxConnection.handleIdle:138-142` 直接 `chctx.close()`。**合法空闲 60 秒的 WebSocket 会被静默掐断** |
| `AbstractWebSocketSession.java:58,78-80,112-113` | 每会话一个 `ConcurrentHashMap`，会话结束即可回收 | ✅ **socketio4j `Store` 的语义已经免费拥有**，不需要新类 |
| `HandshakeWebSocketService.java:77,153,259-267` | `sessionAttributePredicate` 默认 null → `EMPTY_ATTRIBUTES`（不可变空 map） | ⚠️ 官方钩子未暴露，`WebSession` 里的登录态无法自动带进 WS 会话 |
| `WebSocketHandlerAdapter.java:106-108` | `getWebSocketService().handleRequest(exchange, handler).then(Mono.empty())` | ✅ 单一入口，策略层是唯一可控的切面位置（好消息：所有增强都能收敛在 `VertxRequestUpgradeStrategy` 一处） |
| `vertx-spring-boot-web-server/pom.xml` | 依赖 `vertx-spring-boot-autoconfigure` / `vertx-core` / `spring-boot(-autoconfigure)` / `spring-boot-starter-webflux`（排除 reactor-netty）。**没有 micrometer** | ⚠️ 指标能力需要新增 `<optional>true</optional>` 依赖 |
| 模块依赖图 | web-server 与 cluster-zenoh **互不依赖**，都只依赖 `vertx-spring-boot-autoconfigure`；`vertx-spring-boot-starter` 同时聚合两者 | ✅ **SPI 放 web-server、Zenoh 传输也放 web-server（optional 依赖 cluster-zenoh）不会产生 Maven 环，且 cluster-zenoh 零改动** |
| Zenoh EventBus（`vertx-spring-boot-cluster-zenoh`） | `ZenohEventBusBridge.publish(String, Object[, DeliveryOptions])` 返回 **`void`**（`:36,43`），本地 `eventBus.publish` **被注释掉**（`:47`），本地投递靠 Zenoh 回环；payload 是 `JsonObject` 的**纯 JSON 字节**（`ZenohMessageCodec.java:26`），**没有 `senderNodeId` / version / flags 字段**；`nodeId` 是 `UUID.randomUUID()`（`ZenohClusterManager.java:80`）且**不可配置**；跨节点 `request/reply` 是**死代码**（`buildReplyPayload:187`、`eventBusReplyKey:112` 零调用方）；`ZenohEventBusAutoConfiguration:14-29` 是 `@ConditionalOnBean(ZenohClusterManager.class)` | ⚠️ 约束四条：① 扇出只能 fire-and-forget；② 必须自己在 payload 里带 origin node 做自过滤；③ 跨节点载荷**必须是 JSON 兼容类型**，二进制要 base64；④ 桥 Bean 可能不存在，必须可选注入 |

---

## 3. 目标架构

### 3.1 分层与依赖方向

```
┌──────────────────────────── 用户代码 ────────────────────────────┐
│  @Component ChatHandler implements WebSocketHandler              │
│     · rooms.join(session, "room42")                              │
│     · rooms.sendText("room42", payload)                          │
│     · session.getAttributes().put(...)   ← Store 等价物，已有     │
└──────────────────────────────────────────────────────────────────┘
                              ↑ 注入
┌───────────────── vertx-spring-boot-web-server ───────────────────┐
│  VertxRequestUpgradeStrategy   ← 唯一切面：会话生命周期编排       │
│    ├ VertxWebSocketSession     ← 出站流水线化、入站积压上界       │
│    ├ WebSocketSessionRegistry  ← 会话表（新增 SPI）               │
│    │   └ List<WebSocketSessionListener>  ← 生命周期广播           │
│    ├ WebSocketRooms            ← 房间（新增 SPI + 默认实现）      │
│    ├ WebSocketHeartbeat        ← PING/PONG + 超时（新增）         │
│    ├ WebSocketFailureHandler   ← 异常钩子（新增 SPI，可选）       │
│    ├ WebSocketMetrics          ← Micrometer（新增，optional 依赖）│
│    └ WebSocketFanOutTransport  ← 跨节点扇出（新增 SPI）           │
│        ├ NoOpFanOutTransport        默认（单机）                  │
│        └ ZenohFanOutTransport       @ConditionalOnClass 装配      │
└──────────────────────────────────────────────────────────────────┘
                              ↑ optional（编译期）
┌────────────── vertx-spring-boot-cluster-zenoh（零改动）──────────┐
│  ZenohEventBusBridge.publish(address, JsonObject, options) : void │
└──────────────────────────────────────────────────────────────────┘
```

**关键设计选择：Zenoh 传输实现放在 web-server，而不是 cluster-zenoh。**
理由：① 不产生 Maven 环（两模块当前互不依赖，web-server → cluster-zenoh 加 `<optional>true</optional>` 即可）；② cluster-zenoh 零改动，符合「影响最小」；③ 只装 web-server 的用户完全不受影响（`@ConditionalOnClass` 守卫）；④ 这与 Spring Boot 自身 `*-autoconfigure` 模块的做法一致（optional 依赖一切，按 classpath 装配）。

### 3.2 会话生命周期编排（收敛在 `VertxRequestUpgradeStrategy` 一处）

```
upgrade(exchange, handler, subProtocol, handshakeInfoFactory)
  │
  ├─ ① canUpgradeToWebSocket() 校验 ────────────── 失败 → Mono.error（握手前，仍走 WebFlux 错误管道）
  ├─ ② [P2] max-sessions 上界校验 ──────────────── 超限 → 503（握手前，可正常渲染 HTTP 响应）
  ├─ ③ 回写 Sec-WebSocket-Protocol（R3）
  ├─ ④ nativeRequest.toWebSocket()  ← 就地升级，101 上线
  │
  └─ ⑤ 升级后编排（全部在这一个 flatMap 内）
       │
       ├─ Mono.usingWhen(
       │     resource  = registry.register(session)        // 广播 onSessionOpened → rooms / metrics
       │     pipeline  = Mono.usingWhen(
       │                   heartbeat.start(),               // 装 PING 定时器（若开启）
       │                   hb -> handler.handle(session)
       │                          .then(closeOnNormalCompletion(session)),   // ★ R12
       │                   hb -> hb.stop()),
       │     release   = registry.unregister(session))     // 广播 onSessionClosed → rooms 自动离房
       │
       ├─ .checkpoint(uri + " [VertxRequestUpgradeStrategy]")
       └─ .onErrorResume(分级日志 + WebSocketFailureHandler 钩子)   // R11，已有
```

三条不变量（必须在实现里守住）：

- **I1**：`registry.register` 一定发生在 `handler.handle` 之前，`registry.unregister` 一定发生在之后——**即使 handler 返回 `Mono.never()` 且对端断连**。为此 `unregister` 必须挂两个触发源：`usingWhen` 的 release（管道终止）+ `session.closeStatus()` 订阅（`closeHandler` 触发）。`unregister` 幂等（`ConcurrentHashMap.remove`），双触发无害。
- **I2**：升级后的一切失败都**不得**逃出策略层（R11 已建立的约束，新增的心跳/关闭路径同样适用）。
- **I3**：心跳定时器必须在管道终止时取消，否则每次断连泄漏一个 `vertx.setPeriodic` id。

---

## 4. 详细设计

### 4.1 出站写入流水线化（P0，性能）

**问题**：`VertxWebSocketSession.java:84-88` 的 `concatMap` 把 N 条消息变成 N 次串行 flush 往返。

**Vert.x 提供的正确工具**（已核实）：`WebSocketBase extends ReadStream<Buffer>, WriteStream<Buffer>`（`WebSocketBase.java:44`），因此有 `writeQueueFull()`、`setWriteQueueMaxSize(int)`（`:65`）、`drainHandler(Handler<Void>)`（`:68`）。`WebSocketImplBase.writeQueueFull():141-146` 委托给 `conn.writeQueueFull()`；`handleWriteQueueDrained:599-606` 用 `context.dispatch(handler)` 回调（在正确的 context 上）。

**目标语义**：写队列未超高水位 → 写完立刻处理下一条（流水线）；超了 → 等这一条的 `Future` 完成再继续（背压）。这正是 `WriteStream` 的惯用法，也最接近 Reactor Netty 会话「先写后 flush」的行为。

**两个已核实的陷阱，方案必须显式处理：**

| 陷阱 | 证据 | 处理 |
|---|---|---|
| `writeQueueFull()` 在已关闭的 socket 上**抛异常**而不是返回 false | `WebSocketImplBase.java:141-146` 调 `checkClosed()`，`:339-343` `throw new IllegalStateException("WebSocket is closed")` | 包一层 try/catch，关闭时返回 `false`。**不能靠 Reactor 把同步异常转成 onError**——`IllegalStateException("WebSocket is closed")` 不在 `isPeerGone` 的识别范围内，会被当真错误打成 ERROR 日志（回归 R11 的成果） |
| 关闭后写消息返回**失败的 Future**，若不观察就静默丢失 | `WebSocketImplBase.writeFrame:299-303` `if (isClosed()) return context.failedFuture("WebSocket is closed")`；`writeTextMessage:239-246`、`writeBinaryMessage:234-235`、`writePing:255-259`、`writePong:263-267` 全部收敛到 `writeFrame` | 不等待的那条分支也必须 `written.onFailure(record)`，并在循环头和收尾处把记录的失败重新抛出 |

**设计**（`VertxWebSocketSession`，约 20 行）：

```java
private final AtomicReference<Throwable> writeFailure = new AtomicReference<>();

@Override
public Mono<Void> send(Publisher<WebSocketMessage> messages) {
    return Flux.from(messages)
            .concatMap(message -> {
                Throwable prior = this.writeFailure.get();
                if (prior != null) {
                    return Mono.<WebSocketMessage>error(prior).then();   // 尽早终止
                }
                Future<Void> written = write(message);
                if (queueFull()) {
                    // 超高水位才等：这一条 flush 完再继续，形成背压。
                    return Mono.fromCompletionStage(() -> written.toCompletionStage());
                }
                written.onFailure(err -> this.writeFailure.compareAndSet(null, err));
                return Mono.empty();                                     // 流水线
            })
            .then(Mono.defer(() -> {
                Throwable failure = this.writeFailure.get();
                return failure != null ? Mono.error(failure) : Mono.empty();
            }));
}

/** {@code writeQueueFull()} throws once the peer is gone; the pending write Future already
 *  carries that failure, so a closed queue is simply "not full". */
private boolean queueFull() {
    try {
        return getDelegate().writeQueueFull();
    } catch (IllegalStateException closed) {
        return false;
    }
}
```

> 注意：`write(message)` 的 `finally { DataBufferUtils.release(payload) }`（`:119-121`）保持不变——它释放的是入参 `DataBuffer`，而 `writeTextMessage` 内部已经把文本编码成新的 `ByteBuf`，所以流水线化不会造成 use-after-free。这一点在实现时必须用二进制消息回归验证（`writeBinaryMessage` 走 `Buffer.buffer(bytes)` 拷贝，见 `:150-154`，同样安全）。

**同时新增属性**：`vertx.websocket.write-queue-max-size`（默认 `0` = 沿用 Vert.x 连接层默认值，不改），>0 时在升级后调一次 `webSocket.setWriteQueueMaxSize(n)`。

**验收**：单会话连发 10000 条 64 B 文本消息，改造前后对比墙钟时间；预期数量级改善（串行的 N 次 flush 往返 → 流水线）。

### 4.2 R12：handler 正常完成后补发 CLOSE(1000)（P0，语义对齐）

**问题**：有限发布者（如 `session.send(Flux.just(msg))`，不订阅 `receive()`）正常完成后，连接**挂着不关**，直到 60 秒空闲超时被掐（R6）。Reactor Netty 在 `HttpServerOperations.java:1361-1367` 的 `onComplete()` 里做 `sendCloseNow(new CloseWebSocketFrame(NORMAL_CLOSURE), listener)`；我们和 Spring 层都没有对应动作（`HandshakeWebSocketService.java:231-234` 原样返回 `upgrade(...)`）。

**Phase 1 把它推迟的唯一原因是「要先确认不会撞上 `assert !closeSent`」。现在已核实，安全：**

```
WebSocketBase.close(short, String)            WebSocketBase.java:310-311
  └─ shutdown(0L, SECONDS, code, reason)
       └─ WebSocketImplBase.shutdown(Duration, short, String)   :149-153   ← 无 isClosed() 守卫，直接构造 CLOSE 帧
            └─ VertxConnection.shutdown(Duration)               :184-198   ← public final，切到 event loop
                 └─ VertxConnection.shutdown(Duration, ChannelPromise)  :200-218
                      ├─ 首次（shutdown == null）→ shutdown = promise → handleShutdown(...)  :210-217
                      │    └─ WebSocketConnectionImpl.handleShutdown   :92-96
                      │         └─ webSocket.handleShutdown() 无 shutdownHandler → false
                      │              → super.handleShutdown → … → writeClose(promise)  ← closeSent = true
                      └─ 二次（shutdown != null）→ 只 channel.close(promise) 或 completeWhenChannelIsClosed(promise)
                           ★ 永远走不到 writeClose，assert !closeSent 不可能触发
```

而 `writeClose` 的唯一入口 `VertxConnection.terminateClose:228-241` 还有自己独立的 `closeSent == null` 守卫（那是 `VertxConnection` 的 `ChannelPromise` 字段，与 `WebSocketConnectionImpl.java:45` 的 `boolean closeSent` 是两个东西）。**双重保险。**

**设计**（在策略层的 `closeOnNormalCompletion`）：

```java
private Mono<Void> closeOnNormalCompletion(VertxWebSocketSession session) {
    // 对端已先发 CLOSE 时 isClosed() 为真（WebSocketImplBase:346-348 → closed || closeStatusCode != null），
    // VertxWebSocketSession.close():98-100 直接返回 Mono.empty()，不会重复关。
    return session.close(CloseStatus.NORMAL);
}
```

**等待 vs 不等待的取舍（必须在文档里写明）**：

- 本方案选择**等待**（`.then(closeOnNormalCompletion(session))`）。上界由**已有**的 `vertx.websocket.closing-timeout`（默认 10 秒，`HttpServerOptions.java:177` `DEFAULT_WEBSOCKET_CLOSING_TIMEOUT = 10`）控制：`WebSocketConnectionImpl.writeClose:119-128` 在 CLOSE 帧写出后装配该定时器，到点 `finishClose()`；对端 CLOSE 先到则 `handleWsFrame:190-198` 立即 `finishClose()`。**正常情况是毫秒级**，10 秒只发生在对端无响应时。
- 代价：对端无响应的最坏情况下，这个 `Mono<Void>` 会悬挂 10 秒，期间持住 exchange/session 引用。它不阻塞 event loop（是悬挂的订阅，不是阻塞调用），但在高断连率下会增加存活对象。
- 备选（**不采纳**，理由记录在此）：完全对齐 Reactor Netty 的 fire-and-forget。它会让 `Mono<Void>` 在 CLOSE 帧上线**之前**就完成，用户无法从管道终止信号推断关闭是否成功；而 `closing-timeout` 已经提供了有界等待，没有必要放弃这个确定性。

### 4.3 心跳：`ping-interval` + `ping-timeout`（P0，修 R6）

**R6 的完整机理（已核实，比 Phase 1 的推测更精确）**：

```
NetServerImpl.java:474-479
  idleTimeout      = config.getIdleTimeout().toMillis()          // 我们的 60s
  readIdleTimeout  = 0   (未设置)
  writeIdleTimeout = 0   (未设置)
  pipeline.addLast("idle", new IdleStateHandler(readIdleTimeout, writeIdleTimeout, idleTimeout, MILLISECONDS))
                                                              ↑ 第三个参数 = Netty all-idle
VertxHandler.userEventTriggered → conn.handleIdle(IdleStateEvent)
WebSocketConnectionImpl.java:37  extends VertxConnection
                                 只覆写 handleShutdown(:92) / writeClose(:99)，★ 未覆写 handleIdle
VertxConnection.java:138-142     handleIdle → log.debug + chctx.close()
```

**由此得到两个结论：**

1. **R6 确认为真实缺陷**：合法但空闲的 WebSocket 会在 60 秒后被静默关闭，且只有一行 DEBUG 日志。
2. **all-idle 语义决定了「写也算活动」**：Netty 的 `IdleStateHandler` all-idle 在 read **和** write 都没有发生时才触发。所以**服务端周期 PING 单独就足以救活连接**，不依赖对端回 PONG。

**但由此推出一个反直觉的强制约束：**

> ⚠️ **一旦开启 PING，就必须同时开启 PONG 超时。**
> 因为我们的 PING 写操作会不断重置 all-idle 计时器，原本 60 秒的兜底上界**被我们自己拆掉了**。此时若对端进程消失（拔网线、容器被 kill，没有 FIN/RST），连接将一直存活到 TCP 重传超时（Linux 默认 `tcp_retries2=15`，约 13-30 分钟）才暴露。**开启心跳而不做 PONG 超时，是把一个 60 秒的问题换成一个十几分钟的问题。**

所以 `ping-timeout` 不是可选优化项，而是 `ping-interval` 的配套安全阀。设计：

| 属性 | 默认 | 语义 |
|---|---|---|
| `vertx.websocket.ping-interval` | `0`（关闭） | >0 时升级后 `vertx.setPeriodic(interval, id -> webSocket.writePing(Buffer.buffer()))`。**推荐值 25 秒**，与 socketio4j `pingInterval` 25000 对齐，且明显小于 `idleTimeout` 60 秒 |
| `vertx.websocket.ping-timeout` | `60` | 连续 `ping-timeout` 秒内未收到任何 PONG，则以 `CloseStatus.SESSION_NOT_RELIABLE`(1001) 关闭。**推荐值 60 秒**，与 socketio4j `pingTimeout` 60000 对齐 |

**启动期校验**（`VertxWebSocketProperties` 的 `@PostConstruct` 或自动配置里的 `Assert`）：`ping-interval > 0` 时必须 `ping-timeout > ping-interval`，否则**启动失败并打印明确原因**（不做静默纠正——静默改用户配置违反项目既有风格）。

**PONG 观测点**：`VertxWebSocketSession.java:75` 已经装了 `pongHandler`。但它在 `Flux.create` 内部，只有用户订阅了 `receive()` 才生效——**纯推送型 handler 不订阅 `receive()`，就收不到 PONG 时间戳**。因此心跳不能依赖 `receive()`，必须**独立**在 `webSocket.pongHandler(...)` 上记录 `lastPongAt`。

> ⚠️ 冲突点：`receive()` 里已经设置了 `pongHandler`，心跳再设一次会**互相覆盖**（Vert.x 是单 handler 语义，不是 addListener）。
> **处理**：把 PONG 时间戳的记录上移到 `VertxWebSocketSession` 构造器里统一安装一次，`receive()` 内部改为通过会话自身的中转把 PONG 事件推给 sink。这是本次改造对 `VertxWebSocketSession` 唯一有侵入性的调整，必须有回归测试覆盖「订阅 receive() 时仍能收到 PONG 消息」（对应 Phase 1 的 T4）。

**定时器实现**：`WebSocketHeartbeat` 持有 `Vertx`、`ServerWebSocket`、两个 `long` timer id。

- `start()` → `Mono<WebSocketHeartbeat>`：装 ping 定时器；若 `ping-timeout > 0` 再装一个检查定时器（周期 = `ping-timeout / 2`，避免边界抖动），检查 `now - lastPongAt > pingTimeout` 则关闭。
- `stop()` → `Mono<Void>`：`vertx.cancelTimer(id)`，幂等（不变量 I3）。
- `writePing` 返回的 `Future` 必须 `onFailure` 观察——关闭后 `writeFrame:299-303` 会返回失败 Future；若不观察，高频断连场景下会静默丢弃（不会崩，但丢失可诊断性）。

**为什么不照搬 `firstDataTimeout`**：见 1.3。核心是它属于 Engine.IO 协议约束（客户端升级后必须发握手包），原生 WebSocket 无此约束，照搬会误杀合法的纯推送通道。

### 4.4 会话注册表与生命周期监听（P1）

新增两个类型：

```java
/** 会话生命周期观察者。房间、指标、审计都通过它挂载，不需要各自去 hook 策略层。 */
public interface WebSocketSessionListener {
    default void onSessionOpened(WebSocketSession session) {}
    default void onSessionClosed(WebSocketSession session) {}
}

public interface WebSocketSessionRegistry {
    void register(WebSocketSession session);              // 由策略层调用，幂等
    void unregister(WebSocketSession session);            // 由策略层调用，幂等，广播 onSessionClosed
    Optional<WebSocketSession> get(String sessionId);
    Collection<WebSocketSession> sessions();              // 快照
    int size();
    void addListener(WebSocketSessionListener listener);
}
```

默认实现 `DefaultWebSocketSessionRegistry`：`ConcurrentHashMap<String, WebSocketSession>` + `CopyOnWriteArrayList<WebSocketSessionListener>`。

**会话 id 从哪来**：`VertxWebSocketSession` 构造时用 `UUID.randomUUID().toString()`（`:45`）。**这是个可改进点**——Spring 的 `AbstractWebSocketSession` 把 id 用于 `getLogPrefix()`（`:87-89`），而 Vert.x 侧的 `webSocket.textHandlerID()` 是天然的、集群内唯一的、与连接同生命周期的标识。但改 id 生成方式会影响 Phase 1 已交付的示例（回显前缀 `[sessionId]`）和用户可见行为，**收益不抵风险，本期不改**，仅在此记录。

**`unregister` 的双触发（不变量 I1）**：

```java
// 触发源 1：管道终止（正常完成 / 失败 / 取消）
Mono.usingWhen(Mono.just(session), s -> pipeline, s -> Mono.fromRunnable(() -> registry.unregister(s)));
// 触发源 2：对端关闭，但 handler 返回 Mono.never() 之类永不终止的管道
session.closeStatus().subscribe(status -> registry.unregister(session));
```

`closeStatus()` 是 `Sinks.one()`（`VertxWebSocketSession.java:36,106-108`），replay-once，多订阅者安全。`unregister` 幂等，双触发无副作用。

**为什么不在 `VertxWebSocketSession` 里直接调 registry**：会话是每连接新建的适配对象，让它反向依赖注册表会把生命周期管理散到两处。策略层是唯一同时看到「升级成功」和「管道终止」的地方（`WebSocketHandlerAdapter.java:106-108` 是单一入口），编排收敛在那里最省。

### 4.5 房间（P1，本次最大能力增量）

对齐 socketio4j 的 API 形状，但保持 WebFlux 语义：

```java
public interface WebSocketRooms extends WebSocketSessionListener {

    /** 同步：纯内存 map 操作，无 I/O。 */
    void join(WebSocketSession session, String room);
    void leave(WebSocketSession session, String room);

    /** 会话所属房间（快照）。 */
    Set<String> roomsOf(WebSocketSession session);

    /** 房间成员（本机快照）。跨节点场景下不含其他节点的成员——见 deliverLocally 的说明。 */
    Collection<WebSocketSession> members(String room);

    /**
     * 向房间广播。反应式：本机写入全部完成后完成。
     * 启用分布式扇出时，同时 fire-and-forget 地把信封发给其他节点；
     * ★ 返回的 Mono 不代表远端已投递（Zenoh publish 是 void，无确认）。
     */
    Mono<Void> sendText(String room, String text);
    Mono<Void> send(String room, Publisher<WebSocketMessage> messages);

    /** 以指定状态关闭房间内的全部会话。 */
    Mono<Void> closeRoom(String room, CloseStatus status);

    int roomCount();
    int roomSize(String room);

    /** 内部：投递来自其他节点的广播。不触发扇出，避免回环。 */
    void deliverLocally(String room, WebSocketEnvelope envelope);

    /** 内部：由 registry 在 onSessionClosed 时调用，实现「断连自动离房」。 */
    @Override
    default void onSessionClosed(WebSocketSession session) { removeAll(session); }

    void removeAll(WebSocketSession session);
}
```

**设计要点与理由：**

| 决策 | 理由 |
|---|---|
| `join` / `leave` 是 `void` 而不是 `Mono<Void>` | 纯内存操作，没有异步性。强行包 `Mono` 只会让用户在 handler 里写无意义的 `flatMap`。socketio4j 同样是 `void`。**只有 `send*` 是 `Mono<Void>`，因为那里才有 I/O、背压和跨节点** |
| 成员集合用 `ConcurrentHashMap<String, Set<WebSocketSession>>` + `ConcurrentHashMap.newKeySet()` | 广播是「遍历快照 + 并发写」，不需要 `CopyOnWriteArraySet` 的写时全量拷贝（房间成员变动频繁时那是 O(n) 每次）。遍历时的弱一致性可接受——正在离房的会话写失败会被 4.1 的失败记录机制捕获 |
| 反向索引 `Map<String sessionId, Set<String> rooms>` | `removeAll(session)` 必须 O(该会话的房间数) 而不是 O(全部房间)。socketio4j 的「断连自动离房」保证要靠它 |
| 自动离房由 `WebSocketSessionListener.onSessionClosed` 驱动，而不是在 `join()` 里订阅 `closeStatus()` | 订阅会随 join 次数累积（同一会话加入 5 个房间 = 5 个订阅），是泄漏源。走 registry 的单一广播点是 O(1) 挂载 |
| `sendText` 的本地部分 = `Flux.fromIterable(members(room)).flatMap(s -> s.send(Mono.just(s.textMessage(text)))).then()` | 并发写全部成员，天然复用 4.1 的流水线。**不做**串行 for 循环 |
| 不提供 `getRoomOperations(room)` 这种「房间句柄对象」 | socketio4j 需要它是因为 `BroadcastOperations` 同时实现了「组播 + 房间管理 + namespace 绑定」三件事。我们已经有 `WebSocketRooms` 这个单一门面，再多一层句柄对象只是增加概念数 |

**用户侧收益示例**（对比：今天用户要自己写 `CopyOnWriteArraySet` + 广播循环 + 断连清理 + 跨节点）：

```java
@Component
public class ChatHandler implements WebSocketHandler {

    private final WebSocketRooms rooms;

    public ChatHandler(WebSocketRooms rooms) { this.rooms = rooms; }

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        return session.receive().concatMap(msg -> dispatch(session, msg)).then();
    }

    private Mono<Void> dispatch(WebSocketSession session, WebSocketMessage msg) {
        JsonObject json = new JsonObject(msg.getPayloadAsText());     // 项目约定：统一用 JsonObject，不用 Jackson
        return switch (json.getString("op")) {
            case "join" -> { rooms.join(session, json.getString("room")); yield Mono.empty(); }
            case "leave" -> { rooms.leave(session, json.getString("room")); yield Mono.empty(); }
            case "msg" -> rooms.sendText(json.getString("room"), json.getString("text"));
            default -> Mono.empty();
        };
    }
}
```

10 行，且**自动获得跨节点广播**（启用集群时）。

### 4.6 跨节点扇出 SPI（P2）

**为什么必须有 SPI 而不是直接调 Zenoh**：`vertx-spring-boot-web-server` 不能依赖 `vertx-spring-boot-cluster-zenoh` 的运行时存在——`ZenohEventBusAutoConfiguration.java:14-29` 是 `@ConditionalOnBean(ZenohClusterManager.class)`，而 `ZenohClusterAutoConfiguration` 是 `@ConditionalOnProperty(prefix="vertx.cluster", name="enabled", havingValue="true")`（默认 `false`）。**绝大多数用户根本没开集群。**

```java
// vertx-spring-boot-web-server
public interface WebSocketFanOutTransport {

    /** 本节点 id，用于忽略自己发出的广播（Zenoh 回环会把它送回来）。 */
    String nodeId();

    /** 开始监听其他节点的广播。inbound 回调可能在任意线程上被调用——实现方负责调度。 */
    Future<Void> start(Handler<WebSocketEnvelope> inbound);

    Future<Void> stop();

    /** fire-and-forget。失败只记录，不传播（对齐 socketio4j「EventStore 不定义投递保证」的诚实表述）。 */
    void publish(WebSocketEnvelope envelope);
}
```

默认实现 `NoOpWebSocketFanOutTransport`：`nodeId()` 返回固定值，`publish` 空操作。单机用户拿到的就是它，**房间纯本地，零额外开销**。

**信封格式**（必须是 JSON 兼容类型——`ZenohMessageCodec.java:26` 用 `JsonObject` 序列化，任意 POJO 活不过一次跨节点）：

```json
{
  "v": 1,
  "node": "8f2c1a4e-...",        // ← origin nodeId，接收方据此自过滤
  "room": "room42",
  "op": "TEXT",                   // TEXT | BINARY | CLOSE
  "text": "hello",                // op=TEXT
  "b64": "aGVsbG8=",              // op=BINARY（base64，因为 JSON 装不下裸字节）
  "code": 1000,                   // op=CLOSE
  "reason": "bye"
}
```

> **为什么 origin node 放 payload 而不是 `DeliveryOptions` header**：header 确实能活过序列化（`ZenohMessageCodec` 会把 `DeliveryOptions.headers` 写进 `JsonObject`），但 payload 字段更简单——信封本来就是我们自己构造的 `JsonObject`，多一个键即可，不需要碰任何 Zenoh 侧代码，也不依赖「headers 一定被保留」这个我们不完全控制的契约。

**Zenoh 实现**（`ZenohWebSocketFanOutTransport`，放在 web-server，`@ConditionalOnClass(ZenohEventBusBridge.class)`）：

```java
public void publish(WebSocketEnvelope envelope) {
    bridge.publish(ADDRESS, envelope.toJson());     // ZenohEventBusBridge.java:43 → void
}

public Future<Void> start(Handler<WebSocketEnvelope> inbound) {
    MessageConsumer<JsonObject> consumer = bridge.consumer(ADDRESS);
    consumer.handler(message -> {
        WebSocketEnvelope envelope = WebSocketEnvelope.fromJson(message.body());
        if (nodeId.equals(envelope.node())) {
            return;                                  // ★ 自过滤：本地 publish 被注释掉了
        }                                              //   (ZenohEventBusBridge.java:47)，
        inbound.handle(envelope);                    //   本地投递靠 Zenoh 回环重注入
    });
    return Future.succeededFuture();
}
```

**四条必须守住的 Zenoh 侧约束（全部来自已核实事实）：**

| # | 约束 | 证据 | 应对 |
|---|---|---|---|
| Z1 | 扇出只能 fire-and-forget，无法给出「已投递」语义 | `ZenohEventBusBridge.publish` 返回 `void`（`:36,43`）；`ZenohEventBusPublisher.java:47-49` 失败只记日志 | `WebSocketRooms.send*` 的 Javadoc **必须**写明「返回的 `Mono<Void>` 只代表本机写入完成，不代表远端已投递」。这是 socketio4j 文档同款诚实表述，照抄它的做法 |
| Z2 | 发起节点会收到自己的广播 | `ZenohEventBusBridge.java:47` 本地 `eventBus.publish` **被注释掉**，本地投递靠 Zenoh pub/sub 回环重注入 | payload 里的 `node` 字段自过滤 |
| Z3 | 跨节点载荷必须 JSON 兼容 | `ZenohMessageCodec.java:26` 基于 `io.vertx.core.json.JsonObject` → `json.toBuffer().getBytes()` | 二进制帧走 base64（`b64` 字段）。**代价：体积 +33%**。文档需提示「大二进制广播不适合跨节点，建议改为广播资源 URL 或走对象存储」——这正是 socketio4j 官方最佳实践的第一条（avoid large objects，prefer IDs/deltas/streamed binary） |
| Z4 | `EventBus` 消费者回调的线程上下文是间接正确的 | Zenoh 接收跑在阻塞守护线程（`zenoh-pub-poll-*`，`ZenohEventBusSubscriber.java:101-124`），但回调是通过 `vertx.eventBus().publish/send`（`:114`）重入的，Vert.x 会在消费者注册的 context 上派发 | 我们**用 `bridge.consumer(...)` 而不是直接碰 Zenoh 订阅**，因此自动继承这个正确性。**不要**自己起线程或直连 `zenoh-jni`（`Session.put` / `Subscriber.recv()` 全是阻塞 `byte[]` API，会把 event loop 打爆） |

**装配**：

```java
@AutoConfiguration(after = {VertxWebServerAutoConfiguration.class, ZenohEventBusAutoConfiguration.class})
// 嵌套配置类 + @ConditionalOnClass，避免只装 web-server 时加载失败
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(ZenohEventBusBridge.class)
@ConditionalOnBean(ZenohEventBusBridge.class)
@ConditionalOnProperty(prefix = "vertx.websocket.rooms", name = "distributed", havingValue = "true", matchIfMissing = true)
static class ZenohFanOutConfiguration {
    @Bean @ConditionalOnMissingBean(WebSocketFanOutTransport.class)
    WebSocketFanOutTransport zenohWebSocketFanOutTransport(ZenohEventBusBridge bridge, Vertx vertx) { ... }
}
```

`pom.xml` 新增：

```xml
<dependency>
    <groupId>com.workoss.boot</groupId>
    <artifactId>vertx-spring-boot-cluster-zenoh</artifactId>
    <version>${project.version}</version>
    <optional>true</optional>
</dependency>
```

> **必须验证的前置条件**：加这个依赖前要先跑 `mvn -o dependency:tree` 确认 `vertx-spring-boot-cluster-zenoh` 不（直接或间接）依赖 `vertx-spring-boot-web-server`。已核实当前 pom 里它只依赖 `vertx-spring-boot-autoconfigure` / `vertx-core` / `zenoh-jni` / `slf4j-api` / `spring-boot-autoconfigure` / `spring-boot-starter-actuator`(optional)，**无环**。但这是一条会被后续改动破坏的隐式约束，应写进模块 README 或在本文件留档（本文即留档）。

### 4.7 异常钩子（P0，扩展已有 R11 分类）

对齐 socketio4j 的 `ExceptionListener`，但不新增分层——`VertxRequestUpgradeStrategy` 里已经有分级日志（`isPeerGone`），只需把「真错误」这一支开放出去：

```java
public interface WebSocketFailureHandler {
    /** handler 管道在升级之后抛出的、且不属于「对端已断开」的异常。 */
    void onHandlerFailure(WebSocketSession session, Throwable error);
}
```

- `@ConditionalOnMissingBean`，默认无实现 → 行为与 Phase 1 完全一致（只打 ERROR 日志）。
- 用户提供一个 Bean 就能接 Sentry / 告警。
- **`onHandlerFailure` 自身抛异常必须被吞掉并记 WARN**——它跑在 `onErrorResume` 里，再抛出去就违反不变量 I2。

**不新增 `onDisconnectException`**：socketio4j 需要它是因为它的断连清理是用户回调（`@OnDisconnect`）里跑的，那里会抛异常。我们的断连清理是框架内的（registry / rooms），用户没有回调点，因此没有这个失败面。

### 4.8 可观测性（P2，optional）

新增 `<optional>true</optional>` 的 `micrometer-core`（web-server 当前没有），`@ConditionalOnClass(MeterRegistry.class)` 守卫。全部指标从已有的 registry + listener 派生，**对用户零 API 侵入**：

| 指标 | 类型 | 标签 |
|---|---|---|
| `vertx.websocket.sessions.active` | Gauge | `path` |
| `vertx.websocket.sessions.opened` / `.closed` | Counter | `path`, `closeCode` |
| `vertx.websocket.messages` | Counter | `path`, `direction`(in/out), `type`(text/binary/ping/pong) |
| `vertx.websocket.bytes` | DistributionSummary | `path`, `direction` |
| `vertx.websocket.rooms.size` / `.members` | Gauge | — |
| `vertx.websocket.fanout.published` / `.received` / `.dropped` | Counter | — |

> `dropped` 计数扇出信封因 origin 自过滤 / 反序列化失败 / 房间不存在而被丢弃的次数——这是分布式场景下唯一能发现「消息没到」的信号，必须有。

### 4.9 入站积压上界（P2，内存安全）

**问题**：`VertxWebSocketSession.java:56-80` 的 `Flux.create(..., OverflowStrategy.BUFFER)` 无上界。慢消费者 + 快生产者 → 堆无界增长，且每条排队消息持有一份 4.1 里保留的字节拷贝。

**设计**：`vertx.websocket.inbound-buffer-size`（默认 `0` = 保持 Phase 1 行为，无上界；>0 时启用）。启用时改为：

```java
Flux.create(sink -> {...}, FluxSink.OverflowStrategy.BUFFER)
     .onBackpressureBuffer(maxSize, dropped -> {}, BufferOverflowStrategy.ERROR)
```

并在 `receive()` 的错误映射里把溢出翻译成 `CloseStatus` **1013（Try Again Later）** 后关闭会话——1013 是 RFC 6455 §7.4.1 里唯一语义正确的「服务端过载，请稍后重连」码，客户端据此可以重连；用 1009（Message Too Big）会误导客户端以为是单条消息太大。

**为什么默认关闭**：改变默认行为会让「慢消费者」从「内存增长」变成「连接被关」，这是行为变更，不是 bug 修复。按「影响最小」原则，作为可选加固项提供，并在文档里推荐生产环境开启。

### 4.10 优雅关闭（P2，R7）

**已核实的关键陷阱**：`WebSocketConnectionImpl.handleShutdown:92-96`

```java
protected void handleShutdown(Duration timeout, ChannelPromise promise) {
    if (!webSocket.handleShutdown()) {          // WebSocketImplBase:588-597
        super.handleShutdown(timeout, promise);  // 没装 shutdownHandler → 走这里，正常关连接
    }
}
```

而 `WebSocketImplBase.handleShutdown():588-597` 是：

```java
boolean handleShutdown() {
    Handler<Void> handler = this.shutdownHandler;
    if (handler != null) context.emit(handler);
    return handler != null;      // ★ 装了 handler 就返回 true
}
```

> ⚠️ **一旦安装 `shutdownHandler`，Vert.x 就不再负责关闭连接——关闭责任完全转移到我们手上。** 如果 handler 里没有主动调 `webSocket.shutdown(...)`，连接会一直挂着直到 shutdown 超时。这是很容易踩的坑，任何实现都必须成对出现：装 handler + 在 handler 内发起关闭。

**设计**：`VertxWebServer.stop()` 时（或 Spring 的 `SmartLifecycle.stop(Runnable)`），对 registry 里的全部会话：
1. 装 `shutdownHandler`，在 handler 内 `webSocket.shutdown(gracePeriod, 1001, "server shutting down")`（`WebSocketBase.java:385`，`Duration` 版本）。
2. `vertx.websocket.graceful-shutdown-timeout`（默认 10 秒）作为 `shutdown` 的 timeout 参数——超时后 Vert.x 自己 `channel.close()`。
3. 1001（Going Away）是 RFC 6455 为「端点离开」规定的正确码；1000 会让客户端以为是自己正常关的，不会重连。

**优先级说明**：这是 P2 而不是 P0，因为当前行为（不装 handler → Vert.x 直接 `chctx.close()`）虽然不发 CLOSE 帧，但**不会泄漏连接**，只是不够礼貌。

### 4.11 用户 API 分档（在 Phase 1 三档基础上补 Tier 4）

| 档 | 形态 | 状态 |
|---|---|---|
| Tier 1 | 标准 WebFlux：`WebSocketHandler` Bean + `SimpleUrlHandlerMapping`（order = -1） | ✅ Phase 1 已交付 |
| Tier 2 | `@WebSocketEndpoint(path=..., subProtocols=...)` + `WebSocketEndpointRegistrar`（消除 `HandlerMapping` 样板，路径冲突启动即失败） | ⬜ Phase 1 文档 §5.6 已设计未实现，**本方案 P1 一并交付**（房间能力落地后，Tier 2 的价值才完整） |
| Tier 3 | `RouterFunction` | ❌ 明确不支持（返回 `ServerResponse`，承载不了 101），文档需继续警示 |
| **Tier 4** | **注入 `WebSocketRooms` / `WebSocketSessionRegistry`**（本方案新增） | ⬜ P1 |

### 4.12 配置属性汇总

**新增（`vertx.websocket.*`）**

| 属性 | 默认 | 期 | 说明 |
|---|---|---|---|
| `ping-interval` | `0`（秒，关闭） | P0 | >0 时服务端周期发 PING。推荐 25（对齐 socketio4j）。修 R6 |
| `ping-timeout` | `60`（秒） | P0 | 无 PONG 超过该时长则以 1001 关闭。`ping-interval>0` 时**必须 > `ping-interval`**，否则启动失败 |
| `write-queue-max-size` | `0`（沿用 Vert.x 默认） | P0 | >0 时调 `setWriteQueueMaxSize` |
| `session-attribute-names` | 空 | P0 | 透传给 `HandshakeWebSocketService.setSessionAttributePredicate`，把 `WebSession` 属性带进 WS 会话 |
| `rooms.enabled` | `true` | P1 | 关闭后不注册 `WebSocketRooms` Bean（省掉两张 map） |
| `rooms.distributed` | `true` | P2 | 有 Zenoh 桥时是否启用跨节点扇出 |
| `inbound-buffer-size` | `0`（无上界） | P2 | >0 时启用入站积压上界，溢出以 1013 关闭 |
| `graceful-shutdown-timeout` | `10`（秒） | P2 | 优雅关闭等待对端 CLOSE 的上界 |
| `max-sessions` | `0`（不限） | P2 可选 | 超限时**握手前**返回 503。借 `firstDataTimeout` 的防滥用意图，但用 WebSocket 语境下正确的手段 |

**已有（Phase 1 交付，不改）**：`enabled`、`max-frame-size`(65536)、`max-message-size`(262144)、`sub-protocols`、`per-message-compression`、`per-frame-compression`、`compression-level`(6)、`allow-server-no-context`、`preferred-client-no-context`、`closing-timeout`(10)。

**继续复用 Spring Boot 标准 `server.*`**（项目既有约束）：`server.port`、`server.address`、`server.ssl.*`、`server.compression.*`、`server.http2.*`。**同端口共用是构造性保证**，不新增任何端口属性。

---

## 5. 关键技术风险

| ID | 风险 | 证据 | 处置 | 优先级 |
|---|---|---|---|---|
| **N1** | 流水线化后 `writeQueueFull()` 在对端断开时**抛 `IllegalStateException`**，被当真错误打成 ERROR 日志（回归 R11 的成果） | `WebSocketImplBase.java:141-146` → `checkClosed():339-343` | `queueFull()` 包 try/catch 返回 `false`，失败由 `write()` 返回的失败 Future 承载（`:299-303`） | **P0** |
| **N2** | 不等待的写 Future 失败被静默丢弃 | `writeFrame:299-303` 返回 `failedFuture`；Reactor 不观察未订阅的 Future | 每条都 `onFailure(record)`；循环头 + 收尾重抛 | **P0** |
| **N3** | 开启 PING 却不做 PONG 超时 → 60 秒上界变成 TCP 重传超时（13-30 分钟） | `NetServerImpl.java:478` all-idle 语义：写操作也重置计时器 | `ping-timeout` 强制配套；启动期校验 `ping-timeout > ping-interval`，不满足则**启动失败** | **P0** |
| **N4** | 心跳与 `receive()` **争抢 `pongHandler`**（Vert.x 单 handler 语义，后设覆盖先设） | `VertxWebSocketSession.java:75` 已设 `pongHandler` | PONG 时间戳记录上移到构造器统一安装一次，`receive()` 通过会话内部中转推给 sink。**必须有回归测试**（覆盖 Phase 1 的 T4） | **P0** |
| **N5** | 装了 `shutdownHandler` 后 Vert.x **不再关闭连接** | `WebSocketConnectionImpl.java:92-96` + `WebSocketImplBase.java:588-597`（`return handler != null`） | 装 handler 与在 handler 内 `webSocket.shutdown(...)` **必须成对**；Javadoc 明示 | P2 |
| **N6** | R12 的 CLOSE 在对端无响应时让管道悬挂至 `closing-timeout`（默认 10 s），期间持住 exchange/session | `WebSocketConnectionImpl.writeClose:119-128` 的定时器；`HttpServerOptions.java:177` 默认 10 | 接受（有界、可调、不阻塞 event loop）。备选 fire-and-forget 已在 4.2 记录并说明为何不采纳 | P0 |
| **N7** | 跨节点广播**无投递保证**，用户误以为 `Mono<Void>` 完成 = 全集群送达 | `ZenohEventBusBridge.publish` 返回 `void`（`:36,43`）；`ZenohEventBusPublisher.java:47-49` 失败只记日志 | Javadoc 明示；`vertx.websocket.fanout.dropped` 计数；文档给出「需要可靠投递请改用 HTTP/消息队列」的指引 | P2 |
| **N8** | 二进制帧跨节点 base64 后**体积 +33%** | `ZenohMessageCodec.java:26` 纯 JSON，装不下裸字节 | 文档提示大二进制不适合跨节点广播，改广播 URL（对齐 socketio4j 官方最佳实践第一条） | P2 |
| **N9** | web-server → cluster-zenoh 的 optional 依赖**将来可能成环** | 当前已核实无环（cluster-zenoh 只依赖 autoconfigure / vertx-core / zenoh-jni / slf4j / spring-boot-autoconfigure / actuator） | 本文档 4.6 留档；加依赖前跑 `mvn -o dependency:tree` | P2 |
| **N10** | 房间成员集合的弱一致遍历导致「刚离房的会话被广播到」 | `ConcurrentHashMap.newKeySet()` 的迭代器是弱一致的 | 可接受：写入会失败并被 N2 的机制捕获，最终一致。**不要**为此加锁 | P1 |
| **R12**（承 Phase 1） | handler 正常完成不发 CLOSE 帧 | `HttpServerOperations.java:1361-1367`（Reactor Netty 有）vs `HandshakeWebSocketService.java:231-234`（Spring 无）vs 我们无 | **疑点已解除**，见 4.2 的 `VertxConnection.shutdown` 双重守卫分析 | **P0** |
| **R6**（承 Phase 1） | 空闲 60 秒的 WebSocket 被静默关闭 | `VertxHttpServerProperties.java:23` + `NetServerImpl.java:478` + `VertxConnection.handleIdle:138-142` + `WebSocketConnectionImpl.java:37` 未覆写 `handleIdle` | 见 4.3。**机理比 Phase 1 的推测更精确**：是 all-idle，写操作即可重置 | **P0** |
| **R4**（承 Phase 1） | HTTP/2 下 WS 握手失败并打 WARN | `HttpUtils.java:941-944` `canUpgradeToWebSocket()` 要求 HTTP/1.1 | 维持不支持（RFC 8441 不在范围）。启动期若 `server.http2.enabled=true` 则打一条明确的 WARN，说明 WS 端点在 HTTP/2 上不可用 | P2 |
| **R8**（承 Phase 1） | 无会话注册表、无跨节点扇出 | — | 即本方案 4.4 / 4.6 | P1 / P2 |

---

## 6. 测试与验收

沿用 Phase 1 的测试基线（模块内 23 个测试全绿）。**测试环境限制不变**：`vertx-spring-boot-web-server` 的 test classpath 只有 `slf4j-api-2.0.7`（无日志后端）、**没有 reactor-test**（无 `StepVerifier`）。因此继续用 `Mono.block(Duration)` + AssertJ，**不为此改 pom**（符合「影响最小」）。

| ID | 用例 | 覆盖 | 期 |
|---|---|---|---|
| **T16** | 出站流水线：mock `ServerWebSocket`，`writeQueueFull()` 返回 false，发 1000 条消息，断言 `writeTextMessage` 被调用 1000 次且**没有**创建 1000 个 `CompletionStage`（用计数 spy 或验证 `Mono.fromCompletionStage` 未触发） | 4.1 | P0 |
| **T17** | `writeQueueFull()` 抛 `IllegalStateException("WebSocket is closed")` 时 `send()` **不抛**，且失败来自 `writeFrame` 的失败 Future 而非同步异常 | N1 | P0 |
| **T18** | 队列不满但写失败（关闭后写）→ `send()` 以该失败终止，失败未被静默丢弃 | N2 | P0 |
| **T19** | 队列满时 `send()` 等待写完成才继续（背压生效） | 4.1 | P0 |
| **T20** | handler 正常完成 → 发出 CLOSE(1000)；对端已先发 CLOSE → **不重复发**（`isClosed()` 短路） | R12 | P0 |
| **T21** | **二次关闭不触发 `assert !closeSent`**：在 `-ea` 下连续调两次 `session.close(...)`，断言无 `AssertionError` | R12 / 4.2 | P0 |
| **T22** | 心跳：`ping-interval=1s` 时 `writePing` 被周期调用；管道终止后定时器被取消（`cancelTimer` 被调用，不变量 I3） | 4.3 | P0 |
| **T23** | 心跳与 `receive()` **共存**：订阅 `receive()` 的同时 PONG 时间戳仍被更新，且 PONG 消息仍出现在 `receive()` 流里 | **N4（回归 Phase 1 的 T4）** | P0 |
| **T24** | `ping-timeout` 到期无 PONG → 以 1001 关闭 | 4.3 / N3 | P0 |
| **T25** | 启动期校验：`ping-interval=30, ping-timeout=10` → 上下文启动失败且消息指明原因 | N3 | P0 |
| **T26** | `WebSocketFailureHandler` 被调用；其自身抛异常时**不外泄**（不变量 I2） | 4.7 | P0 |
| **T27** | `session-attribute-names` 配置后 `WebSession` 属性出现在 `session.getAttributes()` | 4.12 | P0 |
| **T28** | 注册表：`register`/`unregister` 幂等；`unregister` 广播 `onSessionClosed` 恰好一次（双触发下） | 4.4 / I1 | P1 |
| **T29** | 房间：join/leave/members/roomsOf；断连自动离房（走 `onSessionClosed`，且**不随 join 次数累积订阅**） | 4.5 | P1 |
| **T30** | 房间广播：`sendText` 并发写全部成员；成员写入失败不影响其他成员 | 4.5 / N10 | P1 |
| **T31** | Tier 2 `@WebSocketEndpoint`：路径冲突时启动失败并打印冲突双方；`subProtocols` 回灌到 `HttpServerOptions` | 4.11 | P1 |
| **T32** | 扇出信封：`TEXT`/`BINARY`(base64)/`CLOSE` 三种 op 的 JSON 往返；**origin 自过滤**（同 nodeId 的信封不投递） | 4.6 / Z2 | P2 |
| **T33** | 无 Zenoh 桥时装配 `NoOpWebSocketFanOutTransport`，房间纯本地可用（上下文能起来） | 4.6 | P2 |
| **T34** | 入站积压上界：`inbound-buffer-size=2`，慢消费者 + 3 条消息 → 以 1013 关闭 | 4.9 | P2 |
| **T35** | 优雅关闭：装 `shutdownHandler` 后**确实发起了** `webSocket.shutdown(...)`（N5 的成对性） | 4.10 / N5 | P2 |
| **T36** | `max-sessions` 超限 → **握手前**返回 503（HTTP 响应可正常渲染，不是升级后失败） | 4.12 | P2 |
| **T37** | **端到端集成**（示例应用，两实例 + `vertx.cluster.enabled=true`）：实例 A 的客户端 join room，实例 B 的客户端 join 同一 room，A 发消息 B 收到 | 4.5 + 4.6 | P2 |
| **T38** | **性能基线**：单会话 10000 条 64 B 消息，改造前 vs 改造后墙钟时间对比，记录进文档 | 4.1 | P0 |

**每个新增修复都必须做负向对照**（Phase 1 已验证过这个方法有效）：临时把修复回退，确认对应测试**恰好**失败、既有测试仍绿，再恢复。这能证明测试是真的在守住这个缺陷，而不是碰巧通过。

---

## 7. 实施计划

### P0 · 语义与性能补齐（约 1.5 人日）

1. `VertxWebSocketSession.send()` 流水线化（4.1），含 N1/N2 两处防护 → T16-T19、T38
2. `VertxRequestUpgradeStrategy` 补 R12：正常完成后 `session.close(CloseStatus.NORMAL)`（4.2） → T20、T21
3. 新增 `vertx.websocket.ping-interval` / `ping-timeout` / `write-queue-max-size` / `session-attribute-names` 四个属性 + 启动期校验（4.3、4.12） → T25、T27
4. 新增 `WebSocketHeartbeat`；**`pongHandler` 上移到构造器统一安装**（N4，本次唯一有侵入性的会话改动） → T22-T24
5. 新增 `WebSocketFailureHandler` SPI + `@ConditionalOnMissingBean` 装配（4.7） → T26
6. `VertxWebFluxConfigurer` 接 `setSessionAttributePredicate`（4.12）
7. 更新 `docs/vertx-websocket-support-design.md`：R6 / R12 从「Phase 2 待定」改为「已核实、已实现」，并回填 `VertxConnection.shutdown` 的守卫分析与 `NetServerImpl:478` 的 all-idle 机理

### P1 · 会话注册表 + 房间 + Tier 2 注解（约 1.5 人日）

8. `WebSocketSessionListener` / `WebSocketSessionRegistry` / `DefaultWebSocketSessionRegistry`（4.4），双触发 unregister（I1） → T28
9. `WebSocketRooms` / `DefaultWebSocketRooms`（4.5），含反向索引与断连自动离房 → T29、T30
10. 策略层编排改造为 `Mono.usingWhen` 双层（3.2），守住 I1/I2/I3
11. Tier 2：`@WebSocketEndpoint` + `WebSocketEndpointRegistrar`（Phase 1 文档 §5.6 已设计） → T31
12. 示例应用补 `/ws/chat` 房间端点 + 更新静态测试页（加入房间、房间广播）

### P2 · 跨节点扇出 + 可观测性 + 加固（约 2 人日）

13. `WebSocketEnvelope` + `WebSocketFanOutTransport` SPI + `NoOpWebSocketFanOutTransport`（4.6） → T32、T33
14. `ZenohWebSocketFanOutTransport` + 嵌套条件装配 + pom optional 依赖（**先跑 `dependency:tree` 验环**，N9）
15. Micrometer 指标（4.8，optional 依赖）
16. 入站积压上界（4.9） → T34
17. 优雅关闭（4.10，注意 N5 的成对性） → T35
18. `max-sessions` 握手前限流（4.12，可选） → T36
19. R4：`server.http2.enabled=true` 时的启动 WARN
20. 端到端两实例集群验证 → T37
21. 新增 `docs/vertx-websocket-usage.md`：三档 API 用法、房间示例、Ack 的应用层写法（15 行示例，说明为什么不在传输层做）、跨节点投递保证的诚实表述、大二进制不建议跨节点广播

### 明确不做

- 引入 socketio4j / netty-socketio 依赖（C1-C4）
- Socket.IO / Engine.IO 线协议、HTTP 长轮询回退
- Namespace（路径路由已提供同等隔离）
- 传输层 Ack（原生 WS 无 ack 帧，做了就破坏 `new WebSocket(...)` 互通）
- `AuthorizationListener`（`WebFilter` 严格更优，T11 已验证）
- JSR-356 `@ServerEndpoint`、STOMP 消息代理、RFC 8441（承 Phase 1 非目标）
- 改动 `vertx-spring-boot-cluster-zenoh`（本方案对它零改动）
- 改动会话 id 生成方式（4.4 已记录收益不抵风险）

---

## 8. 文件改动清单

### 新增（`vertx-spring-boot-web-server/src/main/java/com/workoss/boot/vertx/web/server/websocket/`）

| 文件 | 期 | 职责 |
|---|---|---|
| `WebSocketHeartbeat.java` | P0 | PING/PONG 定时器 + 超时关闭 |
| `WebSocketFailureHandler.java` | P0 | 升级后真错误的可插拔钩子 |
| `WebSocketSessionListener.java` | P1 | 会话生命周期观察者 |
| `WebSocketSessionRegistry.java` | P1 | 会话注册表 SPI |
| `DefaultWebSocketSessionRegistry.java` | P1 | 默认实现 |
| `WebSocketRooms.java` | P1 | 房间 SPI |
| `DefaultWebSocketRooms.java` | P1 | 本地房间实现 |
| `WebSocketEndpoint.java` | P1 | Tier 2 注解 |
| `WebSocketEndpointRegistrar.java` | P1 | Tier 2 注册器 |
| `WebSocketEnvelope.java` | P2 | 跨节点信封（JSON 兼容） |
| `WebSocketFanOutTransport.java` | P2 | 扇出 SPI |
| `NoOpWebSocketFanOutTransport.java` | P2 | 单机默认实现 |
| `ZenohWebSocketFanOutTransport.java` | P2 | Zenoh 实现（web-server 内，optional 依赖） |
| `WebSocketMetrics.java` | P2 | Micrometer 指标（optional） |

### 修改

| 文件 | 期 | 改动 |
|---|---|---|
| `websocket/VertxWebSocketSession.java` | P0 | `send()` 流水线化；`pongHandler` 上移到构造器；新增 `writeFailure` 记录 |
| `websocket/VertxRequestUpgradeStrategy.java` | P0/P1/P2 | R12 关闭；心跳编排；`usingWhen` 双层生命周期；`WebSocketFailureHandler` 钩子；`max-sessions` 校验 |
| `websocket/VertxWebSocketProperties.java` | P0/P2 | 新增 9 个属性 + 启动期校验 |
| `websocket/VertxWebSocketAutoConfiguration.java` | P0/P1/P2 | 新增 Bean 装配 + 嵌套 `ZenohFanOutConfiguration` |
| `websocket/VertxWebFluxConfigurer.java` | P0 | 接 `setSessionAttributePredicate` |
| `VertxWebServer.java` | P2 | 优雅关闭时驱动 registry |
| `vertx-spring-boot-web-server/pom.xml` | P2 | `vertx-spring-boot-cluster-zenoh` + `micrometer-core`，均 `<optional>true</optional>` |
| `vertx-spring-boot-web-server/src/main/resources/META-INF/spring/...AutoConfiguration.imports` | P1/P2 | 若拆分出新自动配置类则登记 |
| `docs/vertx-websocket-support-design.md` | P0 | 回填 R6 / R12 的核实结论 |
| `example/vertx-server-spring-boot-example/**` | P1 | `/ws/chat` 房间端点 + 静态测试页更新 |

### 不改动（明确）

| 目标 | 理由 |
|---|---|
| `vertx-spring-boot-cluster-zenoh/**` | 本方案对它零改动。扇出通过**已有**的 `ZenohEventBusBridge.publish(String, Object, DeliveryOptions)` 实现 |
| `VertxHttpHandlerAdapter.java` | R1 的 `pause()` 防护与 `releaseIfNotUpgraded` 在 Phase 1 已交付，本方案不涉及 |
| `VertxServerHttpResponse.java` | R10 的 `isCommitted()` / `headWritten()` 已交付 |
| `VertxServerHttpRequest.java` | 无涉及 |
| `VertxReactiveWebServerFactory.java` | WebSocket 参数已在 Phase 1 接入，本方案新增属性不落在 `HttpServerOptions` 上（心跳是应用层定时器，不是连接层参数） |

---

## 附录 A · 已核实的 API 事实清单

### A.1 Vert.x 5.2.0（`vertx-core-5.2.0-sources.jar`）

| 事实 | 位置 |
|---|---|
| `WebSocketBase extends ReadStream<Buffer>, WriteStream<Buffer>` | `io/vertx/core/http/WebSocketBase.java:44` |
| `setWriteQueueMaxSize(int)` / `drainHandler(Handler<Void>)` | `WebSocketBase.java:65` / `:68` |
| `close(short, String)` → `shutdown(0L, SECONDS, code, reason)` | `WebSocketBase.java:310-311` |
| `shutdown(Duration, short, String)` 抽象方法 | `WebSocketBase.java:385` |
| `writeQueueFull()` **调 `checkClosed()`，已关闭时抛 `IllegalStateException`** | `WebSocketImplBase.java:141-146` + `:339-343` |
| `writeFrame` 已关闭时返回 `context.failedFuture("WebSocket is closed")` | `WebSocketImplBase.java:299-303` |
| `writeTextMessage` / `writeBinaryMessage` / `writePing` / `writePong` 全部收敛到 `writeFrame` | `WebSocketImplBase.java:239-246` / `:234-235` / `:255-259` / `:263-267` |
| `isClosed()` = `closed \|\| closeStatusCode != null` | `WebSocketImplBase.java:346-348` |
| `shutdown(Duration, short, String)` **无 `isClosed()` 守卫**，直接构造 CLOSE 帧 | `WebSocketImplBase.java:149-153` |
| `handleShutdown()` 触发 `shutdownHandler` 并 `return handler != null` | `WebSocketImplBase.java:588-597` |
| `handleWriteQueueDrained` 用 `context.dispatch` 回调 | `WebSocketImplBase.java:599-606` |
| `WebSocketConnectionImpl extends VertxConnection`，只覆写 `handleShutdown` / `writeClose`，**未覆写 `handleIdle`** | `WebSocketConnectionImpl.java:37`、`:92-96`、`:99-101` |
| `writeClose` 有 `assert !closeSent`；`closingTimeoutMS > 0` 时装配关闭定时器 | `WebSocketConnectionImpl.java:99-101`、`:119-128` |
| 收到对端 CLOSE 且已 `closeSent` → 服务端立即 `finishClose()` | `WebSocketConnectionImpl.java:190-198` |
| `shutdown(Duration)` 是 `public final`，切到 event loop 后调私有重载 | `VertxConnection.java:184-198` |
| **二次 `shutdown` 走 `shutdown != null` 分支，永不到达 `writeClose`** | `VertxConnection.java:200-218` |
| `terminateClose` 有独立的 `closeSent == null` 守卫，是 `writeClose` 的唯一入口 | `VertxConnection.java:228-241` |
| `handleIdle` → `log.debug` + `chctx.close()` | `VertxConnection.java:138-142` |
| **`idleTimeout` 装进 `IdleStateHandler` 第三个参数（all-idle）**，read/write idle 默认 0 | `io/vertx/core/net/impl/tcp/NetServerImpl.java:474-479` |
| `DEFAULT_MAX_WEBSOCKET_FRAME_SIZE = 65536` | `HttpServerOptions.java:66` |
| `DEFAULT_MAX_WEBSOCKET_MESSAGE_SIZE = 65536 * 4` | `HttpServerOptions.java:72` |
| `DEFAULT_WEBSOCKET_COMPRESSION_LEVEL = 6` | `HttpServerOptions.java:162` |
| `DEFAULT_WEBSOCKET_CLOSING_TIMEOUT = 10` | `HttpServerOptions.java:177` |
| `canUpgradeToWebSocket()` 要求 HTTP/1.1（R4） | `io/vertx/core/http/impl/HttpUtils.java:941-944` |

### A.2 Spring Framework 6.2.19（`spring-webflux-6.2.19-sources.jar`）

| 事实 | 位置 |
|---|---|
| **每会话一个 `ConcurrentHashMap` 属性表**（= socketio4j `Store` 的等价物，已免费拥有） | `org/springframework/web/reactive/socket/adapter/AbstractWebSocketSession.java:58` |
| 从 `HandshakeInfo` 拷入初始属性，过滤 null 键值 | `AbstractWebSocketSession.java:78-80` |
| `getAttributes()` 暴露该 map | `AbstractWebSocketSession.java:112-113` |
| `getLogPrefix()` 用会话 id 构造日志前缀 | `AbstractWebSocketSession.java:87-89`、`:116-118` |
| **`setSessionAttributePredicate(Predicate<String>)` 官方钩子** | `org/springframework/web/reactive/socket/server/support/HandshakeWebSocketService.java:153` |
| 默认 `EMPTY_ATTRIBUTES` = `Mono.just(Collections.emptyMap())`（不可变） | `HandshakeWebSocketService.java:77`、`:259-267` |
| **`handleRequest` 原样返回 `upgrade(...)`，不做任何关闭动作**（R12 的 Spring 侧证据） | `HandshakeWebSocketService.java:231-234` |
| 握手校验：GET/CONNECT、`Upgrade: WebSocket`、`Connection: Upgrade`、`Sec-WebSocket-Key` | `HandshakeWebSocketService.java:208-227` |
| 子协议协商取 `handler.getSubProtocols()` 与请求头的交集 | `HandshakeWebSocketService.java:245-256` |
| 单一入口：`webSocketService.handleRequest(...).then(Mono.empty())` | `org/springframework/web/reactive/socket/server/support/WebSocketHandlerAdapter.java:106-108` |
| Reactor Netty 策略第一行是 `response.setComplete()`（R2 对照） | `org/springframework/web/reactive/socket/server/upgrade/ReactorNettyRequestUpgradeStrategy.java:169` |

### A.3 Reactor Netty（R12 的对齐目标）

| 事实 | 位置 |
|---|---|
| handler 发布者 `onComplete()` → `sendCloseNow(new CloseWebSocketFrame(NORMAL_CLOSURE), listener)` | `reactor/netty/http/server/HttpServerOperations.java:1361-1367` |
| `onError(t)` → `ops.onError(t)`（不发 CLOSE） | 同上 |

### A.4 socketio4j / netty-socketio 4.0.1（`https://www.socketio4j.org`，48 页）

| 事实 | 出处 |
|---|---|
| 自有端口 + 自有服务端：`config.setPort(9092)` → `new SocketIOServer(config)` → `server.start()` / `startAsync()` / `stop()` | `/server-api` |
| `setContext("/socket.io")`、`setTransports(Transport.POLLING, Transport.WEBSOCKET)`、polling→WS `upgradeTimeout` 10000 ms | `/server-api` |
| `bossThreads` / `workerThreads`（默认 0 → CPU×2）、`transportType` AUTO = IO_URING→EPOLL→KQUEUE→NIO | `/server-api` |
| `pingInterval` 25000 / `pingTimeout` 60000（0 关闭）/ `firstDataTimeout` 5000（"prevents silent channel attacks"） | `/server-api` |
| `maxHttpContentLength` 64 KB / `maxFramePayloadLength` 64 KB；`maxInitialLineLength` / `maxHeaderSize` / `maxChunkSize` | `/server-api` |
| `httpCompression` true / `websocketCompression` true（permessage-deflate） | `/server-api` |
| `ackMode` AUTO / AUTO_SUCCESS_ONLY（默认）/ MANUAL；「Acks are sent at most once and only if requested; Manual ack always suppresses auto-ack」 | `/server-api` |
| `randomSession` false / `needClientAuth` false / `SocketSslConfig`（keyStore、trustStore） | `/server-api` |
| `authorizationListener`（`data -> AuthorizationResult.SUCCESS`，`data.getSingleUrlParam("token")`） | `/server-api` |
| `exceptionListener.onEventException(Exception, List<Object>, SocketIOClient)` / `onDisconnectException(Exception, SocketIOClient)` | `/server-api` |
| 房间：`client.joinRoom/leaveRoom`、断连自动离房、`server.getRoomOperations(room)` → `BroadcastOperations.sendEvent` / `getClients()` | `/rooms` |
| Namespace：`server.addNamespace("/chat")`，默认 `""`，单物理连接、各自独立 listener/生命周期/鉴权/广播 | `/namespaces` |
| `Store`：`client.getStore().set/get/has/del`；「A Store instance lives for exactly one client session and is destroyed when the session ends」；backend Memory / Hazelcast / Redis | `/store` |
| `EventStore`：`publish`/`subscribe`/`unsubscribe`/`shutdown`；`EventStoreMode` MULTI_CHANNEL\|SINGLE_CHANNEL；`EventStoreType` STREAM\|PUBSUB\|LOCAL；`PublishMode` RELIABLE\|UNRELIABLE；`nodeId` 自过滤；**「The EventStore interface does not define delivery guarantees」** | `/event-store` |
| `StoreFactory`：`createStore(sessionId)` / `createMap(name)` / `eventStore()` / `init` / `shutdown`；存储与事件传播解耦，backend 可自由组合 | `/store-factory` |
| 注解式：`@OnConnect` / `@OnDisconnect` / `@OnEvent("chatevent")`，`server.addListeners(Object)` | `/annotations` |
| 最佳实践：avoid large objects（prefer IDs/deltas/streamed binary）；no blocking or DB access in event handlers；keep handlers lightweight；offload business logic；always validate payloads；use rooms to target subsets；use namespaces to partition；handle disconnects to prevent leaks | `/best-practices` |
| 性能报告 v4.0.0（2026-05-11）：单机 9,999.69 msg/s / 66.21 ms / P99 116 ms / 0% err / 0% loss / 堆峰 3414 MB / GC 后 37 MB；分布式 8,571.24 msg/s / 672.94 ms / P99 775 ms / 堆峰 2998 MB / GC 后 102 MB | `/performance` |
| Spring Boot 集成：`netty-socket-io.*` 属性；未提供 `SocketIOServer` Bean 时自动创建；`ExceptionListener` Bean | `/spring-boot` |

### A.5 本项目 Zenoh 集群层（`vertx-spring-boot-cluster-zenoh`）

| 事实 | 位置 |
|---|---|
| `ZenohEventBusBridge.publish(String, Object)` / `publish(String, Object, DeliveryOptions)` 返回 **`void`** | `ZenohEventBusBridge.java:36`、`:43` |
| **本地 `eventBus.publish` 被注释掉**，本地投递靠 Zenoh 回环重注入 | `ZenohEventBusBridge.java:47` |
| `consumer(String)` / `consumer(String, Handler<Message<T>>)` / `getEventBus()` | `ZenohEventBusBridge.java:62`、`:69`、`:76` |
| 发布到 `vertx/{app}/{env}/eventbus/pub/{address}`，按 key 缓存 `Publisher`，失败只记日志 | `ZenohEventBusPublisher.java:40`、`:55`、`:83-94`、`:47-49` |
| 编解码基于 `io.vertx.core.json.JsonObject` → `json.toBuffer().getBytes()`；**无 version / flags / senderNodeId** | `ZenohMessageCodec.java:26` |
| 跨节点 `request/reply` 是**死代码**（零调用方） | `ZenohMessageCodec.java:187`、`:209`；`KeyPrefixStrategy.java:112` |
| `nodeId = UUID.randomUUID().toString()`，**不可配置** | `ZenohClusterManager.java:80`、`:121-123` |
| 自动配置条件：`@ConditionalOnProperty("vertx.cluster.enabled")`（默认 false）+ `@ConditionalOnClass(ClusterManager.class)` | `ZenohClusterAutoConfiguration.java:16-34` |
| 桥 Bean 条件：`@ConditionalOnBean(ZenohClusterManager.class)` | `ZenohEventBusAutoConfiguration.java:14-29` |
| EventBus 消费者回调的 context 正确性是**间接**获得的：阻塞轮询线程 `zenoh-pub-poll-*` 调 `vertx.eventBus().publish/send`，由 Vert.x 在消费者 context 上派发 | `ZenohEventBusSubscriber.java:101-124`、`:114`、`:154` |
| 模块依赖：cluster-zenoh → autoconfigure / vertx-core / zenoh-jni / slf4j-api / spring-boot-autoconfigure / actuator(optional)。**不依赖 web-server** | `vertx-spring-boot-cluster-zenoh/pom.xml` |
| web-server → autoconfigure / vertx-core / spring-boot(-autoconfigure) / spring-boot-starter-webflux（排除 reactor-netty）。**无 micrometer** | `vertx-spring-boot-web-server/pom.xml` |

### A.6 本项目 Phase 1 已交付（本方案的基线）

| 事实 | 位置 |
|---|---|
| `send()` 严格串行 `concatMap` + 每消息一个 `CompletionStage` | `VertxWebSocketSession.java:84-88` |
| `write()` 按类型分派，`finally` 释放 `DataBuffer` | `VertxWebSocketSession.java:110-122` |
| 入站每帧一次全量字节拷贝（附零拷贝为何不安全的 javadoc） | `VertxWebSocketSession.java:140-148` |
| `closeHandler` 置 `closed` + 完成 sink | `VertxWebSocketSession.java:47-51` |
| `close()` 有 `isClosed()` 短路 | `VertxWebSocketSession.java:96-103` |
| `receive()` 用 `OverflowStrategy.BUFFER`，无上界 | `VertxWebSocketSession.java:56-80` |
| 升级后失败在策略层就地终止 + 分级日志（R11） | `VertxRequestUpgradeStrategy.java:67-79`、`:83-97` |
| `isCommitted()` 用 `headWritten()` 补齐（R10） | `VertxServerHttpResponse.java:53-60` |
| `idleTimeout` 默认 60 秒 | `VertxHttpServerProperties.java:23` |
| 现有 10 个 WebSocket 属性，**无任何心跳项** | `VertxWebSocketProperties.java:19-74` |
| 自动配置：`@ConditionalOnProperty("vertx.websocket.enabled", matchIfMissing=true)` + 两个 Bean | `VertxWebSocketAutoConfiguration.java:23-40` |
