# Vert.x WebSocket 扩展支持方案（HTTP / WebSocket 同端口）

> 基线：Java 21 · Vert.x 5.2.0 · Spring Boot 3.5.16 · Spring Framework 6.2.19
> 落地模块：`vertx-spring-boot-web-server`
> 交付物：本方案文档。**不含代码改动**。
> 编写日期：2026-09-24
> 文中所有 Vert.x / Spring 结论均来自本地 jar 源码与字节码核对，附录 A 给出可追溯清单。
>
> **后续**：Phase 1 已落地并验证。本文档 §8 的 Phase 2 / Phase 3 待办已被
> [`docs/vertx-websocket-optimization-design.md`](vertx-websocket-optimization-design.md) 接管并重排；
> 其中 R6（空闲超时误杀）与 R12（完成不发 CLOSE 帧）的疑点已在新文档 §4.2 / §4.3 中通过
> Vert.x 源码核实解除，可安全实现。

---

## 0. 结论摘要（TL;DR）

1. **同端口共用不需要额外设计**。`VertxWebServer` 目前只注册 `requestHandler`、**没有**注册 `webSocketHandler`，因此升级请求本来就会流进现有 WebFlux 管道；用 `HttpServerRequest.toWebSocket()` 就地升级即可。不需要第二个端口、不需要 Vert.x Router、不绕过 WebFlux。
2. **核心新增只有 3 个类**：`VertxRequestUpgradeStrategy`（实现 Spring 官方扩展点 `RequestUpgradeStrategy`）、`VertxWebSocketSession`（会话适配）、`VertxWebSocketAutoConfiguration`（装配）。
3. **注入点是 Spring 6.2 的 `WebFluxConfigurer.getWebSocketService()`**——这是框架为「非 Netty/Tomcat 容器」预留的官方接缝，零 Bean 覆盖冲突，`WebSocketHandlerAdapter` 会自动用上我们的策略。
4. **用户侧零学习成本**：标准 WebFlux 的 `HandlerMapping` + `WebSocketHandler` 写法开箱可用；本 starter 额外提供 `@WebSocketEndpoint` 注解把样板代码压到 3 行。
5. **有 3 个必须处理的陷阱**（第 6 章，已定位到 Vert.x 源码行号），其中 R1「`toWebSocket()` 抛 `Request has already been read`」是 P0 阻塞项，必须在 `VertxHttpHandlerAdapter` 里加一处 `pause()` 防护。

---

## 1. 目标与非目标

### 1.1 目标

| # | 目标 | 验收口径 |
|---|---|---|
| G1 | HTTP 与 WebSocket **共用同一监听端口** | 同一 `server.port` 上，`curl /api/**` 与 `wscat ws://host:port/ws/**` 同时可用 |
| G2 | 对齐 `spring-boot-starter-websocket`（WebFlux 形态）的使用体验 | 用户只写 `WebSocketHandler` + 路径映射，不接触 Vert.x API |
| G3 | 不引入架构变更：全程走 WebFlux 管道 | `WebFilter`、`HandlerMapping`、`HandlerInterceptor`、CORS、异常处理、`ServerWebExchange` 属性对 WS 握手同样生效 |
| G4 | 会话模型对齐 Spring 抽象 | 业务代码面向 `org.springframework.web.reactive.socket.WebSocketSession`，可无改动迁到 Reactor Netty |
| G5 | 支持文本 / 二进制 / Ping-Pong / 分片续帧 / 背压 / 优雅关闭 | 见第 5.2 节 |
| G6 | 可通过 `vertx.websocket.*` 配置帧大小、消息大小、压缩、子协议、心跳 | 见第 5.7 节 |

### 1.2 非目标（本期明确不做）

1. **JSR-356 `@ServerEndpoint`**——那是 Servlet 栈的东西，与 WebFlux 不兼容，不做。
2. **SockJS 回退**——`SockJsService` 依赖 Servlet/HTTP 长轮询语义，投入产出比低，本期不做（预留接口，见 8.3）。
3. **STOMP 消息代理（`@MessageMapping` / `@EnableWebSocketMessageBroker`）**——技术上可行（`StompSubProtocolHandler` 实现的是**反应式** `WebSocketHandler`，只要我们的 `WebSocketService` 合规就能复用），但需要引入 `spring-websocket` + `spring-messaging`，且本地 `.m2` 无对应 6.2.x 版本可核对，**列为 Phase 3 且标注「待验证」**，本期不承诺。
4. **WebSocket over HTTP/2（RFC 8441）**——Vert.x 的 `canUpgradeToWebSocket()` 显式要求 HTTP/1.1（`HttpUtils.java:941-944`），本期不支持，见 R4。
5. **不做 Vert.x Router 混编**——项目既有约束：所有改进必须在现有 WebFlux 适配层内实施，禁止混合路由 / 绕过 WebFlux 管道。

---

## 2. 现状盘点（已核实的代码事实）

| 位置 | 事实 | 对 WebSocket 的意义 |
|---|---|---|
| `VertxWebServer.java:36-51` | `start()` 只做 `vertx.createHttpServer(options)` + `requestHandler(handler)` + `listen()`，**未注册 `webSocketHandler` / `webSocketHandshakeHandler`** | ✅ **同端口共用的前提已天然满足**。`HttpServerRequest.toWebSocket()` 的 javadoc 明确要求「only be used if no WebSocket handler is set on the HttpServer」。**方案必须保持不注册 `webSocketHandler`** |
| `VertxServerHttpRequest.java:103-106` | `getNativeRequest()` 返回 Vert.x `HttpServerRequest` | ✅ 升级策略可直接取到原生请求 |
| `VertxServerHttpResponse.java:47-51` | `getNativeResponse()` 返回 Vert.x `HttpServerResponse` | ✅ 可在握手前往 101 响应里塞头（子协议，见 R3） |
| `VertxServerHttpRequest.java:31-33, 40-45` | GET/HEAD/OPTIONS/TRACE 直接返回共享的 `Flux.empty()`，不创建 body publisher | ✅ WS 握手是 GET，管道不会触碰请求体，不与 `toWebSocket()` 抢 handler |
| `VertxHttpHandlerAdapter.java:38-42` | 完成回调里 `if (!request.response().ended()) request.response().end();` | ✅ **无需改动**。Vert.x 升级成功后 `Http1ServerResponse.completeHandshake()` 会把 `written=true`（`Http1ServerResponse.java:426-436`），而 `ended()` 直接返回 `written`（同文件 `:578-582`），所以升级后这个守卫自然跳过 `end()` |
| `VertxReactiveWebServerFactory.java:50-96` | `buildHttpServerOptions()` 目前不设任何 WebSocket 参数 | ⚠️ 需补 `maxWebSocketFrameSize` / `maxWebSocketMessageSize` / 子协议 / per-message 压缩 |
| `VertxHttpServerProperties.java:23` | `idleTimeout` 默认 **60 秒** | ⚠️ **R6**：Netty 的 `IdleStateHandler`（名为 `"idle"`）在升级后**不会**被移出 pipeline——`acceptHandshake` 只移除了 `HttpChunkContentCompressor`。所以 60s 空闲的 WebSocket 会被直接掐断 |
| `vertx-spring-boot-web-server/pom.xml` | 依赖 `spring-boot-starter-webflux`，排除了 `spring-boot-starter-reactor-netty` | ✅ `spring-webflux` 自带全套反应式 WebSocket SPI，**不需要新增任何依赖** |
| Spring Boot `WebSocketReactiveAutoConfiguration` | `@ConditionalOnClass({Servlet.class, ServerContainer.class})` | ✅ 只管 Tomcat/Jetty 定制器，不会与本方案冲突 |

---

## 3. 方案选型

### 3.1 三条候选路线

| 路线 | 做法 | 结论 |
|---|---|---|
| **A. Vert.x 原生** | `httpServer.webSocketHandler(ws -> ...)` 或 `webSocketHandshakeHandler(...)` | ❌ **与同端口路由冲突**：一旦注册 `webSocketHandler`，Vert.x 会在 server 层直接完成升级，升级请求**不再进入 `requestHandler`**，WebFlux 的 `WebFilter` / CORS / 鉴权 / 异常处理全部失效。同时也堵死了 `toWebSocket()` 这条路 |
| **B. Vert.x Router 混编** | 引入 `vertx-web`，`router.route("/ws/*").handler(...)` 与 WebFlux 并存 | ❌ 违反项目既有约束（禁止混合路由 / 绕过 WebFlux 管道），且引入 `vertx-web` 新依赖 |
| **C. Spring WebFlux `RequestUpgradeStrategy` 扩展点** ← **选定** | 实现 `VertxRequestUpgradeStrategy`，塞进 Spring 原生的 `HandshakeWebSocketService`，通过 `WebFluxConfigurer.getWebSocketService()` 注入 | ✅ 与 `ReactorNettyRequestUpgradeStrategy` / `TomcatRequestUpgradeStrategy` 完全对等的地位；复用 Spring 全部握手校验与路由；新增依赖为 0 |

### 3.2 关键发现：官方注入点是 `WebFluxConfigurer.getWebSocketService()`

这是本方案能「零冲突」落地的核心。核对 Spring 6.2.19 字节码得到：

```
WebFluxConfigurationSupport
  ├─ @Bean public WebSocketHandlerAdapter webFluxWebSocketHandlerAdapter()   // Bean 名带 webFlux 前缀
  │      └─ new WebSocketHandlerAdapter(initWebSocketService())
  └─ protected WebSocketService initWebSocketService()
         ├─ getWebSocketService() != null  →  直接用它
         ├─ 否则 new HandshakeWebSocketService()
         └─ 抛异常则 new NoUpgradeStrategyWebSocketService(ex)   // 兜底：请求即失败

DelegatingWebFluxConfiguration.getWebSocketService()
  └─ WebFluxConfigurerComposite.getWebSocketService()  // 遍历所有 WebFluxConfigurer
     └─ 非 null 则返回，否则回落 super.getWebSocketService()

WebFluxConfigurer.getWebSocketService()   // @since 5.3，default 返回 null
```

推论：

1. 当前项目（`spring-boot-starter-webflux` 且排除 reactor-netty、无 Tomcat）走到 `new HandshakeWebSocketService()` → `initUpgradeStrategy()` 只认 Tomcat/Jetty/Undertow/ReactorNetty1/ReactorNetty2，全不命中 → 落到 `StandardWebSocketUpgradeStrategy`（需要 JSR-356 `ServerContainer`）或直接抛异常被兜底成 `NoUpgradeStrategyWebSocketService`。**即：现状下 WebSocket 握手必然失败。**
2. 只要注册一个 `WebFluxConfigurer` Bean 并让 `getWebSocketService()` 返回 `new HandshakeWebSocketService(new VertxRequestUpgradeStrategy(props))`，`webFluxWebSocketHandlerAdapter` 这个 Bean 会**自动**用上它。
3. Bean 名是 `webFluxWebSocketHandlerAdapter` 而不是 `webSocketHandlerAdapter`，且我们**不定义同类型 Bean**，因此不存在 `BeanDefinitionOverrideException` 风险，也不需要 `@ConditionalOnMissingBean` 去和 `WebFluxAutoConfiguration` 抢。
4. 用户如果自己写了 `@EnableWebFlux`（`DelegatingWebFluxConfiguration`），`WebFluxConfigurer` 同样被收集，方案不变。

> **注意**：`HandshakeWebSocketService` 实现了 `Lifecycle`，由 Spring 容器托管启停，我们只需把它作为 configurer 的返回值构造出来即可，不需要单独暴露成 Bean（暴露反而可能与 `initWebSocketService()` 的结果产生两个实例）。

---

## 4. 目标架构与请求流

### 4.1 组件关系

```
                    ┌──────────────────────────────────────────────────────┐
                    │  VertxWebServerAutoConfiguration（现有，微调）        │
                    │   └─ VertxReactiveWebServerFactory                   │
                    │        └─ HttpServerOptions + WebSocket 参数(新增)    │
                    │        └─ VertxHttpHandlerAdapter (+ pause 防护)      │
                    └───────────────────────┬──────────────────────────────┘
                                            │
   ┌────────────────────────────────────────┴─────────────────────────────────────┐
   │  VertxWebSocketAutoConfiguration（新增）                                      │
   │   ├─ VertxWebSocketProperties            @ConfigurationProperties            │
   │   │                                     ("vertx.websocket")                  │
   │   ├─ VertxRequestUpgradeStrategy         implements RequestUpgradeStrategy   │
   │   └─ VertxWebFluxConfigurer              implements WebFluxConfigurer        │
   │        └─ getWebSocketService() = new HandshakeWebSocketService(strategy)     │
   │   └─ (可选) WebSocketEndpointRegistrar   扫描 @WebSocketEndpoint → HandlerMapping│
   └───────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 一次握手的完整时序

```
Client ──GET /ws/echo HTTP/1.1  Connection: Upgrade  Upgrade: websocket  Sec-WebSocket-Key: xxx──▶

[Vert.x event loop]
 1  Http1ServerConnection.handleMessage(DefaultHttpRequest)
      └─ req.context.emit(req, requestHandler)            // 内联执行，早于 LastHttpContent
 2  VertxHttpHandlerAdapter.handle(req)
      ├─ if (req.canUpgradeToWebSocket()) req.pause()     // ★ R1 防护
      └─ httpHandler.handle(vReq, vResp).subscribe(...)
 3  HttpWebHandlerAdapter → ExceptionHandlingWebHandler → FilteringWebHandler(WebFilter 链)
 4  DispatcherHandler
      ├─ HandlerMapping：SimpleUrlHandlerMapping 命中 /ws/echo → EchoHandler
      └─ WebSocketHandlerAdapter.handle(exchange, handler)
 5  HandshakeWebSocketService.handleRequest(exchange, handler)
      ├─ 校验 method==GET / Upgrade==WebSocket / Connection 含 Upgrade / Sec-WebSocket-Key 存在
      ├─ selectProtocol(headers, handler)                 // 与 handler.getSubProtocols() 取交集
      ├─ initAttributes(exchange)                         // 复制握手期 exchange 属性到 session
      └─ upgradeStrategy.upgrade(exchange, handler, protocol, handshakeInfoFactory)
 6  VertxRequestUpgradeStrategy.upgrade(...)              // ★ 本次新增
      ├─ 不调用 response.setComplete()                    // ★ R2
      ├─ if (protocol != null) vResp.putHeader("Sec-WebSocket-Protocol", protocol)   // ★ R3
      ├─ vReq.toWebSocket()  → Future<ServerWebSocket>
      │     └─ webSocketHandshake(): 设 handler+endHandler → resume()
      │          └─ conn.createWebSocket() → Netty WebSocketServerHandshaker
      │               .handshake(channel, fullRequest, response.headers(), promise)
      │          └─ 写 101 Switching Protocols
      │          └─ completeHandshake(): statusCode=101, headWritten=written=true
      │          └─ pipeline.replace(VertxHandler) → WebSocketConnectionImpl
      └─ .flatMap(ws -> handler.handle(new VertxWebSocketSession(ws, info, bufFactory)))
 7  双向帧流：ws.textMessageHandler / binaryMessageHandler / pongHandler ⇄ session.send()
 8  关闭：session.close(status) → ws.close(code, reason) → closeHandler → closeStatus sink
 9  handler.handle() 的 Mono 完成 → 步骤 2 的 onComplete 回调
      └─ vReq.response().ended() == true → 跳过 end()     // 现有守卫天然生效
```

**同端口共用的本质**：HTTP 请求走完 3→4 后由 `RequestMappingHandlerMapping` + `RequestMappingHandlerAdapter` 处理并 `response.end()`；WebSocket 请求走完 3→4 后由 `SimpleUrlHandlerMapping` + `WebSocketHandlerAdapter` 处理并在第 6 步把 TCP 连接从 HTTP 语义切换为 WS 帧语义。两者共享同一个 `HttpServer`、同一个 `requestHandler`、同一套 `WebFilter`。

---

## 5. 详细设计

### 5.1 `VertxRequestUpgradeStrategy`

```java
package com.workoss.boot.vertx.web.server.websocket;

public class VertxRequestUpgradeStrategy implements RequestUpgradeStrategy {

    @Override
    public Mono<Void> upgrade(ServerWebExchange exchange,
                             WebSocketHandler handler,
                             @Nullable String subProtocol,
                             Supplier<HandshakeInfo> handshakeInfoFactory) {

        ServerHttpRequest request  = exchange.getRequest();
        HttpServerRequest nativeRequest = request.getNativeRequest();   // VertxServerHttpRequest:104
        HandshakeInfo info = handshakeInfoFactory.get();

        // R3：子协议通过响应头协商。Netty 的 handshaker 会把 response.headers() 合并进 101 响应
        //      （ServerWebSocketHandshaker.acceptHandshake: handshaker.handshake(ch, req, (HttpHeaders) response.headers(), p)）
        if (subProtocol != null && !subProtocol.isEmpty()) {
            nativeRequest.response().putHeader(HttpHeaders.SEC_WEBSOCKET_PROTOCOL, subProtocol);
        }

        // R2：绝不能调用 exchange.getResponse().setComplete()
        //      AbstractServerHttpResponse.setComplete() → writeWithInternal(Mono.empty())
        //      → VertxServerHttpResponse:125 response.end() → 200 已提交，升级不可能
        return Mono.fromCompletionStage(nativeRequest::toWebSocket)
                .flatMap(webSocket -> {
                    VertxWebSocketSession session = new VertxWebSocketSession(
                            webSocket, info, bufferFactory());
                    return handler.handle(session)
                            .checkpoint(request.getURI() + " [VertxRequestUpgradeStrategy]");
                });
    }
}
```

设计要点：

| 点 | 说明 |
|---|---|
| 不实现 `WebSocketService` | 复用 Spring 的 `HandshakeWebSocketService`，白拿它的握手校验、子协议协商、属性初始化、`ContextWebSocketHandler` 包装、`Lifecycle` |
| `Mono.fromCompletionStage(Supplier)` | 用 `Supplier` 重载实现懒执行，避免 Future 在装配期就被触发 |
| `bufferFactory()` | 用 `NettyDataBufferFactory`（不是 `VertxServerHttpResponse` 里的 `DefaultDataBufferFactory`），配合 `BufferInternal.getByteBuf()` 做帧体零拷贝，与项目既有的 body 零拷贝实践一致 |
| 前置校验 | 进入 `upgrade()` 前 `HandshakeWebSocketService` 已校验完毕；这里只需再断言 `nativeRequest.canUpgradeToWebSocket()`，为 false 时返回 `Mono.error(...)` 而非静默失败（例如 HTTP/2 请求打进来） |

### 5.2 `VertxWebSocketSession`

```java
public class VertxWebSocketSession extends AbstractWebSocketSession<ServerWebSocket> {

    public VertxWebSocketSession(ServerWebSocket ws, HandshakeInfo info, DataBufferFactory factory) {
        super(ws, UUID.randomUUID().toString(), info, factory);
    }
}
```

`AbstractWebSocketSession` 已提供 `getId/getHandshakeInfo/bufferFactory/getAttributes/textMessage/binaryMessage/pingMessage/pongMessage`，只需实现 5 个方法：

#### (1) `receive()` —— 入站

用 Vert.x 的**类型化 handler**而不是 `frameHandler`。核对 `WebSocketImplBase`（`:444-565`）确认其内部有 `FrameAggregator`，设置 `textMessageHandler` / `binaryMessageHandler` 后会自动聚合 CONTINUATION 续帧，省掉手写状态机。

> ⚠️ **必须同时设置 `textMessageHandler` 和 `binaryMessageHandler`**。`WebSocketImplBase:538-541 / 561-564` 显示：只设其一时，聚合器会把另一个置为 null，导致该类型消息**静默丢弃**。

```java
@Override
public Flux<WebSocketMessage> receive() {
    return Flux.<WebSocketMessage>create(sink -> {
        ServerWebSocket ws = getDelegate();
        ws.textMessageHandler(s -> sink.next(textMessage(s)));
        ws.binaryMessageHandler(b -> sink.next(binaryMessage(f -> toDataBuffer(b))));
        ws.pongHandler(b -> sink.next(pongMessage(f -> toDataBuffer(b))));
        ws.closeHandler(v -> sink.complete());
        ws.exceptionHandler(sink::error);
        sink.onRequest(n -> ws.resume());
        sink.onCancel(ws::pause);
        sink.onDispose(ws::pause);
    }, FluxSink.OverflowStrategy.BUFFER);
}
```

背压：`onRequest → resume()` / `onCancel → pause()`，映射到 Vert.x 的 `fetch(Long.MAX_VALUE)` / `queue().pause()`。

#### (2) `send(Publisher)` —— 出站

```java
@Override
public Mono<Void> send(Publisher<WebSocketMessage> messages) {
    return Flux.from(messages)
            .concatMap(msg -> Mono.fromCompletionStage(() -> write(msg).toCompletionStage()))
            .then();
}

private Future<Void> write(WebSocketMessage msg) {
    ServerWebSocket ws = getDelegate();
    DataBuffer payload = msg.getPayload();
    try {
        switch (msg.getType()) {
            case TEXT   -> { return ws.writeFinalTextFrame(payloadAsText(payload)); }
            case BINARY -> { return ws.writeFinalBinaryFrame(toVertxBuffer(payload)); }
            case PING   -> { return ws.writePing(toVertxBuffer(payload)); }
            case PONG   -> { return ws.writePong(toVertxBuffer(payload)); }
            default     -> throw new IllegalArgumentException("Unexpected type: " + msg.getType());
        }
    } finally {
        DataBufferUtils.release(payload);   // 与 Spring 各 session 适配器一致的所有权约定
    }
}
```

写侧背压：`concatMap` 天然串行化；如需更强控制，用 `ws.writeQueueFull()` + `drainHandler` 包装成 `Mono`（列为 P2 优化）。

#### (3) `close(CloseStatus)` / `closeStatus()` / `isOpen()`

```java
@Override public boolean isOpen() { return !getDelegate().isClosed(); }

@Override
public Mono<Void> close(CloseStatus status) {
    return Mono.fromCompletionStage(() ->
        getDelegate().close((short) status.getCode(), status.getReason()).toCompletionStage());
}

@Override
public Mono<CloseStatus> closeStatus() {
    return closeStatusSink.asMono();   // Sinks.One，在构造时挂 ws.closeHandler 里 complete
}
```

`closeHandler` 触发时从 `ws.closeStatusCode()` / `ws.closeReason()` 组装 `CloseStatus`。注意 Vert.x 的 `closeStatusCode()` 返回 `Short`，可能为 null（对端未带状态码），需回落 `CloseStatus.NO_STATUS_CODE`。

#### (4) Buffer 互转

| 方向 | 实现 | 是否零拷贝 |
|---|---|---|
| Vert.x `Buffer` → Spring `DataBuffer` | `NettyDataBufferFactory.wrap(((BufferInternal) buf).getByteBuf())` | ✅ 零拷贝（与 `VertxServerHttpRequest` 现有做法一致） |
| Spring `DataBuffer` → Vert.x `Buffer` | `Buffer.buffer(((NettyDataBuffer) db).getNativeBuffer())` | ✅ 零拷贝 |
| 回落（非 Netty DataBuffer） | 读字节数组 → `Buffer.buffer(bytes)` | 一次拷贝 |

> `BufferInternal` 在 `io.vertx.core.buffer.impl` 包下，属 Vert.x 内部 API。项目已在用同样的手法，风险已知；需在 pom 里注意 `module-info` / JPMS 不阻断（当前非模块化，无影响）。

#### (5) 能力边界（需在 Javadoc 中明示）

| 项 | 状态 | 原因 |
|---|---|---|
| TEXT / BINARY / 续帧聚合 | ✅ | `FrameAggregator` |
| PONG 上抛 | ✅ | `WebSocketBase.pongHandler` |
| **PING 上抛** | ❌ **不支持** | `WebSocketBase` **没有** `pingHandler`（见 javap 全量方法列表）。Vert.x 自动回 PONG，PING 帧不会到达业务层。业务不应依赖「收到 PING」事件 |
| 部分帧（partial frame） | ❌ | 类型化 handler 只给完整消息；需要时用 `frameHandler` 自行聚合，本期不做 |
| 单帧上限 | 由 `vertx.websocket.max-frame-size` 控制 | `WebSocketServerConfig.getMaxFrameSize()` |
| 单消息上限（聚合后） | 由 `vertx.websocket.max-message-size` 控制 | `WebSocketServerConfig.getMaxMessageSize()` |

### 5.3 ★ R1 防护：`VertxHttpHandlerAdapter` 增加 `pause()`

这是本方案唯一的 P0 阻塞点，**必须做**，否则一旦引入异步 `WebFilter`（限流、鉴权查库、日志 MDC 异步）握手就会随机失败。

**问题链条**（全部有源码依据）：

```
Http1ServerRequest.toWebSocket()                      // :419-421
  └─ webSocketHandshake().compose(h -> h.accept())
       └─ handler(buff -> ...)      ← 内部调用 checkEnded()   // :659-663
       └─ endHandler(v -> ...)      ← 内部调用 checkEnded()
              throw new IllegalStateException("Request has already been read")  如果 ended==true

Http1ServerRequest.onEnd()                              // :554-569
  └─ ended = true

Http1ServerConnection.handleMessage()                   // :205-207
  └─ if (msg == LastHttpContent.EMPTY_LAST_CONTENT) onEnd();   // 无 body 的 GET 走这条快路径
```

无 body 的 GET，Netty 在交付 `DefaultHttpRequest` 之后**紧接着**就交付 `EMPTY_LAST_CONTENT`。只要 WebFlux 链条在中间让出过一次 event loop（任何 `Mono` 异步边界），`onEnd()` 就会先跑，`ended=true`，随后 `toWebSocket()` 抛异常。

**修复**：在管道入口把请求暂停。核对 `Http1ServerRequest`：

```java
public HttpServerRequest pause() { queue().pause(); return this; }   // :344-347
```

`queue()` 会**懒创建** `InboundMessageQueue`；一旦队列存在，`handleEnd()` 走的是 `queue.add(END_SENTINEL)` 而不是直接 `context.execute(::onEnd)`（`:159-173`），`onEnd()` 就被推迟到 `resume()` 之后。而 `webSocketHandshake()` 的最后一行正是 `resume()`（`:464-465`），所以升级路径自洽。

**改动**（`VertxHttpHandlerAdapter.java`）：

```java
@Override
public void handle(HttpServerRequest request) {
    boolean mayUpgrade = websocketEnabled && request.canUpgradeToWebSocket();
    if (mayUpgrade) {
        // 保持请求处于「未读完」状态，直到 RequestUpgradeStrategy 调用 toWebSocket()
        request.pause();
    }
    VertxServerHttpRequest  httpRequest  = new VertxServerHttpRequest(request);
    VertxServerHttpResponse httpResponse = new VertxServerHttpResponse(request.response());

    httpHandler.handle(httpRequest, httpResponse).subscribe(
            null,
            err -> { logger.error(...); resumeIfPending(request, mayUpgrade);
                     if (!request.response().ended()) request.response().setStatusCode(500).end(); },
            ()  -> { resumeIfPending(request, mayUpgrade);
                     if (!request.response().ended()) request.response().end(); });
}

private static void resumeIfPending(HttpServerRequest request, boolean mayUpgrade) {
    // 升级成功后 ended()==true，此时 Vert.x 已接管连接，不能再碰 request
    if (mayUpgrade && !request.response().ended()) {
        request.resume();
    }
}
```

要点：

- `websocketEnabled` 由 `VertxReactiveWebServerFactory` 构造时传入，**默认关闭时热路径零改动**（连 `canUpgradeToWebSocket()` 都不调用）。
- `canUpgradeToWebSocket()` 内部先比 `version()` 再比 `method()`（`HttpUtils.java:941-947`），非 GET 请求两次整型比较就返回，开销可忽略。
- `resumeIfPending` 覆盖「GET 带了 Upgrade 头但没命中任何 WS 路由」的场景——不 resume 会让 `END_SENTINEL` 永远滞留，`bytesRead` 指标不上报。

### 5.4 `HttpServerOptions` 的 WebSocket 参数

`VertxReactiveWebServerFactory.buildHttpServerOptions()` 追加：

```java
VertxWebSocketProperties ws = vertxProperties.getWebsocket();
if (ws.isEnabled()) {
    options.setMaxWebSocketFrameSize(ws.getMaxFrameSize());
    options.setMaxWebSocketMessageSize(ws.getMaxMessageSize());
    options.setPerMessageWebSocketCompressionSupported(ws.isPerMessageCompression());
    options.setPerFrameWebSocketCompressionSupported(ws.isPerFrameCompression());
    options.setWebSocketCompressionLevel(ws.getCompressionLevel());
    options.setWebSocketAllowServerNoContext(ws.isAllowServerNoContext());
    options.setWebSocketPreferredClientNoContext(ws.isPreferredClientNoContext());
    options.setWebSocketClosingTimeout(ws.getClosingTimeout());
    ws.getSubProtocols().forEach(options::addWebSocketSubProtocol);   // 见 R3
    options.setRegisterWebSocketWriteHandlers(false);                 // 不用 EventBus socket bridge，省内存
}
```

依据：`HttpServerOptions` 的 `setMaxWebSocketFrameSize` / `setMaxWebSocketMessageSize` / `addWebSocketSubProtocol` / `setPerMessageWebSocketCompressionSupported` / `setWebSocketClosingTimeout` / `isRegisterWebSocketWriteHandlers` 等 setter 均已用 javap 确认存在。

### 5.5 自动配置

```java
@AutoConfiguration(after = VertxWebServerAutoConfiguration.class)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
@ConditionalOnClass({HttpServer.class, WebSocketService.class, RequestUpgradeStrategy.class})
@EnableConfigurationProperties(VertxWebSocketProperties.class)
public class VertxWebSocketAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(RequestUpgradeStrategy.class)
    public VertxRequestUpgradeStrategy vertxRequestUpgradeStrategy(VertxWebSocketProperties props) {
        return new VertxRequestUpgradeStrategy(props);
    }

    @Bean
    public VertxWebFluxConfigurer vertxWebFluxConfigurer(RequestUpgradeStrategy strategy) {
        return new VertxWebFluxConfigurer(strategy);
    }

    @Bean
    @ConditionalOnMissingBean(name = "vertxWebSocketHandlerMapping")
    @ConditionalOnBean(WebSocketHandler.class)
    public HandlerMapping vertxWebSocketHandlerMapping(List<WebSocketHandler> handlers) {
        return new WebSocketEndpointRegistrar().buildMapping(handlers);   // 见 5.6 Tier 2
    }
}
```

```java
class VertxWebFluxConfigurer implements WebFluxConfigurer {
    private final RequestUpgradeStrategy strategy;
    @Override public WebSocketService getWebSocketService() {
        return new HandshakeWebSocketService(strategy);
    }
}
```

装配顺序说明：`WebFluxConfigurer` 是通过 `DelegatingWebFluxConfiguration.setConfigurers(List<WebFluxConfigurer>)` 的 `@Autowired(required=false)` 收集的，**任何** `WebFluxConfigurer` Bean 都会被拿到，与本自动配置的先后无关。`@ConditionalOnMissingBean(RequestUpgradeStrategy.class)` 让用户可以整体替换升级策略。

`VertxWebServerAutoConfiguration` 需要一处联动：把 `websocket.enabled` 透传给 `VertxReactiveWebServerFactory`，进而决定 `VertxHttpHandlerAdapter` 的 `websocketEnabled` 与 `HttpServerOptions` 的 WS 参数。为此 `VertxHttpServerProperties` 增加嵌套的 `WebSocket websocket = new WebSocket()`。

### 5.6 用户 API（三档，任选）

#### Tier 1 · 标准 WebFlux 写法（零新增概念，**永远可用**）

```java
@Configuration
public class WsConfig {

    @Bean
    public HandlerMapping webSocketHandlerMapping(EchoHandler echoHandler) {
        SimpleUrlHandlerMapping mapping = new SimpleUrlHandlerMapping();
        mapping.setUrlMap(Map.of("/ws/echo", echoHandler));
        mapping.setOrder(-1);          // 必须先于 RequestMappingHandlerMapping
        return mapping;
    }

    @Bean
    public EchoHandler echoHandler() { return new EchoHandler(); }
}

public class EchoHandler implements WebSocketHandler {
    @Override
    public Mono<Void> handle(WebSocketSession session) {
        return session.send(session.receive().map(msg -> session.textMessage("echo: " + msg.getPayloadAsText())));
    }
}
```

#### Tier 2 · 注解式（本 starter 增强，**推荐**）

```java
@WebSocketEndpoint(path = "/ws/echo", subProtocols = {"v1.chat"})
@Component
public class EchoHandler implements WebSocketHandler {
    @Override
    public Mono<Void> handle(WebSocketSession session) {
        return session.send(session.receive()
                .map(msg -> session.textMessage("echo: " + msg.getPayloadAsText())));
    }
}
```

—— 一个注解 + 一个 `@Component`，**没有 `HandlerMapping` 样板**。

`WebSocketEndpointRegistrar` 的实现：注入 `List<WebSocketHandler>`，反射读 `@WebSocketEndpoint`，合并成 `Map<String, WebSocketHandler>`，构造 `SimpleUrlHandlerMapping`（`order = -1`）。同时把注解里的 `subProtocols` 汇总，回灌到 `HttpServerOptions.addWebSocketSubProtocol(...)`（解决 R3 的服务端全局清单问题）。路径冲突时启动即失败并打印冲突双方，不做静默覆盖。

> 未标注解的 `WebSocketHandler` Bean 直接忽略，不影响 Tier 1。

#### Tier 3 · 函数式路由（`RouterFunction`）

不额外支持。`RouterFunctions.route(...).GET("/ws/echo", ...)` 返回的是 `ServerResponse`，无法承载 101 升级。**文档中明确写「WS 端点不要走 RouterFunction」**，避免用户踩坑。

### 5.7 配置属性

`vertx.websocket.*`（挂在现有 `vertx.http-server.websocket.*` 之下亦可，倾向前者，层级更浅）：

| 属性 | 默认值 | 说明 |
|---|---|---|
| `vertx.websocket.enabled` | `true` | 关闭后 `VertxHttpHandlerAdapter` 热路径完全不变 |
| `vertx.websocket.max-frame-size` | `65536` | 单帧上限（Vert.x 默认 `DEFAULT_MAX_WEBSOCKET_FRAME_SIZE`） |
| `vertx.websocket.max-message-size` | `262144` | 聚合后单消息上限，防 OOM |
| `vertx.websocket.sub-protocols` | 空 | 服务端子协议白名单（R3） |
| `vertx.websocket.per-message-compression` | `false` | permessage-deflate |
| `vertx.websocket.per-frame-compression` | `false` | 与 per-message 互斥，同时开启时 Vert.x 以 per-frame 优先 |
| `vertx.websocket.compression-level` | `6` | 与 HTTP 压缩级别独立 |
| `vertx.websocket.allow-server-no-context` | `false` | 压缩上下文参数 |
| `vertx.websocket.preferred-client-no-context` | `false` | 压缩上下文参数 |
| `vertx.websocket.closing-timeout` | `10`（秒） | 关闭握手等待对端 CLOSE 帧的超时，对应 `HttpServerOptions#setWebSocketClosingTimeout(int)`；负值表示不超时 |
| `vertx.websocket.server-heartbeat-interval` | `0`（关闭） | **Phase 2**：>0 时由 session 定时发 PING，用于对冲 R6 |

复用现有 `server.*` 标准属性的部分（端口、地址、SSL、HTTP/2）不做重复定义，与 `VertxReactiveWebServerFactory` 现有行为保持一致。

---

## 6. 关键技术风险

| ID | 风险 | 证据 | 处置 | 优先级 |
|---|---|---|---|---|
| **R1** | `toWebSocket()` 抛 `IllegalStateException("Request has already been read")` | `Http1ServerRequest.java:419-421`（`webSocketHandshake` 内 `handler()`/`endHandler()` 均调 `checkEnded()`）、`:659-663`、`:554-569`（`onEnd` 置 `ended=true`）、`Http1ServerConnection.java:205-207`（无 body GET 的 `EMPTY_LAST_CONTENT` 快路径） | 5.3 的 `pause()` 防护 + `resumeIfPending`。**必做** | **P0** |
| **R2** | 误调 `response.setComplete()` 导致 200 提前提交，升级失败 | `ReactorNettyRequestUpgradeStrategy.upgrade()` 第一行就是 `response.setComplete()`（Netty 语义安全），而本项目 `VertxServerHttpResponse.writeWithInternal:125` 在空 body 时直接 `response.end()` | 策略里**不实现** `setComplete()`；代价是 `response.beforeCommit(...)` 注册的回调不会执行（例如靠 `beforeCommit` 加 CORS 头的写法失效）。需在 Javadoc + 文档明示：**WS 端点的响应头请在 `WebFilter` 里直接 `response.getHeaders().add(...)`，不要用 `beforeCommit`** | **P0** |
| **R3** | 子协议协商语义不对齐：Vert.x 是 **server 全局**清单，Spring 是 **per-handler** | `Http1ServerConnection.createHandshaker:392-394` 用 `webSocketConfig.getSubProtocols()`；`ServerWebSocketHandshaker.acceptHandshake` 里 `webSocket.subProtocol(handshaker.selectedSubprotocol())`；`ServerWebSocketHandshake.accept()` **无子协议入参** | 双保险：① 升级前 `putHeader("Sec-WebSocket-Protocol", protocol)`——Netty handshaker 会把 `response.headers()` 合并进 101（`acceptHandshake` 第 3 个实参）；② `VertxWebSocketSession` 的子协议以 Spring 协商结果为准（`HandshakeInfo.getSubProtocol()`），不读 `ws.subProtocol()`。同时由 `WebSocketEndpointRegistrar` 汇总所有 `@WebSocketEndpoint.subProtocols` 回灌 `HttpServerOptions`，让 Netty 侧的 `selectedSubprotocol` 也正确 | P1 |
| **R4** | HTTP/2 下 WebSocket 不可用 | `HttpUtils.canUpgradeToWebSocket:941-944` 显式 `req.version() != HTTP_1_1 → false` | 策略入口断言并返回明确错误（`501 Not Implemented` 语义），日志提示「WebSocket over HTTP/2 (RFC 8441) 暂不支持，请让 WS 流量走 HTTP/1.1」。若用户开了 `server.http2.enabled=true` + ALPN，浏览器会协商到 h2 → **WS 直接不可用**。需在启动日志里 WARN | P1 |
| **R5** | PING 帧不上抛业务层 | `WebSocketBase` 无 `pingHandler`（javap 全量方法已确认） | 文档明示；`WebSocketMessage.Type.PING` 在 `receive()` 中永不出现。业务心跳请用**应用层消息**或依赖 Vert.x 自动 PONG | P2 |
| **R6** | **默认 `idleTimeout=60s` 会掐断空闲 WebSocket** | `VertxHttpServerProperties.java:23`；Netty `IdleStateHandler`（pipeline 名 `"idle"`，见 `HttpServerConnectionInitializer.computeChannelName:275-283`）在升级后**不会**被移除——`ServerWebSocketHandshaker.acceptHandshake` 只 `pipeline.remove(HttpChunkContentCompressor.class)` | 三选一（推荐 a+b）：<br>a) 文档强制说明，并建议把 `vertx.http-server.idle-timeout` 调到 0（禁用）或远大于心跳周期；<br>b) 提供 `vertx.websocket.server-heartbeat-interval`，由 session 定时 `ws.writePing()`；<br>c) 客户端心跳（不由本 starter 保证） | P1 |
| **R7** | 优雅停机对长连接的行为 | `VertxWebServer.shutDownGracefully:94` 调 `httpServer.shutdown(30, SECONDS)`；Vert.x 5 的 `WebSocketBase` 提供了 `shutdownHandler` / `shutdown(Duration, short, String)` | Vert.x 的 `HttpServer.shutdown` 会等待并主动关闭 WS 连接。需实测：① 30s 是否够（k8s `terminationGracePeriodSeconds` 默认 30s，**贴边**，建议可配）；② 是否在关闭前给业务一次 `shutdownHandler` 通知机会（可用于推「服务即将重启」消息）。列为验收项 T7 | P1 |
| **R8** | 集群多实例下的连接归属 | WS 连接绑定在具体节点的 event loop 上 | 不新增机制：**跨节点广播复用项目既有的 Zenoh EventBus**（`publish` 到 topic，各节点订阅后 fan-out 给本节点持有的 session）。需提供一个 `WebSocketSessionRegistry`（节点内 `sessionId → session`）作为 fan-out 落点。列为 Phase 2 | P2 |
| **R9** | `VertxServerHttpResponse.applyHeaders()` 会注入 `Date` 与 `Connection: keep-alive` | `VertxServerHttpResponse.java:62-81` | 升级路径**不走** `applyHeaders()`（我们不调 `setComplete()` / `writeWith()`），所以不会污染 101 响应。需在测试 T4 中断言 101 响应头里**没有** `Date` | P2 |
| **R10** | **升级后 `isCommitted()` 误报 false，错误管线往已升级连接上写 500**（Phase 1 实测发现） | `AbstractServerHttpResponse.isCommitted()` 只看内部 `state`；`Http1ServerResponse.completeHandshake()` 直接置 `headWritten/written`，**不经过** `commit()`。客户端异常断连时 `HttpWebHandlerAdapter.handleUnresolvedError` 走 `setComplete()` → `applyStatusCode()` → `Http1ServerResponse.checkHeadWritten:673` 抛 `IllegalStateException("Response head already sent")` | 已修复：`VertxServerHttpResponse` 覆写 `isCommitted()`，叠加 `response.headWritten()`。回归测试 `VertxServerHttpResponseTest` | **P0** |
| **R11** | **升级后 handler 异常外泄到 WebFlux 错误管线**（Phase 1 实测发现） | 101 已上线，HTTP 响应不复存在，任何异常都无法渲染；`handleUnresolvedError` 只会再打一条 ERROR，客户端断连一次产生 2 条带栈的 ERROR 日志 | 已修复：`VertxRequestUpgradeStrategy` 在 `handler.handle(...)` 后就地 `onErrorResume` 终止。peer-gone（`HttpClosedException` / `SocketException` / `ClosedChannelException` / 消息含 `connection reset`、`broken pipe` 的 `IOException`）降为 DEBUG，真实 handler 失败仍为 ERROR。注意 `DisconnectedClientHelper` 只认字面量 `connection reset by peer`，Vert.x 抛出的是 `Connection reset`，**必须自行补齐分类**。回归测试 `VertxRequestUpgradeStrategyTest` | **P0** |
| **R12** | handler 的 `Mono<Void>` **正常完成**时不发 CLOSE 帧，与 Reactor Netty 语义不一致 | `HttpServerOperations.java:1362-1367`：Reactor Netty 在 handler publisher `onComplete` 且 channel 仍活跃时主动 `sendCloseNow(NORMAL_CLOSURE)`。Vert.x 侧无此行为 | 影响面：`session.send(Flux.just(msg))` 这类有限发布者写完后连接会一直挂着（对端不关就不断）。Phase 2 处理——在策略里 handler 完成后补一次 `session.close(CloseStatus.NORMAL)`，需先确认与 `ServerWebSocketImpl` 的 `assert !closeSent` 不冲突 | P1 |

---

## 7. 测试与验收

| ID | 用例 | 断言 |
|---|---|---|
| T1 | 同端口共存 | 同一 `server.port` 上 `/api/ping`（`@RestController`）返回 200，`/ws/echo` 升级成功并回声 |
| T2 | 基础收发 | 文本、二进制、>64KB 分片续帧均正确聚合 |
| T3 | **R1 回归** | 注册一个**强制异步**的 `WebFilter`（`.delayElement(Duration.ofMillis(10))`），握手仍成功、无 `IllegalStateException` |
| T4 | 握手报文 | 101 状态码、`Upgrade: websocket`、`Connection: Upgrade`、`Sec-WebSocket-Accept` 正确；**不含** `Date` 头（R9） |
| T5 | 子协议 | 客户端带 `Sec-WebSocket-Protocol: v1.chat`，服务端 `@WebSocketEndpoint(subProtocols="v1.chat")`，101 响应回显该协议；`HandshakeInfo.getSubProtocol()` 一致（R3） |
| T6 | 背压 | 慢消费者场景下 `writeQueueFull()` 不导致内存无界增长；`receive()` 取消订阅后 Vert.x 侧 `pause()` 生效 |
| T7 | 优雅停机 | `shutDownGracefully` 触发后，活跃 WS 收到 CLOSE 帧、`closeStatus()` 有值、进程在超时内退出（R7） |
| T8 | 关闭语义 | 客户端主动关闭 → 服务端 `receive()` 完成、`closeStatus()` 发出对应码；服务端 `session.close(CloseStatus.POLICY_VIOLATION)` → 客户端收到 1008 |
| T9 | HTTP/2 | `server.http2.enabled=true` 时启动有 WARN；h1.1 客户端仍可正常升级（R4） |
| T10 | 空闲超时 | `vertx.http-server.idle-timeout=5` + 无心跳 → 5s 后连接被关；开启 `server-heartbeat-interval=2s` → 连接存活（R6） |
| T11 | WebFilter 链生效 | 鉴权 `WebFilter` 返回 401 时，WS 握手得到 401 而非 101（G3） |
| T12 | 并发 | 500 并发连接 × 100 条消息回声，无错序、无泄漏（Netty `ByteBuf` 引用计数归零） |
| T13 | 基准 | 与 Reactor Netty 同机对比握手延迟与单帧吞吐，作为回归基线（不设为通过门槛） |
| T14 | **升级后响应已提交**（R10） | 101 写完后 `VertxServerHttpResponse.isCommitted()` 返回 `true`，错误管线不再尝试写状态码 |
| T15 | **升级后异常就地终止**（R11） | handler 抛异常 / 对端异常断连时 `upgrade(...)` 返回的 `Mono` 正常完成而非报错；peer-gone 分类覆盖 `HttpClosedException`、`SocketException`、`ClosedChannelException`、native transport 的 `IOException`，且真实 handler 失败不被误判 |

测试栈：`spring-boot-starter-test`（已有）+ `vertx-junit5` + Java 11 `HttpClient`/`WebSocket`（JDK 自带，无需新增依赖）。

---

## 8. 实施计划

### Phase 1 · 核心可用（P0，约 2 人日）

1. `VertxWebSocketProperties`（`vertx.websocket.*`）
2. `VertxHttpServerProperties` 增加 `websocket` 嵌套段（或独立顶层，二选一，倾向独立顶层）
3. `VertxRequestUpgradeStrategy`
4. `VertxWebSocketSession`
5. `VertxWebFluxConfigurer` + `VertxWebSocketAutoConfiguration` + `AutoConfiguration.imports` 追加一行
6. `VertxReactiveWebServerFactory`：WS 参数写入 `HttpServerOptions`，`websocketEnabled` 透传给 adapter
7. **`VertxHttpHandlerAdapter`：R1 `pause()` 防护 + `releaseIfNotUpgraded`**
8. **`VertxServerHttpResponse.isCommitted()` 叠加 `headWritten()`（R10）**
9. **`VertxRequestUpgradeStrategy` 升级后异常就地终止 + peer-gone 分级日志（R11）**
10. 测试 T1–T5、T8、T11、T14、T15
11. `example/vertx-server-spring-boot-example` 加一个 `/ws/echo` 端点 + 一个静态 HTML 测试页

**Phase 1 完成即可交付 Tier 1（标准 WebFlux 写法）。**

### Phase 2 · 易用性与生产化（P1，约 2 人日）

1. `@WebSocketEndpoint` + `WebSocketEndpointRegistrar`（Tier 2）
2. `vertx.websocket.server-heartbeat-interval` 服务端心跳（对冲 R6）
3. `WebSocketSessionRegistry`（节点内 `sessionId → session`），为跨节点 fan-out 打底（R8）
4. R4 的启动 WARN；R7 优雅停机实测与 `shutdownHandler` 接入
5. R12：handler `Mono` 正常完成后补发 CLOSE(1000)，对齐 Reactor Netty 语义
6. 测试 T6、T7、T9、T10、T12
7. 文档：`docs/vertx-websocket-usage.md`（面向使用者）

### Phase 3 · 可选（P2，待验证后再承诺）

1. STOMP 消息代理：引入 `spring-websocket` + `spring-messaging`，验证 `StompSubProtocolHandler` 与本 `WebSocketService` 的兼容性（本地 `.m2` 无 6.2.x `spring-websocket`，**结论未知**）
2. WebSocket over HTTP/2（RFC 8441）：需 Vert.x 侧支持，当前 `canUpgradeToWebSocket` 直接拒绝，**不可行**，仅跟踪上游
3. SockJS
4. 写侧背压强化（`writeQueueFull` + `drainHandler`）
5. Micrometer 指标（连接数、消息数、帧大小分布）

---

## 9. 文件改动清单

### 新增（`vertx-spring-boot-web-server/src/main/java/com/workoss/boot/vertx/web/server/websocket/`）

| 文件 | 说明 |
|---|---|
| `VertxRequestUpgradeStrategy.java` | 实现 `RequestUpgradeStrategy`，核心 |
| `VertxWebSocketSession.java` | 继承 `AbstractWebSocketSession<ServerWebSocket>` |
| `VertxWebSocketProperties.java` | `@ConfigurationProperties("vertx.websocket")` |
| `VertxWebFluxConfigurer.java` | 实现 `WebFluxConfigurer.getWebSocketService()` |
| `VertxWebSocketAutoConfiguration.java` | 自动配置 |
| `WebSocketEndpoint.java`（Phase 2） | 注解 |
| `WebSocketEndpointRegistrar.java`（Phase 2） | 扫描并构建 `SimpleUrlHandlerMapping` |
| `WebSocketSessionRegistry.java`（Phase 2） | 节点内会话注册表 |

### 修改

| 文件 | 改动 |
|---|---|
| `VertxHttpHandlerAdapter.java` | **R1 防护**：构造入参加 `boolean websocketEnabled`；`handle()` 加 `pause()`；终态回调加 `resumeIfPending()` |
| `VertxReactiveWebServerFactory.java` | `buildHttpServerOptions()` 追加 WS 参数；`getWebServer()` 把 `websocketEnabled` 传给 adapter |
| `VertxHttpServerProperties.java` | （若采用嵌套方案）增加 `websocket` 段 |
| `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` | 追加 `...websocket.VertxWebSocketAutoConfiguration` |
| `example/vertx-server-spring-boot-example/**` | 增加 `/ws/echo` 示例端点与测试页 |

### 不改动（明确）

- `VertxWebServer.java` —— 现有 `ended()` 守卫天然适配升级后的响应状态（见第 2 章）
- `VertxServerHttpRequest.java` / `VertxServerHttpResponse.java` —— 已暴露 `getNativeRequest()` / `getNativeResponse()`，够用
- `pom.xml` —— **零新增依赖**

---

## 附录 A · 已核实的 API 事实清单

**Vert.x 5.2.0**（源码：`~/.m2/repository/io/vertx/vertx-core/5.2.0/vertx-core-5.2.0-sources.jar`）

| 事实 | 位置 |
|---|---|
| `HttpServerRequest.toWebSocket(): Future<ServerWebSocket>`、`canUpgradeToWebSocket(): boolean` | `io.vertx.core.http.HttpServerRequest`（javap） |
| `toWebSocket()` javadoc：「can only be used if no WebSocket handler is set on the HttpServer」 | `HttpServerRequest.java:355-360` |
| `toWebSocket()` = `webSocketHandshake().compose(accept())` | `Http1ServerRequest.java:419-421` |
| `webSocketHandshake()` 设 `handler`/`endHandler` 并以 `resume()` 收尾 | `Http1ServerRequest.java:434-466` |
| `handler()`/`endHandler()` 调 `checkEnded()` → `IllegalStateException("Request has already been read")` | `Http1ServerRequest.java:319-330, 361-372, 659-663` |
| `onEnd()` 置 `ended = true` | `Http1ServerRequest.java:554-569` |
| `pause()` = `queue().pause()`，`queue()` 懒创建 `InboundMessageQueue` | `Http1ServerRequest.java:100-125, 344-347` |
| 队列存在时 `handleEnd()` 入队 `END_SENTINEL`，否则直接 `context.execute(::onEnd)` | `Http1ServerRequest.java:159-173` |
| 无 body 的 `LastHttpContent.EMPTY_LAST_CONTENT` 走 `onEnd()` 快路径 | `Http1ServerConnection.java:205-207` |
| `eagerCreateRequestQueue = threadingModel != EVENT_LOOP`（非 EVENT_LOOP 会立即 `req.resume()`） | `Http1ServerConnection.java:137, 214-216` |
| `createHandshaker` 的子协议取自 **server 级** `webSocketConfig.getSubProtocols()` | `Http1ServerConnection.java:392-394, 402` |
| `handshaker.handshake(channel, fullRequest, (HttpHeaders) response.headers(), p)` —— **响应头会被合并进 101** | `ServerWebSocketHandshaker.acceptHandshake` |
| 升级成功仅移除 `HttpChunkContentCompressor`，**不移除 `IdleStateHandler`** | `ServerWebSocketHandshaker.acceptHandshake` |
| `completeHandshake()`：`setStatusCode(101)`、`headWritten=written=true` | `Http1ServerResponse.java:426-436` |
| `ended()` 返回 `written`；`headWritten()` 返回 `headWritten` | `Http1ServerResponse.java:578-596` |
| `canUpgradeToWebSocket` 要求 `HTTP_1_1` + `GET` + `Connection` 含 `Upgrade` + `Upgrade` 含 `WebSocket` | `HttpUtils.java:941-953` |
| `WebSocketBase` 写 API：`writeTextMessage/writeBinaryMessage/writePing/writePong/writeFinalTextFrame/writeFinalBinaryFrame/writeFrame` 均返回 `Future<Void>`；流控 `setWriteQueueMaxSize/drainHandler`；关闭 `close(short,String)`、`shutdown(...)`；`isClosed()`、`closeStatusCode()`、`closeReason()`、`subProtocol()` | `io.vertx.core.http.WebSocketBase`（javap） |
| **`WebSocketBase` 无 `pingHandler`**，只有 `pongHandler` | `io.vertx.core.http.WebSocketBase`（javap 全量） |
| `FrameAggregator`：设 `textMessageHandler` 会置空 `binaryMessageHandler`（反之亦然），**必须同时设置** | `WebSocketImplBase.java:444-459, 487-488, 511-512, 528-565` |
| `ServerWebSocketHandshake.accept()` **无子协议参数**，仅 `accept()` / `reject(int)` | `io.vertx.core.http.ServerWebSocketHandshake`（javap） |
| `HttpServerOptions` 的 WS setter：`setMaxWebSocketFrameSize/setMaxWebSocketMessageSize/addWebSocketSubProtocol/setWebSocketSubProtocols/setPerFrameWebSocketCompressionSupported/setPerMessageWebSocketCompressionSupported/setWebSocketCompressionLevel/setWebSocketAllowServerNoContext/setWebSocketPreferredClientNoContext/setWebSocketClosingTimeout/isRegisterWebSocketWriteHandlers` | `io.vertx.core.http.HttpServerOptions`（javap） |
| 服务端 pipeline 的 idle handler 名为 `"idle"` | `HttpServerConnectionInitializer.java:275-283` |

**Spring Framework 6.2.19 / Spring Boot 3.5.16**

| 事实 | 位置 |
|---|---|
| `RequestUpgradeStrategy.upgrade(ServerWebExchange, WebSocketHandler, String subProtocol, Supplier<HandshakeInfo>)` | `o.s.web.reactive.socket.server.RequestUpgradeStrategy` |
| `HandshakeWebSocketService(RequestUpgradeStrategy)`；`handleRequest` 校验 GET/CONNECT、`Upgrade`、`Connection`、`Sec-WebSocket-Key`，再 `selectProtocol` → `initAttributes` → `upgradeStrategy.upgrade` | `HandshakeWebSocketService`（源码） |
| `HandshakeWebSocketService.initUpgradeStrategy()` 仅识别 Tomcat/Jetty/JettyCore/Undertow/ReactorNetty1/ReactorNetty2，否则 `StandardWebSocketUpgradeStrategy` | `HandshakeWebSocketService`（字节码） |
| `WebSocketHandlerAdapter(WebSocketService)`，实现 `HandlerAdapter` + `Ordered` | `o.s.web.reactive.socket.server.support.WebSocketHandlerAdapter` |
| `WebFluxConfigurer.getWebSocketService()` default 返回 null（`@since 5.3`） | `WebFluxConfigurer`（字节码 + 源码 javadoc） |
| `DelegatingWebFluxConfiguration.getWebSocketService()` 优先取 configurer 的值 | `DelegatingWebFluxConfiguration`（字节码） |
| `WebFluxConfigurationSupport.webFluxWebSocketHandlerAdapter()` 由 `initWebSocketService()` 构造；后者失败时回落 `NoUpgradeStrategyWebSocketService` | `WebFluxConfigurationSupport`（字节码） |
| `AbstractWebSocketSession(T delegate, String id, HandshakeInfo, DataBufferFactory)`；已实现 `getId/getHandshakeInfo/bufferFactory/getAttributes/textMessage/binaryMessage/pingMessage/pongMessage` | `o.s.web.reactive.socket.adapter.AbstractWebSocketSession` |
| `WebSocketSession` 待实现：`receive()/send()/isOpen()/close(CloseStatus)/closeStatus()` | `o.s.web.reactive.socket.WebSocketSession` |
| `ReactorNettyRequestUpgradeStrategy.upgrade()` 首行 `response.setComplete()`（Netty 语义安全，**Vert.x 不可照搬**） | `ReactorNettyRequestUpgradeStrategy`（源码） |
| Spring Boot `WebSocketReactiveAutoConfiguration` 带 `@ConditionalOnClass({Servlet.class, ServerContainer.class})`，只含 Tomcat/Jetty 定制器 | `spring-boot-autoconfigure-3.5.16.jar`（字节码 + `AutoConfiguration.imports`） |

**本项目**

| 事实 | 位置 |
|---|---|
| 未注册 `webSocketHandler` | `VertxWebServer.java:36-51` |
| `getNativeRequest()` 暴露 Vert.x 请求 | `VertxServerHttpRequest.java:103-106` |
| GET/HEAD/OPTIONS/TRACE 返回共享 `Flux.empty()` | `VertxServerHttpRequest.java:22-23, 31-33, 40-45` |
| `getNativeResponse()` 暴露 Vert.x 响应 | `VertxServerHttpResponse.java:47-51` |
| 空 body 时 `writeWithInternal` 直接 `response.end()` | `VertxServerHttpResponse.java:121-126` |
| 终态回调 `if (!ended()) end()` | `VertxHttpHandlerAdapter.java:38-42` |
| `applyHeaders()` 注入 `Date` / `Connection: keep-alive` | `VertxServerHttpResponse.java:62-81` |
| `idleTimeout` 默认 60s | `VertxHttpServerProperties.java:23` |
| `buildHttpServerOptions()` 未设任何 WS 参数 | `VertxReactiveWebServerFactory.java:50-96` |
| 优雅停机 `httpServer.shutdown(30, SECONDS)` | `VertxWebServer.java:94` |
