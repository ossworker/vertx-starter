package com.workoss.boot.vertx.ws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.workoss.boot.vertx.web.server.websocket.ClusterWebSocketBroadcaster;
import io.vertx.core.Vertx;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 把聊天室 {@link WebSocketHandler} 挂到 {@code /ws/chat}，并复用 web-server 自动配置的
 * {@link ClusterWebSocketBroadcaster}（集群级扇出）。
 * <p>
 * 路由用 {@link SimpleUrlHandlerMapping} 声明，与 web-server 集成测试里的写法一致；顺序设为
 * {@code -1}，高于 WebFlux 自带的静态资源映射，因此 {@code /ws/chat} 走 WebSocket 升级、
 * 页面等静态资源仍由 {@code classpath:/static/**} 正常服务。
 */
@Configuration
public class WebSocketExampleConfig {

    @Bean
    public ChatWebSocketHandler chatWebSocketHandler(ClusterWebSocketBroadcaster broadcaster,
                                                     ObjectMapper objectMapper, Vertx vertx) {
        return new ChatWebSocketHandler(broadcaster, objectMapper, vertx);
    }

    @Bean
    public SimpleUrlHandlerMapping chatWebSocketMapping(ChatWebSocketHandler chatWebSocketHandler) {
        Map<String, Object> urlMap = new LinkedHashMap<>();
        urlMap.put("/ws/chat", chatWebSocketHandler);
        SimpleUrlHandlerMapping mapping = new SimpleUrlHandlerMapping();
        mapping.setUrlMap(urlMap);
        mapping.setOrder(-1);
        return mapping;
    }
}
