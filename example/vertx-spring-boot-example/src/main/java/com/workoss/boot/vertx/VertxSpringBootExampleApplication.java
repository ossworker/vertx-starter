package com.workoss.boot.vertx;

import com.workoss.boot.vertx.cluster.zenoh.ZenohEventBusBridge;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
@SpringBootApplication
public class VertxSpringBootExampleApplication {

    @Resource
    private Vertx vertx;

    @Autowired(required = false)
    private ZenohEventBusBridge eventBusBridge;

    public static void main(String[] args) {
        SpringApplication.run(VertxSpringBootExampleApplication.class, args);
    }

    @PostConstruct
    public void eventBusSubscribe() {
        // 订阅 test.address 地址，接收发送到此地址的消息
        vertx.eventBus().consumer("test.address",
                (Handler<Message<String>>) event -> log.info("收到消息: {}", event.body()));
    }

    @GetMapping("/eventbus/pub")
    public void publish(@RequestParam("msg") String msg) {
        if (eventBusBridge != null) {
            // 使用桥接器发布，消息会通过 Zenoh 发送到其他节点
            eventBusBridge.publish("test.address", msg);
        } else {
            // 降级到本地 EventBus（非集群模式）
            vertx.eventBus().publish("test.address", msg);
        }
    }

    @GetMapping("/eventbus/send")
    public void send(@RequestParam("msg") String msg) {
        vertx.eventBus().send("test.address", msg);
        log.info("发送消息: {}", msg);
    }
}
