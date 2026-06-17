package com.workoss.boot.vertx;

import com.workoss.boot.vertx.autoconfigure.annotation.EventBusListener;
import com.workoss.boot.vertx.cluster.zenoh.ZenohEventBusBridge;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
@EnableCaching
@SpringBootApplication
public class VertxSpringBootExampleApplication {

    @Resource
    private Vertx vertx;

    @Autowired(required = false)
    private ZenohEventBusBridge eventBusBridge;

    public static void main(String[] args) {
        SpringApplication.run(VertxSpringBootExampleApplication.class, args);
    }


    @GetMapping("/ping")
    public String ok(){
        return "pong";
    }

    /**
     * 使用 @EventBusListener 注解声明式监听 EventBus 地址。
     * 无需手动调用 vertx.eventBus().consumer()，框架会自动注册 consumer。
     */
    @EventBusListener("test.address")
    public void onTestAddress(String body) {
        log.info("收到消息: {}", body);
    }

    /**
     * 也可以通过 Message 参数获取完整消息信息。
     */
    @EventBusListener("test.address.full")
    public void onTestAddressFull(Message<String> message) {
        log.info("收到完整消息: address={}, body={}", message.address(), message.body());
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
