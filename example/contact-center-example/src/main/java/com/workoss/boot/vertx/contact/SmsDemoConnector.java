package com.workoss.boot.vertx.contact;

import com.workoss.boot.vertx.contactcenter.channel.ChannelConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 短信演示连接器：出站记录 outbox（供 /api/channel/sms/outbox 审计验证）并打日志；
 * 入站由 ChannelDemoController 的 /api/channel/sms/inbound 模拟运营商回调。
 * 生产替换为真实运营商 SDK 连接器（阿里云/腾讯云），Bean 同名替换即可。
 */
@Component
public class SmsDemoConnector implements ChannelConnector {

    private static final Logger log = LoggerFactory.getLogger(SmsDemoConnector.class);

    private final List<Map<String, String>> outbox = new CopyOnWriteArrayList<>();

    @Override
    public String channel() {
        return "sms";
    }

    @Override
    public void sendMessage(OutboundMessage message) {
        Map<String, String> record = Map.of(
                "to", String.valueOf(message.address()),
                "content_type", String.valueOf(message.contentType()),
                "body", String.valueOf(message.body()),
                "msg_id", String.valueOf(message.msgId()),
                "cid", String.valueOf(message.conversationId()),
                "sent_at", Instant.now().toString());
        outbox.add(record);
        if (outbox.size() > 200) {
            outbox.remove(0);
        }
        log.info("[sms:outbound] to={} msgId={} body={}", message.address(), message.msgId(), message.body());
    }

    public List<Map<String, String>> outbox() {
        return List.copyOf(outbox);
    }
}
