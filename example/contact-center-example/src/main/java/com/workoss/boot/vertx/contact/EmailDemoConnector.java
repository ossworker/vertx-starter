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
 * 邮件演示连接器：出站记录 outbox 并打日志；入站由 /api/channel/email/inbound 模拟。
 * 生产替换为 SMTP 出站 + IMAP/Webhook 入站连接器。
 */
@Component
public class EmailDemoConnector implements ChannelConnector {

    private static final Logger log = LoggerFactory.getLogger(EmailDemoConnector.class);

    private final List<Map<String, String>> outbox = new CopyOnWriteArrayList<>();

    @Override
    public String channel() {
        return "email";
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
        log.info("[email:outbound] to={} msgId={} body={}", message.address(), message.msgId(), message.body());
    }

    public List<Map<String, String>> outbox() {
        return List.copyOf(outbox);
    }
}
