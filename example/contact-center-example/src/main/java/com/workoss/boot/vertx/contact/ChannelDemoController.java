package com.workoss.boot.vertx.contact;

import com.workoss.boot.vertx.contactcenter.core.ConversationService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 外部渠道演示入口：模拟短信运营商回调与邮件入站，验证全渠道归一化链路。
 * 生产环境替换为带 HMAC 签名的真实 webhook（方案 §4）。
 */
@RestController
@RequestMapping("/api/channel")
public class ChannelDemoController {

    private final ConversationService conversationService;
    private final SmsDemoConnector smsConnector;
    private final EmailDemoConnector emailConnector;

    public ChannelDemoController(ConversationService conversationService,
                                 SmsDemoConnector smsConnector,
                                 EmailDemoConnector emailConnector) {
        this.conversationService = conversationService;
        this.smsConnector = smsConnector;
        this.emailConnector = emailConnector;
    }

    /** 模拟短信运营商入站回调。 */
    @PostMapping("/sms/inbound")
    public Map<String, String> smsInbound(@RequestBody Map<String, String> body) {
        String from = body.get("from");
        String cid = conversationService.ingestExternalMessage("sms", from, from,
                "text/plain", body.get("text"), body.get("msg_id"));
        return Map.of("accepted", "true", "cid", cid);
    }

    @GetMapping("/sms/outbox")
    public List<Map<String, String>> smsOutbox() {
        return smsConnector.outbox();
    }

    /** 模拟邮件入站（主题+正文合并为文本消息）。 */
    @PostMapping("/email/inbound")
    public Map<String, String> emailInbound(@RequestBody Map<String, String> body) {
        String from = body.get("from");
        String text = (body.get("subject") == null ? "" : "[主题] " + body.get("subject") + "\n")
                + body.getOrDefault("text", "");
        String cid = conversationService.ingestExternalMessage("email", from, from,
                "text/plain", text, body.get("msg_id"));
        return Map.of("accepted", "true", "cid", cid);
    }

    @GetMapping("/email/outbox")
    public List<Map<String, String>> emailOutbox() {
        return emailConnector.outbox();
    }
}
