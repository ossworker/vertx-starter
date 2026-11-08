package com.workoss.boot.vertx.contact;

import com.workoss.boot.vertx.contactcenter.channel.ChannelConnector;
import com.workoss.boot.vertx.contactcenter.channel.sip.SipGateway;
import io.vertx.core.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * SIP 渠道占位连接器（P2 前）：
 * <ul>
 * <li>文本类消息经 SIP 网关投递（MESSAGE 请求）前仅记录日志；</li>
 * <li>{@link SipGateway} 呼叫能力返回 failed_future(501 not-implemented)，
 * 供上层按 UCTP 错误语义转换。</li>
 * </ul>
 */
@Component
public class SipDemoConnector implements ChannelConnector, SipGateway {

    private static final Logger log = LoggerFactory.getLogger(SipDemoConnector.class);

    @Override
    public String channel() {
        return "sip";
    }

    @Override
    public void sendMessage(OutboundMessage message) {
        log.info("[sip:outbound:stub] to={} msgId={} body={}（P2 接 SIP MESSAGE）",
                message.address(), message.msgId(), message.body());
    }

    @Override
    public Future<String> initiateCall(String cid, String sid, String fromExtension, String toUri) {
        return Future.failedFuture("501 not-implemented: SIP media bridge pending (P2)");
    }

    @Override
    public Future<Void> hangup(String gatewayCallId) {
        return Future.failedFuture("501 not-implemented: SIP media bridge pending (P2)");
    }

    @Override
    public Future<Void> sendDtmf(String gatewayCallId, String digits) {
        return Future.failedFuture("501 not-implemented: SIP media bridge pending (P2)");
    }
}
