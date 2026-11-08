package com.workoss.boot.vertx.contactcenter.channel;

/**
 * 渠道连接器 SPI：把 UCTP 消息接入外部渠道（短信/邮件/SIP 等）。
 * <p>
 * 出站：ConversationService 投递 message.send 时，目标 Participant 带 channel 标识
 * （{@code kind=external}）则经对应连接器发送，不走路由到 WS。
 * 入站：连接器收到外部消息后调用
 * {@code ConversationService.ingestExternalMessage(...)} 归一化为 UCTP 消息。
 * <p>
 * 实现类注册为 Spring Bean 即被 {@link ChannelRegistry} 自动发现；
 * starter 内置实现见 example 的演示连接器。
 */
public interface ChannelConnector {

    /** 渠道标识：sms / email / sip。 */
    String channel();

    /**
     * 出站投递。实现方负责渠道协议 specifics（签名、重试、留痕），
     * 投递结果不影响 UCTP 侧 message.delivered 语义（连接器 ack 属渠道层）。
     */
    void sendMessage(OutboundMessage message);

    /** 渠道地址当前可达（演示连接器恒 true；真实渠道可接运营商回执）。 */
    default boolean reachable(String address) {
        return true;
    }

    /** 出站消息（record 字段与 UCTP message.send 对齐）。 */
    record OutboundMessage(String conversationId, String participantId, String identityId,
                           String address, String msgId, String contentType, String body,
                           String senderParticipantId) {
    }
}
