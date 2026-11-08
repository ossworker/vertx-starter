package com.workoss.boot.vertx.contactcenter.channel.sip;

import io.vertx.core.Future;

/**
 * SIP/PSTN 网关 SPI（预留，P2 落地）：接 Asterisk/FreeSWITCH + rtpengine 媒体桥后，
 * 浏览器 WebRTC 坐席可呼叫/接听 PSTN 号码，反之亦然。
 * <p>
 * 接入路径（见 docs/retail-omnichannel-solution.md §2）：SIP 网关 ↔ rtpengine（RTP↔SRTP）↔
 * 媒体桥服务 ↔ 事件总线；Connection.substrate 声明 {@code sip-rtp}。
 * 未接入实现前，相关能力按 UCTP 约定返回 501 not-implemented。
 */
public interface SipGateway {

    /**
     * 向 PSTN/SIP 地址发起呼叫。
     *
     * @param cid           会话 id
     * @param sid           实时 Session id（媒体腿归属）
     * @param fromExtension 坐席侧分机/URI
     * @param toUri         被叫 SIP URI 或 E.164（sip:+8613800000000@gateway 或 tel:）
     * @return 网关侧呼叫标识（connid 关联用）
     */
    Future<String> initiateCall(String cid, String sid, String fromExtension, String toUri);

    /** 挂断网关侧呼叫。 */
    Future<Void> hangup(String gatewayCallId);

    /** 通话中发送 DTMF（IVR 导航等）。 */
    Future<Void> sendDtmf(String gatewayCallId, String digits);
}
