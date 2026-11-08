# 零售售后全渠道客服系统方案（UCTP / VoIP-3）

> 目标：零售行业售后场景（咨询、订单/物流查询、退款退货、发票、地址确认、投诉）下，生产可用（高性能、高扩展）的统一会话服务。
> 协议基础：UCTP v0 over WebSocket（信令）+ WebRTC（媒体），会话模型遵循 VoIP-3（Conversation / Session / Message / Participant / Connection / Stream）。

## 1. 场景与指标

| 维度 | 目标 |
|---|---|
| 渠道 | App/网页 IM（已具备）、短信、邮件、语音/视频通话；预留 SIP/PSTN 网络电话接入 |
| 会话模型 | 全渠道统一：同一客户跨渠道汇入同一 Conversation，售后上下文连续 |
| 规模 | 单接入节点 10 万级长连接；会话状态按 cid 分片水平扩展 |
| 可靠性 | 消息 at-least-once + 信封 id 幂等；断线 resume_token 重连；journal 可回放 |
| 合规 | PII 脱敏日志、渠道出站留痕（审计）、录音预留 |

## 2. 总体架构

```
                ┌──────────────── 接入层（Channel Gateway）────────────────┐
                │  WS/IM(已有)   SMS webhook   Email IMAP/SMTP   SIP(预留)  │
                └──────────────┬───────────────────────────────┬──────────┘
                               │ 归一化                        │ 归一化
                ┌──────────────▼───────────────────────────────▼──────────┐
                │            UCTP 会话核心（本模块）                        │
                │  Conversation / Session / Message / Participant          │
                │  Workforce 路由（技能组+优先级+最长空闲+转接）             │
                └──────┬───────────────────────┬─────────────────────────┘
                       │ 出站分发               │ 媒体协商
        ┌──────────────▼──────────┐   ┌────────▼──────────────────────────┐
        │ 渠道连接器 SPI           │   │ 媒体层                               │
        │ sms/email/sip 适配器     │   │ P2P WebRTC(1:1) / SFU(预留,会议录音) │
        └─────────────────────────┘   │ SIP 媒体桥 rtpengine(预留)           │
                                      └─────────────────────────────────────┘
                       ┌──────────────▼─────────────────────────┐
                       │ 数据层：journal(PG) / 幂等存储 / 在线状态 │
                       └──────────────────────────────────────────┘
```

关键决策：

1. **短信/邮件不走 Session**。VoIP-3 中 Session 是"同步、有界互动"，短信/邮件是异步消息流，只挂在 Conversation 上（`message.send`，无 sid）。语音/视频通话才建立 Session（已有实现）。
2. **外部身份即参与者**：手机号/邮箱客户建模为 `Participant{kind: external, channel: sms|email|sip, address}`，身份 ID 规范 `ext:<channel>:<address>`（如 `ext:sms:+8613800000000`）。同一客户绑定多渠道时由集成层（CRM/CDP）归并到同一 customer 档案。
3. **渠道双向归一化**：入站由 ChannelConnector 转成系统信封调用 `ConversationService.ingestExternalMessage`；出站 `message.send` 时按目标 participant 的 channel 分发到连接器，WS 渠道维持现有本地写路径。
4. **SIP 预留**：`SipGatewayConnector` SPI 定义发起/挂断/DTMF；媒体面通过 RTP↔WebRTC 桥（rtpengine 类）接入，Connection.substrate 声明 `sip-rtp`。未接入前相关能力返回 501（协议标准行为）。

## 3. 高性能设计

- **事件循环内无锁状态**：会话状态只在归属节点（cid 一致性哈希 over 集群节点）的事件循环上读写；跨节点信令经事件总线转发（既有架构）。
- **出站扇出**：本地 WS 直写（WriteChain 串行化防乱序），跨节点经 zenoh 事件总线；身份在线状态 presence 全集群同步。
- **journal 异步批量写 PG**：热状态全内存，journal 追加先写内存（容量受限双写保护），后台批量落库；冷启动按归属分片加载。写库失败不阻断会话（日志告警 + 补偿任务）。
- **背压**：慢消费者触发 WriteChain 排队上限后断连保护（客户端 resume_token 重连补拉 history）。
- **连接模型**：Vert.x event loop（2×CPU）+ 每连接零拷贝帧解析；10 万连接 ≈ 每 loop 6k 连接，信令帧 <1KB 时 CPU 余量充足。

## 4. 可靠性与安全

- 信封 id 幂等：进程内 seen-set（已有）→ 生产换 DB/Redis 唯一约束（`message_id` 唯一索引，冲突即丢弃重复）。
- 出站 at-least-once：连接器级 ack + 指数退避重试 + 死信表（短信/邮件天然异步，天然适合）。
- webhook 入站：HMAC-SHA256 签名验证（短信运营商/邮件推送回调）；IP 白名单。
- 敏感字段（手机号/邮箱/地址）日志脱敏；渠道出站全量留痕表 `channel_outbox`（审计/合规）。
- RBAC：participant role（customer/agent/supervisor/system）已有；外部参与者仅 customer 角色，不可接收信令面消息。

## 5. 部署拓扑

- 接入节点（vertx-web-server + contact-center）无状态水平扩展 N 个；zenoh router 独立集群（既有模式）。
- PostgreSQL（或 PolarDB-X，MySQL 语法双方言兼容，见 §8）存 journal + outbox + 幂等键；Redis 存跨节点共享热数据（外部渠道绑定、幂等 seen-set、限流计数）；Kafka 可选（消息副本进分析/数仓）。
- SIP 接入时旁路部署：SIP 网关（Asterisk/FreeSWITCH）↔ rtpengine ↔ 媒体桥服务 ↔ 事件总线。

## 5.1 Parley 编排模式（借鉴 eisenzopf/parley）

Parley 在 UCTP 之上证明了**阶段化多方会话编排**（G6-071 参考场景：17 阶段、显式 audience、业务门、可观测结果）。本方案吸收以下模式到零售售后：

| Parley 模式 | 零售售后映射 |
|---|---|
| Stage path（有序阶段图，拒绝环与未决分支） | 售后流程模板：申请→审核→退货寄回→验收→退款完成；每阶段绑定受理方 audience |
| 业务门（consent / readiness / attributed decision） | 退款超阈值需主管 attributed 审批（动作必须归属到具体 participant，服务端以认证主体为权威 from，AI 输出不可冒认为客户决策） |
| Private consultation（私有咨询腿，其他方不可见） | 坐席↔主管私语协商：定向 message.send（participant 集合）已支持，编排层保证客户不在 audience |
| DTMF 电话确认（answer ≠ confirmation） | SIP 接入后：电话通知退款结果，客户按 1 确认；接起不算确认（SipGateway.sendDtmf 已预留） |
| 出站 queued ≠ delivered，回执签名回调 | 渠道连接器出站返回受理即 queued；投递/失败回执由 webhook 签名入站，更新 outbox 状态 |
| Fixture 化隔离测试 | 演示连接器即 fixture 形态（SMS HTTP fixture / SIP UDP fixture），生产连接器替换不改变核心链路 |

编排引擎（stage 状态机 + 门校验）属 P3；P0 已具备其 primitives：权威归属动作（card-action）、定向受众消息、出站回执接口。

## 6. 分期落地

| 期 | 内容 | 状态 |
|---|---|---|
| P0 | 渠道 SPI + 外部参与者 + 短信/邮件演示连接器 + SIP 骨架 + 零售售后卡片（退款/退货/发票/地址/订单/位置/富媒体） | 本次落地 |
| P1 | journal PG 持久化 + 幂等存储 + 真实短信(阿里云等)/SMTP 连接器 + webhook 签名 | 待排期 |
| P2 | SIP 媒体桥（rtpengine + 网关对接）+ SFU（会议/录音/质检） | 待排期 |
| P3 | 智能路由（意图识别/客户等级）、报表质检、CRM 深度集成 | 待排期 |

## 7. 零售售后卡片协议（content-type 约定）

| content_type | 说明 |
|---|---|
| `text/plain` | 文本/表情 |
| `application/vnd.uctp.card.order+json` | 订单卡片（订单号/明细/金额/状态） |
| `application/vnd.uctp.card.invoice+json` | 发票卡片（抬头/税号/金额/开具状态） |
| `application/vnd.uctp.card.address+json` | 收货地址确认卡片（客户确认动作） |
| `application/vnd.uctp.card.location+json` | 地理位置 |
| `application/vnd.uctp.card.refund+json` | 退款/退货卡片（金额/原因/状态/审核动作） |
| `application/vnd.uctp.media.image+json` | 图片（url） |
| `application/vnd.uctp.media.video+json` | 视频 |
| `application/vnd.uctp.media.audio+json` | 音频 |
| `application/vnd.uctp.card-action+json` | 卡片动作回执 `{ref_msg_id, action}` |

卡片交互（确认退款、确认地址、申请开票）以 card-action 消息回流，服务端透传 + journal 回放，无需服务端理解卡片语义（领域逻辑在业务系统与前端渲染层）。
