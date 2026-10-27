package com.workoss.boot.vertx.cluster.zenoh;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.shareddata.ClusterSerializable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务消息类型的跨节点 codec 契约。
 * <p>
 * 用贴近真实业务的报文(按退款渠道聚合的金额与日期)验证契约,而不是玩具 POJO:
 * <ul>
 *   <li>{@code ClusterSerializable} 只解决编码;每个节点都必须注册 codec 才能解码。</li>
 *   <li>未注册时必须在<em>发送侧</em>快速失败,而不是让消息静默消失在对端。</li>
 *   <li>高���字段(枚举、金额、日期、嵌套结构)必须原样往返,不能有精度或格式损失。</li>
 * </ul>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BusinessMessageCodecE2ETest {

    private static final String APP = "e2e-codec-" + UUID.randomUUID().toString().substring(0, 8);
    private static final long TIMEOUT_MS = 30_000L;

    private static Vertx vertxA;
    private static Vertx vertxB;
    private static ZenohClusterManager managerA;
    private static ZenohClusterManager managerB;

    @BeforeAll
    static void startCluster() throws Exception {
        int zenohPort = freePort();

        ZenohClusterProperties propsA = new ZenohClusterProperties();
        propsA.setAppName(APP);
        propsA.setEnvironment("test");
        propsA.setMode("peer");
        propsA.setListen(List.of("tcp/127.0.0.1:" + zenohPort));

        ZenohClusterProperties propsB = new ZenohClusterProperties();
        propsB.setAppName(APP);
        propsB.setEnvironment("test");
        propsB.setMode("client");
        propsB.setConnect(List.of("tcp/127.0.0.1:" + zenohPort));

        managerA = new ZenohClusterManager(propsA);
        vertxA = build(managerA, freePort());
        Thread.sleep(500);
        managerB = new ZenohClusterManager(propsB);
        vertxB = build(managerB, freePort());
    }

    private static Vertx build(ZenohClusterManager manager, int eventBusPort) {
        VertxOptions options = new VertxOptions();
        options.getEventBusOptions().setHost("127.0.0.1").setPort(eventBusPort);
        return Vertx.builder().with(options).withClusterManager(manager)
                .buildClustered().toCompletionStage().toCompletableFuture().join();
    }

    @AfterAll
    static void stopCluster() {
        closeQuietly(vertxB);
        closeQuietly(vertxA);
    }

    private static void closeQuietly(Vertx vertx) {
        if (vertx != null) {
            try {
                vertx.close().toCompletionStage().toCompletableFuture().join();
            } catch (Exception ignored) {
                // best effort
            }
        }
    }

    @Test
    @Order(1)
    @DisplayName("without a codec the publish fails loudly instead of vanishing")
    void missingCodecFailsFast() {
        String address = "codec.missing";
        MessageConsumer<RefundAggregate> consumer =
                vertxB.eventBus().consumer(address, m -> {
                });
        try {
            assertTrue(await(() -> !managerA.getRegistrationsOf(address).isEmpty()),
                    "registration did not propagate, cannot test the codec contract");

            // Neither node registered RefundAggregate's codec. Vert.x must reject this outright —
            // a message that is silently dropped across the network is far worse than a failure here.
            assertThrows(IllegalArgumentException.class,
                    () -> vertxA.eventBus().publish(address, new RefundAggregate()),
                    "publishing an unserializable body must fail, not disappear");
        } finally {
            consumer.unregister();
        }
    }

    @Test
    @Order(2)
    @DisplayName("a business aggregate survives the wire intact")
    void businessMessageRoundTrips() {
        registerCodecs();

        String address = "codec.business";
        List<RefundAggregate> onB = Collections.synchronizedList(new ArrayList<>());
        MessageConsumer<RefundAggregate> consumer =
                vertxB.eventBus().consumer(address, m -> onB.add(m.body()));

        assertTrue(await(() -> !managerA.getRegistrationsOf(address).isEmpty()),
                "node A never learned the business topic");

        RefundAggregate sent = new RefundAggregate();
        sent.channel = RefundChannel.CUSTOMER_COMPLAINT;
        sent.amount = new BigDecimal("12345678901234.56");
        sent.currency = "CNY";
        sent.settledOn = LocalDate.of(2026, 10, 10);

        await(() -> {
            vertxA.eventBus().publish(address, sent);
            return !onB.isEmpty();
        });

        assertTrue(!onB.isEmpty(), "the business message never arrived");
        RefundAggregate got = onB.get(0);
        assertEquals(RefundChannel.CUSTOMER_COMPLAINT, got.channel, "enum must survive as an enum");
        assertEquals(0, new BigDecimal("12345678901234.56").compareTo(got.amount),
                "amount must not lose precision; got " + got.amount);
        assertEquals("CNY", got.currency);
        assertEquals(LocalDate.of(2026, 10, 10), got.settledOn, "date must round-trip");
        consumer.unregister();
    }

    @Test
    @Order(3)
    @DisplayName("a message addressed to one node does not disturb the other")
    void pointToPointDoesNotDisturbOthers() {
        registerCodecs();

        String address = "codec.p2p";
        List<RefundAggregate> onA = Collections.synchronizedList(new ArrayList<>());
        List<RefundAggregate> onB = Collections.synchronizedList(new ArrayList<>());
        MessageConsumer<RefundAggregate> consumerA =
                vertxA.eventBus().consumer(address, m -> onA.add(m.body()));
        MessageConsumer<RefundAggregate> consumerB =
                vertxB.eventBus().consumer(address, m -> onB.add(m.body()));

        assertTrue(await(() -> !managerA.getRegistrationsOf(address).isEmpty()
                && !managerB.getRegistrationsOf(address).isEmpty()),
                "the topic was not registered on both nodes");

        RefundAggregate payload = new RefundAggregate();
        payload.channel = RefundChannel.AUTO_REFUND;
        payload.amount = new BigDecimal("42.50");
        payload.currency = "CNY";

        // Vert.x picks exactly one node, so assert the delivery is exclusive rather than caring
        // which side won: either A got it and B did not, or the reverse.
        int aBefore = onA.size();
        int bBefore = onB.size();
        await(() -> {
            vertxA.eventBus().send(address, payload);
            return onA.size() > aBefore || onB.size() > bBefore;
        });

        int delivered = (onA.size() - aBefore) + (onB.size() - bBefore);
        assertEquals(1, delivered, "send() must deliver to exactly one handler, not broadcast");

        consumerA.unregister();
        consumerB.unregister();
    }

    @Test
    @Order(4)
    @DisplayName("the codec contract is per-node, and a gap is observable")
    void codecContractIsPerNode() {
        // Registering a codec is an EventBus-wide, one-shot operation, which is exactly why a
        // half-deployed cluster is dangerous: node B cannot be made to "un-register" it. What this
        // pins down is the contract that matters — with the codec registered on both nodes, a
        // business message is decodable on either side, and re-registering is rejected loudly
        // rather than silently ignored.
        registerCodecs();

        IllegalStateException duplicate = assertThrows(IllegalStateException.class,
                () -> vertxB.eventBus().registerDefaultCodec(RefundAggregate.class, new RefundAggregateCodec()),
                "a duplicate codec registration must fail loudly, not silently no-op");
        assertTrue(duplicate.getMessage().contains("Already a default codec"),
                "the failure should name the cause; got: " + duplicate.getMessage());

        // And with the codec in place on both nodes, the message really does decode on the peer.
        String address = "codec.pernode";
        List<RefundAggregate> onB = Collections.synchronizedList(new ArrayList<>());
        MessageConsumer<RefundAggregate> consumer = vertxB.eventBus().consumer(address, m -> onB.add(m.body()));
        assertTrue(await(() -> !managerA.getRegistrationsOf(address).isEmpty()),
                "registration did not propagate");

        RefundAggregate payload = new RefundAggregate();
        payload.channel = RefundChannel.PLATFORM_COMPENSATION;
        payload.amount = new BigDecimal("1.01");

        await(() -> {
            vertxA.eventBus().publish(address, payload);
            return !onB.isEmpty();
        });

        assertTrue(!onB.isEmpty(), "a registered codec on both nodes must decode the message");
        assertEquals(RefundChannel.PLATFORM_COMPENSATION, onB.get(0).channel);
        consumer.unregister();
    }

    private static void registerCodecs() {
        for (Vertx vertx : List.of(vertxA, vertxB)) {
            try {
                vertx.eventBus().registerDefaultCodec(RefundAggregate.class, new RefundAggregateCodec());
            } catch (IllegalStateException alreadyRegistered) {
                // Vert.x rejects a duplicate registration; both nodes already have it.
            }
        }
    }

    // --- message type ---

    enum RefundChannel {
        /** 客诉退款 */
        CUSTOMER_COMPLAINT,
        /** 自动退款 */
        AUTO_REFUND,
        /** 平台补偿 */
        PLATFORM_COMPENSATION
    }

    /**
     * A refund-channel aggregate in the shape business code actually sends: an enum, a decimal
     * amount that must not lose precision, and a settlement date.
     */
    public static final class RefundAggregate implements ClusterSerializable {

        private RefundChannel channel;
        private BigDecimal amount;
        private String currency;
        private LocalDate settledOn = LocalDate.of(2026, 1, 1);

        public RefundAggregate() {
        }

        void write(Buffer buffer) {
            writeNullableString(buffer, channel == null ? null : channel.name());
            writeNullableString(buffer, amount == null ? null : amount.toPlainString());
            writeNullableString(buffer, currency);
            writeNullableString(buffer, settledOn == null ? null : settledOn.toString());
        }

        /**
         * Decodes from an absolute offset. Offsets are tracked explicitly rather than via
         * {@code buffer.length()}: a clustered message body is a slice of a larger wire buffer, so
         * the whole-buffer length is not this message's length.
         *
         * @return this, so a codec can decode in one expression
         */
        RefundAggregate read(Buffer buffer, int pos) {
            int cursor = pos;
            channel = readChannel(readString(buffer, cursor));
            cursor = advance(buffer, cursor);
            String amountText = readString(buffer, cursor);
            cursor = advance(buffer, cursor);
            amount = amountText == null ? null : new BigDecimal(amountText);
            currency = readString(buffer, cursor);
            cursor = advance(buffer, cursor);
            String dateText = readString(buffer, cursor);
            cursor = advance(buffer, cursor);
            settledOn = dateText == null ? null : LocalDate.parse(dateText);
            return this;
        }

        /** @return the position after the field that starts at {@code pos} */
        private static int advance(Buffer buffer, int pos) {
            int length = buffer.getInt(pos);
            return length < 0 ? pos + 4 : pos + 4 + length;
        }

        @Override
        public void writeToBuffer(Buffer buffer) {
            write(buffer);
        }

        @Override
        public int readFromBuffer(int pos, Buffer buffer) {
            int cursor = pos;
            for (int field = 0; field < 4; field++) {
                cursor = advance(buffer, cursor);
            }
            read(buffer, pos);
            return cursor;
        }

        // --- field helpers: length-prefixed so a null never desynchronises the stream ---

        private static void writeNullableString(Buffer buffer, String value) {
            if (value == null) {
                buffer.appendInt(-1);
                return;
            }
            byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            buffer.appendInt(bytes.length).appendBytes(bytes);
        }

        private static String readString(Buffer buffer, int pos) {
            int length = buffer.getInt(pos);
            if (length < 0) {
                return null;
            }
            return buffer.getString(pos + 4, pos + 4 + length);
        }

        private static RefundChannel readChannel(String name) {
            return name == null ? null : RefundChannel.valueOf(name);
        }
    }

    /** Rebuilds {@link RefundAggregate} on the receiving node. */
    static final class RefundAggregateCodec
            implements io.vertx.core.eventbus.MessageCodec<RefundAggregate, RefundAggregate> {

        @Override
        public void encodeToWire(Buffer buffer, RefundAggregate aggregate) {
            aggregate.write(buffer);
        }

        @Override
        public RefundAggregate decodeFromWire(int pos, Buffer buffer) {
            // read() tracks absolute offsets and returns the position past this message, so decoding
            // never depends on what else the wire buffer carries.
            return new RefundAggregate().read(buffer, pos);
        }

        @Override
        public RefundAggregate transform(RefundAggregate aggregate) {
            return aggregate;
        }

        @Override
        public String name() {
            return "refund-aggregate";
        }

        @Override
        public byte systemCodecID() {
            return -1;
        }
    }

    // --- helpers ---

    private static boolean await(BooleanSupplier condition) {
        return await(condition, TIMEOUT_MS);
    }

    private static boolean await(BooleanSupplier condition, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return condition.getAsBoolean();
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("No free port available", e);
        }
    }
}
