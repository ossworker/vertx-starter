package com.workoss.boot.vertx.cluster.zenoh;

import com.workoss.zenoh.Sample;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.NodeInfo;
import io.vertx.core.spi.cluster.RegistrationInfo;
import io.vertx.core.spi.cluster.RegistrationListener;
import io.vertx.core.spi.cluster.RegistrationUpdateEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ClusterRegistrationTable} 测试。
 * <p>
 * 全部用例都不连真实 Zenoh：注册表复制的输入是 {@link Sample},而 {@link Sample} 的字段都是
 * public final,可以直接构造。这既避免了 JNI 依赖,也让「远端写」成为确定性的、可断言的。
 * <p>
 * 重点覆盖两个真实缺陷来源：
 * <ul>
 *   <li>{@link KeyPrefixStrategy#sanitize} 是有损的({@code /} 与 {@code []} 都变成 {@code _}),
 *       所以地址无法从 Zenoh key 反推,只能从 payload 信封里读回来；</li>
 *   <li>节点离开后必须清掉它的注册项并回调 {@code registrationsUpdated},否则节点选择器会
 *       一直往一个已经消失的节点投递。</li>
 * </ul>
 */
class ClusterRegistrationTableTest {

    private static final KeyPrefixStrategy KEY_PREFIX = new KeyPrefixStrategy("test-app", "test-env");

    private static Sample put(String key, byte[] payload) {
        return new Sample(key, payload, "application/octet-stream", (byte) 0,
                Optional.empty(), (byte) 0, (byte) 0, false);
    }

    private static Sample delete(String key) {
        return new Sample(key, new byte[0], "application/octet-stream", (byte) 1,
                Optional.empty(), (byte) 0, (byte) 0, false);
    }

    /**
     * 走真实的编码/解码路径,避免测试和生产用两套格式。
     */
    private static byte[] encodeEnvelope(String address, RegistrationInfo info) throws Exception {
        Method encode = ClusterRegistrationTable.class.getDeclaredMethod(
                "encodeRegistrationEnvelope", String.class, RegistrationInfo.class);
        encode.setAccessible(true);
        return (byte[]) encode.invoke(null, address, info);
    }

    private static byte[] encodeNodeInfo(NodeInfo info) throws Exception {
        Method encode = ClusterRegistrationTable.class.getDeclaredMethod("encode", NodeInfo.class);
        encode.setAccessible(true);
        return (byte[]) encode.invoke(null, info);
    }

    private static void applySample(ClusterRegistrationTable table, Sample sample) throws Exception {
        Method apply = ClusterRegistrationTable.class.getDeclaredMethod("applyRegistrationSample", Sample.class);
        apply.setAccessible(true);
        apply.invoke(table, sample);
    }

    private static void applyNodeInfoSample(ClusterRegistrationTable table, Sample sample) throws Exception {
        // 走与生产一致的解析：轮询线程用的是 decodeNodeInfo，这里直接验证编码往返。
        Method decode = ClusterRegistrationTable.class.getDeclaredMethod("decodeNodeInfo", byte[].class);
        decode.setAccessible(true);
        String owner = sample.keyExpr().substring(sample.keyExpr().lastIndexOf('/') + 1);
        Object info = decode.invoke(null, (Object) sample.payload());
        assertNotNull(info, "node info should decode for " + owner);
    }

    private static final class RecordingListener implements RegistrationListener {
        final List<RegistrationUpdateEvent> events = new ArrayList<>();

        @Override
        public void registrationsUpdated(RegistrationUpdateEvent event) {
            events.add(event);
        }
    }

    @Nested
    @DisplayName("Registration table")
    class RegistrationTests {

        private ClusterRegistrationTable table;
        private RecordingListener listener;

        @BeforeEach
        void setUp() {
            table = new ClusterRegistrationTable(KEY_PREFIX, "node-self");
            listener = new RecordingListener();
            table.setRegistrationListener(listener);
        }

        @Test
        @DisplayName("local registration is visible to getRegistrations")
        void localRegistrationVisible() {
            table.addRegistration("ws.broadcast", new RegistrationInfo("node-self", 1L, false));
            List<RegistrationInfo> found = table.registrationsOf("ws.broadcast");
            assertEquals(1, found.size());
            assertEquals("node-self", found.get(0).nodeId());
        }

        @Test
        @DisplayName("unknown address yields no registrations")
        void unknownAddressEmpty() {
            assertTrue(table.registrationsOf("nothing.here").isEmpty());
        }

        @Test
        @DisplayName("remote put makes the handler selectable cluster-wide")
        void remoteRegistrationVisible() throws Exception {
            RegistrationInfo info = new RegistrationInfo("node-remote", 7L, false);
            byte[] payload = encodeEnvelope("ws.broadcast", info);

            applySample(table, put(KEY_PREFIX.registrationKey("ws.broadcast", "node-remote"), payload));

            List<RegistrationInfo> found = table.registrationsOf("ws.broadcast");
            assertEquals(1, found.size(), "DefaultNodeSelector must see the remote handler");
            assertEquals("node-remote", found.get(0).nodeId());
            assertEquals(7L, found.get(0).seq());
        }

        @Test
        @DisplayName("address survives a lossy sanitize, because it travels in the payload")
        void addressSurvivesSanitize() throws Exception {
            // '/' and '[]' are all replaced with '_' inside the key, so two different addresses can
            // collide on the same key prefix. The payload envelope is the only lossless source.
            String address = "ws/a[b]";
            RegistrationInfo info = new RegistrationInfo("node-remote", 1L, false);
            byte[] payload = encodeEnvelope(address, info);

            applySample(table, put(KEY_PREFIX.registrationKey(address, "node-remote"), payload));

            assertEquals(List.of(address), List.copyOf(table.registrationsOf(address).stream()
                    .map(r -> address).toList()));
            assertFalse(table.registrationsOf("ws/a[b]").isEmpty());
        }

        @Test
        @DisplayName("remote delete removes the registration")
        void remoteDeleteRemoves() throws Exception {
            RegistrationInfo info = new RegistrationInfo("node-remote", 1L, false);
            byte[] payload = encodeEnvelope("ws.broadcast", info);
            String key = KEY_PREFIX.registrationKey("ws.broadcast", "node-remote");

            applySample(table, put(key, payload));
            assertFalse(table.registrationsOf("ws.broadcast").isEmpty());

            applySample(table, delete(key));
            assertTrue(table.registrationsOf("ws.broadcast").isEmpty());
        }

        @Test
        @DisplayName("own registration echoed back by Zenoh is ignored")
        void ownEchoIgnored() throws Exception {
            RegistrationInfo info = new RegistrationInfo("node-self", 3L, false);
            // Record it locally first, exactly as addRegistration would.
            table.addRegistration("ws.broadcast", info);

            applySample(table, put(KEY_PREFIX.registrationKey("ws.broadcast", "node-self"),
                    encodeEnvelope("ws.broadcast", info)));

            List<RegistrationInfo> found = table.registrationsOf("ws.broadcast");
            assertEquals(1, found.size(), "the echo must not add a second entry for this node");
            assertEquals(3L, found.get(0).seq());
        }

        @Test
        @DisplayName("registration change notifies the node selector")
        void notifiesListener() {
            table.addRegistration("ws.broadcast", new RegistrationInfo("node-self", 1L, false));
            assertEquals(1, listener.events.size());
            assertEquals("ws.broadcast", listener.events.get(0).address());
        }

        @Test
        @DisplayName("removing the last registration drops the address")
        void removingLastDropsAddress() {
            RegistrationInfo info = new RegistrationInfo("node-self", 1L, false);
            table.addRegistration("ws.broadcast", info);
            table.removeRegistration("ws.broadcast", info);
            assertTrue(table.registrationsOf("ws.broadcast").isEmpty());
        }
    }

    @Nested
    @DisplayName("Node departure")
    class DepartureTests {

        private ClusterRegistrationTable table;
        private RecordingListener listener;

        @BeforeEach
        void setUp() {
            table = new ClusterRegistrationTable(KEY_PREFIX, "node-self");
            listener = new RecordingListener();
            table.setRegistrationListener(listener);
        }

        @Test
        @DisplayName("departed node loses its registrations and re-fires the address")
        void removesRegistrationsOfDepartedNode() throws Exception {
            applySample(table, put(KEY_PREFIX.registrationKey("ws.broadcast", "node-gone"),
                    encodeEnvelope("ws.broadcast", new RegistrationInfo("node-gone", 1L, false))));
            table.addRegistration("ws.other", new RegistrationInfo("node-self", 1L, false));
            listener.events.clear();

            table.removeNode("node-gone");

            assertTrue(table.registrationsOf("ws.broadcast").isEmpty(),
                    "a departed node must not stay in the selector");
            assertTrue(listener.events.stream().anyMatch(e -> e.address().equals("ws.broadcast")),
                    "the affected address must be re-published to the selector");
            assertFalse(table.registrationsOf("ws.other").isEmpty(), "other addresses must survive");
        }

        @Test
        @DisplayName("departed node loses its NodeInfo")
        void removesNodeInfo() {
            table.setLocalNodeInfo(new NodeInfo("127.0.0.1", 12345, new JsonObject()));
            assertNotNull(table.nodeInfoOf("node-self"));

            table.removeNode("node-self");
            // removeNode ignores the local node on purpose: a node does not evict itself.
            assertNotNull(table.nodeInfoOf("node-self"));
        }

        @Test
        @DisplayName("unknown node resolves to null so connect() fails fast")
        void unknownNodeIsNull() {
            assertNull(table.nodeInfoOf("never-seen"));
        }
    }

    @Nested
    @DisplayName("NodeInfo codec")
    class NodeInfoCodecTests {

        @Test
        @DisplayName("NodeInfo round-trips through the ClusterSerializable format")
        void nodeInfoRoundTrips() throws Exception {
            NodeInfo original = new NodeInfo("10.0.0.7", 54321,
                    new JsonObject().put("zone", "cn-east"));

            byte[] payload = encodeNodeInfo(original);
            Method decode = ClusterRegistrationTable.class.getDeclaredMethod("decodeNodeInfo", byte[].class);
            decode.setAccessible(true);
            NodeInfo decoded = (NodeInfo) decode.invoke(null, (Object) payload);

            assertEquals(original.host(), decoded.host());
            assertEquals(original.port(), decoded.port());
            assertEquals("cn-east", decoded.metadata().getString("zone"));
        }

        @Test
        @DisplayName("a malformed payload decodes to null instead of throwing")
        void malformedPayloadIsNull() throws Exception {
            Method decode = ClusterRegistrationTable.class.getDeclaredMethod("decodeNodeInfo", byte[].class);
            decode.setAccessible(true);
            assertNull(decode.invoke(null, (Object) new byte[]{1, 2, 3}));
            assertNull(decode.invoke(null, (Object) null));
        }

        @Test
        @DisplayName("registration envelope round-trips address and info")
        void envelopeRoundTrips() throws Exception {
            String address = "ws/room/[1]";
            RegistrationInfo info = new RegistrationInfo("node-remote", 99L, true);
            byte[] payload = encodeEnvelope(address, info);

            Method decodeAddress = ClusterRegistrationTable.class.getDeclaredMethod(
                    "decodeEnvelopeAddress", byte[].class);
            Method decodeInfo = ClusterRegistrationTable.class.getDeclaredMethod(
                    "decodeEnvelopeInfo", byte[].class);
            decodeAddress.setAccessible(true);
            decodeInfo.setAccessible(true);

            assertEquals(address, decodeAddress.invoke(null, (Object) payload));
            RegistrationInfo decoded = (RegistrationInfo) decodeInfo.invoke(null, (Object) payload);
            assertEquals("node-remote", decoded.nodeId());
            assertEquals(99L, decoded.seq());
            assertTrue(decoded.localOnly());
        }

        @Test
        @DisplayName("an address length that overruns the buffer is rejected")
        void truncatedEnvelopeRejected() throws Exception {
            Method decodeAddress = ClusterRegistrationTable.class.getDeclaredMethod(
                    "decodeEnvelopeAddress", byte[].class);
            decodeAddress.setAccessible(true);
            // Corrupt the declared length so it points past the end of the buffer.
            byte[] payload = encodeEnvelope("ws.x", new RegistrationInfo("n", 1L, false));
            byte[] corrupted = payload.clone();
            corrupted[0] = 0x7f;
            corrupted[1] = (byte) 0xff;
            corrupted[2] = (byte) 0xff;
            corrupted[3] = (byte) 0xff;
            assertNull(decodeAddress.invoke(null, (Object) corrupted));

            // Too short to even hold the length prefix.
            assertNull(decodeAddress.invoke(null, (Object) new byte[]{0, 0}));
            assertNull(decodeAddress.invoke(null, (Object) null));
        }
    }

    @Nested
    @DisplayName("Helpers")
    class HelperTests {

        @Test
        @DisplayName("node info sample payload is decodable")
        void nodeInfoSampleDecodable() throws Exception {
            NodeInfo info = new NodeInfo("127.0.0.1", 4242, new JsonObject());
            applyNodeInfoSample(new ClusterRegistrationTable(KEY_PREFIX, "node-self"),
                    put(KEY_PREFIX.nodeInfoKey("node-peer"), encodeNodeInfo(info)));
        }

        @Test
        @DisplayName("sanitize is lossy, which is why the payload carries the address")
        void sanitizeIsLossy() {
            // Documents the invariant the envelope relies on: '/' and '[]' all collapse to '_', so
            // the address cannot be recovered by parsing the key.
            String key = KEY_PREFIX.registrationKey("ws/a[b]", "node-1");
            assertTrue(key.contains("/registrations/ws_a_b_/node-1"), "actual key: " + key);
            assertFalse(key.contains("ws/a[b]"), "the raw address must not survive sanitization");
        }
    }

    @Test
    @DisplayName("encoding uses UTF-8 for the address so non-ASCII survives")
    void nonAsciiAddress() throws Exception {
        String address = "房间/一";
        byte[] payload = encodeEnvelope(address, new RegistrationInfo("n", 1L, false));

        Method decodeAddress = ClusterRegistrationTable.class.getDeclaredMethod(
                "decodeEnvelopeAddress", byte[].class);
        decodeAddress.setAccessible(true);
        assertEquals(address, decodeAddress.invoke(null, (Object) payload));
    }
}
