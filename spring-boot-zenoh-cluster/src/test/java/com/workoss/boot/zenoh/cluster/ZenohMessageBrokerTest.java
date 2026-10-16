package com.workoss.boot.zenoh.cluster;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Two {@link ZenohClusterManager}s on one Zenoh network exercise the message broker: broadcast
 * (including self-delivery), point-to-point targeting, and the source node id carried on every
 * message.
 */
@DisplayName("Cluster message broker: broadcast and point-to-point")
class ZenohMessageBrokerTest {

    private static final String APP = "broker";

    private int port;
    private ZenohClusterManager managerA;
    private ZenohClusterManager managerB;

    private final List<ClusterMessage> receivedOnA = new CopyOnWriteArrayList<>();
    private final List<ClusterMessage> receivedOnB = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        port = ZenohTestSupport.freePort();
        managerA = new ZenohClusterManager(ZenohTestSupport.peerProperties(APP, "node-a", port));
        managerB = new ZenohClusterManager(ZenohTestSupport.clientProperties(APP, "node-b", port));
        managerA.start();
        managerB.start();

        managerA.getMessageBroker().subscribe("news", receivedOnA::add);
        managerA.getMessageBroker().onNodeMessage(receivedOnA::add);
        managerB.getMessageBroker().subscribe("news", receivedOnB::add);
        managerB.getMessageBroker().onNodeMessage(receivedOnB::add);
    }

    @AfterEach
    void tearDown() {
        managerB.stop();
        managerA.stop();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("broadcast from A reaches B")
    void broadcastAtoB() {
        managerA.getMessageBroker().broadcast("news", bytes("hello"));
        ZenohTestSupport.await(() -> receivedOnB.stream()
                        .anyMatch(m -> "news".equals(m.address()) && "hello".equals(m.payloadAsString())),
                5000, "B received broadcast 'hello' on 'news'");
        ClusterMessage m = receivedOnB.stream()
                .filter(msg -> "news".equals(msg.address())).findFirst().orElseThrow();
        assertEquals("node-a", m.sourceNodeId());
        assertEquals("hello", m.payloadAsString());
    }

    @Test
    @DisplayName("broadcast is delivered to the sender too")
    void broadcastSelfDelivery() {
        managerA.getMessageBroker().broadcast("news", bytes("self"));
        ZenohTestSupport.await(() -> receivedOnA.stream()
                        .anyMatch(m -> "news".equals(m.address()) && "self".equals(m.payloadAsString())),
                5000, "A received its own broadcast");
    }

    @Test
    @DisplayName("sendToNode delivers only to the target node")
    void pointToPoint() {
        managerA.getMessageBroker().sendToNode("node-b", bytes("pm"));
        ZenohTestSupport.await(() -> receivedOnB.stream()
                        .anyMatch(m -> m.targetNodeId() != null && "pm".equals(m.payloadAsString())),
                5000, "B received point-to-point 'pm'");
        ClusterMessage m = receivedOnB.stream()
                .filter(msg -> msg.targetNodeId() != null).findFirst().orElseThrow();
        assertEquals("node-b", m.targetNodeId());
        assertEquals("node-a", m.sourceNodeId());
        // A must not see the message it addressed to B.
        assertTrue(receivedOnA.stream().noneMatch(msg -> "pm".equals(msg.payloadAsString())),
                "A should not receive a message addressed to B");
    }

    @Test
    @DisplayName("point-to-point from B to A is delivered")
    void pointToPointReverse() {
        managerB.getMessageBroker().sendToNode("node-a", bytes("back"));
        ZenohTestSupport.await(() -> receivedOnA.stream()
                        .anyMatch(m -> m.targetNodeId() != null && "back".equals(m.payloadAsString())),
                5000, "A received point-to-point 'back' from B");
    }
}
