package com.workoss.boot.zenoh.example;

import com.workoss.boot.zenoh.cluster.ZenohCluster;
import com.workoss.boot.zenoh.cluster.autoconfigure.ZenohClusterHealthIndicator;
import com.workoss.boot.zenoh.cluster.autoconfigure.ZenohClusterProperties;
import com.workoss.boot.zenoh.cluster.internal.transport.ZenohTransport.Mode;
import com.workoss.zenoh.link.LinkProtocol;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.io.FileSystemResource;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Single-node integration test: boots the example application and verifies the four cluster
 * capabilities wired through the Spring Boot autoconfiguration and exercised via the demo
 * controller. Membership, shared map, counter, lock and self-broadcast all run on one node.
 */
@SpringBootTest(webEnvironment = WebEnvironment.NONE, properties = {
        "spring.profiles.active=testsingle",
        "zenoh.cluster.app-name=ex-single",
        "zenoh.cluster.environment=test",
        "zenoh.cluster.node-id=node-single",
        "zenoh.cluster.mode=peer",
        "zenoh.cluster.listen=tcp/127.0.0.1:17747",
        "zenoh.cluster.heartbeat-interval=200ms",
        "zenoh.cluster.failure-timeout=1s",
        "zenoh.cluster.anti-entropy-interval=0"
})
class ZenohClusterExampleApplicationTests {

    @Autowired
    private ZenohCluster cluster;

    @Autowired
    private ClusterDemoController controller;

    @Autowired
    private MessageCollector collector;

    @Autowired(required = false)
    private ZenohClusterHealthIndicator healthIndicator;

    @Test
    void contextLoadsAndClusterIsActive() {
        assertTrue(cluster.isActive(), "cluster should be running after context refresh");
        assertEquals("node-single", cluster.nodeId());
        assertTrue(cluster.members().stream().anyMatch(m -> m.nodeId().equals("node-single")),
                "this node must be a member of itself");
    }

    @Test
    void k8sProfileLoadsDownwardApiValuesWithoutStaticConnect() {
        var runner = new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withPropertyValues("spring.profiles.active=k8s", "POD_NAME=zenoh-cluster-app-abc",
                        "POD_NAMESPACE=zenoh-demo", "POD_IP=10.0.0.1");
        runner.run(context -> {
            var properties = Binder.get(context.getEnvironment())
                    .bind("zenoh.cluster", Bindable.of(ZenohClusterProperties.class)).get();
            assertEquals("zenoh-cluster-app-abc", properties.getNodeId());
            assertEquals("prod", properties.getEnvironment());
            assertEquals(Mode.PEER, properties.getMode());
            assertEquals(LinkProtocol.TCP, properties.getProtocol());
            assertTrue(properties.getConnect().isEmpty());
            assertEquals(List.of("10.0.0.1:7447"), properties.getListen());
            assertEquals("zenoh-peers.zenoh-demo.svc.cluster.local", properties.getDns().getName());
            assertEquals("10.0.0.1", properties.getDns().getSelfAddress());
            assertEquals(7447, properties.getDns().getPort());
            assertEquals(Duration.ofSeconds(5), properties.getDns().getRefreshInterval());
            assertEquals(Duration.ofSeconds(2), properties.getDns().getTimeout());
            assertEquals(Boolean.FALSE, properties.getScouting().getMulticastEnabled());
            assertEquals(Boolean.TRUE, properties.getScouting().getGossipEnabled());
            assertEquals(Boolean.TRUE, properties.getScouting().getGossipMultihop());
            assertEquals("peer", properties.getScouting().getGossipAutoconnect());
        });
        runner.withPropertyValues("ZENOH_DNS_NAME=custom-peers.other.svc.cluster.local").run(context -> {
            var properties = Binder.get(context.getEnvironment())
                    .bind("zenoh.cluster", Bindable.of(ZenohClusterProperties.class)).get();
            assertEquals("custom-peers.other.svc.cluster.local", properties.getDns().getName());
        });
    }

    @Test
    void k8sManifestProvidesPeerDiscoveryWithoutRouter() throws IOException {
        var documents = new YamlPropertySourceLoader().load("k8s", new FileSystemResource("k8s/10-app.yaml"));
        assertEquals(4, documents.size());
        assertEquals("Namespace", documents.get(0).getProperty("kind"));
        var discovery = documents.get(1);
        assertEquals("Service", discovery.getProperty("kind"));
        assertEquals("zenoh-peers", discovery.getProperty("metadata.name"));
        assertEquals("None", discovery.getProperty("spec.clusterIP"));
        assertEquals(Boolean.TRUE, discovery.getProperty("spec.publishNotReadyAddresses"));
        var deployment = documents.get(3);
        assertEquals("Deployment", deployment.getProperty("kind"));
        assertEquals(3, deployment.getProperty("spec.replicas"));
        assertEquals(discovery.getProperty("spec.selector.app"),
                deployment.getProperty("spec.template.metadata.labels.app"));
        assertEquals("k8s", deployment.getProperty("spec.template.spec.containers[0].env[0].value"));
        assertEquals("metadata.name", deployment.getProperty(
                "spec.template.spec.containers[0].env[1].valueFrom.fieldRef.fieldPath"));
        assertEquals("metadata.namespace", deployment.getProperty(
                "spec.template.spec.containers[0].env[2].valueFrom.fieldRef.fieldPath"));
        assertEquals("status.podIP", deployment.getProperty(
                "spec.template.spec.containers[0].env[3].valueFrom.fieldRef.fieldPath"));
    }

    @Test
    void sharedMapPutGetThroughController() {
        controller.mapSet("demo", "k", "v", 0L);
        var body = controller.mapGet("demo", "k");
        assertEquals("ok", body.get("status"));
        assertEquals("v", body.get("value"));
    }

    @Test
    void counterIncrements() {
        controller.counterInc("hits", 3);
        var body = controller.counterGet("hits");
        assertEquals(3L, ((Number) body.get("value")).longValue());
    }

    @Test
    void lockIsAcquiredAndReleasedOnSingleNode() {
        var body = controller.lockWith("job", 200);
        assertEquals("ok", body.get("status"));
        assertEquals(true, body.get("held"));
    }

    @Test
    void broadcastIsDeliveredToTheSender() {
        controller.broadcast("hello-from-single", MessageCollector.DEMO_BROADCAST_ADDRESS);
        AtomicReference<MessageCollector.ReceivedView> found = new AtomicReference<>();
        await(() -> collector.recent().stream()
                        .filter(m -> "hello-from-single".equals(m.payload()) && "node-single".equals(m.sourceNodeId()))
                        .findFirst()
                        .map(m -> {
                            found.set(m);
                            return true;
                        })
                        .orElse(false),
                5000, "sender receives its own broadcast");
        assertNotNull(found.get());
        assertEquals(MessageCollector.DEMO_BROADCAST_ADDRESS, found.get().address());
    }

    @Test
    void healthIndicatorIsUp() {
        assertNotNull(healthIndicator, "ZenohClusterHealthIndicator bean should be present (actuator on classpath)");
        var health = healthIndicator.health();
        assertEquals(org.springframework.boot.actuate.health.Status.UP, health.getStatus());
    }

    @Test
    void annotatedSubscribeHandlersFire() {
        // exact broadcast -> ClusterMessage handler
        controller.broadcast("exact-1", MessageCollector.DEMO_BROADCAST_ADDRESS);
        // wildcard broadcast -> ClusterMessage handler
        controller.demoAlert("eu", "alert-1");
        // wildcard broadcast -> byte[] handler
        controller.demoRaw("cpu", "raw-1");
        // wildcard broadcast -> String handler
        controller.demoNotice("deploy", "notice-1");
        // point-to-point to self -> ClusterMessage direct handler
        controller.send("node-single", "direct-1");

        awaitView(m -> "broadcast".equals(m.kind()) && MessageCollector.DEMO_BROADCAST_ADDRESS.equals(m.address())
                && "exact-1".equals(m.payload()) && "node-single".equals(m.sourceNodeId()), "exact @ZenohSubscribe handler");
        awaitView(m -> "alert".equals(m.kind()) && "alerts/eu".equals(m.address())
                && "alert-1".equals(m.payload()), "wildcard @ZenohSubscribe (alerts/**) handler");
        awaitView(m -> "raw".equals(m.kind()) && "raw-1".equals(m.payload())
                && m.address() == null, "byte[] @ZenohSubscribe (raw/**) handler");
        awaitView(m -> "notice".equals(m.kind()) && "notice-1".equals(m.payload())
                && m.address() == null, "String @ZenohSubscribe (notices/**) handler");
        awaitView(m -> "direct".equals(m.kind()) && "direct-1".equals(m.payload())
                && "node-single".equals(m.targetNodeId()), "direct @ZenohSubscribe handler");
    }

    @Test
    void replyToPublishesTheReturnValue() {
        controller.demoEcho("hi");
        awaitView(m -> "reply".equals(m.kind())
                        && MessageCollector.ECHO_REPLY_ADDRESS.equals(m.address())
                        && "echo:hi".equals(m.payload())
                        && "node-single".equals(m.sourceNodeId()),
                "the handler's return value was published to the replyTo address");
    }

    @Test
    void futureReplyIsPublishedWithoutBlockingTheSender() {
        long startedAt = System.currentTimeMillis();
        controller.demoSlowEcho("later");
        long requestMillis = System.currentTimeMillis() - startedAt;
        assertTrue(requestMillis < 400,
                "the request must not wait for the future, took " + requestMillis + " ms");
        awaitView(m -> "reply".equals(m.kind()) && "slow-echo:later".equals(m.payload()),
                "the future's result was published once it completed");
    }

    @Test
    void orderedHandlersRunInDeclaredSequence() {
        controller.demoOrdered("x");
        await(() -> collector.recentOrderedSequence().containsAll(List.of("first:x", "second:x")),
                5000, "both ordered handlers ran");
        List<String> sequence = collector.recentOrderedSequence();
        int first = sequence.indexOf("first:x");
        int second = sequence.indexOf("second:x");
        assertTrue(first >= 0 && second >= 0 && first < second,
                "@Order(1) must run before @Order(2), got " + sequence);
    }

    private void awaitView(java.util.function.Predicate<MessageCollector.ReceivedView> predicate,
                           String description) {
        long deadline = System.nanoTime() + Duration.ofMillis(5000).toNanos();
        while (System.nanoTime() < deadline) {
            if (collector.recent().stream().anyMatch(predicate)) {
                return;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new AssertionError("Not satisfied within 5000 ms: " + description);
    }

    private static void await(java.util.function.BooleanSupplier condition, long timeoutMillis, String description) {
        long deadline = System.nanoTime() + Duration.ofMillis(timeoutMillis).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new AssertionError("Not satisfied within " + timeoutMillis + " ms: " + description);
    }
}
