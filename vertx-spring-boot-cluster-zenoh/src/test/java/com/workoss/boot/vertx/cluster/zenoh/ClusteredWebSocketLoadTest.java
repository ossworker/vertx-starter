package com.workoss.boot.vertx.cluster.zenoh;

import com.workoss.boot.vertx.web.server.VertxServerHttpRequest;
import com.workoss.boot.vertx.web.server.VertxServerHttpResponse;
import com.workoss.boot.vertx.web.server.websocket.ClusterWebSocketBroadcaster;
import com.workoss.boot.vertx.web.server.websocket.VertxRequestUpgradeStrategy;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.reactive.socket.HandshakeInfo;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.ServerWebExchangeDecorator;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Clustered WebSocket fan-out under load. Opt-in: {@code -Dws.load=true}.
 * <p>
 * {@link ClusteredWebSocketE2ETest} proves the cross-node path works with one session per node. This
 * measures what it costs at IM scale, which is the thing the design doc promised as acceptance item
 * T12 (500 concurrent connections x 100 messages) and never delivered. Two scenarios:
 * <ul>
 *   <li><b>echo</b> — every connection is its own group, so each message comes straight back.
 *       Isolates the upgrade + write path from fan-out amplification.</li>
 *   <li><b>room</b> — connections are spread over rooms that straddle both nodes, so every message
 *       is delivered to the whole room, half of it over the clustered event bus. This is the real
 *       IM shape and the one where amplification shows up.</li>
 * </ul>
 * Reported per scenario: send rate, delivery rate, p50/p90/p99/max delivery latency, lost frames,
 * out-of-order frames, event-loop jitter and heap growth. Lost and out-of-order are asserted zero.
 * <p>
 * Latency is measured from the sender's {@code System.nanoTime()} stamped into the payload to the
 * receiver's clock. Both live in this JVM, so the two are directly comparable.
 * <p>
 * The cluster and server scaffolding mirrors {@link ClusteredWebSocketE2ETest} rather than sharing
 * it: that test is the only proof the production upgrade path works, and a benchmark harness that
 * can be tuned freely is worth the duplication.
 * <p>
 * Scale is configurable: {@code ws.load.connections} (500), {@code ws.load.echoMessages} (100),
 * {@code ws.load.rooms} (10), {@code ws.load.roomMessages} (10), {@code ws.load.payloadBytes} (200).
 * {@code connections} must be divisible by {@code rooms} so every room has the same membership and
 * the expected delivery count stays exact.
 * <p>
 * The report goes to {@code System.out}, not to a logger: this module's test classpath has slf4j-api
 * but no provider, so every logged line would be silently dropped by the NOP logger.
 */
@EnabledIfSystemProperty(named = "ws.load", matches = "true")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClusteredWebSocketLoadTest {

    private static final String APP = "load-ws-" + UUID.randomUUID().toString().substring(0, 8);
    private static final String PATH = "/ws/load";

    private static final int CONNECTIONS = intProp("ws.load.connections", 500);
    private static final int ECHO_MESSAGES = intProp("ws.load.echoMessages", 100);
    private static final int ROOMS = intProp("ws.load.rooms", 10);
    private static final int ROOM_MESSAGES = intProp("ws.load.roomMessages", 10);
    private static final int PAYLOAD_BYTES = intProp("ws.load.payloadBytes", 200);
    /** Distinct JDK clients, so no single selector thread becomes the measurement bottleneck. */
    private static final int CLIENT_POOL = intProp("ws.load.clientPool", 4);

    private static final long READY_TIMEOUT_MS = 120_000L;
    private static final long SEND_TIMEOUT_MIN = 10L;
    private static final long DRAIN_TIMEOUT_MS = 300_000L;

    private static Vertx vertxA;
    private static Vertx vertxB;
    private static ZenohClusterManager managerA;
    private static ZenohClusterManager managerB;
    private static HttpServer serverA;
    private static HttpServer serverB;
    private static ClusterWebSocketBroadcaster broadcasterA;
    private static ClusterWebSocketBroadcaster broadcasterB;
    private static int portA;
    private static int portB;
    private static final List<HttpClient> CLIENT_POOL_INSTANCES = new ArrayList<>();

    @BeforeAll
    static void startCluster() throws Exception {
        long joinStartedAt = System.nanoTime();
        int zenohPort = freePort();
        managerA = new ZenohClusterManager(peerProperties(zenohPort));
        vertxA = clustered(managerA, freePort());
        Thread.sleep(500);
        managerB = new ZenohClusterManager(clientProperties(zenohPort));
        vertxB = clustered(managerB, freePort());

        broadcasterA = new ClusterWebSocketBroadcaster(vertxA);
        broadcasterA.registerListener();
        broadcasterB = new ClusterWebSocketBroadcaster(vertxB);
        broadcasterB.registerListener();

        portA = startServer(vertxA, broadcasterA);
        portB = startServer(vertxB, broadcasterB);

        deployProbes(vertxA);
        deployProbes(vertxB);

        for (int i = 0; i < CLIENT_POOL; i++) {
            CLIENT_POOL_INSTANCES.add(HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(30))
                    .executor(Executors.newVirtualThreadPerTaskExecutor())
                    .build());
        }
        // Gate on both directions being routable before measuring anything. A late joiner cannot
        // resolve a pre-existing peer's node info until that peer re-publishes it on its heartbeat
        // cadence, and until then every publish it makes to that peer is dropped with only a
        // "Node info not found" warning. Without this gate the numbers below would describe the join
        // race rather than steady-state fan-out.
        assertTrue(awaitUntil(() -> managerA.getNodeInfoOf(managerB.getNodeId()) != null
                        && managerB.getNodeInfoOf(managerA.getNodeId()) != null),
                "cluster never became routable in both directions: A resolves B="
                        + (managerA.getNodeInfoOf(managerB.getNodeId()) != null)
                        + ", B resolves A=" + (managerB.getNodeInfoOf(managerA.getNodeId()) != null));
        long convergeMillis = (System.nanoTime() - joinStartedAt) / 1_000_000;

        System.out.printf("Load cluster ready: app=%s nodes=A(peer,ws:%d)/B(client,ws:%d) connections=%d rooms=%d%n",
                APP, portA, portB, CONNECTIONS, ROOMS);
        System.out.printf("routing converged in %d ms from first node start%n", convergeMillis);
    }

    @AfterAll
    static void stopCluster() {
        closeQuietly(serverA);
        closeQuietly(serverB);
        closeQuietly(vertxB);
        closeQuietly(vertxA);
    }

    // --- scenarios ---

    @Test
    @Order(1)
    @DisplayName("500 connections x 100 echoed messages: no loss, no reordering, no session leak")
    void echoAtScale() throws Exception {
        List<LoadClient> clients = connect(CONNECTIONS, i -> "solo-" + i, ClusteredWebSocketLoadTest::nodeOfIndex);
        try {
            warmUp(clients, 1);
            Report report = runPhase("echo", clients, ECHO_MESSAGES, 1);
            report.print();
            printRoutingDiagnostics("echo");
            assertClean(report);
        } finally {
            closeAll(clients);
            assertNoSessionLeak();
        }
    }

    @Test
    @Order(2)
    @DisplayName("cross-node room broadcast: every room member on both nodes receives every message")
    void roomBroadcastAtScale() throws Exception {
        if (CONNECTIONS % ROOMS != 0) {
            fail("ws.load.connections (" + CONNECTIONS + ") must be divisible by ws.load.rooms (" + ROOMS
                    + ") so each room has an exact membership");
        }
        int roomSize = CONNECTIONS / ROOMS;
        // Node and room are assigned on independent axes on purpose. Pairing node = i % 2 with
        // room = i % ROOMS correlates them whenever ROOMS is even, which puts every room entirely on
        // one node and turns the cross-node scenario into a local one that can never lose a frame.
        List<LoadClient> clients = connect(CONNECTIONS, i -> "room-" + (i % ROOMS),
                i -> nodeOfIndex(i / ROOMS));
        try {
            assertRoomsStraddleNodes(clients);
            warmUp(clients, roomSize);
            Report report = runPhase("room", clients, ROOM_MESSAGES, roomSize);
            report.print();
            printRoutingDiagnostics("room");
            assertClean(report);
        } finally {
            closeAll(clients);
            assertNoSessionLeak();
        }
    }

    /** Alternates nodes so both hold half the connections. */
    private static int nodeOfIndex(int index) {
        return index % 2;
    }

    private static void assertRoomsStraddleNodes(List<LoadClient> clients) {
        Map<String, Set<Integer>> nodesPerRoom = new HashMap<>();
        for (LoadClient client : clients) {
            nodesPerRoom.computeIfAbsent(client.group, key -> new HashSet<>()).add(client.node);
        }
        for (Map.Entry<String, Set<Integer>> entry : nodesPerRoom.entrySet()) {
            assertEquals(2, entry.getValue().size(),
                    "room " + entry.getKey() + " is entirely on one node, so it proves nothing about "
                            + "cross-node fan-out; fix the node/room assignment");
        }
    }

    private static void assertClean(Report report) {
        assertEquals(report.expectedDeliveries, report.actualDeliveries,
                "frames were lost: expected " + report.expectedDeliveries + ", delivered "
                        + report.actualDeliveries + ", client errors " + report.clientErrors);
        assertEquals(0, report.reordered, "frames arrived out of order per sender");
        assertEquals(0, report.clientErrors, "client-side send/receive failures");
        assertTrue(report.heapGrowthBytes < 256L * 1024 * 1024,
                "heap grew by " + report.heapGrowthBytes / 1024 / 1024 + "MB across the scenario");
    }

    private static void assertNoSessionLeak() {
        assertTrue(awaitUntil(() -> broadcasterA.localSessionCount() == 0 && broadcasterB.localSessionCount() == 0),
                "sessions were still registered after every client closed: A="
                        + broadcasterA.localSessionCount() + " B=" + broadcasterB.localSessionCount());
    }

    // --- measured phase ---

    private static Report runPhase(String scenario, List<LoadClient> clients, int messages,
                                   int deliveriesPerMessage) throws Exception {
        int perClient = messages * deliveriesPerMessage;
        clients.forEach(client -> client.resetStats(perClient));
        LoopJitterProbe.reset();
        long heapBefore = usedHeap();

        Report report = new Report(scenario, clients.size(), messages, deliveriesPerMessage);
        long start = System.nanoTime();
        sendBursts(clients, messages, report);
        report.sendMillis = (System.nanoTime() - start) / 1_000_000;

        long deadline = System.currentTimeMillis() + DRAIN_TIMEOUT_MS;
        while (delivered(clients) < report.expectedDeliveries && System.currentTimeMillis() < deadline) {
            Thread.sleep(25);
        }
        report.wallMillis = (System.nanoTime() - start) / 1_000_000;
        report.heapGrowthBytes = usedHeap() - heapBefore;
        report.summarize(clients);
        return report;
    }

    /** One virtual thread per client: a client's own messages stay sequential, clients run in parallel. */
    private static void sendBursts(List<LoadClient> clients, int messages, Report report) throws Exception {
        ExecutorService senders = Executors.newVirtualThreadPerTaskExecutor();
        try {
            List<Future<?>> futures = new ArrayList<>(clients.size());
            for (LoadClient client : clients) {
                futures.add(senders.submit(() -> {
                    client.sendBurst(messages);
                    return null;
                }));
            }
            for (Future<?> future : futures) {
                try {
                    future.get(SEND_TIMEOUT_MIN, TimeUnit.MINUTES);
                } catch (ExecutionException e) {
                    report.clientErrors++;
                    System.err.println("Send burst failed: " + e.getCause());
                } catch (TimeoutException e) {
                    report.clientErrors++;
                    System.err.println("Send burst timed out after " + SEND_TIMEOUT_MIN + " minutes");
                }
            }
        } finally {
            senders.shutdownNow();
        }
    }

    private static long delivered(List<LoadClient> clients) {
        long total = 0;
        for (LoadClient client : clients) {
            total += client.received();
        }
        return total;
    }

    /**
     * Sends one probe message per client and waits for a round in which every client receives all
     * {@code expectedPerClient} frames. A broadcast issued before the node selector has resolved the
     * peer legitimately stays local, so early rounds under-deliver; the criterion is per-round rather
     * than cumulative, because a deficit from an early round is never made up later. Stats are reset
     * after this, so warm-up traffic never enters the measurement.
     */
    private static void warmUp(List<LoadClient> clients, int expectedPerClient) throws InterruptedException {
        long deadline = System.currentTimeMillis() + READY_TIMEOUT_MS;
        int round = 0;
        while (System.currentTimeMillis() < deadline) {
            round++;
            // Reset every counter before any probe goes out. Interleaving the two loses frames on
            // paper: a client late in a sequential send loop clears its counter after earlier
            // room-mates' probes have already been delivered to it, so it can never reach the target
            // no matter how healthy the cluster is.
            for (LoadClient client : clients) {
                client.resetStats(expectedPerClient);
            }
            sendWarmUpBurst(clients);
            long roundDeadline = System.currentTimeMillis() + 15_000L;
            while (System.currentTimeMillis() < roundDeadline && !allReceived(clients, expectedPerClient)) {
                Thread.sleep(50);
            }
            if (allReceived(clients, expectedPerClient)) {
                System.out.println("Warm-up settled in round " + round);
                return;
            }
            System.out.printf("Warm-up round %d incomplete: %d of %d clients short of %d frames%n",
                    round, clients.stream().filter(client -> client.received() < expectedPerClient).count(),
                    clients.size(), expectedPerClient);
            describeShortfall(clients);
        }
        describeShortfall(clients);
        fail("Warm-up never settled within " + READY_TIMEOUT_MS + "ms: "
                + clients.stream().filter(client -> client.received() < expectedPerClient).count()
                + " of " + clients.size() + " clients still short of " + expectedPerClient + " frames");
    }

    /** Fires one probe per client concurrently, so no client's send waits on another's handshake. */
    private static void sendWarmUpBurst(List<LoadClient> clients) throws InterruptedException {
        ExecutorService senders = Executors.newVirtualThreadPerTaskExecutor();
        try {
            List<Future<?>> futures = new ArrayList<>(clients.size());
            for (LoadClient client : clients) {
                futures.add(senders.submit(client::sendWarmUp));
            }
            for (Future<?> future : futures) {
                try {
                    future.get(SEND_TIMEOUT_MIN, TimeUnit.MINUTES);
                } catch (ExecutionException | TimeoutException e) {
                    System.err.println("Warm-up probe failed: " + e);
                }
            }
        } finally {
            senders.shutdown();
        }
    }

    /**
     * Separates the two ways a warm-up round can come up short: frames still queued server-side
     * (sockets open, counts clustered just below the target, session counts intact) versus frames
     * genuinely gone (sockets closed, or sessions unregistered on the node that owned them). Without
     * this the failure message says only "not enough frames", which cannot be acted on.
     */
    private static void describeShortfall(List<LoadClient> clients) {
        Map<Long, Long> histogram = clients.stream()
                .collect(Collectors.groupingBy(LoadClient::received, TreeMap::new, Collectors.counting()));
        long closedInputs = clients.stream().filter(LoadClient::inputClosed).count();
        long clientErrors = clients.stream().mapToLong(LoadClient::errorCount).sum();
        long overflow = clients.stream().mapToLong(LoadClient::overflowCount).sum();
        System.out.println("----- warm-up shortfall diagnostics -----");
        System.out.println("frames per client : " + histogram);
        System.out.printf("sockets           : inputClosed=%d clientErrors=%d recordOverflow=%d%n",
                closedInputs, clientErrors, overflow);
        System.out.printf("server sessions   : A=%d B=%d (expected %d each, %d total)%n",
                broadcasterA.localSessionCount(), broadcasterB.localSessionCount(),
                clients.size() / 2, clients.size());
        System.out.println("-----------------------------------------");
    }

    private static boolean allReceived(List<LoadClient> clients, int expectedPerClient) {
        for (LoadClient client : clients) {
            if (client.received() < expectedPerClient) {
                return false;
            }
        }
        return true;
    }

    // --- clients ---

    private static List<LoadClient> connect(int count, IntFunction<String> groupOfIndex,
                                            IntUnaryOperator nodeOfIndex) throws Exception {
        ExecutorService connector = Executors.newVirtualThreadPerTaskExecutor();
        List<LoadClient> clients = new ArrayList<>(count);
        try {
            List<Future<LoadClient>> futures = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                final int index = i;
                futures.add(connector.submit(() -> {
                    int node = nodeOfIndex.applyAsInt(index);
                    return LoadClient.open(index, groupOfIndex.apply(index), node,
                            node == 0 ? portA : portB, clientFor(index));
                }));
            }
            for (Future<LoadClient> future : futures) {
                clients.add(future.get(60, TimeUnit.SECONDS));
            }
        } finally {
            connector.shutdownNow();
        }
        assertTrue(awaitUntil(() -> broadcasterA.localSessionCount() + broadcasterB.localSessionCount() == count),
                "not every connection was registered: A=" + broadcasterA.localSessionCount()
                        + " B=" + broadcasterB.localSessionCount() + " expected=" + count);
        return clients;
    }

    private static HttpClient clientFor(int index) {
        return CLIENT_POOL_INSTANCES.get(index % CLIENT_POOL_INSTANCES.size());
    }

    private static void closeAll(List<LoadClient> clients) {
        ExecutorService closer = Executors.newVirtualThreadPerTaskExecutor();
        try {
            for (LoadClient client : clients) {
                closer.submit(client::close);
            }
        } finally {
            closer.shutdown();
            try {
                if (!closer.awaitTermination(60, TimeUnit.SECONDS)) {
                    closer.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                closer.shutdownNow();
            }
        }
    }

    /**
     * One connection plus the stats gathered on it. The JDK guarantees a listener is never invoked
     * concurrently with itself, so the stats need no locking; {@code lastSeq} is concurrent only
     * because the test thread clears it between phases.
     */
    private static final class LoadClient implements AutoCloseable {

        private final String id;
        private final String group;
        /** 0 = node A, 1 = node B. */
        private final int node;
        private final Recorder recorder = new Recorder();
        /** Assigned after the handshake, which needs the listener up front. */
        private WebSocket socket;

        private volatile long[] latencies;
        private final AtomicInteger fill = new AtomicInteger();
        private final AtomicLong reordered = new AtomicLong();
        private final AtomicLong overflow = new AtomicLong();
        private final AtomicLong errors = new AtomicLong();
        private final ConcurrentHashMap<String, Long> lastSeq = new ConcurrentHashMap<>();

        static LoadClient open(int index, String group, int node, int port, HttpClient client) throws Exception {
            URI uri = URI.create("ws://127.0.0.1:" + port + PATH + "?group=" + group);
            LoadClient opened = new LoadClient("c" + index, group, node);
            opened.socket = client.newWebSocketBuilder()
                    .buildAsync(uri, opened.recorder)
                    .get(60, TimeUnit.SECONDS);
            return opened;
        }

        private LoadClient(String id, String group, int node) {
            this.id = id;
            this.group = group;
            this.node = node;
        }

        /** Clears the counters for a new round without allocating a latency array. */
        void resetCount() {
            fill.set(0);
            reordered.set(0);
            overflow.set(0);
            errors.set(0);
            lastSeq.clear();
        }

        void resetStats(int capacity) {
            latencies = new long[capacity];
            resetCount();
        }

        void sendBurst(int messages) {
            for (int seq = 1; seq <= messages; seq++) {
                socket.sendText(payload(id, seq, System.nanoTime()), true).join();
            }
        }

        void sendWarmUp() {
            socket.sendText(payload(id, 0, System.nanoTime()), true).join();
        }

        long received() {
            return fill.get();
        }

        boolean inputClosed() {
            return socket == null || socket.isInputClosed();
        }

        long errorCount() {
            return errors.get();
        }

        long overflowCount() {
            return overflow.get();
        }

        @Override
        public void close() {
            if (socket == null) {
                return;
            }
            try {
                socket.sendClose(WebSocket.NORMAL_CLOSURE, "bye").join();
            } catch (Exception ignored) {
                // best effort; abort below tears the connection down either way
            }
            socket.abort();
        }

        /** Parses {@code sender|seq|sendNanos|padding} and records the delivery. */
        private final class Recorder implements WebSocket.Listener {

            private final StringBuilder partial = new StringBuilder();

            @Override
            public void onOpen(WebSocket webSocket) {
                webSocket.request(Long.MAX_VALUE);
            }

            @Override
            public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                partial.append(data);
                if (last) {
                    record(partial.toString());
                    partial.setLength(0);
                }
                webSocket.request(1);
                return null;
            }

            @Override
            public void onError(WebSocket webSocket, Throwable error) {
                errors.incrementAndGet();
                System.err.println("Client " + id + " socket error: " + error);
            }

            private void record(String text) {
                int first = text.indexOf('|');
                int second = text.indexOf('|', first + 1);
                int third = text.indexOf('|', second + 1);
                if (first < 0 || second < 0 || third < 0) {
                    overflow.incrementAndGet();
                    return;
                }
                String sender = text.substring(0, first);
                long seq = Long.parseLong(text.substring(first + 1, second));
                long sentNanos = Long.parseLong(text.substring(second + 1, third));

                Long previous = lastSeq.put(sender, seq);
                if (previous != null && seq <= previous) {
                    reordered.incrementAndGet();
                }
                int slot = fill.getAndIncrement();
                long[] target = latencies;
                if (target != null && slot < target.length) {
                    target[slot] = System.nanoTime() - sentNanos;
                } else {
                    overflow.incrementAndGet();
                }
            }
        }
    }

    private static String payload(String sender, long seq, long sentNanos) {
        StringBuilder text = new StringBuilder(PAYLOAD_BYTES).append(sender).append('|')
                .append(seq).append('|').append(sentNanos).append('|');
        while (text.length() < PAYLOAD_BYTES) {
            text.append('x');
        }
        return text.toString();
    }

    // --- report ---

    private static final class Report {

        private final String scenario;
        private final int clients;
        private final int messagesPerClient;
        private final int deliveriesPerMessage;
        private final long expectedDeliveries;

        private long actualDeliveries;
        private long reordered;
        private long overflow;
        private long clientErrors;
        private long sendMillis;
        private long wallMillis;
        private long heapGrowthBytes;
        private double sendRate;
        private double deliveryRate;
        private long p50Micros;
        private long p90Micros;
        private long p99Micros;
        private long maxMicros;
        private long jitterSamples;
        private long jitterAvgMicros;
        private long jitterMaxMicros;

        private Report(String scenario, int clients, int messagesPerClient, int deliveriesPerMessage) {
            this.scenario = scenario;
            this.clients = clients;
            this.messagesPerClient = messagesPerClient;
            this.deliveriesPerMessage = deliveriesPerMessage;
            this.expectedDeliveries = (long) clients * messagesPerClient * deliveriesPerMessage;
        }

        private void summarize(List<LoadClient> measured) {
            int total = 0;
            for (LoadClient client : measured) {
                total += Math.min(client.fill.get(), client.latencies == null ? 0 : client.latencies.length);
                reordered += client.reordered.get();
                overflow += client.overflow.get();
                clientErrors += client.errors.get();
            }
            actualDeliveries = delivered(measured);
            long[] all = new long[total];
            int cursor = 0;
            for (LoadClient client : measured) {
                long[] source = client.latencies;
                int count = Math.min(client.fill.get(), source == null ? 0 : source.length);
                System.arraycopy(source, 0, all, cursor, count);
                cursor += count;
            }
            Arrays.sort(all);
            if (all.length > 0) {
                p50Micros = micros(all, 0.50);
                p90Micros = micros(all, 0.90);
                p99Micros = micros(all, 0.99);
                maxMicros = all[all.length - 1] / 1000;
            }
            sendRate = sendMillis == 0 ? 0 : (double) clients * messagesPerClient * 1000 / sendMillis;
            deliveryRate = wallMillis == 0 ? 0 : (double) actualDeliveries * 1000 / wallMillis;
            jitterSamples = LoopJitterProbe.SAMPLES.sum();
            long totalJitter = LoopJitterProbe.TOTAL_JITTER_NANOS.sum();
            jitterAvgMicros = jitterSamples == 0 ? 0 : totalJitter / jitterSamples / 1000;
            jitterMaxMicros = LoopJitterProbe.MAX_JITTER_NANOS.get() / 1000;
        }

        private static long micros(long[] sorted, double quantile) {
            int index = (int) Math.min(sorted.length - 1, Math.floor(sorted.length * quantile));
            return sorted[index] / 1000;
        }

        private void print() {
            System.out.printf("""

                    ===== WebSocket cluster load: %s =====
                    scale          : %d connections x %d messages, %d deliveries per message (%d expected)
                    sent           : %d messages in %d ms (%.0f/s)
                    delivered      : %d of %d frames in %d ms (%.0f/s)
                    latency        : p50=%dus p90=%dus p99=%dus max=%dus
                    correctness    : lost=%d reordered=%d unexpected=%d clientErrors=%d
                    event loop     : %d probe samples, avg late=%dus, max late=%dus
                    heap           : grew %d MB across the scenario
                    ============================================%n""",
                    scenario, clients, messagesPerClient, deliveriesPerMessage, expectedDeliveries,
                    (long) clients * messagesPerClient, sendMillis, sendRate,
                    actualDeliveries, expectedDeliveries, wallMillis, deliveryRate,
                    p50Micros, p90Micros, p99Micros, maxMicros,
                    expectedDeliveries - actualDeliveries, reordered, overflow, clientErrors,
                    jitterSamples, jitterAvgMicros, jitterMaxMicros,
                    heapGrowthBytes / 1024 / 1024);
        }
    }

    /**
     * Periodic timer per event loop; the gap between when a tick was due and when it actually ran is
     * how long that loop was blocked. Vert.x's own BlockedThreadChecker only reports past a 2s
     * threshold, which a fan-out stall never reaches while still being fatal to latency.
     */
    public static final class LoopJitterProbe extends AbstractVerticle {

        private static final long INTERVAL_MS = 20L;
        private static final long INTERVAL_NS = INTERVAL_MS * 1_000_000L;

        static final LongAdder SAMPLES = new LongAdder();
        static final LongAdder TOTAL_JITTER_NANOS = new LongAdder();
        static final AtomicLong MAX_JITTER_NANOS = new AtomicLong();

        private long expectedNanos;

        static void reset() {
            SAMPLES.reset();
            TOTAL_JITTER_NANOS.reset();
            MAX_JITTER_NANOS.set(0);
        }

        @Override
        public void start() {
            expectedNanos = System.nanoTime() + INTERVAL_NS;
            vertx.setPeriodic(INTERVAL_MS, id -> {
                long now = System.nanoTime();
                long late = now - expectedNanos;
                SAMPLES.increment();
                if (late > 0) {
                    TOTAL_JITTER_NANOS.add(late);
                    MAX_JITTER_NANOS.accumulateAndGet(late, Math::max);
                }
                expectedNanos = now + INTERVAL_NS;
            });
        }
    }

    private static void deployProbes(Vertx vertx) {
        int instances = Math.max(2, Math.min(16, Runtime.getRuntime().availableProcessors()));
        vertx.deployVerticle(LoopJitterProbe::new, new DeploymentOptions().setInstances(instances))
                .toCompletionStage().toCompletableFuture().join();
    }

    private static long usedHeap() {
        System.gc();
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }

    // --- cluster scaffolding (mirrors ClusteredWebSocketE2ETest) ---

    private static ZenohClusterProperties peerProperties(int zenohPort) {
        ZenohClusterProperties props = new ZenohClusterProperties();
        props.setAppName(APP);
        props.setEnvironment("test");
        props.setMode("peer");
        props.setListen(List.of("tcp/127.0.0.1:" + zenohPort));
        return props;
    }

    private static ZenohClusterProperties clientProperties(int zenohPort) {
        ZenohClusterProperties props = new ZenohClusterProperties();
        props.setAppName(APP);
        props.setEnvironment("test");
        props.setMode("client");
        props.setConnect(List.of("tcp/127.0.0.1:" + zenohPort));
        return props;
    }

    private static Vertx clustered(ZenohClusterManager manager, int eventBusPort) {
        VertxOptions options = new VertxOptions();
        options.getEventBusOptions().setHost("127.0.0.1").setPort(eventBusPort);
        return Vertx.builder().with(options).withClusterManager(manager)
                .buildClustered().toCompletionStage().toCompletableFuture().join();
    }

    /**
     * Whether each node can resolve the other's node info, which is what the clustered event
     * bus needs to dial a peer. A {@code false} here is why publishes get dropped with
     * "Node info not found" and never reach the other node's sessions.
     */
    private static void printRoutingDiagnostics(String scenario) {
        String nodeA = managerA.getNodeId();
        String nodeB = managerB.getNodeId();
        System.out.printf(
                "routing[%s]   : A=%s resolvesB=%s regs=%d | B=%s resolvesA=%s regs=%d%n",
                scenario,
                nodeA, managerA.getNodeInfoOf(nodeB) != null,
                managerA.getRegistrationsOf(ClusterWebSocketBroadcaster.DEFAULT_ADDRESS).size(),
                nodeB, managerB.getNodeInfoOf(nodeA) != null,
                managerB.getRegistrationsOf(ClusterWebSocketBroadcaster.DEFAULT_ADDRESS).size());
    }

    private static int startServer(Vertx vertx, ClusterWebSocketBroadcaster broadcaster) throws Exception {
        WebSocketHandler handler = session -> {
            String group = groupOf(session);
            broadcaster.register(session, group);
            return session.receive()
                    .doOnNext(message -> broadcaster.broadcast(message.getPayloadAsText(), group))
                    .doFinally(signal -> broadcaster.unregister(session.getId()))
                    .then();
        };

        HttpServer server = vertx.createHttpServer();
        server.requestHandler(request -> {
            if (request.canUpgradeToWebSocket() && pathOf(request.path()).equals(PATH)) {
                upgrade(request, handler);
            } else {
                request.response().setStatusCode(404).end();
            }
        });
        return server.listen(0, "127.0.0.1")
                .toCompletionStage().toCompletableFuture().join().actualPort();
    }

    private static void upgrade(HttpServerRequest request, WebSocketHandler handler) {
        VertxServerHttpRequest springRequest = new VertxServerHttpRequest(request);
        VertxServerHttpResponse springResponse = new VertxServerHttpResponse(request.response());
        MockServerWebExchange base = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.GET, pathOf(request.path())).build());

        ServerWebExchange exchange = new ServerWebExchangeDecorator(base) {
            @Override
            public ServerHttpRequest getRequest() {
                return springRequest;
            }

            @Override
            public ServerHttpResponse getResponse() {
                return springResponse;
            }
        };

        HandshakeInfo handshake = new HandshakeInfo(
                URI.create(request.absoluteURI()),
                new HttpHeaders(),
                reactor.core.publisher.Mono.just(() -> "load"),
                null);

        new VertxRequestUpgradeStrategy()
                .upgrade(exchange, handler, null, () -> handshake)
                .subscribe(null, t -> request.response().setStatusCode(500).end());
    }

    private static String groupOf(WebSocketSession session) {
        String query = session.getHandshakeInfo().getUri().getQuery();
        if (query == null) {
            return null;
        }
        for (String pair : query.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0 && "group".equals(pair.substring(0, eq))) {
                return pair.substring(eq + 1);
            }
        }
        return null;
    }

    private static String pathOf(String uri) {
        int q = uri.indexOf('?');
        return q < 0 ? uri : uri.substring(0, q);
    }

    private static void closeQuietly(HttpServer server) {
        if (server != null) {
            try {
                server.close().toCompletionStage().toCompletableFuture().join();
            } catch (Exception ignored) {
                // best effort
            }
        }
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

    private static boolean awaitUntil(BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + READY_TIMEOUT_MS;
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
            throw new IllegalStateException("No free port for the load cluster", e);
        }
    }

    private static int intProp(String key, int fallback) {
        String value = System.getProperty(key);
        return value == null || value.isBlank() ? fallback : Integer.parseInt(value.trim());
    }
}
