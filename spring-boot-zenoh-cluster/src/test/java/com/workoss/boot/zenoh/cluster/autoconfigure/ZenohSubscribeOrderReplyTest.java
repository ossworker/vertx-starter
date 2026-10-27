package com.workoss.boot.zenoh.cluster.autoconfigure;

import com.workoss.boot.zenoh.cluster.ClusterMessage;
import com.workoss.boot.zenoh.cluster.ClusterMessaging;
import com.workoss.boot.zenoh.cluster.ZenohSubscribe;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.annotation.Order;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("@ZenohSubscribe ordering and reply publication over real Zenoh")
class ZenohSubscribeOrderReplyTest {

    private static final long WAIT = 8_000L;

    private final ApplicationContextRunner withHandlers = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ZenohClusterAutoConfiguration.class))
            .withBean("ordered", OrderedHandlers.class, OrderedHandlers::new)
            .withBean("replies", ReplyHandlers.class, ReplyHandlers::new)
            .withBean("async", AsyncReplyHandlers.class, AsyncReplyHandlers::new);

    @Test
    @DisplayName("handlers of one address run one at a time in @Order sequence")
    void orderedHandlersRunInDeclaredOrder() throws IOException {
        singleNode(withHandlers, "ord").run(context -> {
            OrderedHandlers handlers = context.getBean(OrderedHandlers.class);
            ClusterMessaging messaging = context.getBean(ClusterMessaging.class);
            // Warm up first: the transport-level subscription only becomes live asynchronously, so
            // resend until one full round trip has looped back. Warmup messages can still arrive
            // late, so the real broadcast is tagged and only tagged entries are asserted on.
            resendUntil(() -> messaging.broadcast("orders/warmup", "warm".getBytes(StandardCharsets.UTF_8)),
                    () -> handlers.sequence.size() >= 2, "subscription is live");

            // "orders/probe" (not "orders/eu") so no reply handler fires: its "ack:..." reply to
            // "orders/replies" would also match "orders/*" and pollute the exact sequence.
            messaging.broadcast("orders/probe", "probe".getBytes(StandardCharsets.UTF_8));
            await(() -> handlers.tagged().size() >= 2, "each handler ran exactly once");
            awaitStable(handlers.sequence, "handlers finished");
            assertThat(handlers.tagged()).containsExactly("first", "second");
        });
    }

    @Test
    @DisplayName("a returned value is published to the replyTo address")
    void returnValueIsPublishedToTheReplyAddress() throws IOException {
        singleNode(withHandlers, "rep").run(context -> {
            ReplyHandlers handlers = context.getBean(ReplyHandlers.class);
            ClusterMessaging messaging = context.getBean(ClusterMessaging.class);
            resendUntil(() -> messaging.broadcast("orders/eu", "shipped".getBytes(StandardCharsets.UTF_8)),
                    () -> !handlers.replies.isEmpty(), "reply arrived on orders/replies");
            assertThat(handlers.replies).containsExactly("ack:shipped");
        });
    }

    @Test
    @DisplayName("a CompletableFuture result is published when it completes")
    void futureResultIsPublishedAsynchronously() throws IOException {
        singleNode(withHandlers, "fut").run(context -> {
            AsyncReplyHandlers handlers = context.getBean(AsyncReplyHandlers.class);
            ClusterMessaging messaging = context.getBean(ClusterMessaging.class);
            resendUntil(() -> messaging.broadcast("jobs/run", "42".getBytes(StandardCharsets.UTF_8)),
                    () -> !handlers.done.isEmpty(), "async reply arrived on jobs/done");
            assertThat(handlers.done).containsExactly("done:42");
        });
    }

    @Test
    @DisplayName("a failed future publishes nothing")
    void failedFuturePublishesNothing() throws IOException {
        singleNode(withHandlers, "fail").run(context -> {
            AsyncReplyHandlers handlers = context.getBean(AsyncReplyHandlers.class);
            ClusterMessaging messaging = context.getBean(ClusterMessaging.class);
            messaging.broadcast("jobs/fail", "boom".getBytes(StandardCharsets.UTF_8));
            await(() -> !handlers.failures.isEmpty(), "the failing handler ran");
            await(300);
            assertThat(handlers.done).isEmpty();
        });
    }

    @Test
    @DisplayName("a void handler publishes no reply")
    void voidHandlerPublishesNoReply() throws IOException {
        singleNode(withHandlers, "void").run(context -> {
            ReplyHandlers handlers = context.getBean(ReplyHandlers.class);
            ClusterMessaging messaging = context.getBean(ClusterMessaging.class);
            messaging.broadcast("orders/void", "nothing".getBytes(StandardCharsets.UTF_8));
            await(() -> !handlers.voids.isEmpty(), "the void handler ran");
            await(300);
            assertThat(handlers.replies).isEmpty();
        });
    }

    @Test
    @DisplayName("a direct handler sends its return value back to the sender")
    void directHandlerRepliesToTheSender() throws IOException {
        int port = freePort();
        String app = "dir-" + UUID.randomUUID().toString().substring(0, 8);
        String endpoint = "tcp/127.0.0.1:" + port;

        ApplicationContextRunner nodeA = withHandlers.withPropertyValues(
                nodeProperties(app, "node-a", "peer", endpoint));
        // B registers a plain (void) direct handler, so the reply does not bounce back.
        ApplicationContextRunner nodeB = plainNode(app, "node-b", "client", endpoint)
                .withBean("sink", DirectSink.class, DirectSink::new);

        nodeA.run(ctxA -> nodeB.run(ctxB -> {
            ReplyHandlers handlersA = ctxA.getBean(ReplyHandlers.class);
            DirectSink sink = ctxB.getBean(DirectSink.class);
            ClusterMessaging messagingB = ctxB.getBean(ClusterMessaging.class);
            resendUntil(() -> messagingB.send("node-a", "ping".getBytes(StandardCharsets.UTF_8)),
                    () -> !sink.received.isEmpty(), "A's reply reached B");
            assertThat(sink.received).containsExactly("ack:ping");
            assertThat(handlersA.directReplies).containsExactly("handled:ping");
        }));
    }

    @Test
    @DisplayName("an unsupported return type with replyTo fails the context at startup")
    void unsupportedReturnTypeRejected() throws IOException {
        withHandlers.withBean("bad", BadReplyHandlers.class, BadReplyHandlers::new)
                .withPropertyValues(nodeProperties("bad-" + UUID.randomUUID(), "node-bad",
                        "peer", "tcp/127.0.0.1:" + freePort()))
                .run(context -> assertThat(context).hasFailed());
    }

    private static ApplicationContextRunner singleNode(ApplicationContextRunner runner, String appPrefix)
            throws IOException {
        return runner.withPropertyValues(nodeProperties(appPrefix + "-" + UUID.randomUUID(),
                "node-a", "peer", "tcp/127.0.0.1:" + freePort()));
    }

    private static ApplicationContextRunner plainNode(String app, String nodeId, String mode, String endpoint) {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ZenohClusterAutoConfiguration.class))
                .withPropertyValues(nodeProperties(app, nodeId, mode, endpoint));
    }

    private static String[] nodeProperties(String app, String nodeId, String mode, String endpoint) {
        String endpointProperty = "client".equals(mode)
                ? "zenoh.cluster.connect=" + endpoint
                : "zenoh.cluster.listen=" + endpoint;
        return new String[]{
                "zenoh.cluster.app-name=" + app,
                "zenoh.cluster.environment=test",
                "zenoh.cluster.node-id=" + nodeId,
                "zenoh.cluster.mode=" + mode,
                endpointProperty,
                "zenoh.cluster.heartbeat-interval=200ms",
                "zenoh.cluster.failure-timeout=1s"
        };
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private static void await(BooleanSupplier condition, String description) {
        long deadline = System.nanoTime() + WAIT * 1_000_000L;
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            sleep(25);
        }
        throw new AssertionError("Not satisfied: " + description);
    }

    private static void await(long millis) {
        sleep(millis);
    }

    /**
     * Waits until the list stops growing for 300ms, so queued deliveries have drained.
     */
    private static void awaitStable(List<?> list, String description) {
        long deadline = System.nanoTime() + WAIT * 1_000_000L;
        int last = -1;
        long stableSince = System.nanoTime();
        while (System.nanoTime() < deadline) {
            int size = list.size();
            if (size != last) {
                last = size;
                stableSince = System.nanoTime();
            } else if (System.nanoTime() - stableSince >= 300_000_000L) {
                return;
            }
            sleep(25);
        }
        throw new AssertionError("Not satisfied: " + description);
    }

    private static void resendUntil(Runnable send, BooleanSupplier delivered, String description) {
        long deadline = System.nanoTime() + WAIT * 1_000_000L;
        while (System.nanoTime() < deadline) {
            send.run();
            long until = System.nanoTime() + 200_000_000L;
            while (System.nanoTime() < until) {
                if (delivered.getAsBoolean()) {
                    return;
                }
                sleep(20);
            }
        }
        throw new AssertionError("Not delivered: " + description);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static class OrderedHandlers {

        final List<String> sequence = new CopyOnWriteArrayList<>();

        @ZenohSubscribe("orders/*")
        @Order(1)
        void first(ClusterMessage message) {
            sequence.add("first|" + message.payloadAsString());
        }

        @ZenohSubscribe("orders/*")
        @Order(2)
        void second(ClusterMessage message) {
            sequence.add("second|" + message.payloadAsString());
        }

        /** Handler names of the tagged (non-warmup) deliveries, in execution order. */
        List<String> tagged() {
            return sequence.stream()
                    .filter(entry -> entry.endsWith("|probe"))
                    .map(entry -> entry.substring(0, entry.indexOf('|')))
                    .toList();
        }
    }

    static class ReplyHandlers {

        final List<String> replies = new CopyOnWriteArrayList<>();
        final List<String> voids = new CopyOnWriteArrayList<>();
        final List<String> directReplies = new CopyOnWriteArrayList<>();

        @ZenohSubscribe(value = "orders/eu", replyTo = "orders/replies")
        String onOrder(String payload) {
            return "ack:" + payload;
        }

        @ZenohSubscribe("orders/replies")
        void onReply(String reply) {
            replies.add(reply);
        }

        @ZenohSubscribe(value = "orders/void", replyTo = "orders/replies")
        void onVoid(String payload) {
            voids.add(payload);
        }

        @ZenohSubscribe(direct = true)
        String onDirect(String payload) {
            directReplies.add("handled:" + payload);
            return "ack:" + payload;
        }
    }

    static class AsyncReplyHandlers {

        final List<String> done = new CopyOnWriteArrayList<>();
        final List<String> failures = new CopyOnWriteArrayList<>();

        @ZenohSubscribe(value = "jobs/run", replyTo = "jobs/done")
        CompletableFuture<String> onJob(String payload) {
            return CompletableFuture.supplyAsync(() -> "done:" + payload);
        }

        @ZenohSubscribe(value = "jobs/fail", replyTo = "jobs/done")
        CompletableFuture<String> onFailingJob(String payload) {
            failures.add(payload);
            return CompletableFuture.failedFuture(new IllegalStateException("boom"));
        }

        @ZenohSubscribe("jobs/done")
        void onDone(String result) {
            done.add(result);
        }
    }

    static class BadReplyHandlers {

        @ZenohSubscribe(value = "x", replyTo = "y")
        Integer onX(String payload) {
            return 1;
        }
    }

    /** A plain void direct handler: receives replies without replying to them. */
    static class DirectSink {

        final List<String> received = new CopyOnWriteArrayList<>();

        @ZenohSubscribe(direct = true)
        void onDirect(ClusterMessage message) {
            received.add(message.payloadAsString());
        }
    }
}
