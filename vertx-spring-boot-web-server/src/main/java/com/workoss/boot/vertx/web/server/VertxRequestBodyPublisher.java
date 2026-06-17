package com.workoss.boot.vertx.web.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.core.io.buffer.NettyDataBufferFactory;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lightweight {@link Publisher} bridging Vert.x request body events to Reactive Streams.
 * <p>
 * Replaces the previous {@code Sinks.many().unicast().onBackpressureBuffer()} which
 * allocates ~1KB of internal state (MpscLinkedQueue, state machine) per request.
 * This implementation:
 * <ul>
 *   <li>Registers Vert.x handlers <em>only</em> on subscribe (zero overhead if never subscribed)</li>
 *   <li>Uses a simple ArrayDeque for buffering (single-threaded event-loop guarantee)</li>
 *   <li>Performs zero-copy {@code BufferImpl.getByteBuf()} extraction when possible</li>
 *   <li>Honors backpressure via {@link HttpServerRequest#pause()}/{@link HttpServerRequest#resume()}</li>
 * </ul>
 * Single-subscriber only (matches the unicast contract of HTTP request bodies).
 */
final class VertxRequestBodyPublisher implements Publisher<DataBuffer> {

    private static final NettyDataBufferFactory NETTY_FACTORY =
            new NettyDataBufferFactory(UnpooledByteBufAllocator.DEFAULT);
    private static final DefaultDataBufferFactory DEFAULT_FACTORY = DefaultDataBufferFactory.sharedInstance;

    private final HttpServerRequest request;

    VertxRequestBodyPublisher(HttpServerRequest request) {
        this.request = request;
    }

    @Override
    public void subscribe(Subscriber<? super DataBuffer> subscriber) {
        BodySubscription sub = new BodySubscription(request, subscriber);
        subscriber.onSubscribe(sub);
        sub.start();
    }

    private static ByteBuf extractByteBuf(Buffer buffer) {
        if (buffer instanceof io.vertx.core.buffer.impl.BufferImpl impl) {
            return impl.getByteBuf();
        }
        return null;
    }

    private static DataBuffer toDataBuffer(Buffer buffer) {
        ByteBuf byteBuf = extractByteBuf(buffer);
        if (byteBuf != null) {
            // Zero-copy: share underlying memory with Vert.x buffer
            return NETTY_FACTORY.wrap(byteBuf.retain());
        }
        return DEFAULT_FACTORY.wrap(buffer.getBytes());
    }

    /**
     * Subscription handling demand and forwarding Vert.x events. Runs on the Vert.x
     * event loop; no cross-thread synchronization required for buffer/state.
     */
    private static final class BodySubscription implements Subscription {

        private final HttpServerRequest request;
        private final Subscriber<? super DataBuffer> subscriber;
        private final Queue<DataBuffer> queue = new ArrayDeque<>(2);
        private final AtomicLong demand = new AtomicLong();

        private boolean started;
        private boolean completed;
        private boolean cancelled;
        private Throwable pendingError;
        private boolean draining;

        BodySubscription(HttpServerRequest request, Subscriber<? super DataBuffer> subscriber) {
            this.request = request;
            this.subscriber = subscriber;
        }

        void start() {
            if (started || cancelled) return;
            started = true;
            request.handler(this::onData);
            request.endHandler(v -> onEnd());
            request.exceptionHandler(this::onError);
            // Pause until first request(n)
            request.pause();
            drain();
        }

        private void onData(Buffer buffer) {
            if (cancelled) return;
            queue.add(toDataBuffer(buffer));
            drain();
        }

        private void onEnd() {
            completed = true;
            drain();
        }

        private void onError(Throwable err) {
            pendingError = err;
            completed = true;
            drain();
        }

        @Override
        public void request(long n) {
            if (n <= 0 || cancelled) return;
            // Saturating add
            long current;
            long next;
            do {
                current = demand.get();
                next = current + n;
                if (next < 0) next = Long.MAX_VALUE;
            } while (!demand.compareAndSet(current, next));
            drain();
        }

        @Override
        public void cancel() {
            cancelled = true;
            // Drain queued buffers to release memory
            DataBuffer db;
            while ((db = queue.poll()) != null) {
                org.springframework.core.io.buffer.DataBufferUtils.release(db);
            }
            try {
                request.pause();
            } catch (Exception ignored) {
            }
        }

        private void drain() {
            if (draining) return;
            draining = true;
            try {
                while (!cancelled) {
                    long d = demand.get();
                    boolean emitted = false;
                    while (d > 0 && !queue.isEmpty()) {
                        DataBuffer db = queue.poll();
                        subscriber.onNext(db);
                        d = demand.decrementAndGet();
                        emitted = true;
                        if (cancelled) return;
                    }
                    if (completed && queue.isEmpty()) {
                        if (pendingError != null) subscriber.onError(pendingError);
                        else subscriber.onComplete();
                        return;
                    }
                    if (!emitted) break;
                }
                // Resume Vert.x reads when downstream wants more
                if (!cancelled && !completed && demand.get() > 0) {
                    request.resume();
                } else if (!cancelled && !completed && demand.get() == 0) {
                    request.pause();
                }
            } finally {
                draining = false;
            }
        }
    }
}
