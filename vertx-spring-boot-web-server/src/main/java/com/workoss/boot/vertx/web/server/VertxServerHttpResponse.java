package com.workoss.boot.vertx.web.server;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseCookie;
import org.springframework.http.server.reactive.AbstractServerHttpResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Adapts a Vert.x HttpServerResponse to Spring's ServerHttpResponse.
 */
public class VertxServerHttpResponse extends AbstractServerHttpResponse {

    /** RFC 1123 date formatter for the Date header (e.g. "Wed, 17 Jun 2026 03:17:28 GMT"). */
    private static final DateTimeFormatter RFC_1123 = DateTimeFormatter.RFC_1123_DATE_TIME;

    /** Cached Date header value, updated every second to avoid per-request formatting overhead. */
    private static volatile String cachedDateHeader;
    private static volatile long cachedDateTimestamp;

    private static String getDateHeader() {
        long now = System.currentTimeMillis();
        if (now - cachedDateTimestamp >= 1000) {
            cachedDateHeader = RFC_1123.format(ZonedDateTime.now(ZoneOffset.UTC));
            cachedDateTimestamp = now;
        }
        return cachedDateHeader;
    }

    private final HttpServerResponse response;

    public VertxServerHttpResponse(HttpServerResponse response) {
        super(org.springframework.core.io.buffer.DefaultDataBufferFactory.sharedInstance);
        this.response = response;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getNativeResponse() {
        return (T) response;
    }

    @Override
    public boolean isCommitted() {
        // A WebSocket upgrade writes the 101 straight through Vert.x and never goes through
        // commit(), so the inherited state stays NEW while the head is already on the wire. Left
        // unreported, the error pipeline tries to render a 500 over the upgraded connection and
        // Vert.x rejects it with "Response head already sent".
        return super.isCommitted() || response.headWritten();
    }

    @Override
    protected void applyStatusCode() {
        HttpStatusCode statusCode = getStatusCode();
        if (statusCode != null) {
            response.setStatusCode(statusCode.value());
        }
    }

    @Override
    protected void applyHeaders() {
        // putHeader is set-semantic in Vert.x; skip O(n) contains() probe.
        io.vertx.core.MultiMap target = response.headers();
        target.set("Date", getDateHeader());
        target.set("Connection", "keep-alive");

        HttpHeaders headers = getHeaders();
        if (headers.isEmpty()) {
            return;
        }
        // Bulk-apply via Vert.x MultiMap to avoid per-value method dispatch overhead.
        headers.forEach((name, values) -> {
            int size = values.size();
            if (size == 1) {
                target.set(name, values.get(0));
            } else if (size > 1) {
                target.set(name, values);
            }
        });
    }

    @Override
    protected void applyCookies() {
        getCookies().forEach((name, cookies) -> {
            for (ResponseCookie cookie : cookies) {
                response.putHeader("Set-Cookie", formatCookie(cookie));
            }
        });
    }

    private String formatCookie(ResponseCookie cookie) {
        StringBuilder sb = new StringBuilder();
        sb.append(cookie.getName()).append("=").append(cookie.getValue());
        if (cookie.getMaxAge().getSeconds() > 0) {
            sb.append("; Max-Age=").append(cookie.getMaxAge().getSeconds());
        }
        if (cookie.getDomain() != null && !cookie.getDomain().isEmpty()) {
            sb.append("; Domain=").append(cookie.getDomain());
        }
        if (cookie.getPath() != null && !cookie.getPath().isEmpty()) {
            sb.append("; Path=").append(cookie.getPath());
        }
        if (cookie.isSecure()) {
            sb.append("; Secure");
        }
        if (cookie.isHttpOnly()) {
            sb.append("; HttpOnly");
        }
        if (cookie.getSameSite() != null && !cookie.getSameSite().isEmpty()) {
            sb.append("; SameSite=").append(cookie.getSameSite());
        }
        return sb.toString();
    }

    @Override
    protected Mono<Void> writeWithInternal(Publisher<? extends DataBuffer> body) {
        // Optimized write path:
        // - Single buffer: direct end(buffer) (avoids write + end round-trip)
        // - Multiple buffers: merge then end
        return Flux.from(body)
                .collectList()
                .flatMap(buffers -> {
                    if (buffers.isEmpty()) {
                        return Mono.fromCompletionStage(response.end().toCompletionStage());
                    }
                    if (buffers.size() == 1) {
                        // Fast path: single buffer -> direct end(buffer)
                        DataBuffer db = buffers.get(0);
                        byte[] bytes = new byte[db.readableByteCount()];
                        db.read(bytes);
                        DataBufferUtils.release(db);
                        return Mono.fromCompletionStage(response.end(Buffer.buffer(bytes)).toCompletionStage());
                    }
                    // Multiple buffers: merge then end
                    Buffer merged = Buffer.buffer();
                    for (DataBuffer db : buffers) {
                        byte[] bytes = new byte[db.readableByteCount()];
                        db.read(bytes);
                        DataBufferUtils.release(db);
                        merged.appendBytes(bytes);
                    }
                    return Mono.fromCompletionStage(response.end(merged).toCompletionStage());
                });
    }

    @Override
    protected Mono<Void> writeAndFlushWithInternal(Publisher<? extends Publisher<? extends DataBuffer>> body) {
        return Flux.from(body)
                .flatMap(publisher -> writeWithInternal(publisher))
                .then();
    }

    public Mono<Void> end() {
        return Mono.fromCompletionStage(response.end().toCompletionStage());
    }
}
