package com.vertx.spring.boot.web.server;

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

import java.util.List;

/**
 * Adapts a Vert.x HttpServerResponse to Spring's ServerHttpResponse.
 */
public class VertxServerHttpResponse extends AbstractServerHttpResponse {

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
    protected void applyStatusCode() {
        HttpStatusCode statusCode = getStatusCode();
        if (statusCode != null) {
            response.setStatusCode(statusCode.value());
        }
    }

    @Override
    protected void applyHeaders() {
        HttpHeaders headers = getHeaders();
        headers.forEach((name, values) -> {
            for (String value : values) {
                response.putHeader(name, value);
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
        return Flux.from(body)
                .flatMap(dataBuffer -> {
                    byte[] bytes = new byte[dataBuffer.readableByteCount()];
                    dataBuffer.read(bytes);
                    DataBufferUtils.release(dataBuffer);
                    Buffer vertxBuffer = Buffer.buffer(bytes);
                    return Mono.fromCompletionStage(
                            response.write(vertxBuffer).toCompletionStage()
                    );
                })
                .then();
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
