package com.vertx.spring.boot.web.server;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpCookie;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.server.reactive.AbstractServerHttpRequest;
import org.springframework.http.server.reactive.SslInfo;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.net.InetSocketAddress;
import java.net.URI;

/**
 * Adapts a Vert.x HttpServerRequest to Spring's ServerHttpRequest.
 */
public class VertxServerHttpRequest extends AbstractServerHttpRequest {

    private final HttpServerRequest request;
    private final Flux<DataBuffer> body;

    public VertxServerHttpRequest(HttpServerRequest request) {
        super(HttpMethod.valueOf(request.method().name()), buildUri(request), null, buildHeaders(request));
        this.request = request;
        this.body = buildBody(request);
    }

    private static URI buildUri(HttpServerRequest request) {
        try {
            return URI.create(request.absoluteURI());
        } catch (Exception e) {
            // Fallback for relative URIs
            String scheme = request.isSSL() ? "https" : "http";
            String host = request.authority() != null ? request.authority().toString() : "localhost";
            return URI.create(scheme + "://" + host + request.uri());
        }
    }

    private static HttpHeaders buildHeaders(HttpServerRequest request) {
        HttpHeaders headers = new HttpHeaders();
        request.headers().forEach(entry -> headers.add(entry.getKey(), entry.getValue()));
        return headers;
    }

    private Flux<DataBuffer> buildBody(HttpServerRequest request) {
        Sinks.Many<DataBuffer> sink = Sinks.many().unicast().onBackpressureBuffer();

        request.handler(buffer -> {
            DataBuffer dataBuffer = DefaultDataBufferFactory.sharedInstance
                    .wrap(buffer.getBytes());
            sink.tryEmitNext(dataBuffer);
        });

        request.endHandler(v -> sink.tryEmitComplete());

        request.exceptionHandler(err -> sink.tryEmitError(err));

        // Pause initially; resume when subscribed (backpressure support)
        request.pause();

        return sink.asFlux()
                .doOnSubscribe(s -> request.resume())
                .doOnCancel(() -> request.pause());
    }

    @Override
    protected MultiValueMap<String, HttpCookie> initCookies() {
        MultiValueMap<String, HttpCookie> cookies = new LinkedMultiValueMap<>();
        request.cookies().forEach(cookie -> {
            HttpCookie httpCookie = new HttpCookie(cookie.getName(), cookie.getValue());
            cookies.add(cookie.getName(), httpCookie);
        });
        return cookies;
    }

    @Override
    protected SslInfo initSslInfo() {
        if (request.isSSL()) {
            return new VertxSslInfo(request.sslSession());
        }
        return null;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        var addr = request.localAddress();
        if (addr != null) {
            return new InetSocketAddress(addr.host(), addr.port());
        }
        return null;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        var addr = request.remoteAddress();
        if (addr != null) {
            return new InetSocketAddress(addr.host(), addr.port());
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getNativeRequest() {
        return (T) request;
    }

    @Override
    public Flux<DataBuffer> getBody() {
        return body;
    }
}
