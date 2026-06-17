package com.workoss.boot.vertx.web.server;

import io.vertx.core.http.HttpServerRequest;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpCookie;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.server.reactive.AbstractServerHttpRequest;
import org.springframework.http.server.reactive.SslInfo;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import reactor.core.publisher.Flux;

import java.net.InetSocketAddress;
import java.net.URI;

/**
 * Adapts a Vert.x HttpServerRequest to Spring's ServerHttpRequest.
 */
public class VertxServerHttpRequest extends AbstractServerHttpRequest {

    /** Empty body flux shared by all body-less requests (GET, HEAD, etc.). */
    private static final Flux<DataBuffer> EMPTY_BODY = Flux.empty();

    private final HttpServerRequest request;
    private final Flux<DataBuffer> body;

    public VertxServerHttpRequest(HttpServerRequest request) {
        super(HttpMethod.valueOf(request.method().name()), buildUri(request), null, buildHeaders(request));
        this.request = request;
        this.body = hasBody(request.method())
                ? Flux.from(new VertxRequestBodyPublisher(request))
                : EMPTY_BODY;
    }

    /**
     * Returns {@code true} if the HTTP method typically carries a request body.
     * GET, HEAD, DELETE, OPTIONS, TRACE never have a meaningful body.
     */
    private static boolean hasBody(io.vertx.core.http.HttpMethod method) {
        return method != io.vertx.core.http.HttpMethod.GET
                && method != io.vertx.core.http.HttpMethod.HEAD
                && method != io.vertx.core.http.HttpMethod.OPTIONS
                && method != io.vertx.core.http.HttpMethod.TRACE;
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

    /**
     * Build Spring {@link HttpHeaders} backed by a lazy view over Vert.x MultiMap.
     * Avoids the eager full-copy of headers on every request.
     */
    private static HttpHeaders buildHeaders(HttpServerRequest request) {
        return new HttpHeaders(new VertxHttpHeadersAdapter(request.headers()));
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
