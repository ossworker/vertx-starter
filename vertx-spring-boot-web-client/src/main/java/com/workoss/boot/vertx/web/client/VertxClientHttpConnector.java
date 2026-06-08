package com.workoss.boot.vertx.web.client;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseCookie;
import org.springframework.http.client.reactive.ClientHttpConnector;
import org.springframework.http.client.reactive.ClientHttpRequest;
import org.springframework.http.client.reactive.ClientHttpResponse;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * ClientHttpConnector implementation backed by Vert.x HttpClient.
 * Used by WebClient for reactive HTTP calls.
 */
public class VertxClientHttpConnector implements ClientHttpConnector, AutoCloseable {

    private final HttpClient httpClient;

    public VertxClientHttpConnector(Vertx vertx, VertxHttpClientProperties properties) {
        HttpClientOptions options = new HttpClientOptions();
        options.setConnectTimeout(properties.getConnectTimeout());
        options.setIdleTimeout(properties.getIdleTimeout());
        options.setIdleTimeoutUnit(TimeUnit.SECONDS);
        options.setKeepAlive(properties.isKeepAlive());
        options.setPipelining(properties.isPipelining());
        this.httpClient = vertx.createHttpClient(options);
    }

    @Override
    public Mono<ClientHttpResponse> connect(
            org.springframework.http.HttpMethod method,
            URI uri,
            Function<? super ClientHttpRequest, Mono<Void>> requestCallback) {

        return Mono.defer(() -> {
            int port = uri.getPort();
            if (port == -1) {
                port = "https".equals(uri.getScheme()) ? 443 : 80;
            }
            boolean ssl = "https".equals(uri.getScheme());

            return Mono.fromCompletionStage(
                    httpClient.request(
                            new io.vertx.core.http.RequestOptions()
                                    .setMethod(HttpMethod.valueOf(method.name()))
                                    .setHost(uri.getHost())
                                    .setPort(port)
                                    .setSsl(ssl)
                                    .setURI(uri.getRawPath() + (uri.getRawQuery() != null ? "?" + uri.getRawQuery() : ""))
                    ).toCompletionStage()
            ).flatMap(vertxRequest -> {
                VertxReactiveClientHttpRequest request = new VertxReactiveClientHttpRequest(vertxRequest, method, uri);
                return requestCallback.apply(request)
                        .then(Mono.defer(() -> {
                            // Send the request body and get response
                            byte[] body = request.getBodyBytes();
                            if (body != null && body.length > 0) {
                                return Mono.fromCompletionStage(
                                        vertxRequest.send(Buffer.buffer(body)).toCompletionStage()
                                );
                            } else {
                                return Mono.fromCompletionStage(
                                        vertxRequest.send().toCompletionStage()
                                );
                            }
                        }));
            }).map(VertxReactiveClientHttpResponse::new);
        });
    }

    @Override
    public void close() {
        try {
            httpClient.close()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join();
        } catch (RejectedExecutionException e) {
            // Vert.x event executor already terminated during shutdown, ignore
        }
    }

    /**
     * Reactive ClientHttpRequest backed by Vert.x.
     */
    private static class VertxReactiveClientHttpRequest extends org.springframework.http.client.reactive.AbstractClientHttpRequest {

        private final HttpClientRequest vertxRequest;
        private final org.springframework.http.HttpMethod method;
        private final URI uri;
        private byte[] bodyBytes;

        VertxReactiveClientHttpRequest(HttpClientRequest vertxRequest,
                                       org.springframework.http.HttpMethod method,
                                       URI uri) {
            this.vertxRequest = vertxRequest;
            this.method = method;
            this.uri = uri;
        }

        @Override
        public org.springframework.http.HttpMethod getMethod() {
            return method;
        }

        @Override
        public URI getURI() {
            return uri;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T getNativeRequest() {
            return (T) vertxRequest;
        }

        @Override
        public org.springframework.core.io.buffer.DataBufferFactory bufferFactory() {
            return DefaultDataBufferFactory.sharedInstance;
        }

        @Override
        public Mono<Void> writeWith(Publisher<? extends DataBuffer> body) {
            return Flux.from(body)
                    .collectList()
                    .doOnNext(buffers -> {
                        int totalSize = buffers.stream().mapToInt(DataBuffer::readableByteCount).sum();
                        byte[] combined = new byte[totalSize];
                        int offset = 0;
                        for (DataBuffer buffer : buffers) {
                            int len = buffer.readableByteCount();
                            buffer.read(combined, offset, len);
                            DataBufferUtils.release(buffer);
                            offset += len;
                        }
                        this.bodyBytes = combined;
                    })
                    .then();
        }

        @Override
        public Mono<Void> writeAndFlushWith(Publisher<? extends Publisher<? extends DataBuffer>> body) {
            return writeWith(Flux.from(body).flatMap(Flux::from));
        }

        @Override
        public Mono<Void> setComplete() {
            this.bodyBytes = new byte[0];
            return doCommit();
        }

        @Override
        protected void applyHeaders() {
            getHeaders().forEach((name, values) -> {
                for (String value : values) {
                    vertxRequest.putHeader(name, value);
                }
            });
        }

        @Override
        protected void applyCookies() {
            // Cookies are applied via headers
        }

        byte[] getBodyBytes() {
            return bodyBytes;
        }
    }

    /**
     * Reactive ClientHttpResponse backed by Vert.x.
     */
    private static class VertxReactiveClientHttpResponse implements ClientHttpResponse {

        private final HttpClientResponse response;

        VertxReactiveClientHttpResponse(HttpClientResponse response) {
            this.response = response;
        }

        @Override
        public HttpStatusCode getStatusCode() {
            return HttpStatusCode.valueOf(response.statusCode());
        }

        @Override
        public HttpHeaders getHeaders() {
            HttpHeaders headers = new HttpHeaders();
            response.headers().forEach(entry -> headers.add(entry.getKey(), entry.getValue()));
            return headers;
        }

        @Override
        public Flux<DataBuffer> getBody() {
            return Mono.fromCompletionStage(response.body().toCompletionStage())
                    .flatMapMany(buffer -> {
                        if (buffer != null && buffer.length() > 0) {
                            DataBuffer dataBuffer = DefaultDataBufferFactory.sharedInstance
                                    .wrap(buffer.getBytes());
                            return Flux.just(dataBuffer);
                        }
                        return Flux.empty();
                    });
        }

        @Override
        public MultiValueMap<String, ResponseCookie> getCookies() {
            MultiValueMap<String, ResponseCookie> cookies = new LinkedMultiValueMap<>();
            List<String> setCookieHeaders = getHeaders().get("Set-Cookie");
            if (setCookieHeaders != null) {
                for (String header : setCookieHeaders) {
                    // Simple parsing - get name=value
                    String[] parts = header.split(";")[0].split("=", 2);
                    if (parts.length == 2) {
                        ResponseCookie cookie = ResponseCookie.from(parts[0].trim(), parts[1].trim()).build();
                        cookies.add(parts[0].trim(), cookie);
                    }
                }
            }
            return cookies;
        }
    }
}
