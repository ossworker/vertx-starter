package com.workoss.boot.vertx.web.client;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * ClientHttpRequestFactory implementation backed by Vert.x HttpClient.
 * Used by RestTemplate.
 */
public class VertxClientHttpRequestFactory implements ClientHttpRequestFactory, AutoCloseable {

    private final HttpClient httpClient;

    public VertxClientHttpRequestFactory(Vertx vertx, VertxHttpClientProperties properties) {
        HttpClientOptions options = new HttpClientOptions();
        options.setConnectTimeout(properties.getConnectTimeout());
        options.setIdleTimeout(properties.getIdleTimeout());
        options.setIdleTimeoutUnit(TimeUnit.SECONDS);
        options.setKeepAlive(properties.isKeepAlive());
        options.setPipelining(properties.isPipelining());
        this.httpClient = vertx.createHttpClient(options);
    }

    @Override
    public ClientHttpRequest createRequest(URI uri, org.springframework.http.HttpMethod httpMethod) throws IOException {
        return new VertxClientHttpRequest(httpClient, uri, httpMethod);
    }

    @Override
    public void close() {
        httpClient.close()
                .toCompletionStage()
                .toCompletableFuture()
                .join();
    }

    /**
     * ClientHttpRequest backed by Vert.x HttpClient.
     */
    private static class VertxClientHttpRequest implements ClientHttpRequest {

        private final HttpClient httpClient;
        private final URI uri;
        private final org.springframework.http.HttpMethod method;
        private final HttpHeaders headers = new HttpHeaders();
        private final ByteArrayOutputStream bodyStream = new ByteArrayOutputStream();
        private final Map<String, Object> attributes = new HashMap<>();

        VertxClientHttpRequest(HttpClient httpClient, URI uri, org.springframework.http.HttpMethod method) {
            this.httpClient = httpClient;
            this.uri = uri;
            this.method = method;
        }

        @Override
        public Map<String, Object> getAttributes() {
            return attributes;
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
        public HttpHeaders getHeaders() {
            return headers;
        }

        @Override
        public OutputStream getBody() {
            return bodyStream;
        }

        @Override
        public ClientHttpResponse execute() throws IOException {
            try {
                int port = uri.getPort();
                if (port == -1) {
                    port = "https".equals(uri.getScheme()) ? 443 : 80;
                }
                boolean ssl = "https".equals(uri.getScheme());

                HttpClientRequest request = httpClient.request(
                                new io.vertx.core.http.RequestOptions()
                                        .setMethod(HttpMethod.valueOf(method.name()))
                                        .setHost(uri.getHost())
                                        .setPort(port)
                                        .setSsl(ssl)
                                        .setURI(uri.getRawPath() + (uri.getRawQuery() != null ? "?" + uri.getRawQuery() : ""))
                        )
                        .toCompletionStage()
                        .toCompletableFuture()
                        .join();

                // Set headers
                headers.forEach((name, values) -> {
                    for (String value : values) {
                        request.putHeader(name, value);
                    }
                });

                // Send body and get response
                byte[] body = bodyStream.toByteArray();
                HttpClientResponse response;
                if (body.length > 0) {
                    response = request.send(Buffer.buffer(body))
                            .toCompletionStage()
                            .toCompletableFuture()
                            .join();
                } else {
                    response = request.send()
                            .toCompletionStage()
                            .toCompletableFuture()
                            .join();
                }

                // Read response body
                Buffer responseBody = response.body()
                        .toCompletionStage()
                        .toCompletableFuture()
                        .join();

                return new VertxClientHttpResponse(response, responseBody);
            } catch (Exception e) {
                throw new IOException("Failed to execute HTTP request via Vert.x", e);
            }
        }
    }

    /**
     * ClientHttpResponse backed by Vert.x HttpClientResponse.
     */
    private static class VertxClientHttpResponse implements ClientHttpResponse {

        private final HttpClientResponse response;
        private final Buffer body;
        private final HttpHeaders headers;

        VertxClientHttpResponse(HttpClientResponse response, Buffer body) {
            this.response = response;
            this.body = body;
            this.headers = new HttpHeaders();
            response.headers().forEach(entry -> headers.add(entry.getKey(), entry.getValue()));
        }

        @Override
        public org.springframework.http.HttpStatusCode getStatusCode() {
            return org.springframework.http.HttpStatusCode.valueOf(response.statusCode());
        }

        @Override
        public String getStatusText() {
            return response.statusMessage();
        }

        @Override
        public HttpHeaders getHeaders() {
            return headers;
        }

        @Override
        public InputStream getBody() {
            return new ByteArrayInputStream(body != null ? body.getBytes() : new byte[0]);
        }

        @Override
        public void close() {
            // No-op: response already fully consumed
        }
    }
}
