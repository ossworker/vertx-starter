package com.vertx.spring.boot.web.server;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.server.reactive.HttpHandler;

/**
 * Bridges Spring's HttpHandler to Vert.x's Handler&lt;HttpServerRequest&gt;.
 * This is the core adapter that connects Vert.x HTTP server to Spring WebFlux.
 */
public class VertxHttpHandlerAdapter implements Handler<HttpServerRequest> {

    private static final Logger logger = LoggerFactory.getLogger(VertxHttpHandlerAdapter.class);

    private final HttpHandler httpHandler;

    public VertxHttpHandlerAdapter(HttpHandler httpHandler) {
        this.httpHandler = httpHandler;
    }

    @Override
    public void handle(HttpServerRequest request) {
        VertxServerHttpRequest httpRequest = new VertxServerHttpRequest(request);
        VertxServerHttpResponse httpResponse = new VertxServerHttpResponse(request.response());

        httpHandler.handle(httpRequest, httpResponse)
                .doOnSuccess(v -> {
                    if (!request.response().ended()) {
                        request.response().end();
                    }
                })
                .doOnError(err -> {
                    logger.error("Error handling request: {} {}", request.method(), request.uri(), err);
                    if (!request.response().ended()) {
                        request.response()
                                .setStatusCode(500)
                                .end();
                    }
                })
                .subscribe();
    }
}
