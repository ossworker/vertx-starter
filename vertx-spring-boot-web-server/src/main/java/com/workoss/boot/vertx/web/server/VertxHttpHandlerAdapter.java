package com.workoss.boot.vertx.web.server;

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
    private final boolean webSocketEnabled;

    public VertxHttpHandlerAdapter(HttpHandler httpHandler, boolean webSocketEnabled) {
        this.httpHandler = httpHandler;
        this.webSocketEnabled = webSocketEnabled;
    }

    @Override
    public void handle(HttpServerRequest request) {
        // Vert.x ends a body-less GET almost immediately (LastHttpContent.EMPTY_LAST_CONTENT takes a
        // fast path straight to Http1ServerRequest#onEnd), and toWebSocket() refuses an already-read
        // request. Pausing installs the inbound queue, which holds the end event back until the
        // upgrade strategy resumes it.
        boolean mayUpgrade = webSocketEnabled && request.canUpgradeToWebSocket();
        if (mayUpgrade) {
            request.pause();
        }

        VertxServerHttpRequest httpRequest = new VertxServerHttpRequest(request);
        VertxServerHttpResponse httpResponse = new VertxServerHttpResponse(request.response());

        // Three-arg subscribe avoids the extra LambdaSubscriber wrappers introduced
        // by doOnSuccess/doOnError, reducing the operator chain length per request.
        httpHandler.handle(httpRequest, httpResponse).subscribe(
                null,
                err -> {
                    logger.error("Error handling request: {} {}", request.method(), request.uri(), err);
                    releaseIfNotUpgraded(request, mayUpgrade);
                    if (!request.response().ended()) {
                        request.response().setStatusCode(500).end();
                    }
                },
                () -> {
                    releaseIfNotUpgraded(request, mayUpgrade);
                    if (!request.response().ended()) {
                        request.response().end();
                    }
                });
    }

    /**
     * Releases the end event held back by {@code pause()}. A successful {@code toWebSocket()} leaves
     * the request ended and hands the connection to Vert.x, so it must not be touched again; a
     * request that advertised an upgrade but was answered over HTTP instead still needs the release,
     * otherwise Vert.x never completes its request metrics. This runs outside the
     * {@code response().ended()} guard precisely because a WebFilter short-circuiting with an error
     * status has already ended the response.
     */
    private static void releaseIfNotUpgraded(HttpServerRequest request, boolean mayUpgrade) {
        if (mayUpgrade && !request.isEnded()) {
            request.resume();
        }
    }
}
