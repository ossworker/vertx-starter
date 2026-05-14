package com.vertx.spring.boot.web.server;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.server.GracefulShutdownCallback;
import org.springframework.boot.web.server.GracefulShutdownResult;
import org.springframework.boot.web.server.WebServer;
import org.springframework.boot.web.server.WebServerException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * WebServer implementation backed by Vert.x HttpServer.
 */
public class VertxWebServer implements WebServer {

    private static final Logger logger = LoggerFactory.getLogger(VertxWebServer.class);

    private final Vertx vertx;
    private final HttpServerOptions options;
    private final VertxHttpHandlerAdapter handler;
    private HttpServer httpServer;
    private int actualPort;

    public VertxWebServer(Vertx vertx, HttpServerOptions options, VertxHttpHandlerAdapter handler) {
        this.vertx = vertx;
        this.options = options;
        this.handler = handler;
    }

    @Override
    public void start() throws WebServerException {
        try {
            httpServer = vertx.createHttpServer(options);
            httpServer.requestHandler(handler);

            actualPort = httpServer.listen()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join()
                    .actualPort();

            logger.info("Vert.x HTTP server started on port {}", actualPort);
        } catch (Exception e) {
            throw new WebServerException("Failed to start Vert.x HTTP server", e);
        }
    }

    @Override
    public void stop() throws WebServerException {
        if (httpServer != null) {
            try {
                httpServer.close()
                        .toCompletionStage()
                        .toCompletableFuture()
                        .join();
                logger.info("Vert.x HTTP server stopped");
            } catch (Exception e) {
                throw new WebServerException("Failed to stop Vert.x HTTP server", e);
            }
        }
    }

    @Override
    public int getPort() {
        return actualPort;
    }

    @Override
    public void shutDownGracefully(GracefulShutdownCallback callback) {
        if (httpServer != null) {
            logger.info("Initiating graceful shutdown of Vert.x HTTP server...");
            httpServer.shutdown(30, TimeUnit.SECONDS)
                    .toCompletionStage()
                    .toCompletableFuture()
                    .whenComplete((v, err) -> {
                        if (err != null) {
                            logger.warn("Graceful shutdown completed with error", err);
                            callback.shutdownComplete(GracefulShutdownResult.REQUESTS_ACTIVE);
                        } else {
                            logger.info("Graceful shutdown completed successfully");
                            callback.shutdownComplete(GracefulShutdownResult.IDLE);
                        }
                    });
        } else {
            callback.shutdownComplete(GracefulShutdownResult.IDLE);
        }
    }
}
