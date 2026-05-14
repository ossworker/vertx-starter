package com.vertx.spring.boot.autoconfigure;

import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Manages the lifecycle of the Vert.x instance within the Spring context.
 * Ensures graceful shutdown of Vert.x when the application context is closed.
 */
public class VertxLifecycleManager implements SmartLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(VertxLifecycleManager.class);

    private final Vertx vertx;
    private volatile boolean running = false;

    public VertxLifecycleManager(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public void start() {
        running = true;
        logger.info("Vert.x lifecycle manager started");
    }

    @Override
    public void stop() {
        if (running) {
            running = false;
            logger.info("Shutting down Vert.x instance...");
            vertx.close()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join();
            logger.info("Vert.x instance shut down successfully");
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        // Shut down after web server but before other beans
        return Integer.MAX_VALUE - 1;
    }
}
