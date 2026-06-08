package com.workoss.boot.vertx.autoconfigure;

import io.vertx.core.Vertx;
import io.vertx.core.spi.cluster.ClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Manages the lifecycle of the Vert.x instance within the Spring context.
 * Ensures graceful shutdown of Vert.x when the application context is closed.
 * <p>
 * Note: When using buildClustered(), Vert.x handles ClusterManager lifecycle
 * (init/join/leave) automatically. This manager only handles Vert.x shutdown.
 */
public class VertxLifecycleManager implements SmartLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(VertxLifecycleManager.class);

    private final Vertx vertx;
    private final long shutdownTimeoutSeconds;
    private volatile boolean running = false;

    public VertxLifecycleManager(Vertx vertx, VertxProperties properties, ClusterManager clusterManager) {
        this.vertx = vertx;
        this.shutdownTimeoutSeconds = properties.getGracefulShutdownTimeoutSeconds();
    }

    @Override
    public void start() {
        running = true;
        logger.info("Vert.x lifecycle manager started");
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        running = false;

        logger.info("Initiating graceful shutdown of Vert.x instance...");
        try {
            vertx.close()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(shutdownTimeoutSeconds, TimeUnit.SECONDS);
            logger.info("Vert.x instance shutdown successfully");
        } catch (RejectedExecutionException e) {
            logger.debug("Vert.x event loop already terminated (expected during shutdown)");
        } catch (TimeoutException e) {
            logger.warn("Vert.x shutdown timed out after {} seconds", shutdownTimeoutSeconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Vert.x shutdown interrupted");
        } catch (Exception e) {
            if (isEventLoopTerminated(e)) {
                logger.debug("Vert.x already shutdown (event loop terminated)");
            } else {
                logger.error("Error during Vert.x shutdown", e);
            }
        }
    }

    /**
     * Checks if the exception is caused by event loop termination.
     */
    private boolean isEventLoopTerminated(Throwable e) {
        Throwable cause = e;
        while (cause != null) {
            if (cause instanceof RejectedExecutionException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        // Shut down after web server (which uses Integer.MAX_VALUE) but before other beans
        // This ensures HTTP connections are drained before closing Vert.x
        return Integer.MAX_VALUE - 1;
    }
}
