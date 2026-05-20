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
 * Manages the lifecycle of the Vert.x instance and optional cluster manager within the Spring context.
 * Ensures graceful shutdown of Vert.x and cluster resources when the application context is closed.
 * <p>
 * This implementation supports Spring Boot's graceful shutdown mechanism by:
 * <ul>
 *   <li>Using asynchronous close with timeout control</li>
 *   <li>Proper phase ordering to shut down after web server</li>
 *   <li>Handling shutdown errors gracefully, including event loop termination</li>
 *   <li>Managing Zenoh cluster join/leave lifecycle if a Zenoh ClusterManager is present</li>
 * </ul>
 */
public class VertxLifecycleManager implements SmartLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(VertxLifecycleManager.class);

    private final Vertx vertx;
    private final ClusterManager clusterManager;
    private final long shutdownTimeoutSeconds;
    private volatile boolean running = false;
    private volatile boolean clusterJoined = false;

    public VertxLifecycleManager(Vertx vertx, VertxProperties properties, ClusterManager clusterManager) {
        this.vertx = vertx;
        this.shutdownTimeoutSeconds = properties.getGracefulShutdownTimeoutSeconds();
        this.clusterManager = clusterManager;
    }

    @Override
    public void start() {
        running = true;
        logger.info("Vert.x lifecycle manager started");

        // Initialize and join Zenoh cluster if present
        if (isZenohClusterManager(clusterManager)) {
            initZenohCluster();
        }
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        running = false;

        // Leave Zenoh cluster first if joined
        if (clusterJoined && isZenohClusterManager(clusterManager)) {
            leaveZenohCluster();
        }

        // Then shutdown Vert.x
        logger.info("Initiating graceful shutdown of Vert.x instance...");
        try {
            vertx.close()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(shutdownTimeoutSeconds, TimeUnit.SECONDS);
            logger.info("Vert.x instance shutdown successfully");
        } catch (RejectedExecutionException e) {
            // Event loop already terminated, this is expected during JVM shutdown
            logger.debug("Vert.x event loop already terminated (expected during shutdown)");
        } catch (TimeoutException e) {
            logger.warn("Vert.x shutdown timed out after {} seconds", shutdownTimeoutSeconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Vert.x shutdown interrupted");
        } catch (Exception e) {
            // Check if it's caused by event loop termination
            if (isEventLoopTerminated(e)) {
                logger.debug("Vert.x already shutdown (event loop terminated)");
            } else {
                logger.error("Error during Vert.x shutdown", e);
            }
        }
    }

    /**
     * Initialize and join the Zenoh cluster asynchronously.
     */
    private void initZenohCluster() {
        logger.info("Initializing Zenoh cluster...");
        clusterManager.init(vertx);

        // Join the cluster asynchronously to avoid blocking Spring startup
        new Thread(() -> {
            try {
                java.util.concurrent.CompletableFuture<Void> joinFuture = new java.util.concurrent.CompletableFuture<>();
                clusterManager.join((result, err) -> {
                    if (err != null) {
                        joinFuture.completeExceptionally(err);
                    } else {
                        joinFuture.complete(null);
                    }
                });
                joinFuture.get(30, TimeUnit.SECONDS);
                clusterJoined = true;
                logger.info("Zenoh cluster joined successfully");
            } catch (Exception e) {
                clusterJoined = false;
                logger.error("Failed to join Zenoh cluster", e);
            }
        }, "zenoh-cluster-join").start();
    }

    /**
     * Leave the Zenoh cluster synchronously during shutdown.
     */
    private void leaveZenohCluster() {
        logger.info("Leaving Zenoh cluster...");
        try {
            java.util.concurrent.CompletableFuture<Void> leaveFuture = new java.util.concurrent.CompletableFuture<>();
            clusterManager.leave((result, err) -> {
                if (err != null) {
                    leaveFuture.completeExceptionally(err);
                } else {
                    leaveFuture.complete(null);
                }
            });
            leaveFuture.get(10, TimeUnit.SECONDS);
            logger.info("Zenoh cluster left successfully");
        } catch (Exception e) {
            logger.error("Failed to leave Zenoh cluster gracefully", e);
        } finally {
            clusterJoined = false;
        }
    }

    /**
     * Check if the cluster manager is Zenoh-based.
     */
    private boolean isZenohClusterManager(ClusterManager clusterManager) {
        return clusterManager != null &&
               (clusterManager.getClass().getName().contains("zenoh") ||
                clusterManager.getClass().getSimpleName().contains("Zenoh"));
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
