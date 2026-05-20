package com.workoss.boot.vertx.autoconfigure;

import io.vertx.core.Vertx;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * Health indicator for the Vert.x instance.
 * Reports UP if Vert.x is not closed, DOWN otherwise.
 */
public class VertxHealthIndicator implements HealthIndicator {

    private final Vertx vertx;

    public VertxHealthIndicator(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public Health health() {
        if (vertx == null) {
            return Health.down().withDetail("error", "Vert.x instance is null").build();
        }

        try {
            // Check if Vert.x is closed by querying deployment IDs
            // If closed, this will throw IllegalStateException
            int deploymentCount = vertx.deploymentIDs().size();

            return Health.up()
                    .withDetail("deploymentCount", deploymentCount)
                    .withDetail("nativeTransport", vertx.isNativeTransportEnabled())
                    .build();
        } catch (IllegalStateException e) {
            return Health.down()
                    .withDetail("error", "Vert.x instance is closed")
                    .build();
        }
    }
}
