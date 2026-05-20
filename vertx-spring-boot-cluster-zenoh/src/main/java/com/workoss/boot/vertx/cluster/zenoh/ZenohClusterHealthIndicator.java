package com.workoss.boot.vertx.cluster.zenoh;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

import java.util.List;

/**
 * Health indicator for the Zenoh cluster.
 * Reports cluster connectivity status, node count, and node ID.
 */
public class ZenohClusterHealthIndicator implements HealthIndicator {

    private final ZenohClusterManager clusterManager;

    public ZenohClusterHealthIndicator(ZenohClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    @Override
    public Health health() {
        if (clusterManager == null) {
            return Health.unknown()
                    .withDetail("error", "ClusterManager is null")
                    .build();
        }

        boolean active = clusterManager.isActive();
        String nodeId = clusterManager.getNodeId();
        List<String> nodes = clusterManager.getNodes();
        int nodeCount = nodes.size();

        Health.Builder builder = active ? Health.up() : Health.down();

        builder
                .withDetail("nodeId", nodeId)
                .withDetail("active", active)
                .withDetail("nodeCount", nodeCount)
                .withDetail("nodes", nodes);

        if (!active) {
            builder.withDetail("error", "Zenoh cluster is not active");
        }

        return builder.build();
    }
}
