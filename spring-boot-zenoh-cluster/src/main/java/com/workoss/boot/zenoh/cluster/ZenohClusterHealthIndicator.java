package com.workoss.boot.zenoh.cluster;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

import java.util.List;

/**
 * Health indicator for the Zenoh cluster. Reports cluster connectivity status, node count, node id
 * and the Zenoh session id. Only registered when Spring Boot Actuator is on the classpath.
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
        String zid = clusterManager.getSession() == null ? null : clusterManager.getSession().zid();

        Health.Builder builder = active ? Health.up() : Health.down();

        builder
                .withDetail("nodeId", nodeId)
                .withDetail("active", active)
                .withDetail("nodeCount", nodes.size())
                .withDetail("nodes", nodes)
                .withDetail("zid", zid);

        if (!active) {
            builder.withDetail("error", "Zenoh cluster is not active");
        }

        return builder.build();
    }
}
