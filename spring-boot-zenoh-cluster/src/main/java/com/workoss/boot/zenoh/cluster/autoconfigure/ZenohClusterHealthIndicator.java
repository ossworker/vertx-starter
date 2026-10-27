package com.workoss.boot.zenoh.cluster.autoconfigure;

import com.workoss.boot.zenoh.cluster.NodeInfo;
import com.workoss.boot.zenoh.cluster.ZenohCluster;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.util.List;

public class ZenohClusterHealthIndicator extends AbstractHealthIndicator {

    private final ZenohCluster cluster;

    public ZenohClusterHealthIndicator(ZenohCluster cluster) {
        this.cluster = cluster;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) {
        boolean active = cluster.isActive();
        List<NodeInfo> members = cluster.members();
        builder.status(active ? Status.UP : Status.DOWN)
                .withDetail("nodeId", cluster.nodeId())
                .withDetail("active", active)
                .withDetail("memberCount", members.size())
                .withDetail("members", members.stream().map(NodeInfo::nodeId).toList());
        cluster.sessionId().ifPresent(id -> builder.withDetail("sessionId", id));
    }
}
