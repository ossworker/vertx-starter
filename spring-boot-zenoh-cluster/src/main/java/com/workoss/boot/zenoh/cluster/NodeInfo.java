package com.workoss.boot.zenoh.cluster;

/**
 * A cluster member.
 *
 * @param nodeId      stable node id
 * @param incarnation unique per start of the node; distinguishes a restarted node from its
 *                    previous run
 * @param startedAt   wall-clock millis when this incarnation started
 * @param hostName    host the node runs on, or {@code null} when unknown
 */
public record NodeInfo(String nodeId, String incarnation, long startedAt, String hostName) {
}
