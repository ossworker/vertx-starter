package com.workoss.boot.zenoh.cluster.internal.membership;

import com.workoss.boot.zenoh.cluster.NodeInfo;
import com.workoss.boot.zenoh.cluster.internal.KeySpace;
import com.workoss.boot.zenoh.cluster.internal.transport.ZenohTransport;
import com.workoss.zenoh.Reply;
import com.workoss.zenoh.Sample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Cluster membership over {@code members/{nodeId}}.
 * <p>
 * Each node puts its {@link NodeInfo} every heartbeat interval and deletes the key when it leaves
 * gracefully. A peer is declared gone when either the delete arrives or no heartbeat has been seen
 * for the failure timeout, which covers crashes and network partitions. A queryable on the same
 * keyspace lets a starting node discover existing members immediately; both the first heartbeat
 * and the discovery query are repeated in a short burst after start, because either can race the
 * interest propagation over a fresh link.
 * <p>
 * All state changes and the event callbacks happen under one monitor, so events are emitted in the
 * exact order the membership view changed. {@link Events} implementations must not block.
 */
public final class MembershipService implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(MembershipService.class);
    private static final byte KIND_DELETE = 1;
    private static final String WIRE_VERSION = "1";
    private static final int ENDED_INCARNATIONS_CAPACITY = 1024;

    public interface Events {

        /**
         * @param rejoined the same incarnation was previously declared gone by timeout, i.e. a
         *                 partition healed and the two sides may have diverged
         */
        void joined(NodeInfo node, boolean rejoined);

        void left(NodeInfo node);
    }

    private enum Ending { LEFT, TIMED_OUT }

    private static final class Peer {
        private final NodeInfo info;
        private long lastSeenNanos;

        private Peer(NodeInfo info, long lastSeenNanos) {
            this.info = info;
            this.lastSeenNanos = lastSeenNanos;
        }
    }

    private final ZenohTransport transport;
    private final KeySpace keys;
    private final NodeInfo self;
    private final String selfKey;
    private final byte[] selfPayload;
    private final long heartbeatMillis;
    private final long failureTimeoutNanos;
    private final ScheduledExecutorService scheduler;
    private final Events events;
    private final long startedNanos = System.nanoTime();

    private final Map<String, Peer> peers = new HashMap<>();
    private final Map<String, Ending> endedIncarnations = new LinkedHashMap<>(16, 0.75f, false) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Ending> eldest) {
            return size() > ENDED_INCARNATIONS_CAPACITY;
        }
    };
    private final List<AutoCloseable> channels = new ArrayList<>();
    private final List<ScheduledFuture<?>> tasks = new ArrayList<>();

    public MembershipService(ZenohTransport transport, KeySpace keys, NodeInfo self, long heartbeatMillis,
                             long failureTimeoutMillis, ScheduledExecutorService scheduler, Events events) {
        this.transport = transport;
        this.keys = keys;
        this.self = self;
        this.selfKey = keys.memberKey(self.nodeId());
        this.selfPayload = encode(self);
        this.heartbeatMillis = heartbeatMillis;
        this.failureTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(failureTimeoutMillis);
        this.scheduler = scheduler;
        this.events = events;
    }

    public void start() {
        String pattern = keys.membersPattern();
        channels.add(transport.subscribe(pattern, this::onSample, "zenoh-cluster-members"));
        channels.add(transport.serve(pattern, query -> query.reply(selfKey, selfPayload),
                "zenoh-cluster-members-query"));
        heartbeat();
        discover();
        tasks.add(scheduler.scheduleAtFixedRate(this::heartbeat, heartbeatMillis, heartbeatMillis,
                TimeUnit.MILLISECONDS));
        scheduleJoinBurst();
        long checkEvery = Math.max(50L, heartbeatMillis / 2);
        tasks.add(scheduler.scheduleAtFixedRate(this::detectFailures, checkEvery, checkEvery,
                TimeUnit.MILLISECONDS));
        logger.info("Membership started for node {} (heartbeat={}ms, failureTimeout={}ms)", self.nodeId(),
                heartbeatMillis, TimeUnit.NANOSECONDS.toMillis(failureTimeoutNanos));
    }

    @Override
    public void close() {
        tasks.forEach(task -> task.cancel(false));
        tasks.clear();
        try {
            transport.delete(selfKey);
        } catch (Exception e) {
            logger.debug("Failed to withdraw membership key: {}", e.getMessage());
        }
        for (AutoCloseable channel : channels) {
            try {
                channel.close();
            } catch (Exception e) {
                logger.debug("Failed to close membership channel: {}", e.getMessage());
            }
        }
        channels.clear();
    }

    public NodeInfo self() {
        return self;
    }

    public synchronized List<NodeInfo> members() {
        List<NodeInfo> members = new ArrayList<>(peers.size() + 1);
        members.add(self);
        peers.values().forEach(peer -> members.add(peer.info));
        members.sort(Comparator.comparing(NodeInfo::nodeId));
        return members;
    }

    public synchronized Optional<NodeInfo> member(String nodeId) {
        if (self.nodeId().equals(nodeId)) {
            return Optional.of(self);
        }
        Peer peer = peers.get(nodeId);
        return peer == null ? Optional.empty() : Optional.of(peer.info);
    }

    /**
     * Whether the given node incarnation should be treated as alive. An incarnation this node has
     * never seen counts as alive during the first failure-timeout of this node's run, because
     * discovery may simply not have reached it yet.
     */
    public synchronized boolean isLikelyAlive(String nodeId, String incarnation) {
        if (self.nodeId().equals(nodeId)) {
            return self.incarnation().equals(incarnation);
        }
        Peer peer = peers.get(nodeId);
        if (peer != null) {
            return peer.info.incarnation().equals(incarnation);
        }
        if (endedIncarnations.containsKey(incarnation)) {
            return false;
        }
        return System.nanoTime() - startedNanos < failureTimeoutNanos;
    }

    /**
     * Whether this node is the designated answerer of peers' full-store pulls: the alive member
     * with the smallest node id. One replica suffices because merging is idempotent, so this
     * keeps the fan-in of a resync at one responder instead of every member. While membership
     * views converge more than one node may answer, which is harmless.
     */
    public synchronized boolean isPullSource() {
        for (String nodeId : peers.keySet()) {
            if (nodeId.compareTo(self.nodeId()) < 0) {
                return false;
            }
        }
        return true;
    }

    // --- Events from the network ---

    private void onSample(Sample sample) {
        String nodeId = keys.memberId(sample.keyExpr());
        if (nodeId == null) {
            return;
        }
        if (sample.kind() == KIND_DELETE) {
            if (!self.nodeId().equals(nodeId)) {
                departed(nodeId);
            }
            return;
        }
        NodeInfo info = decode(sample.payload());
        if (info == null || !info.nodeId().equals(nodeId)) {
            return;
        }
        if (self.nodeId().equals(nodeId)) {
            if (!self.incarnation().equals(info.incarnation())) {
                logger.warn("Another process is running with node id {} (incarnation {}); node ids must be unique",
                        nodeId, info.incarnation());
            }
            return;
        }
        observed(info);
    }

    private static final long[] JOIN_BURST_MILLIS = {200L, 700L, 1_700L};

    /**
     * Session open races the propagation of subscriber and queryable interests over fresh links:
     * Zenoh drops puts that nobody subscribes to yet and answers no queries before its queryable
     * is known, so the initial heartbeat/discover pair alone can leave both sides unaware of each
     * other for a full heartbeat interval. Repeating both briefly bounds that window to well
     * under a second regardless of the interval.
     */
    private void scheduleJoinBurst() {
        for (long delay : JOIN_BURST_MILLIS) {
            tasks.add(scheduler.schedule(this::heartbeat, delay, TimeUnit.MILLISECONDS));
            tasks.add(scheduler.schedule(this::discover, delay, TimeUnit.MILLISECONDS));
        }
    }

    private void discover() {
        try {
            for (Reply reply : transport.get(keys.membersPattern())) {
                if (!reply.success() || reply.samplePayload().isEmpty()) {
                    continue;
                }
                NodeInfo info = decode(reply.samplePayload().get());
                if (info != null && !self.nodeId().equals(info.nodeId())) {
                    observed(info);
                }
            }
        } catch (Exception e) {
            logger.warn("Member discovery failed, relying on heartbeats: {}", e.getMessage());
        }
    }

    private void heartbeat() {
        try {
            transport.put(selfKey, selfPayload);
        } catch (Exception e) {
            logger.debug("Heartbeat failed: {}", e.getMessage());
        }
    }

    private synchronized void observed(NodeInfo info) {
        long now = System.nanoTime();
        Peer known = peers.get(info.nodeId());
        if (known != null && known.info.incarnation().equals(info.incarnation())) {
            known.lastSeenNanos = now;
            return;
        }
        Ending ending = endedIncarnations.get(info.incarnation());
        if (ending == Ending.LEFT) {
            return; // a heartbeat overtaken by the node's own leave
        }
        if (known != null) {
            if (info.startedAt() < known.info.startedAt()) {
                return; // a late heartbeat from an incarnation that has already been replaced
            }
            peers.remove(info.nodeId());
            endedIncarnations.put(known.info.incarnation(), Ending.LEFT);
            logger.info("Node {} restarted (incarnation {} -> {})", info.nodeId(),
                    known.info.incarnation(), info.incarnation());
            events.left(known.info);
        }
        peers.put(info.nodeId(), new Peer(info, now));
        boolean rejoined = ending == Ending.TIMED_OUT;
        if (rejoined) {
            endedIncarnations.remove(info.incarnation());
        }
        logger.info("Node {} {}", info.nodeId(), rejoined ? "rejoined" : "joined");
        events.joined(info, rejoined);
    }

    private synchronized void departed(String nodeId) {
        Peer peer = peers.remove(nodeId);
        if (peer != null) {
            endedIncarnations.put(peer.info.incarnation(), Ending.LEFT);
            logger.info("Node {} left", nodeId);
            events.left(peer.info);
        }
    }

    private synchronized void detectFailures() {
        long now = System.nanoTime();
        var iterator = peers.values().iterator();
        while (iterator.hasNext()) {
            Peer peer = iterator.next();
            if (now - peer.lastSeenNanos > failureTimeoutNanos) {
                iterator.remove();
                endedIncarnations.put(peer.info.incarnation(), Ending.TIMED_OUT);
                logger.warn("Node {} missed heartbeats for {}ms, declaring it gone", peer.info.nodeId(),
                        TimeUnit.NANOSECONDS.toMillis(now - peer.lastSeenNanos));
                events.left(peer.info);
            }
        }
    }

    // --- Wire format: "1\nnodeId\nincarnation\nstartedAt\nhostName" ---

    static byte[] encode(NodeInfo info) {
        String text = String.join("\n", WIRE_VERSION, info.nodeId(), info.incarnation(),
                Long.toString(info.startedAt()), info.hostName() == null ? "" : info.hostName());
        return text.getBytes(StandardCharsets.UTF_8);
    }

    static NodeInfo decode(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return null;
        }
        String[] parts = new String(payload, StandardCharsets.UTF_8).split("\n", -1);
        if (parts.length < 5 || !WIRE_VERSION.equals(parts[0]) || parts[1].isEmpty() || parts[2].isEmpty()) {
            return null;
        }
        try {
            return new NodeInfo(parts[1], parts[2], Long.parseLong(parts[3]), parts[4].isEmpty() ? null : parts[4]);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
