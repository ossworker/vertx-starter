package com.workoss.boot.zenoh.cluster.internal.store;

import com.workoss.boot.zenoh.cluster.internal.KeySpace;
import com.workoss.boot.zenoh.cluster.internal.transport.ZenohTransport;
import com.workoss.zenoh.Reply;
import com.workoss.zenoh.Sample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

/**
 * Wires a {@link ReplicatedStore} to Zenoh: local writes are published, peer writes are merged,
 * and a queryable serves the replica (tombstones included) to nodes that bootstrap or
 * resynchronise from us. A full-store pull is answered only by the designated member
 * ({@code pullSource}): merging is idempotent and order-independent, so one replica repairs any
 * divergence, and the response fan-in of a resync stays at O(1) instead of one full replica per
 * member.
 */
public final class StoreReplicator implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(StoreReplicator.class);
    private static final byte KIND_DELETE = 1;

    private final ZenohTransport transport;
    private final KeySpace keys;
    private final ReplicatedStore store;
    private final BooleanSupplier pullSource;
    private final List<AutoCloseable> channels = new ArrayList<>();

    public StoreReplicator(ZenohTransport transport, KeySpace keys, ReplicatedStore store,
                           BooleanSupplier pullSource) {
        this.transport = transport;
        this.keys = keys;
        this.store = store;
        this.pullSource = pullSource;
    }

    public void start() {
        String pattern = keys.storePattern();
        channels.add(transport.subscribe(pattern, this::onSample, "zenoh-cluster-store"));
        channels.add(transport.serve(pattern, query -> {
            String selector = query.getKeyExpr();
            if (pattern.equals(selector) && !pullSource.getAsBoolean()) {
                return; // full pull: let the designated member answer
            }
            store.forEachMatching(selector, query::reply);
        }, "zenoh-cluster-store-query"));
        store.setSink(transport::putAsync);
    }

    /**
     * Pulls the designated member's replica (or, while membership views converge, whichever
     * members currently consider themselves designated) and merges it.
     *
     * @return number of entries that changed the local replica
     */
    public int resync() {
        int changed = 0;
        try {
            for (Reply reply : transport.get(keys.storePattern())) {
                if (reply.success() && reply.sampleKeyExpr().isPresent() && reply.samplePayload().isPresent()
                        && store.applyRemote(reply.sampleKeyExpr().get(), reply.samplePayload().get())) {
                    changed++;
                }
            }
        } catch (Exception e) {
            logger.warn("Store resync failed: {}", e.getMessage());
        }
        return changed;
    }

    @Override
    public void close() {
        store.setSink(ReplicatedStore.NO_REPLICATION);
        for (AutoCloseable channel : channels) {
            try {
                channel.close();
            } catch (Exception e) {
                logger.debug("Failed to close store channel: {}", e.getMessage());
            }
        }
        channels.clear();
    }

    private void onSample(Sample sample) {
        // Deletes travel as tombstone puts; a raw Zenoh delete carries no version and is ignored.
        if (sample.kind() != KIND_DELETE) {
            store.applyRemote(sample.keyExpr(), sample.payload());
        }
    }
}
