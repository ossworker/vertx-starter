package com.workoss.boot.zenoh.cluster.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs submitted tasks one at a time in submission order on a virtual thread that exists only
 * while there is work. Needs no shutdown.
 */
public final class SerialExecutor implements Executor {

    private static final Logger logger = LoggerFactory.getLogger(SerialExecutor.class);

    private final String name;
    private final Queue<Runnable> queue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean draining = new AtomicBoolean(false);

    public SerialExecutor(String name) {
        this.name = name;
    }

    @Override
    public void execute(Runnable task) {
        queue.add(task);
        scheduleDrain();
    }

    private void scheduleDrain() {
        if (draining.compareAndSet(false, true)) {
            Thread.ofVirtual().name(name).start(this::drain);
        }
    }

    private void drain() {
        try {
            Runnable task;
            while ((task = queue.poll()) != null) {
                try {
                    task.run();
                } catch (Throwable t) {
                    logger.warn("Task on {} failed", name, t);
                }
            }
        } finally {
            draining.set(false);
            // A task enqueued between the last poll and the flag reset would otherwise be stranded.
            if (!queue.isEmpty()) {
                scheduleDrain();
            }
        }
    }
}
