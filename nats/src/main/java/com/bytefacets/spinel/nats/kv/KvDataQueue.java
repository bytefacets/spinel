// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats.kv;

import static java.util.Objects.requireNonNull;

import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueOperation;
import io.nats.client.api.KeyValueWatcher;
import io.netty.channel.EventLoop;
import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

/** Carries updates from a NATS KeyValueWatcher onto an EventLoop for processing. */
final class KvDataQueue implements Runnable {
    private final AtomicBoolean signal = new AtomicBoolean();
    private final EventLoop eventLoop;
    private final Listener listener;
    private final long timeBudgetNanos;
    private final LongSupplier nanoSupplier;
    private final Subscriber subscriber = new Subscriber();
    private final ConcurrentLinkedDeque<Update> updates = new ConcurrentLinkedDeque<>();
    private final ConcurrentLinkedDeque<Update> free = new ConcurrentLinkedDeque<>();

    KvDataQueue(
            final EventLoop eventLoop,
            final Listener listener,
            final Duration timeBudget,
            final LongSupplier nanoSupplier) {
        this.eventLoop = requireNonNull(eventLoop, "eventLoop");
        this.listener = requireNonNull(listener, "listener");
        this.timeBudgetNanos = requireNonNull(timeBudget, "timeBudget").toNanos();
        this.nanoSupplier = requireNonNull(nanoSupplier, "nanoSupplier");
    }

    KeyValueWatcher kvWatcher() {
        return subscriber;
    }

    private void add(final KeyValueEntry entry) {
        updates.addLast(allocate().set(entry));
        signal();
    }

    private void endOfData() {
        updates.addLast(allocate().endOfData());
        signal();
    }

    private void signal() {
        if (signal.compareAndSet(false, true)) {
            eventLoop.execute(this);
        }
    }

    @Override
    public void run() {
        signal.set(false);
        final long endBudget = nanoSupplier.getAsLong() + timeBudgetNanos;
        while (!updates.isEmpty()) {
            final var update = updates.removeFirst();
            process(update);
            if (nanoSupplier.getAsLong() >= endBudget) {
                listener.fireChanges();
                // fire again because we're not done
                scheduleForMore();
                break;
            }
        }
        listener.fireChanges();
    }

    private void scheduleForMore() {
        eventLoop.schedule(this, 1, TimeUnit.NANOSECONDS);
    }

    private void process(final Update update) {
        if (update.endOfData) {
            listener.caughtUp();
            free(update);
            return;
        }
        final KeyValueEntry entry = update.entry;
        free(update);
        if (entry.getOperation().equals(KeyValueOperation.DELETE)
                || entry.getOperation().equals(KeyValueOperation.PURGE)) {
            listener.delete(entry);
        } else {
            listener.update(entry);
        }
    }

    private final class Subscriber implements KeyValueWatcher {
        @Override
        public void watch(final KeyValueEntry entry) {
            add(entry);
        }

        @Override
        public void endOfData() {
            KvDataQueue.this.endOfData();
        }
    }

    interface Listener {
        void update(KeyValueEntry entry);

        void delete(KeyValueEntry key);

        void fireChanges();

        void caughtUp();
    }

    private void free(final Update update) {
        free.addFirst(update.reset());
    }

    private Update allocate() {
        if (free.isEmpty()) {
            return new Update();
        } else {
            return free.removeFirst();
        }
    }

    private static class Update {
        private KeyValueEntry entry;
        private boolean endOfData;

        Update reset() {
            return set(null);
        }

        Update endOfData() {
            this.entry = null;
            this.endOfData = true;
            return this;
        }

        Update set(final KeyValueEntry entry) {
            this.entry = entry;
            this.endOfData = false;
            return this;
        }
    }

    // VisibleForTesting
    boolean isSignalled() {
        return signal.get();
    }

    // VisibleForTesting
    int pending() {
        return updates.size();
    }

    // VisibleForTesting
    int free() {
        return free.size();
    }
}
