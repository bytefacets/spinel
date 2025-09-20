// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.examples.common;

import com.bytefacets.collections.queue.IntDeque;
import com.bytefacets.spinel.table.IntIndexedStructTable;
import io.netty.channel.EventLoop;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Simulator to grow to a set of maximum items, and then modify them. */
public final class MaxItemSimulator<T> {
    private final EventBatcher<T> batcher;

    private MaxItemSimulator(final Builder<T> builder) {
        this.batcher = new EventBatcher<>(builder);
    }

    public void start() {
        batcher.eventLoop.execute(batcher); // will reschedule itself
    }

    public static <T> Builder<T> maxItemSimulator(
            final IntIndexedStructTable<T> table,
            final Consumer<T> recordInitializer,
            final Consumer<T> recordUpdater,
            final EventLoop eventLoop) {
        return new Builder<>(table, recordInitializer, recordUpdater, eventLoop);
    }

    public static class Builder<T> {
        private final IntIndexedStructTable<T> table;
        private final EventLoop eventLoop;
        private final Consumer<T> recordInitializer;
        private final Consumer<T> recordUpdater;
        private int numEventsPerBatch = 10;
        private int numItems = 10;
        private long minDelayNanos = TimeUnit.MILLISECONDS.toNanos(100);
        private long maxDelayNanos = TimeUnit.MILLISECONDS.toNanos(300);

        public Builder(
                final IntIndexedStructTable<T> table,
                final Consumer<T> recordInitializer,
                final Consumer<T> recordUpdater,
                final EventLoop eventLoop) {
            this.eventLoop = eventLoop;
            this.table = table;
            this.recordInitializer = recordInitializer;
            this.recordUpdater = recordUpdater;
        }

        public Builder<T> numEventsPerBatch(final int numEventsPerBatch) {
            this.numEventsPerBatch = numEventsPerBatch;
            return this;
        }

        public Builder<T> numItems(final int numItems) {
            this.numItems = numItems;
            return this;
        }

        public Builder<T> maxDelay(final Duration maxDelay) {
            this.maxDelayNanos = maxDelay.toNanos();
            return this;
        }

        public Builder<T> minDelay(final Duration minDelay) {
            this.minDelayNanos = minDelay.toNanos();
            return this;
        }

        public MaxItemSimulator<T> build() {
            return new MaxItemSimulator<T>(this);
        }
    }

    private static final class EventBatcher<T> implements Runnable {
        private final Random random = new Random(3876380980L);
        private final EventLoop eventLoop;
        private final T facade;
        private final Consumer<T> recordInitializer;
        private final Consumer<T> recordUpdater;
        private final long minDelayNanos;
        private final long maxDelayNanos;
        private final int numEventsPerBatch;
        private final IntIndexedStructTable<T> table;
        private final IntDeque updateSequence;
        private final int numItems;
        private int id = 0;

        private EventBatcher(final Builder<T> builder) {
            this.eventLoop = builder.eventLoop;
            this.table = builder.table;
            this.facade = builder.table.createFacade();
            this.recordInitializer = builder.recordInitializer;
            this.recordUpdater = builder.recordUpdater;
            this.minDelayNanos = builder.minDelayNanos;
            this.maxDelayNanos = builder.maxDelayNanos;
            this.numEventsPerBatch = builder.numEventsPerBatch;
            this.updateSequence = new IntDeque(builder.numItems);
            this.numItems = builder.numItems;
        }

        @Override
        public void run() {
            createBatch();
            // wait a little and go again!
            final long waitTime = random.nextLong(minDelayNanos, maxDelayNanos);
            eventLoop.schedule(this, waitTime, TimeUnit.NANOSECONDS);
        }

        private void createBatch() {
            for (int i = 0; i < numEventsPerBatch; i++) {
                final int numActive = updateSequence.size();
                if (numActive == 0) {
                    createItem();
                } else if (numActive < numItems && random.nextBoolean()) {
                    createItem();
                } else {
                    updateItem();
                }
            }
            table.fireChanges();
        }

        private void createItem() {
            final int itemId = id++;
            table.beginAdd(itemId, facade);
            recordInitializer.accept(facade);
            table.endAdd();
            updateSequence.addLast(itemId);
        }

        private void updateItem() {
            // pick the item to update next
            final int itemId = updateSequence.removeFirst();
            // begin a change
            table.beginChange(itemId, facade);
            recordUpdater.accept(facade);
            table.endChange();
            // and queue the item to get updated again
            updateSequence.addLast(itemId);
        }
    }
}
