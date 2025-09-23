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
import java.util.function.Predicate;

/**
 * A "simulator" which takes some diversity and speed parameters to generate an endless stream of
 * items with some removal policy.
 */
public final class LimitedSetSimulator<T> {
    private final EventBatcher<T> batcher;

    private LimitedSetSimulator(final EventBatcher<T> batcher) {
        this.batcher = batcher;
    }

    public void start() {
        batcher.eventLoop.execute(batcher);
    }

    public static <T> Builder<T> limitedSetSimulator(
            final IntIndexedStructTable<T> table,
            final Consumer<T> recordInitializer,
            final Consumer<T> recordUpdater,
            final Predicate<T> removalTest,
            final EventLoop eventLoop) {
        return new Builder<>(table, recordInitializer, recordUpdater, removalTest, eventLoop);
    }

    public static final class Builder<T> {
        private final IntIndexedStructTable<T> table;
        private final Consumer<T> recordInitializer;
        private final Consumer<T> recordUpdater;
        private final Predicate<T> removalTest;
        private final EventLoop eventLoop;
        private int activeItemsLo = 300;
        private int activeItemsHi = 1000;
        private int numEventsPerBatch = 10;
        private long minDelayNanos = TimeUnit.MILLISECONDS.toNanos(100);
        private long maxDelayNanos = TimeUnit.MILLISECONDS.toNanos(300);

        private Builder(
                final IntIndexedStructTable<T> table,
                final Consumer<T> recordInitializer,
                final Consumer<T> recordUpdater,
                final Predicate<T> removalTest,
                final EventLoop eventLoop) {

            this.table = table;
            this.recordInitializer = recordInitializer;
            this.recordUpdater = recordUpdater;
            this.removalTest = removalTest;
            this.eventLoop = eventLoop;
        }

        public Builder<T> activeItemsLo(final int activeItemsLo) {
            this.activeItemsLo = activeItemsLo;
            return this;
        }

        public Builder<T> activeItemsHi(final int activeItemsHi) {
            this.activeItemsHi = activeItemsHi;
            return this;
        }

        public Builder<T> numEventsPerBatch(final int numEventsPerBatch) {
            this.numEventsPerBatch = numEventsPerBatch;
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

        public LimitedSetSimulator<T> build() {
            return new LimitedSetSimulator<>(new EventBatcher<>(this));
        }
    }

    /** Little class that creates a batch of items */
    private static final class EventBatcher<T> implements Runnable {
        private final IntIndexedStructTable<T> table;
        private final EventLoop eventLoop;
        private final T facade;
        private final IntDeque activeItems;
        private final Random random = new Random(27652762);
        private final Consumer<T> recordInitializer;
        private final Consumer<T> recordUpdater;
        private final Predicate<T> removalTest;
        private final int activeOrderHi;
        private final int activeOrderLo;
        private final int numEventsPerBatch;
        private final long minDelayNanos;
        private final long maxDelayNanos;
        int id = 1;

        private EventBatcher(final Builder<T> builder) {
            this.table = builder.table;
            // the table provides a facade to the underlying table storage
            this.facade = table.createFacade();
            this.eventLoop = builder.eventLoop;
            this.recordInitializer = builder.recordInitializer;
            this.recordUpdater = builder.recordUpdater;
            this.removalTest = builder.removalTest;
            this.activeOrderLo = builder.activeItemsLo;
            this.activeOrderHi = builder.activeItemsHi;
            this.numEventsPerBatch = builder.numEventsPerBatch;
            this.minDelayNanos = builder.minDelayNanos;
            this.maxDelayNanos = builder.maxDelayNanos;
            activeItems = new IntDeque(activeOrderHi);
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
                final int numActive = activeItems.size();
                if (numActive < activeOrderLo) {
                    createItem();
                } else if (numActive >= activeOrderHi) {
                    updateItem();
                } else if (Math.random() < 0.5) {
                    createItem();
                } else {
                    updateItem();
                }
            }
            table.fireChanges();
        }

        private void createItem() {
            final int orderId = id++;
            table.beginAdd(orderId, facade);
            recordInitializer.accept(facade);
            table.endAdd();
            activeItems.addLast(orderId);
        }

        private void updateItem() {
            // pick the item to update next
            final int itemId = activeItems.removeFirst();
            // where in the table is it?
            final int row = table.lookupKeyRow(itemId);
            // tell the table to move the facade over the row
            table.moveToRow(facade, row);
            if (removalTest.test(facade)) {
                table.remove(itemId);
            } else {
                table.beginChange(itemId, facade);
                recordUpdater.accept(facade);
                table.endChange();
                // and queue the order to get updated again
                activeItems.addLast(itemId);
            }
        }
    }
}
