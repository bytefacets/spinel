// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.examples.common;

import com.bytefacets.collections.queue.IntDeque;
import com.bytefacets.spinel.table.IntIndexedStructTable;
import io.netty.channel.EventLoop;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * An order "simulator" which takes some diversity and speed parameters to generate an endless
 * stream of orders and quantity updates to orders.
 */
public final class OrderSimulator {
    private final OrderActivity orderActivity;

    private OrderSimulator(final OrderActivity orderActivity) {
        this.orderActivity = orderActivity;
    }

    public void start() {
        orderActivity.eventLoop.execute(orderActivity);
    }

    public static Builder mockOrders(
            final IntIndexedStructTable<Order> orders, final EventLoop eventLoop) {
        return new Builder(orders, eventLoop);
    }

    public static final class Builder {
        private final IntIndexedStructTable<Order> orders;
        private final EventLoop eventLoop;
        private int activeOrderLo = 300;
        private int activeOrderHi = 1000;
        private int numEventsPerBatch = 10;
        private int numInstruments = 10;
        private int minDelay = 100;
        private int maxDelay = 300;

        private Builder(final IntIndexedStructTable<Order> orders, final EventLoop eventLoop) {
            this.orders = orders;
            this.eventLoop = eventLoop;
        }

        public Builder activeOrderLo(final int activeOrderLo) {
            this.activeOrderLo = activeOrderLo;
            return this;
        }

        public Builder activeOrderHi(final int activeOrderHi) {
            this.activeOrderHi = activeOrderHi;
            return this;
        }

        public Builder numEventsPerBatch(final int numEventsPerBatch) {
            this.numEventsPerBatch = numEventsPerBatch;
            return this;
        }

        public Builder numInstruments(final int numInstruments) {
            this.numInstruments = numInstruments;
            return this;
        }

        public Builder maxDelay(final int maxDelay) {
            this.maxDelay = maxDelay;
            return this;
        }

        public Builder minDelay(final int minDelay) {
            this.minDelay = minDelay;
            return this;
        }

        public OrderSimulator build() {
            return new OrderSimulator(new OrderActivity(this));
        }
    }

    /** Little class that creates a batch of orders */
    private static final class OrderActivity implements Runnable {
        private final IntIndexedStructTable<Order> orders;
        private final EventLoop eventLoop;
        private final Order facade;
        private final IntDeque activeOrders;
        private final Random random = new Random(27652762);
        private final int activeOrderHi;
        private final int activeOrderLo;
        private final int numEventsPerBatch;
        private final int numInstruments;
        private final int minDelay;
        private final int maxDelay;
        int id = 1;

        private OrderActivity(final Builder builder) {
            // the table provides a facade to the underlying table storage
            this.orders = builder.orders;
            this.eventLoop = builder.eventLoop;
            this.numInstruments = builder.numInstruments;
            this.activeOrderLo = builder.activeOrderLo;
            this.activeOrderHi = builder.activeOrderHi;
            this.numEventsPerBatch = builder.numEventsPerBatch;
            this.minDelay = builder.minDelay;
            this.maxDelay = builder.maxDelay;
            facade = orders.createFacade();
            activeOrders = new IntDeque(activeOrderHi);
        }

        @Override
        public void run() {
            createBatch();
            // wait a little and go again!
            final long waitTime = random.nextInt(minDelay, maxDelay);
            eventLoop.schedule(this, waitTime, TimeUnit.MILLISECONDS);
        }

        private void createBatch() {
            for (int i = 0; i < numEventsPerBatch; i++) {
                final int numActive = activeOrders.size();
                if (numActive < activeOrderLo) {
                    createOrder();
                } else if (numActive >= activeOrderHi) {
                    updateOrder();
                } else if (Math.random() < 0.5) {
                    createOrder();
                } else {
                    updateOrder();
                }
            }
            orders.fireChanges();
        }

        private void createOrder() {
            final int orderId = id++;
            orders.beginAdd(orderId, facade)
                    .setInstrumentId(1 + ((orderId * 31) % numInstruments))
                    .setQty(100 * (1 + (orderId % 23)))
                    .setPrice(5.2 * (orderId & 127));
            orders.endAdd();
            activeOrders.addLast(orderId);
        }

        private void updateOrder() {
            // pick the order to update next
            final int orderId = activeOrders.removeFirst();
            // where in the table is it?
            final int row = orders.lookupKeyRow(orderId);
            // tell the table to move the facade over the row
            orders.moveToRow(facade, row);
            // calculate a new quantity
            final int curQty = facade.getQty();
            final int newQty = curQty - Math.min(100, curQty);
            if (newQty == 0) {
                // remove the order it if it's zero
                orders.remove(orderId);
            } else {
                // otherwise change the quantity
                orders.beginChange(orderId, facade);
                facade.setQty(newQty);
                orders.endChange();
                // and queue the order to get updated again
                activeOrders.addLast(orderId);
            }
        }
    }
}
