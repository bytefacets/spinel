// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.examples.nats;

import static com.bytefacets.spinel.examples.nats.NatsSinkExample.loggerName;

import com.bytefacets.spinel.common.Connector;
import com.bytefacets.spinel.examples.Util;
import com.bytefacets.spinel.examples.common.LimitedSetSimulator;
import com.bytefacets.spinel.examples.common.Order;
import com.bytefacets.spinel.nats.FieldSequenceNatsSubjectBuilder;
import com.bytefacets.spinel.nats.kv.NatsKvSink;
import com.bytefacets.spinel.nats.kv.NatsKvSinkBuilder;
import com.bytefacets.spinel.table.IntIndexedStructTable;
import com.bytefacets.spinel.transform.TransformBuilder;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.netty.channel.EventLoop;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.slf4j.event.Level;

/**
 * An example of a server process which consumes orders from an order feed and puts them into a NATS
 * KeyValue bucket.
 */
final class OrderServer {
    static final int NUM_INSTRUMENTS = 10;
    private static final Duration MIN_DELAY = Duration.ofMillis(500);
    private static final Duration MAX_DELAY = Duration.ofMillis(750);
    private final LimitedSetSimulator<Order> orderSimulator;

    public static void main(final String[] args) throws Exception {
        new OrderServer("nats://127.0.0.1:4222", NatsSinkExample.ORDER_BUCKET_NAME).start();
        Thread.currentThread().join();
    }

    OrderServer(final String natsEndpoint, final String bucketName)
            throws IOException, InterruptedException, JetStreamApiException {
        final EventLoop eventLoop = Util.newEventLoop("order-server-thread");
        final TransformBuilder transform = TransformBuilder.transform();
        transform
                .intIndexedStructTable(Order.class)
                .then()
                .logger(loggerName("order-feed"))
                .alwaysShow("OrderId")
                .logLevel(Level.INFO);
        transform.build();
        final IntIndexedStructTable<Order> orders = transform.lookupNode("Order");

        final Options options =
                Options.builder().server(natsEndpoint).connectionTimeout(5000).build();
        final Connection connection = Nats.connect(options);

        final NatsKvSink sink =
                NatsKvSinkBuilder.natsKvSink()
                        .keyValueBucket(connection, bucketName)
                        .subjectBuilder(
                                FieldSequenceNatsSubjectBuilder.fieldSequenceNatsSubjectBuilder(
                                        List.of("InstrumentId", "OrderId")))
                        .build();

        Connector.connectInputToOutput(sink, orders);

        orderSimulator =
                LimitedSetSimulator.limitedSetSimulator(
                                orders,
                                this::initializeMockOrder,
                                this::updateMockOrder,
                                this::shouldRemoveMockOrder,
                                eventLoop)
                        .activeItemsLo(20)
                        .activeItemsHi(30)
                        .minDelay(MIN_DELAY)
                        .maxDelay(MAX_DELAY)
                        .build();
        orderSimulator.start();
    }

    void start() {
        orderSimulator.start();
    }

    private void updateMockOrder(final Order order) {
        final int curQty = order.getQty();
        order.setQty(curQty - Math.min(100, curQty));
    }

    private boolean shouldRemoveMockOrder(final Order order) {
        return order.getQty() == 0;
    }

    private void initializeMockOrder(final Order order) {
        final int orderId = order.getOrderId();
        order.setInstrumentId(1 + ((orderId * 31) % NUM_INSTRUMENTS))
                .setQty(100 * (1 + (orderId % 23)))
                .setPrice(5.2 * (orderId & 127));
    }
}
