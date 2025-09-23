// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.examples.nats;

import io.nats.client.JetStreamApiException;
import java.io.IOException;

/**
 * An example of 4 server processes working together: 1 order feed, 2 market data feeds, and one
 * view server. The order and market data feeds are setting data in two NATS KeyValue buckets, which
 * the view server is watching and building a view of orders. The view can be used for other
 * business logic (like monitors or risk checks), or for UIs.
 */
public final class NatsSinkExample {
    static final String ORDER_BUCKET_NAME = "orders-4";
    static final String MD_BUCKET_NAME = "md-4";
    static final String NATS_ENDPOINT = "nats://127.0.0.1:4222";
    static final int NUM_INSTRUMENTS = 25;

    private NatsSinkExample() {}

    public static void main(final String[] args)
            throws IOException, InterruptedException, JetStreamApiException {
        // an order feed that posts into a NATS KeyValue bucket
        new OrderServer(NATS_ENDPOINT, ORDER_BUCKET_NAME).start();

        // one market data feed from XNYS posting into a NATS KeyValue bucket
        new MarketDataServer(NATS_ENDPOINT, MD_BUCKET_NAME, "XNYS").start();

        // one market data feed from XNAS posting into a NATS KeyValue bucket
        new MarketDataServer(NATS_ENDPOINT, MD_BUCKET_NAME, "XNAS").start();

        // view server combines the order feed and the market data into one view
        // and starts a gRPC endpoint for consumers to get the consolidated view
        new ViewServer(NATS_ENDPOINT, ORDER_BUCKET_NAME, MD_BUCKET_NAME).start();
        Thread.currentThread().join();
    }

    static String loggerName(final String name) {
        return String.format("%-13s", name);
    }
}
