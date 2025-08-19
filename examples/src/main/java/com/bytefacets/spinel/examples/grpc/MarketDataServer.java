// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.examples.grpc;

import static com.bytefacets.spinel.comms.send.DefaultSubscriptionProvider.defaultSubscriptionProvider;

import com.bytefacets.spinel.comms.send.DefaultSubscriptionProvider;
import com.bytefacets.spinel.comms.send.RegisteredOutputsTable;
import com.bytefacets.spinel.grpc.send.GrpcService;
import com.bytefacets.spinel.grpc.send.GrpcServiceBuilder;
import com.bytefacets.spinel.table.IntIndexedStructTable;
import com.bytefacets.spinel.transform.TransformBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.slf4j.event.Level;

/**
 * Builds a mock market data server which produces price updates for a fixed number of instruments.
 *
 * <p>The data from this server is used to demonstrate a client receiving data from multiple servers
 * and joining the data on the client side by some key.
 */
final class MarketDataServer {
    static final int MD_PORT = 25000;
    private static final int NUM_INSTRUMENTS = OrderServer.NUM_INSTRUMENTS;
    static final LongSupplier SLOW = () -> 5000;
    static final LongSupplier FAST = () -> 50 + (long) (Math.random() * 100);
    private final EventLoop eventLoop;
    private final RegisteredOutputsTable outputs = new RegisteredOutputsTable();
    private final IntIndexedStructTable<MarketData> marketData;
    private final LongSupplier rate;

    public static void main(final String[] args) throws Exception {
        declareMarketDataServer(MD_PORT, SLOW).call();
        Thread.currentThread().join();
    }

    static Callable<Void> declareMarketDataServer(final int port, final LongSupplier rate) {
        final MarketDataServer topologyBuilder = new MarketDataServer(rate);
        final DefaultSubscriptionProvider subscriptionProvider =
                defaultSubscriptionProvider(topologyBuilder.outputs);
        final GrpcService service =
                GrpcServiceBuilder.grpcService(subscriptionProvider, topologyBuilder.eventLoop())
                        .build();
        final Server server =
                ServerBuilder.forPort(port)
                        .addService(service)
                        .executor(topologyBuilder.eventLoop())
                        .build();
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));
        return () -> {
            topologyBuilder.start();
            server.start();
            return null;
        };
    }

    MarketDataServer(final LongSupplier rate) {
        this.rate = rate;
        eventLoop =
                new DefaultEventLoop(
                        r -> {
                            return new Thread(r, "md-server-data-thread");
                        });
        final TransformBuilder builder = TransformBuilder.transform();
        // create a table to store fields as described on the MarketData interface
        builder.intIndexedStructTable(MarketData.class)
                .then()
                // and we'll watch it in the output
                .logger("market-data.server")
                .logLevel(Level.INFO);
        builder.build();
        marketData = builder.lookupNode("MarketData");

        outputs.register("market-data", marketData.output());
    }

    private final class MarketDataCreator implements Runnable {
        private final MarketData facade;

        private MarketDataCreator() {
            facade = marketData.createFacade();
        }

        @Override
        public void run() {
            updateBatch();
            final long waitTime = rate.getAsLong();
            eventLoop.schedule(this, waitTime, TimeUnit.MILLISECONDS);
        }

        private void updateBatch() {
            for (int instrumentId = 1; instrumentId <= NUM_INSTRUMENTS; instrumentId++) {
                doUpdate(instrumentId);
            }
            marketData.fireChanges();
        }

        private void doUpdate(final int instrumentId) {
            final double shift = 0.1 - (Math.random() * .02); // 1% up or down
            final double basePrice = 5.2 * instrumentId;
            final double newPrice =
                    new BigDecimal(basePrice * shift, MathContext.DECIMAL32)
                            .setScale(2, RoundingMode.HALF_UP)
                            .doubleValue();
            // using upsert will check if the instrument is in the table and
            // beginAdd if it's not, or beginChange if it is
            marketData.beginUpsert(instrumentId, facade).setPrice(newPrice);
            marketData.endUpsert();
        }
    }

    EventLoop eventLoop() {
        return eventLoop;
    }

    void start() {
        eventLoop.execute(new MarketDataCreator());
    }

    interface MarketData {
        int getInstrumentId(); // getter only = key field

        double getPrice();

        void setPrice(double value);
    }
}
