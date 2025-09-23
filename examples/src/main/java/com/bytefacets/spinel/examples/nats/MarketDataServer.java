// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.examples.nats;

import static com.bytefacets.spinel.examples.nats.NatsSinkExample.loggerName;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.collections.types.DoubleType;
import com.bytefacets.collections.types.IntType;
import com.bytefacets.spinel.TransformInput;
import com.bytefacets.spinel.common.Connector;
import com.bytefacets.spinel.comms.send.RegisteredOutputsTable;
import com.bytefacets.spinel.examples.Util;
import com.bytefacets.spinel.examples.common.MaxItemSimulator;
import com.bytefacets.spinel.nats.kv.BucketUtil;
import com.bytefacets.spinel.printer.OutputLoggerBuilder;
import com.bytefacets.spinel.schema.ChangedFieldSet;
import com.bytefacets.spinel.schema.DisplayMetadata;
import com.bytefacets.spinel.schema.DoubleField;
import com.bytefacets.spinel.schema.FieldResolver;
import com.bytefacets.spinel.schema.IntField;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.table.IntIndexedStructTable;
import com.bytefacets.spinel.table.IntIndexedStructTableBuilder;
import io.nats.client.Connection;
import io.nats.client.KeyValue;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.KeyValueConfiguration;
import io.netty.channel.EventLoop;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Random;
import org.slf4j.event.Level;

/**
 * Builds a mock market data server which produces price updates for a fixed number of instruments
 * and writes the data to a NATS KeyValue bucket, keyed by an instrument id.
 *
 * <p>This demonstrates an architecture where different feed handlers from different exchanges could
 * write prices for their instruments to a shared KeyValue bucket, e.g. a NYSE handler writes to the
 * same bucket as the Nasdaq handler. The ViewServer then can combine the data from both.
 */
final class MarketDataServer {
    private static final Duration MIN_DELAY = Duration.ofMillis(500);
    private static final Duration MAX_DELAY = Duration.ofMillis(750);
    private static final int NUM_INSTRUMENTS = NatsSinkExample.NUM_INSTRUMENTS;
    private static final Random random = new Random(87638763983L);
    private static final int EVENT_PER_BATCH = 5;
    private final EventLoop eventLoop;
    private final RegisteredOutputsTable outputs = RegisteredOutputsTable.registeredOutputsTable();
    private final IntIndexedStructTable<MarketData> marketData;
    private final MaxItemSimulator<MarketData> simulator;

    public static void main(final String[] args) throws Exception {
        new MarketDataServer("nats://127.0.0.1:4222", NatsSinkExample.MD_BUCKET_NAME, "XNAS")
                .start();
        Thread.currentThread().join();
    }

    MarketDataServer(final String natsEndpoint, final String bucketName, final String exchangeCode)
            throws IOException, InterruptedException {
        eventLoop =
                Util.newEventLoop(String.format("md-%s-data-thread", exchangeCode.toLowerCase()));

        // initialize a NATS connection
        final Options options =
                Options.builder().server(natsEndpoint).connectionTimeout(5000).build();
        final Connection connection = Nats.connect(options);
        // get a handle to a KeyValue bucket
        final KeyValue keyValueBucket =
                BucketUtil.getOrCreateBucket(
                        connection, KeyValueConfiguration.builder().name(bucketName).build());

        // create a table to store fields as described on the MarketData interface
        marketData = IntIndexedStructTableBuilder.intIndexedStructTable(MarketData.class).build();
        Connector.connectInputToOutput(
                OutputLoggerBuilder.logger(loggerName("md-feed-" + exchangeCode.toLowerCase()))
                        .alwaysShow("InstrumentId")
                        .logLevel(Level.INFO)
                        .build(),
                marketData);
        // connect the market data to a NATS KeyValue bucket
        Connector.connectInputToOutput(new CustomKvSink(keyValueBucket, exchangeCode), marketData);

        // create a simulator to modify the records in the marketData table
        simulator =
                MaxItemSimulator.maxItemSimulator(
                                marketData,
                                this::simulatePriceChange,
                                this::simulatePriceChange,
                                eventLoop)
                        .minDelay(MIN_DELAY)
                        .maxDelay(MAX_DELAY)
                        .numEventsPerBatch(EVENT_PER_BATCH)
                        .numItems(NUM_INSTRUMENTS)
                        .build();

        outputs.register("market-data", marketData.output());
    }

    /** Called by the simulator to produce an updated price */
    private void simulatePriceChange(final MarketData record) {
        final double shift = random.nextDouble(-0.01, 0.01); // 1% up or down
        final double basePrice = 5.2 * (record.getInstrumentId() + 1);
        final double newPrice = basePrice + (basePrice * shift);
        record.setPrice(newPrice);
    }

    void start() {
        simulator.start();
    }

    /** Receives updates from the market data table and writes them to NATS */
    private static final class CustomKvSink implements TransformInput {
        private final KeyValue kv;
        private final String keyPrefix;
        private final int exchangeCodeAsInt;
        private IntField instrumentIdField;
        private DoubleField priceField;

        private CustomKvSink(final KeyValue kv, final String exchangeCode) {
            this.kv = kv;
            this.keyPrefix = "price." + exchangeCode + ".";
            this.exchangeCodeAsInt =
                    IntType.readLE(exchangeCode.getBytes(StandardCharsets.UTF_8), 0);
        }

        @Override
        public void schemaUpdated(@Nullable final Schema schema) {
            if (schema != null) {
                final FieldResolver resolver = schema.asFieldResolver();
                instrumentIdField = resolver.findIntField("InstrumentId");
                priceField = resolver.findDoubleField("Price");
            } else {
                priceField = null;
                instrumentIdField = null;
            }
        }

        private void updateValueInBucket(final int row) {
            try {
                final byte[] value = new byte[16]; // reusable?
                final int instrumentId = instrumentIdField.valueAt(row);
                IntType.writeLE(value, 0, exchangeCodeAsInt);
                IntType.writeLE(value, 4, instrumentId);
                DoubleType.writeLE(value, 8, priceField.valueAt(row));
                kv.put(keyPrefix + instrumentId, value);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void rowsAdded(final IntIterable rows) {
            rows.forEach(this::updateValueInBucket);
        }

        @Override
        public void rowsChanged(final IntIterable rows, final ChangedFieldSet changedFields) {
            rows.forEach(this::updateValueInBucket);
        }

        @Override
        public void rowsRemoved(final IntIterable rows) {
            // not expecting removed rows here
        }
    }

    // formatting:off
    interface MarketData {
        int getInstrumentId(); // getter only = table key field
        @DisplayMetadata(precision = 4)
        double getPrice(); void setPrice(double value);
    }
    // formatting:on
}
