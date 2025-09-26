// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.examples.nats;

import static com.bytefacets.spinel.comms.send.DefaultSubscriptionProvider.defaultSubscriptionProvider;
import static com.bytefacets.spinel.examples.nats.NatsSinkExample.loggerName;
import static com.bytefacets.spinel.groupby.lib.DoubleAggregation.doubleAggregation;
import static com.bytefacets.spinel.groupby.lib.IntAggregation.intAggregation;
import static com.bytefacets.spinel.projection.lib.DoubleBiCalculation.doubleBiCalculation;

import com.bytefacets.collections.types.DoubleType;
import com.bytefacets.collections.types.IntType;
import com.bytefacets.spinel.common.Connector;
import com.bytefacets.spinel.comms.send.DefaultSubscriptionProvider;
import com.bytefacets.spinel.comms.send.OutputRegistry;
import com.bytefacets.spinel.comms.send.RegisteredOutputsTable;
import com.bytefacets.spinel.examples.Util;
import com.bytefacets.spinel.groupby.GroupBy;
import com.bytefacets.spinel.groupby.GroupByBuilder;
import com.bytefacets.spinel.groupby.lib.SumFactory;
import com.bytefacets.spinel.grpc.send.GrpcService;
import com.bytefacets.spinel.grpc.send.GrpcServiceBuilder;
import com.bytefacets.spinel.join.Join;
import com.bytefacets.spinel.join.JoinBuilder;
import com.bytefacets.spinel.join.JoinKeyHandling;
import com.bytefacets.spinel.nats.kv.BucketUtil;
import com.bytefacets.spinel.nats.kv.KvUpdateHandler;
import com.bytefacets.spinel.nats.kv.NatsKvAdapter;
import com.bytefacets.spinel.nats.kv.NatsKvAdapterBuilder;
import com.bytefacets.spinel.nats.kv.NatsKvSource;
import com.bytefacets.spinel.nats.kv.NatsKvSourceBuilder;
import com.bytefacets.spinel.printer.OutputLoggerBuilder;
import com.bytefacets.spinel.projection.Projection;
import com.bytefacets.spinel.projection.ProjectionBuilder;
import com.bytefacets.spinel.schema.AttributeConstants;
import com.bytefacets.spinel.schema.DoubleWritableField;
import com.bytefacets.spinel.schema.FieldDescriptor;
import com.bytefacets.spinel.schema.IntWritableField;
import com.bytefacets.spinel.schema.Metadata;
import com.bytefacets.spinel.schema.MetadataBuilder;
import com.bytefacets.spinel.schema.Schema;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueEntry;
import io.netty.channel.EventLoop;
import java.io.IOException;
import java.util.List;
import org.slf4j.event.Level;

/**
 * A ViewServer which consumes orders and market data from NATS, performs some transformations, and
 * serves the transformations through a gRPC server for other components or UIs to consume.
 */
final class ViewServer {
    private static final int PORT = 25001;
    private static final int NUM_INSTRUMENTS = NatsSinkExample.NUM_INSTRUMENTS;
    private final Server server;
    private final NatsKvAdapter mdAdapter;
    private final NatsKvSource orderSource;
    private static final Metadata priceMetadata =
            MetadataBuilder.metadataBuilder().displayPrecision(4).build();
    // the exchange code here is an int that represents the 4-byte ascii code
    private static final Metadata exchangeMetadata =
            MetadataBuilder.metadataBuilder()
                    .contentType(AttributeConstants.ContentTypes.Text)
                    .build();
    private final KeyValue orderBucket;
    private final KeyValue mdBucket;

    public static void main(final String[] args) throws Exception {
        new ViewServer(
                        "nats://127.0.0.1:4222",
                        NatsSinkExample.ORDER_BUCKET_NAME,
                        NatsSinkExample.MD_BUCKET_NAME)
                .start();
        Thread.currentThread().join();
    }

    /**
     * Builds a transformation with orders and market data coming from NATS.
     *
     * <p>This is kind of a large method, and in real life, it would be cleaner without all the
     * loggers attaching. Regarding the loggers, they are using "alwaysShow" so that you can observe
     * the "key" fields during change events; it's a little easier to trace the changes from one
     * thing to another this way.
     */
    ViewServer(final String natsEndpoint, final String orderBucketName, final String mdBucketName)
            throws IOException, InterruptedException, JetStreamApiException {
        final EventLoop eventLoop = Util.newEventLoop("view-server-thread ");
        final Options options =
                Options.builder().server(natsEndpoint).connectionTimeout(5000).build();
        final Connection connection = Nats.connect(options);

        // orders are put into the KeyValue bucket by a NatsKvSink, so use a NatsKvSource
        // to consume the data
        orderBucket =
                BucketUtil.getOrCreateBucket(
                        connection, KeyValueConfiguration.builder().name(orderBucketName).build());
        orderSource = NatsKvSourceBuilder.natsKvSource().eventLoop(eventLoop).build();
        // market data is put into the KeyValue bucket by another type of writer, so use
        // the MdKeyValueHandler to decode the values
        // also note that there are 2 processes populating the bucket, but we only need the
        // one adapter to watch the bucket
        mdBucket =
                BucketUtil.getOrCreateBucket(
                        connection, KeyValueConfiguration.builder().name(mdBucketName).build());
        mdAdapter =
                NatsKvAdapterBuilder.natsKvAdapter()
                        // custom handler for decoding and writing data to schema fields
                        .updateHandler(new MdKeyValueHandler())
                        // adapter must know the schema ahead of time
                        .addFields(marketDataBucketFields())
                        .eventLoop(eventLoop)
                        .build();
        Connector.connectInputToOutput(
                OutputLoggerBuilder.logger(loggerName("md-client"))
                        .alwaysShow("InstrumentId")
                        .logLevel(Level.INFO)
                        .build(),
                mdAdapter);

        // pick the latest update for each instrument
        // this is kind of a "cheap" latest calculation; doesn't handle price deletions
        final GroupBy latestByInstrument =
                GroupByBuilder.groupBy()
                        .groupByFields("InstrumentId")
                        .addAggregation(
                                intAggregation(
                                        "Exchange", // input name
                                        "Exchange", // output name
                                        (currentGroupValue, oldValue, newValue) -> newValue,
                                        exchangeMetadata))
                        .addAggregation(
                                doubleAggregation(
                                        "LastPrice", // input name
                                        "LastPrice", // output name
                                        (currentGroupValue, oldValue, newValue) -> newValue,
                                        priceMetadata))
                        .build();
        Connector.connectInputToOutput(latestByInstrument, mdAdapter);
        Connector.connectInputToOutput(
                OutputLoggerBuilder.logger(loggerName("last-price"))
                        .alwaysShow("InstrumentId")
                        .logLevel(Level.INFO)
                        .build(),
                latestByInstrument);

        // now that they are both structured, we can join them using our normal join
        // operator
        // note that in the example, we've generated data for the same instrument ids
        // in both md feeds, e.g. instrument 5 comes from both XNYS and XNAS feeds
        final Join join =
                JoinBuilder.lookupJoin("order-view")
                        .outer()
                        .joinOn(List.of("InstrumentId"), List.of("InstrumentId"), NUM_INSTRUMENTS)
                        .withJoinKeyHandling(JoinKeyHandling.KeepLeft)
                        .build();
        Connector.connectInputToOutput(join.leftInput(), orderSource);
        Connector.connectInputToOutput(join.rightInput(), latestByInstrument);
        // and we can log it to the output
        Connector.connectInputToOutput(
                OutputLoggerBuilder.logger(loggerName("order-view"))
                        .alwaysShow("OrderId", "InstrumentId")
                        .logLevel(Level.INFO)
                        .build(),
                join);

        // Project open notional
        final Projection orderProjection =
                ProjectionBuilder.projection("OrderProjection")
                        // simple rename
                        .inboundAlias("Qty", "OpenQty")
                        // this is an implementation that has no boxing
                        // there is another expression-based one,
                        // but requires value boxing/unboxing
                        .lazyCalculation(
                                "OpenNotional",
                                doubleBiCalculation(
                                        "Qty", "LastPrice", (qty, price) -> qty * price))
                        .build();
        Connector.connectInputToOutput(orderProjection, join);

        // Build aggregation
        final Metadata notionalMetadata =
                MetadataBuilder.metadataBuilder().displayPrecision(2).build();
        final GroupBy byInstrument =
                GroupByBuilder.groupBy("by-instrument")
                        .groupByFields("InstrumentId")
                        // add count to the output and call it "Count"
                        .includeCountField("Count")
                        // aggregate the OpenNotional
                        .addAggregation(
                                SumFactory.sumToDouble(
                                        "OpenNotional", "OpenNotional", notionalMetadata))
                        .build();
        Connector.connectInputToOutput(byInstrument, orderProjection);
        // and we can log it to the output
        Connector.connectInputToOutput(
                OutputLoggerBuilder.logger(loggerName("by-instrument"))
                        .alwaysShow("InstrumentId")
                        .logLevel(Level.INFO)
                        .build(),
                byInstrument);

        // register the outputs so that clients of the gRPC server can access them
        final RegisteredOutputsTable outputRegistry =
                RegisteredOutputsTable.registeredOutputsTable();
        outputRegistry.register("order-view", orderProjection);
        outputRegistry.register("md", mdAdapter);
        outputRegistry.register("orders", orderSource);
        outputRegistry.register("by-instrument", byInstrument);

        // open a grpc server to serve it
        server = initServer(outputRegistry, eventLoop);
    }

    @SuppressWarnings("resource")
    void start() throws IOException, JetStreamApiException, InterruptedException {
        server.start(); // open the gRPC server to serve the registered outputs
        // start watching the KeyValue bucket for market data
        mdBucket.watchAll(mdAdapter.keyValueWatcher());
        // start watching the KeyValue bucket for orders
        orderBucket.watchAll(orderSource.keyValueWatcher());
    }

    private Server initServer(final OutputRegistry outputRegistry, final EventLoop eventLoop) {
        final DefaultSubscriptionProvider subscriptionProvider =
                defaultSubscriptionProvider(outputRegistry);
        final GrpcService service =
                GrpcServiceBuilder.grpcService(subscriptionProvider, eventLoop).build();
        final Server server =
                ServerBuilder.forPort(PORT).addService(service).executor(eventLoop).build();
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));
        return server;
    }

    private static FieldDescriptor[] marketDataBucketFields() {
        return new FieldDescriptor[] {
            FieldDescriptor.intField("Exchange", exchangeMetadata),
            FieldDescriptor.intField("InstrumentId"),
            FieldDescriptor.doubleField("LastPrice", priceMetadata)
        };
    }

    /**
     * This is our custom NATS KeyValue update handler. Because the data in the bucket has a custom
     * encoding, we have to use a NatsKvAdapter with this handler implementation to parse the data
     * and apply it to our table structure.
     *
     * <p>For a more thorough implementation, you can also handle deletions in this handler which
     * you could use to NaN-out the price fields and then later have special logic to handle that in
     * the "latest-price" GroupBy.
     */
    private static final class MdKeyValueHandler implements KvUpdateHandler {
        private DoubleWritableField priceField;
        private IntWritableField instrumentField;
        private IntWritableField exchangeCodeField;

        /**
         * Handle the application of the data from the NATS KeyValueEntry
         *
         * @param row the row into which data should be written to the fields
         * @param entry the entry that was updated
         */
        @Override
        public void updated(final int row, final KeyValueEntry entry) {
            final byte[] value = entry.getValue();
            final int exchangeCodeAsInt = IntType.readLE(value, 0);
            final int instrumentId = IntType.readLE(value, 4);
            final double price = DoubleType.readLE(value, 8);
            instrumentField.setValueAt(row, instrumentId);
            priceField.setValueAt(row, price);
            exchangeCodeField.setValueAt(row, exchangeCodeAsInt);
        }

        /**
         * We gave the builder FieldDescriptors, and now we need to get references to the fields
         * that were created from those.
         */
        @Override
        public void bindToSchema(final Schema schema) {
            priceField = (DoubleWritableField) schema.field("LastPrice").field();
            instrumentField = (IntWritableField) schema.field("InstrumentId").field();
            exchangeCodeField = (IntWritableField) schema.field("Exchange").field();
        }
    }
}
