// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.examples.prototype;

import static com.bytefacets.spinel.examples.Util.clientChannel;
import static com.bytefacets.spinel.examples.Util.newEventLoop;
import static com.bytefacets.spinel.table.IntIndexedStructTableBuilder.intIndexedStructTable;

import ch.qos.logback.classic.LoggerContext;
import com.bytefacets.spinel.common.Connector;
import com.bytefacets.spinel.comms.ConnectionInfo;
import com.bytefacets.spinel.comms.SubscriptionConfig;
import com.bytefacets.spinel.comms.send.DefaultSubscriptionProvider;
import com.bytefacets.spinel.comms.send.RegisteredOutputsTable;
import com.bytefacets.spinel.examples.Util;
import com.bytefacets.spinel.grpc.receive.GrpcClient;
import com.bytefacets.spinel.grpc.receive.GrpcClientBuilder;
import com.bytefacets.spinel.grpc.receive.GrpcSource;
import com.bytefacets.spinel.grpc.receive.GrpcSourceBuilder;
import com.bytefacets.spinel.grpc.send.GrpcService;
import com.bytefacets.spinel.grpc.send.GrpcServiceBuilder;
import com.bytefacets.spinel.printer.OutputLoggerBuilder;
import com.bytefacets.spinel.prototype.Prototype;
import com.bytefacets.spinel.prototype.PrototypeBuilder;
import com.bytefacets.spinel.schema.FieldDescriptor;
import com.bytefacets.spinel.table.IntIndexedStructTable;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * This example demonstrates how a Prototype operator can be used to stabilize the effect of
 * connection resets.
 *
 * <p>When a connection is reset, schemas are reset, too, meaning that the fields which provide
 * access to the data become unavailable. This can be problematic for things like UIs.
 *
 * <p>When you use a Prototype, you trade off defining the schema locally for schema stability for
 * the consumers of the Prototype output. The prototype can also navigate some degree of type
 * casting, like an int field becoming a short: if your client says it's an int, but the server
 * sends a short, the prototype will cast (using {@link com.bytefacets.spinel.schema.Cast}).
 *
 * @see Prototype
 * @see PrototypeBuilder
 */
final class PrototypeExample {
    private static final int orderPort = 26001;
    private static final Logger log = LoggerFactory.getLogger(PrototypeExample.class);

    private PrototypeExample() {}

    public static void main(final String[] args) throws Exception {
        final LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLogger("io.grpc").setLevel(ch.qos.logback.classic.Level.INFO);
        context.getLogger("io.netty").setLevel(ch.qos.logback.classic.Level.INFO);
        declareServer();
        declareClient();
        Thread.currentThread().join();
    }

    /** Create a client and configure a subscription to the "orders" output on the server. */
    private static void declareClient() {
        final ManagedChannel orderChannel = clientChannel("0.0.0.0:" + orderPort);
        final var eventLoop = newEventLoop("client-data-thread");
        final GrpcClient orderClient =
                GrpcClientBuilder.grpcClient(orderChannel, eventLoop)
                        .connectionInfo(new ConnectionInfo("order-server", "0.0.0.0:" + orderPort))
                        .build();
        final SubscriptionConfig subscription =
                SubscriptionConfig.subscriptionConfig("orders").defaultAll().build();
        final GrpcSource orders =
                GrpcSourceBuilder.grpcSource(orderClient, "local-orders")
                        .subscription(subscription)
                        .build();
        //
        final Prototype prototype =
                PrototypeBuilder.prototype("client-orders-prototype")
                        .addFields(
                                FieldDescriptor.intField("OrderId"),
                                FieldDescriptor.stringField("Account"),
                                FieldDescriptor.intField("Qty"),
                                FieldDescriptor.doubleField("Price"))
                        .build();
        Connector.connectInputToOutput(prototype, orders);

        // log the changes happening out of the prototype
        Connector.connectInputToOutput(
                OutputLoggerBuilder.logger("client-orders").logLevel(Level.INFO).build(),
                prototype);
        // every 5 seconds dump the entire contents of our local view of the output
        eventLoop.scheduleAtFixedRate(
                Util.dumper("OrdersDumper", orders.output()), 1, 5, TimeUnit.SECONDS);
        eventLoop.scheduleAtFixedRate(
                Util.dumper("PrototypeDumper", prototype.output()), 1, 5, TimeUnit.SECONDS);
        final Runnable toggleConnection =
                () -> {
                    if (orderClient.isConnected()) {
                        orderClient.disconnect();
                    } else {
                        orderClient.connect();
                    }
                };
        eventLoop.scheduleAtFixedRate(toggleConnection, 10, 10, TimeUnit.SECONDS);
    }

    private static void declareServer() throws Exception {
        final IntIndexedStructTable<Order> orders = intIndexedStructTable(Order.class).build();
        fillTable(orders);
        final RegisteredOutputsTable registeredOutputs =
                RegisteredOutputsTable.registeredOutputsTable();
        registeredOutputs.register("orders", orders);
        Connector.connectInputToOutput(
                OutputLoggerBuilder.logger("server-orders").logLevel(Level.INFO).build(), orders);

        final var eventLoop = newEventLoop("server-data-thread");
        final DefaultSubscriptionProvider subscriptionProvider =
                DefaultSubscriptionProvider.defaultSubscriptionProvider(registeredOutputs);
        final GrpcService service =
                GrpcServiceBuilder.grpcService(subscriptionProvider, eventLoop).build();
        final Server server =
                ServerBuilder.forPort(orderPort).addService(service).executor(eventLoop).build();
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));
        server.start();
    }

    private static void fillTable(final IntIndexedStructTable<Order> orders) {
        final Order order = orders.createFacade();
        for (int i = 0; i < 5; i++) {
            orders.beginAdd(i, order)
                    .setAccount("ACC" + (i % 3))
                    .setQty((i + 1) * 100)
                    .setPrice(5.4 + i);
            orders.endAdd();
        }
        orders.fireChanges();
    }

    // formatting:off
    /** A simplified model of an Order which will get inspected at turned into a table structure. */
    public interface Order {
        int getOrderId();      // getter only bc it's the key field
        String getAccount();   Order setAccount(String value);
        int getQty();          Order setQty(int value);
        double getPrice();     Order setPrice(double value);
    }
    // formatting:on
}
