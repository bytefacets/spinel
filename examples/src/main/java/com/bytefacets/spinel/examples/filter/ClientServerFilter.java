// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.examples.filter;

import static com.bytefacets.spinel.examples.Util.clientChannel;
import static com.bytefacets.spinel.examples.Util.newEventLoop;
import static com.bytefacets.spinel.table.IntIndexedStructTableBuilder.intIndexedStructTable;

import ch.qos.logback.classic.LoggerContext;
import com.bytefacets.spinel.common.Connector;
import com.bytefacets.spinel.comms.ConnectionInfo;
import com.bytefacets.spinel.comms.SubscriptionConfig;
import com.bytefacets.spinel.comms.receive.SubscriptionListener;
import com.bytefacets.spinel.comms.send.DefaultSubscriptionProvider;
import com.bytefacets.spinel.comms.send.ModificationResponse;
import com.bytefacets.spinel.comms.send.RegisteredOutputsTable;
import com.bytefacets.spinel.comms.subscription.ModificationRequest;
import com.bytefacets.spinel.comms.subscription.ModificationRequestFactory;
import com.bytefacets.spinel.examples.Util;
import com.bytefacets.spinel.grpc.receive.GrpcClient;
import com.bytefacets.spinel.grpc.receive.GrpcClientBuilder;
import com.bytefacets.spinel.grpc.receive.GrpcSource;
import com.bytefacets.spinel.grpc.receive.GrpcSourceBuilder;
import com.bytefacets.spinel.grpc.send.GrpcService;
import com.bytefacets.spinel.grpc.send.GrpcServiceBuilder;
import com.bytefacets.spinel.printer.OutputLoggerBuilder;
import com.bytefacets.spinel.table.IntIndexedStructTable;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * In this example, a client connects to a server and issues requests to change the filter criteria
 * on the server.
 */
final class ClientServerFilter {
    private static final int orderPort = 26001;
    private static final Logger log = LoggerFactory.getLogger("Example");

    private ClientServerFilter() {}

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
        // the listener will print responses to the requests
        final Listener listener = new Listener();

        // define the client and point it to the server
        final GrpcClient orderClient =
                GrpcClientBuilder.grpcClient(orderChannel, eventLoop)
                        .connectionInfo(new ConnectionInfo("order-server", "0.0.0.0:" + orderPort))
                        .build();

        // define the subscription
        final SubscriptionConfig subscription =
                SubscriptionConfig.subscriptionConfig("orders").defaultAll().build();
        final GrpcSource orders =
                GrpcSourceBuilder.grpcSource(orderClient, "local-orders")
                        .subscription(subscription)
                        .withListener(listener)
                        .build();

        // set up a task to periodically change the filter
        final ChangeFilter changer = new ChangeFilter(orders);
        listener.dumper = Util.dumper("OrdersDumper", orders.output());

        // log the changes happening out of the prototype
        Connector.connectInputToOutput(
                OutputLoggerBuilder.logger("client-orders").logLevel(Level.INFO).build(), orders);
        eventLoop.scheduleAtFixedRate(changer, 1, 5, TimeUnit.SECONDS);
        orderClient.connect(); // connect to the server now
    }

    /** Create a server with an "orders" table available for subscription. */
    private static void declareServer() throws Exception {
        final IntIndexedStructTable<Order> orders = intIndexedStructTable(Order.class).build();
        fillTable(orders); // we'll just have this one be static
        Connector.connectInputToOutput(
                OutputLoggerBuilder.logger("server-orders").logLevel(Level.INFO).build(), orders);

        // set it up so that the table can be subscribed to
        final RegisteredOutputsTable registeredOutputs = new RegisteredOutputsTable();
        registeredOutputs.register("orders", orders);

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

    /**
     * A task which will cycle through "ACC1", "ACC2", and "ACC3" account filters. It will request a
     * new filter, then remove the old one.
     */
    private static final class ChangeFilter implements Runnable {
        private final GrpcSource source;
        private int step;
        private ModificationRequest current;

        private ChangeFilter(final GrpcSource source) {
            this.source = source;
        }

        @Override
        public void run() {
            // create a new filter request
            final String acct = "ACC" + (step++ % 3);
            final ModificationRequest newRequest =
                    ModificationRequestFactory.applyFilterExpression(
                            String.format("Account == '%s'", acct));
            log.info("Sending filter for {}", newRequest.arguments()[0]);
            source.subscriptionHandle().add(newRequest);

            // remove the old filter
            if (current != null) {
                log.info("Sending REMOVE for {}", current.arguments()[0]);
                source.subscriptionHandle().remove(current);
            }
            current = newRequest;
        }
    }

    /** A periodic task which logs connection and out-of-band events on the subscription. */
    private static class Listener implements SubscriptionListener {
        private Runnable dumper;

        @Override
        public void onModificationAddResponse(
                final ModificationRequest request, final ModificationResponse response) {
            receivedResponse("Add", request, response);
        }

        @Override
        public void onModificationRemoveResponse(
                final ModificationRequest request, final ModificationResponse response) {
            receivedResponse("Remove", request, response);
        }

        private void receivedResponse(
                final String action,
                final ModificationRequest request,
                final ModificationResponse response) {
            if (!response.success()) {
                System.err.printf(
                        "Modification Failure on %s of %s: %s%n",
                        action, request, response.message());
            } else {
                log.info("Client received response to {} {}: {}", action, request, response);
            }
            // show what is in the output
            dumper.run();
        }
    }

    /** Fills the orders table on the server with 10 rows, cycling through 3 accounts. */
    private static void fillTable(final IntIndexedStructTable<Order> orders) {
        final Order order = orders.createFacade();
        for (int i = 0; i < 10; i++) {
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
