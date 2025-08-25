// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.examples.subscriptions;

import static com.bytefacets.spinel.grpc.receive.auth.JwtCallCredentials.jwtCredentials;
import static com.bytefacets.spinel.grpc.send.auth.MultiTenantJwtInterceptor.multiTenantJwt;
import static com.bytefacets.spinel.table.IntIndexedStructTableBuilder.intIndexedStructTable;

import ch.qos.logback.classic.LoggerContext;
import com.bytefacets.collections.queue.IntDeque;
import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.common.Connector;
import com.bytefacets.spinel.comms.ConnectionInfo;
import com.bytefacets.spinel.comms.SubscriptionConfig;
import com.bytefacets.spinel.comms.send.CommonSubscriptionContext;
import com.bytefacets.spinel.comms.send.DefaultSubscriptionContainer;
import com.bytefacets.spinel.comms.send.DefaultSubscriptionProvider;
import com.bytefacets.spinel.comms.send.ModificationResponse;
import com.bytefacets.spinel.comms.send.RegisteredOutputsTable;
import com.bytefacets.spinel.comms.send.SubscriptionContainer;
import com.bytefacets.spinel.comms.send.SubscriptionFactory;
import com.bytefacets.spinel.comms.subscription.ModificationRequest;
import com.bytefacets.spinel.examples.Util;
import com.bytefacets.spinel.filter.Filter;
import com.bytefacets.spinel.filter.FilterBuilder;
import com.bytefacets.spinel.filter.lib.StringPredicate;
import com.bytefacets.spinel.grpc.receive.GrpcClient;
import com.bytefacets.spinel.grpc.receive.GrpcClientBuilder;
import com.bytefacets.spinel.grpc.receive.GrpcSource;
import com.bytefacets.spinel.grpc.receive.GrpcSourceBuilder;
import com.bytefacets.spinel.grpc.send.GrpcService;
import com.bytefacets.spinel.grpc.send.GrpcServiceBuilder;
import com.bytefacets.spinel.printer.OutputLoggerBuilder;
import com.bytefacets.spinel.table.IntIndexedStructTable;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * In this example, we're intercepting the subscription creation on the server and using the
 * authenticated user to apply a Filter which only lets through rows relating to specific values in
 * the Account field of an Orders table.
 *
 * <p>When the user connects, we take the user's name and pull out a set of permitted accounts. We
 * configure a Filter with a predicate that evaluates each row, limiting what passes by checking
 * that the Account value is in the set of permitted accounts.
 *
 * <p>In this example, we also configure a periodic task on the client side to dump the entire
 * contents of the view that has been received by the user to show that it only contains the
 * permitted accounts.
 */
final class PermissionFilter {
    private static final String[] ACCOUNTS = new String[] {"DEREK", "BOB", "CAROL"};
    private static final int MAX_ACTIVE_ORDERS = 7;
    private static final int orderPort = 26001;
    // this is who will be logging in
    private static final String EXAMPLE_USER = "bob";
    // some set of example permissions - maybe these would be loaded from a database
    // or received from some event system
    // formatting:off
    private static final Map<String, Set<String>> PERMISSIONS =
            Map.of("bob",   Set.of("BOB", "CAROL"), // bob can see BOB and CAROL
                   "carol", Set.of("BOB", "CAROL"), // carol can see BOB and CAROL
                   "derek", Set.of("DEREK"));       // derek can only see DEREK
    // formatting:on
    private static final Random random = new Random(321657645);

    private PermissionFilter() {}

    public static void main(final String[] args) throws Exception {
        final LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLogger("io.grpc").setLevel(ch.qos.logback.classic.Level.INFO);
        context.getLogger("io.netty").setLevel(ch.qos.logback.classic.Level.INFO);
        declareServer();
        declareClient();
        Thread.currentThread().join();
    }

    /**
     * Create a client, with authentication for the user and configure a subscription to the
     * "orders" output on the server.
     */
    private static void declareClient() {
        final ManagedChannel orderChannel = clientChannel("0.0.0.0:" + orderPort);
        final var eventLoop = newEventLoop("client-data-thread");
        final var creds = jwtCredentials("some-issuer", EXAMPLE_USER, "some-secret");
        final GrpcClient orderClient =
                GrpcClientBuilder.grpcClient(orderChannel, eventLoop)
                        .connectionInfo(new ConnectionInfo("order-server", "0.0.0.0:" + orderPort))
                        .withSpecializer(stub -> stub.withCallCredentials(creds))
                        .build();
        final SubscriptionConfig subscription =
                SubscriptionConfig.subscriptionConfig("orders").defaultAll().build();
        final GrpcSource orders =
                GrpcSourceBuilder.grpcSource(orderClient, "local-orders")
                        .subscription(subscription)
                        .build();
        // log the changes happening out of `GrpcSource orders`
        Connector.connectInputToOutput(
                OutputLoggerBuilder.logger("client-orders").logLevel(Level.INFO).build(), orders);
        // every 5 seconds dump the entire contents of our local view of the output
        eventLoop.scheduleAtFixedRate(
                Util.dumper("ClientDump", orders.output()), 1, 5, TimeUnit.SECONDS);
        orderClient.connect();
    }

    private static void declareServer() throws Exception {
        final IntIndexedStructTable<Order> orders = intIndexedStructTable(Order.class).build();
        final RegisteredOutputsTable registeredOutputs = new RegisteredOutputsTable();
        registeredOutputs.register("orders", orders);
        Connector.connectInputToOutput(
                OutputLoggerBuilder.logger("server-orders").logLevel(Level.INFO).build(), orders);

        final var eventLoop = newEventLoop("server-data-thread");
        // every second, perform some modification on the order table
        // kind of slow so you can observe the changes
        eventLoop.scheduleAtFixedRate(new OrderModifier(orders), 1, 1000, TimeUnit.MILLISECONDS);
        final DefaultSubscriptionProvider subscriptionProvider =
                DefaultSubscriptionProvider.defaultSubscriptionProvider(
                        registeredOutputs, new PermissionedSubscriptionFactory());
        final GrpcService service =
                GrpcServiceBuilder.grpcService(subscriptionProvider, eventLoop).build();
        final Map<String, String> tenantSecrets = Map.of("some-issuer", "some-secret");
        final Server server =
                ServerBuilder.forPort(orderPort)
                        .addService(
                                ServerInterceptors.intercept(
                                        service, multiTenantJwt(tenantSecrets::get)))
                        .executor(eventLoop)
                        .build();
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));
        server.start();
    }

    private static ManagedChannel clientChannel(final String target) {
        return ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .enableRetry()
                .keepAliveTime(5, TimeUnit.MINUTES)
                .keepAliveTimeout(20, TimeUnit.SECONDS)
                .build();
    }

    private static EventLoop newEventLoop(final String name) {
        return new DefaultEventLoop(
                r -> {
                    return new Thread(r, name);
                });
    }

    /**
     * This factory intercepts the call the create the subscription and injects a permission filter.
     */
    private static class PermissionedSubscriptionFactory implements SubscriptionFactory {
        private static final Logger log =
                LoggerFactory.getLogger("PermissionedSubscriptionFactory");

        @Override
        public SubscriptionContainer create(final CommonSubscriptionContext subscriptionContext) {
            final String user = subscriptionContext.sessionInfo().getUser();
            final Set<String> allowed = PERMISSIONS.getOrDefault(user, Set.of());
            log.info("Creating permission filter for {}: {}", user, allowed);
            // create a filter which will only pass when the row's Account is in the allowed set
            final Filter permissionedOutput =
                    FilterBuilder.filter()
                            // we always have a predicate, but let's do for explicitness
                            .passesWhenNoPredicate(false)
                            // our permission check which is applied to each row
                            .initialPredicate(
                                    StringPredicate.stringPredicate("Account", allowed::contains))
                            .build();
            Connector.connectInputToOutput(permissionedOutput, subscriptionContext.output());
            // create the standard default container, but we've replaced the output with
            // the permissioned output
            final SubscriptionContainer standardContainer =
                    DefaultSubscriptionContainer.defaultSubscriptionContainer(
                            subscriptionContext.sessionInfo(),
                            subscriptionContext.subscriptionConfig(),
                            subscriptionContext.initialModifications(),
                            permissionedOutput.output(),
                            subscriptionContext.modificationHandler());
            return wrap(subscriptionContext.output(), permissionedOutput, standardContainer);
        }

        // Wrap the default container here so that we can manage the termination.
        private SubscriptionContainer wrap(
                final TransformOutput originalOutput,
                final Filter permissionFilter,
                final SubscriptionContainer delegate) {
            return new SubscriptionContainer() {
                @Override
                public ModificationResponse add(final ModificationRequest update) {
                    return delegate.add(update);
                }

                @Override
                public ModificationResponse remove(final ModificationRequest update) {
                    return delegate.remove(update);
                }

                @Override
                public void terminateSubscription() {
                    originalOutput.detachInput(permissionFilter.input());
                    delegate.terminateSubscription();
                }

                @Override
                public TransformOutput output() {
                    return delegate.output();
                }
            };
        }
    }

    // Simple modification routine that creates a max number of orders and randomly
    // picks one of the 3 accounts. It modifies existing orders and replaces them as they fill.
    private static final class OrderModifier implements Runnable {
        private final IntIndexedStructTable<Order> orders;
        private final IntDeque activeOrderIds = new IntDeque(32);
        private final Order facade;
        private int nextId = 100;

        private OrderModifier(final IntIndexedStructTable<Order> orders) {
            this.orders = orders;
            this.facade = orders.createFacade();
        }

        @Override
        public void run() {
            if (activeOrderIds.size() == MAX_ACTIVE_ORDERS) {
                final int id = activeOrderIds.removeFirst();
                processUpdate(id);
            } else {
                processAdd();
            }
            orders.fireChanges();
        }

        private void processAdd() {
            final int id = nextId++;
            orders.beginAdd(id, facade)
                    .setQty(random.nextInt(1, 5) * 100)
                    .setPrice(((int) (500 + random.nextDouble(20)) / 100d))
                    .setAccount(ACCOUNTS[id % ACCOUNTS.length]);
            orders.endAdd();
            activeOrderIds.addLast(id);
        }

        private void processUpdate(final int id) {
            orders.moveToRow(facade, orders.lookupKeyRow(id));
            final int newQty = Math.max(0, facade.getQty() - 100);
            if (newQty == 0) {
                orders.remove(id);
            } else {
                orders.beginChange(id, facade);
                facade.setQty(newQty);
                orders.endChange();
                activeOrderIds.addLast(id);
            }
        }
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
