// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.examples.grpc;

import static com.bytefacets.spinel.grpc.send.auth.MultiTenantJwtInterceptor.multiTenantJwt;
import static com.bytefacets.spinel.schema.FieldDescriptor.stringField;

import com.bytefacets.spinel.comms.send.DefaultSubscriptionProvider;
import com.bytefacets.spinel.comms.send.RegisteredOutputsTable;
import com.bytefacets.spinel.examples.common.LimitedSetSimulator;
import com.bytefacets.spinel.grpc.send.GrpcService;
import com.bytefacets.spinel.grpc.send.GrpcServiceBuilder;
import com.bytefacets.spinel.join.Join;
import com.bytefacets.spinel.join.JoinKeyHandling;
import com.bytefacets.spinel.table.IntIndexedStructTable;
import com.bytefacets.spinel.table.IntIndexedTable;
import com.bytefacets.spinel.table.TableRow;
import com.bytefacets.spinel.transform.TransformBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import net.datafaker.Faker;
import org.slf4j.event.Level;

/**
 * Builds a mock order data server which produces orders.
 *
 * <p>The data from this server is used to demonstrate a client receiving data from multiple servers
 * and joining the data on the client side by some key.
 *
 * <p>Refer to the {@link Order Order interface} to see the simplified model of an Order which will
 * get inspected at turned into a table structure.
 *
 * @see LimitedSetSimulator
 */
final class OrderServer {
    private static final Random random = new Random(76598739);
    static final int ORDER_PORT = 25001;
    static final int NUM_INSTRUMENTS = 10;
    private static final int ACTIVE_ORDER_HI = 10;
    private static final int ACTIVE_ORDER_LO = 3;
    private static final int NUM_EVENTS_PER_BATCH = 1;
    private static final Duration MIN_DELAY = Duration.ofMillis(10);
    private static final Duration MAX_DELAY = Duration.ofMillis(250);
    private final IntIndexedStructTable<Order> orders;
    private final RegisteredOutputsTable outputs;
    private final IntIndexedTable instruments;
    private final EventLoop eventLoop;

    public static void main(final String[] args) throws Exception {
        declareOrderServer(ORDER_PORT).call();
        Thread.currentThread().join();
    }

    static Callable<Void> declareOrderServer(final int port) {
        final OrderServer topologyBuilder = new OrderServer();
        final DefaultSubscriptionProvider subscriptionProvider =
                DefaultSubscriptionProvider.defaultSubscriptionProvider(topologyBuilder.outputs);
        final GrpcService service =
                GrpcServiceBuilder.grpcService(subscriptionProvider, topologyBuilder.eventLoop)
                        .build();
        final Map<String, String> tenantSecrets = Map.of("bob", "bobs-secret");
        final Server server =
                ServerBuilder.forPort(port)
                        .addService(
                                ServerInterceptors.intercept(
                                        service, multiTenantJwt(tenantSecrets::get)))
                        .executor(topologyBuilder.eventLoop)
                        .build();
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));
        return () -> {
            topologyBuilder.start();
            server.start();
            return null;
        };
    }

    OrderServer() {
        eventLoop =
                new DefaultEventLoop(
                        r -> {
                            return new Thread(r, "order-server-thread");
                        });
        final TransformBuilder builder = TransformBuilder.transform();
        // a table that stores orders according to the model interface
        builder.intIndexedStructTable(Order.class);
        // a table stores instrument symbols by InstrumentId
        builder.intIndexedTable("Instrument")
                .keyFieldName("InstrumentId")
                .addFields(stringField("Symbol"));
        // then join the orders to the instruments by the InstrumentId
        builder.lookupJoin("OrderView")
                .withLeftSource("Order")
                .withRightSource("Instrument")
                .inner() // not letting through orders that are unresolved
                .joinOn(List.of("InstrumentId"), List.of("InstrumentId"), 10)
                // for the two InstrumentId fields, how do we want to handle them in the join's
                // output? in this case, we say we want to keep the InstrumentId from Order and
                // drop the InstrumentId from Instrument
                .withJoinKeyHandling(JoinKeyHandling.KeepLeft)
                // show us the output of this join... or plug it into further things
                .then()
                .logger("order-view.server")
                .logLevel(Level.INFO);
        builder.build();

        final Join join = builder.lookupNode("OrderView");
        orders = builder.lookupNode("Order");
        instruments = builder.lookupNode("Instrument");

        // mock some instruments in the instrument table
        registerInstruments();

        // then register some of the outputs so they are available to clients
        outputs = RegisteredOutputsTable.registeredOutputsTable();
        outputs.register("orders", orders);
        outputs.register("instruments", instruments);
        outputs.register("order-view", join);
    }

    /**
     * Start the order feed simulator
     *
     * @see LimitedSetSimulator
     */
    void start() {
        // this is a "simulator" of an order feed
        LimitedSetSimulator.limitedSetSimulator(
                        orders,
                        this::initializeMockOrder,
                        this::updateMockOrder,
                        this::shouldRemoveMockOrder,
                        eventLoop)
                .activeItemsLo(ACTIVE_ORDER_LO)
                .activeItemsHi(ACTIVE_ORDER_HI)
                .maxDelay(MAX_DELAY)
                .minDelay(MIN_DELAY)
                .numEventsPerBatch(NUM_EVENTS_PER_BATCH)
                .build()
                .start();
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

    private void registerInstruments() {
        final var stocks = new Faker(random).stock();
        final TableRow row = instruments.tableRow();
        final int fieldId = instruments.fieldId("Symbol");
        for (int i = 0; i < NUM_INSTRUMENTS; i++) {
            instruments.beginAdd(i);
            row.setString(fieldId, stocks.nyseSymbol());
            instruments.endAdd();
        }
        instruments.fireChanges();
    }

    /** A simplified model of an Order which will get inspected at turned into a table structure. */
    // formatting:off
    public interface Order {
        int getOrderId(); // getter only bc it's the key field
        int getQty();           Order setQty(int value);
        double getPrice();      Order setPrice(double value);
        int getInstrumentId();  Order setInstrumentId(int value);
    }
    // formatting:on
}
