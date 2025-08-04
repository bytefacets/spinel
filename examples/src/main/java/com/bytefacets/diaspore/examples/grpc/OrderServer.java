// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.examples.grpc;

import static com.bytefacets.diaspore.schema.FieldDescriptor.stringField;

import com.bytefacets.collections.queue.IntDeque;
import com.bytefacets.diaspore.comms.send.OutputRegistryFactory;
import com.bytefacets.diaspore.comms.send.RegisteredOutputsTable;
import com.bytefacets.diaspore.grpc.send.GrpcService;
import com.bytefacets.diaspore.grpc.send.GrpcServiceBuilder;
import com.bytefacets.diaspore.join.Join;
import com.bytefacets.diaspore.join.JoinKeyHandling;
import com.bytefacets.diaspore.table.IntIndexedStructTable;
import com.bytefacets.diaspore.table.IntIndexedTable;
import com.bytefacets.diaspore.table.TableRow;
import com.bytefacets.diaspore.transform.TransformBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.slf4j.event.Level;

/**
 * Builds a mock order data server which produces orders.
 *
 * <p>The data from this server is used to demonstrate a client receiving data from multiple servers
 * and joining the data on the client side by some key.
 */
final class OrderServer {
    static final int ORDER_PORT = 25001;
    static final int NUM_INSTRUMENTS = 10;
    private static final int ACTIVE_ORDER_HI = 1000;
    private static final int ACTIVE_ORDER_LO = 300;
    private static final int NUM_EVENTS_PER_BATCH = 10;
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
        final GrpcService service =
                GrpcServiceBuilder.grpcService(
                                topologyBuilder.registry(), topologyBuilder.eventLoop)
                        .build();
        final Server server =
                ServerBuilder.forPort(port)
                        .addService(service)
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
        outputs = new RegisteredOutputsTable();
        outputs.register("orders", orders);
        outputs.register("instruments", instruments);
        outputs.register("order-view", join);
    }

    void start() {
        eventLoop.execute(new OrderActivity());
    }

    OutputRegistryFactory registry() {
        return session -> outputs;
    }

    /** Little class that creates a batch of orders */
    private final class OrderActivity implements Runnable {
        private final Order facade;
        private final IntDeque activeOrders = new IntDeque(ACTIVE_ORDER_HI);
        int id = 1;

        private OrderActivity() {
            // the table provides a facade to the underlying table storage
            facade = orders.createFacade();
        }

        @Override
        public void run() {
            createBatch();
            // wait a little and go again!
            final long waitTime = 100 + (long) (Math.random() * 250);
            eventLoop.schedule(this, waitTime, TimeUnit.MILLISECONDS);
        }

        private void createBatch() {
            for (int i = 0; i < NUM_EVENTS_PER_BATCH; i++) {
                final int numActive = activeOrders.size();
                if (numActive < ACTIVE_ORDER_LO) {
                    createOrder();
                } else if (numActive >= ACTIVE_ORDER_HI) {
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
                    .setInstrumentId(1 + ((orderId * 31) % NUM_INSTRUMENTS))
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

    private void registerInstruments() {
        final TableRow row = instruments.tableRow();
        final int fieldId = instruments.fieldId("Symbol");
        for (int i = 0; i < NUM_INSTRUMENTS; i++) {
            instruments.beginAdd(i);
            row.setString(fieldId, Integer.toHexString(109010 * (i + 1)).toUpperCase());
            instruments.endAdd();
        }
        instruments.fireChanges();
    }

    /** A simplified model of an Order which will get inspected at turned into a table structure. */
    public interface Order {
        int getOrderId(); // getter only bc it's the key field

        int getQty();

        Order setQty(int value);

        double getPrice();

        Order setPrice(double value);

        int getInstrumentId();

        Order setInstrumentId(int value);
    }
}
