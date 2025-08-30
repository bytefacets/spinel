// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.spring;

import static com.bytefacets.spinel.comms.send.DefaultSubscriptionProvider.defaultSubscriptionProvider;
import static com.bytefacets.spinel.table.IntIndexedStructTableBuilder.intIndexedStructTable;

import com.bytefacets.collections.queue.IntDeque;
import com.bytefacets.spinel.common.Connector;
import com.bytefacets.spinel.comms.send.DefaultSubscriptionProvider;
import com.bytefacets.spinel.comms.send.OutputRegistry;
import com.bytefacets.spinel.comms.send.RegisteredOutputsTable;
import com.bytefacets.spinel.join.Join;
import com.bytefacets.spinel.join.JoinBuilder;
import com.bytefacets.spinel.join.JoinKeyHandling;
import com.bytefacets.spinel.printer.OutputLoggerBuilder;
import com.bytefacets.spinel.table.IntIndexedStructTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.slf4j.event.Level;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;

@Configuration
public final class TopologyBuilder {
    private static final int NUM_INSTRUMENTS = 10;
    private static final int MAX_ORDERS = 15;
    private final IntIndexedStructTable<Order> orders;
    private final RegisteredOutputsTable outputs = new RegisteredOutputsTable();
    private final IntIndexedStructTable<Instrument> instruments;
    private final EventLoop eventLoop;
    private final DefaultSubscriptionProvider subscriptionProvider;

    @SuppressWarnings("this-escape")
    public TopologyBuilder() {
        this.eventLoop =
                new DefaultEventLoop(
                        r -> {
                            return new Thread(r, "server-thread");
                        });
        orders = intIndexedStructTable(Order.class).build();
        instruments = intIndexedStructTable(Instrument.class).build();
        final Join join =
                JoinBuilder.lookupJoin("order-view")
                        .inner()
                        .joinOn(List.of("InstrumentId"), List.of("InstrumentId"), 10)
                        .withJoinKeyHandling(JoinKeyHandling.KeepLeft)
                        .build();
        Connector.connectInputToOutput(join.leftInput(), orders);
        Connector.connectInputToOutput(join.rightInput(), instruments);
        registerInstruments();
        Connector.connectInputToOutput(
                OutputLoggerBuilder.logger("order-view").logLevel(Level.INFO).build(), join);
        outputs.register("orders", orders);
        outputs.register("instruments", instruments);
        outputs.register("order-view", join.output());
        subscriptionProvider = defaultSubscriptionProvider(outputs);
        start();
    }

    @Bean
    HandlerMapping handlerMapping() {
        System.out.println("CREATING HANDLER MAPPING");
        final var mapping =
                new SimpleUrlHandlerMapping(
                        Map.of(
                                "/ws/spinel",
                                new SpinelWebSocketHandler(subscriptionProvider, eventLoop)));
        mapping.setOrder(-1);
        return mapping;
    }

    void start() {
        final OrderCreator creator = new OrderCreator();
        eventLoop.scheduleAtFixedRate(creator, 1, 500, TimeUnit.MILLISECONDS);
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    @Bean
    public EventLoop eventLoop() {
        return eventLoop;
    }

    @Bean
    public OutputRegistry registry() {
        return outputs;
    }

    private final class OrderCreator implements Runnable {
        private final Random random = new Random(7652702);
        private final Order facade;
        private final IntDeque activeOrders = new IntDeque(MAX_ORDERS);
        int id = 1;

        private OrderCreator() {
            facade = orders.createFacade();
        }

        public void run() {
            for (int i = 0, len = random.nextInt(1, 3); i < len; i++) {
                if (activeOrders.size() < MAX_ORDERS) {
                    activeOrders.addLast(createOrder());
                } else {
                    final int orderId = activeOrders.removeFirst();
                    if (updateOrder(orderId)) {
                        activeOrders.addLast(orderId);
                    }
                }
            }
            orders.fireChanges();
        }

        private int createOrder() {
            final int orderId = id++;
            final int instrumentId = (orderId * 31) % NUM_INSTRUMENTS;
            final int numLots = random.nextInt(1, 10);
            orders.beginAdd(orderId, facade)
                    .setInstrumentId(instrumentId)
                    .setQuantity(numLots * 100)
                    .setPrice(5.2 * (instrumentId + 1));
            orders.endAdd();
            return orderId;
        }

        private boolean updateOrder(final int orderId) {
            final int row = orders.lookupKeyRow(orderId);
            orders.moveToRow(facade, row);
            final int newQuantity = facade.getQuantity() - (random.nextInt(1, 3) * 100);
            if (newQuantity <= 0) {
                orders.remove(orderId);
                return false;
            } else {
                orders.beginChange(orderId, facade).setQuantity(newQuantity);
                orders.endChange();
                return true;
            }
        }
    }

    private void registerInstruments() {
        final Instrument facade = instruments.createFacade();
        for (int i = 0; i < NUM_INSTRUMENTS; i++) {
            instruments
                    .beginAdd(i, facade)
                    .setSymbol(Integer.toHexString(109010 * (i + 1)).toUpperCase());
            instruments.endAdd();
        }
        instruments.fireChanges();
    }

    // formatting:off
    interface Order {
        int getOrderId();
        int getQuantity(); Order setQuantity(int value);
        int getInstrumentId(); Order setInstrumentId(int value);
        double getPrice(); Order setPrice(double value);
    }
    interface Instrument {
        int getInstrumentId();
        String getSymbol(); void setSymbol(String symbol);
    }
    // formatting:on
}
