// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.examples.grpc;

import static com.bytefacets.diaspore.examples.grpc.Client.declareClient;
import static com.bytefacets.diaspore.examples.grpc.MarketDataServer.MD_PORT;
import static com.bytefacets.diaspore.examples.grpc.MarketDataServer.SLOW;
import static com.bytefacets.diaspore.examples.grpc.MarketDataServer.declareMarketDataServer;
import static com.bytefacets.diaspore.examples.grpc.OrderServer.ORDER_PORT;
import static com.bytefacets.diaspore.examples.grpc.OrderServer.declareOrderServer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is a convenience example, which will, given no args run all 3 components of the example: the
 * order server, the market data server, and the client.
 */
final class ClientAndServersOverGrpc {
    private ClientAndServersOverGrpc() {}

    public static void main(final String[] args) throws Exception {
        // System.setProperty("io.grpc.internal.ChannelLogger.level", "FINEST");
        Logger.getGlobal().setLevel(Level.INFO);
        final List<Callable<Void>> startup = new ArrayList<>();
        Set.copyOf(Arrays.asList(args)).stream()
                .map(
                        arg ->
                                switch (arg) {
                                    case "md" -> declareMarketDataServer(MD_PORT, SLOW);
                                    case "order" -> declareOrderServer(ORDER_PORT);
                                    case "client" -> declareClient(ORDER_PORT, MD_PORT);
                                    default -> null;
                                })
                .filter(Objects::nonNull)
                .forEach(startup::add);
        if (startup.isEmpty()) {
            startup.add(declareClient(ORDER_PORT, MD_PORT));
            startup.add(declareOrderServer(ORDER_PORT));
            startup.add(declareMarketDataServer(MD_PORT, SLOW));
        }

        for (var startupFunc : startup) {
            startupFunc.call();
        }

        // Keep the client running to receive the response
        Thread.currentThread().join();
    }
}
