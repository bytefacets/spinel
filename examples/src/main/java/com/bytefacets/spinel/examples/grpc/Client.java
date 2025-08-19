// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.examples.grpc;

import static com.bytefacets.spinel.comms.SubscriptionConfig.subscriptionConfig;
import static com.bytefacets.spinel.examples.grpc.MarketDataServer.MD_PORT;
import static com.bytefacets.spinel.examples.grpc.OrderServer.ORDER_PORT;
import static com.bytefacets.spinel.grpc.receive.auth.JwtCallCredentials.jwtCredentials;
import static com.bytefacets.spinel.transform.TransformBuilder.transform;

import com.bytefacets.spinel.comms.ConnectionInfo;
import com.bytefacets.spinel.grpc.receive.GrpcClient;
import com.bytefacets.spinel.grpc.receive.GrpcClientBuilder;
import com.bytefacets.spinel.grpc.receive.GrpcSource;
import com.bytefacets.spinel.grpc.receive.GrpcSourceBuilder;
import com.bytefacets.spinel.join.JoinKeyHandling;
import com.bytefacets.spinel.transform.TransformBuilder;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.netty.channel.DefaultEventLoop;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.slf4j.event.Level;

/**
 * Demonstrates connecting to two GRPC servers and joining the result.
 *
 * <p>Running this class will launch just the client. You'll have to launch the two other servers
 * separately.
 */
final class Client {
    private final GrpcClient orderClient;
    private final GrpcClient mdClient;
    private GrpcSource orders;

    public static void main(final String[] args) throws Exception {
        declareClient(ORDER_PORT, MD_PORT).call();
        Thread.currentThread().join();
    }

    static Callable<Void> declareClient(final int orderPort, final int mdPort) {
        final ManagedChannel orderChannel = clientChannel("0.0.0.0:" + orderPort);
        final ManagedChannel mdChannel = clientChannel("0.0.0.0:" + mdPort);

        final var clientDataEventLoop =
                new DefaultEventLoop(
                        r -> {
                            return new Thread(r, "client-data-thread");
                        });
        final var creds = jwtCredentials("bob", "bob-user", "bobs-secret");
        final var orderClient =
                GrpcClientBuilder.grpcClient(orderChannel, clientDataEventLoop)
                        .connectionInfo(new ConnectionInfo("order-server", "0.0.0.0:" + orderPort))
                        .withSpecializer(stub -> stub.withCallCredentials(creds))
                        .build();
        final var mdClient =
                GrpcClientBuilder.grpcClient(mdChannel, clientDataEventLoop)
                        .connectionInfo(new ConnectionInfo("md-server", "0.0.0.0:" + mdPort))
                        .build();
        new Client(orderClient, mdClient).build();
        return () -> {
            orderClient.connect();
            mdClient.connect();
            return null;
        };
    }

    private static ManagedChannel clientChannel(final String target) {
        return ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .enableRetry()
                .keepAliveTime(5, TimeUnit.MINUTES)
                .keepAliveTimeout(20, TimeUnit.SECONDS)
                .build();
    }

    Client(final GrpcClient orderClient, final GrpcClient mdClient) {
        this.orderClient = orderClient;
        this.mdClient = mdClient;
    }

    void build() {
        // get the order-view output from the orders server using the orderClient
        orders =
                GrpcSourceBuilder.grpcSource(orderClient, "order-view")
                        .subscription(subscriptionConfig("order-view").defaultAll().build())
                        .getOrCreate();
        // get the market-data output from the market data server using the mdClient
        final GrpcSource marketData =
                GrpcSourceBuilder.grpcSource(mdClient, "market-data")
                        .subscription(subscriptionConfig("market-data").defaultAll().build())
                        .getOrCreate();
        // register the order-view in a transform
        final TransformBuilder transform = transform().registerNode("order-view", orders);
        transform
                // register the market-data, too.
                // this is using the "continuation" form so we get a fluent style
                .with("market-data", marketData)
                // then we'll use a projection to do a column rename
                .project("md-price-column-renamed")
                // because both orders and market-data have Price, we'll
                // tell this projection to rename the market-data.Price to CurrentPrice
                .inboundAlias("Price", "CurrentPrice");
        // the fluent-style doesn't fit well with joins because they have multiple inputs
        transform
                .lookupJoin("orders-with-md")
                .outer()
                // we'll join InstrumentId from the orders to InstrumentId of the market data
                .joinOn(List.of("InstrumentId"), List.of("InstrumentId"), 16)
                // we'll keep only the InstrumentId from the orders and drop the
                // InstrumentId field from the market data
                .withJoinKeyHandling(JoinKeyHandling.KeepLeft)
                // when this node gets "built", it will connect the left input to the
                // registered "order-view" in the transformBuilder
                .withLeftSource("order-view")
                // when this node gets "built", it will connect the right input to the
                // registered "md-price-column-renamed" projection in the transformBuilder
                .withRightSource("md-price-column-renamed")
                .then()
                // then log what the output of this join is
                .logger("order-with-md.client")
                .logLevel(Level.INFO);
        transform.build();
    }
}
