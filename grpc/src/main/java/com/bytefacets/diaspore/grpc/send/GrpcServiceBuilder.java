// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.grpc.send;

import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.comms.send.SubscriptionProvider;
import io.netty.channel.EventLoop;

public final class GrpcServiceBuilder {
    private final SubscriptionProvider subscriptionProvider;
    private final EventLoop dataEventLoop;

    private GrpcServiceBuilder(
            final SubscriptionProvider subscriptionProvider, final EventLoop dataEventLoop) {
        this.subscriptionProvider = requireNonNull(subscriptionProvider, "subscriptionProvider");
        this.dataEventLoop = requireNonNull(dataEventLoop, "subscriptionProvider");
    }

    public static GrpcServiceBuilder grpcService(
            final SubscriptionProvider subscriptionProvider, final EventLoop dataEventLoop) {
        return new GrpcServiceBuilder(subscriptionProvider, dataEventLoop);
    }

    public GrpcService build() {
        return new GrpcService(subscriptionProvider, dataEventLoop);
    }
}
