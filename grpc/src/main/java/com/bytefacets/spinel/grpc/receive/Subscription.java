// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.receive;

import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.comms.SubscriptionConfig;
import com.bytefacets.spinel.grpc.proto.SubscriptionRequest;
import com.google.common.annotations.VisibleForTesting;
import java.util.function.Consumer;

final class Subscription {
    private final Object lock = new Object();
    private final int subscriptionId;
    private final SubscriptionConfig config;
    private final GrpcDecoder decoder;
    private final Consumer<SubscriptionRequest> messageSink;
    private final MsgHelp msgHelp;
    private boolean isSubscribed;

    Subscription(
            final int subscriptionId,
            final GrpcDecoder decoder,
            final SubscriptionConfig config,
            final MsgHelp msgHelp,
            final Consumer<SubscriptionRequest> messageSink) {
        this.subscriptionId = subscriptionId;
        this.decoder = requireNonNull(decoder, "decoder");
        this.config = requireNonNull(config, "config");
        this.msgHelp = requireNonNull(msgHelp, "msgHelp");
        this.messageSink = requireNonNull(messageSink, "messageSink");
    }

    int subscriptionId() {
        return subscriptionId;
    }

    SubscriptionConfig config() {
        return config;
    }

    GrpcDecoder decoder() {
        return decoder;
    }

    boolean isSubscribed() {
        return isSubscribed;
    }

    void markUnsubscribed() {
        synchronized (lock) {
            isSubscribed = false;
        }
    }

    void requestSubscriptionIfNecessary() {
        synchronized (lock) {
            if (!isSubscribed) {
                messageSink.accept(createRequest());
                isSubscribed = true;
            }
        }
    }

    @VisibleForTesting
    SubscriptionRequest createRequest() {
        return msgHelp.request(subscriptionId, msgHelp.subscription(config));
    }

    @Override
    public String toString() {
        return String.format(
                "[subscription-id=%d][subscribed=%b]: %s", subscriptionId, isSubscribed, config);
    }
}
