package com.bytefacets.diaspore.grpc.receive;

import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.comms.SubscriptionConfig;
import com.bytefacets.diaspore.grpc.proto.SubscriptionRequest;
import com.google.common.annotations.VisibleForTesting;
import java.util.function.Consumer;

final class Subscription {
    private final Object lock = new Object();
    private final int subscriptionId;
    private final SubscriptionConfig config;
    private final GrpcDecoder decoder;
    private boolean isSubscribed;

    Subscription(
            final int subscriptionId, final GrpcDecoder decoder, final SubscriptionConfig config) {
        this.subscriptionId = subscriptionId;
        this.decoder = requireNonNull(decoder, "decoder");
        this.config = requireNonNull(config, "config");
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

    void requestSubscriptionIfNecessary(
            final int token, final Consumer<SubscriptionRequest> consumer) {
        synchronized (lock) {
            if (!isSubscribed) {
                consumer.accept(createRequest(token));
                isSubscribed = true;
            }
        }
    }

    @VisibleForTesting
    SubscriptionRequest createRequest(final int token) {
        return MsgHelp.request(token, subscriptionId, MsgHelp.subscription(config));
    }

    @Override
    public String toString() {
        return String.format(
                "[subscription-id=%d][subscribed=%b]: %s", subscriptionId, isSubscribed, config);
    }
}
