package com.bytefacets.diaspore.grpc.receive;

import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.comms.SubscriptionConfig;
import com.bytefacets.diaspore.grpc.proto.SubscriptionRequest;
import com.google.common.annotations.VisibleForTesting;
import java.util.function.Consumer;

final class Subscription {
    private final Object lock = new Object();
    private final int token;
    private final SubscriptionConfig config;
    private final GrpcDecoder decoder;
    private boolean isSubscribed;

    Subscription(final int token, final GrpcDecoder decoder, final SubscriptionConfig config) {
        this.token = token;
        this.decoder = requireNonNull(decoder, "decoder");
        this.config = requireNonNull(config, "config");
    }

    int token() {
        return token;
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

    void requestSubscriptionIfNecessary(final Consumer<SubscriptionRequest> consumer) {
        synchronized (lock) {
            if (!isSubscribed) {
                consumer.accept(createRequest());
                isSubscribed = true;
            }
        }
    }

    @VisibleForTesting
    SubscriptionRequest createRequest() {
        return MsgHelp.request(token, MsgHelp.subscription(config));
    }

    @Override
    public String toString() {
        return String.format("[token=%d][subscribed=%b]: %s", token, isSubscribed, config);
    }
}
