package com.bytefacets.diaspore.grpc.receive;

import com.bytefacets.collections.hash.IntGenericIndexedMap;
import com.bytefacets.diaspore.comms.SubscriptionConfig;
import com.bytefacets.diaspore.grpc.proto.SubscriptionRequest;
import com.bytefacets.diaspore.grpc.proto.SubscriptionResponse;
import com.google.common.annotations.VisibleForTesting;
import java.util.function.Consumer;

final class SubscriptionStore {
    private final IntGenericIndexedMap<Subscription> subscriptions = new IntGenericIndexedMap<>(16);

    SubscriptionStore() {}

    void resubscribe(final Consumer<SubscriptionRequest> consumer) {
        synchronized (subscriptions) {
            subscriptions.forEachValue(sub -> consumer.accept(sub.createRequest()));
        }
    }

    Subscription createSubscription(
            final int token, final GrpcDecoder decoder, final SubscriptionConfig config) {
        final var sub = new Subscription(token, decoder, config);
        synchronized (subscriptions) {
            subscriptions.put(token, sub);
        }
        return sub;
    }

    void accept(final SubscriptionResponse response) {
        final int token = response.getRefToken();
        synchronized (subscriptions) {
            final Subscription sub = subscriptions.getOrDefault(token, null);
            if (sub != null) {
                sub.decoder().accept(response);
            }
        }
    }

    void remove(final int token) {
        synchronized (subscriptions) {
            subscriptions.remove(token);
        }
    }

    @VisibleForTesting
    int numSubscriptions() {
        return subscriptions.size();
    }

    @VisibleForTesting
    Subscription get(final int token) {
        return subscriptions.getOrDefault(token, null);
    }
}
