package com.bytefacets.diaspore.grpc.receive;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.IntGenericIndexedMap;
import com.bytefacets.diaspore.comms.ConnectionInfo;
import com.bytefacets.diaspore.comms.SubscriptionConfig;
import com.bytefacets.diaspore.grpc.proto.SubscriptionRequest;
import com.bytefacets.diaspore.grpc.proto.SubscriptionResponse;
import com.google.common.annotations.VisibleForTesting;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class SubscriptionStore {
    private static final Logger log = LoggerFactory.getLogger(SubscriptionStore.class);
    private final IntGenericIndexedMap<Subscription> subscriptions = new IntGenericIndexedMap<>(16);
    private final ConnectionInfo connectionInfo;
    private IntSupplier tokenSupplier;

    SubscriptionStore(final ConnectionInfo connectionInfo) {
        this.connectionInfo = requireNonNull(connectionInfo, "connectionInfo");
    }

    void connect(final IntSupplier tokenSupplier) {
        this.tokenSupplier = requireNonNull(tokenSupplier, "tokenSupplier");
    }

    private int token() {
        return tokenSupplier.getAsInt();
    }

    void resubscribe(final Consumer<SubscriptionRequest> consumer) {
        synchronized (subscriptions) {
            subscriptions.forEachValue(
                    sub -> sub.requestSubscriptionIfNecessary(token(), consumer));
        }
    }

    Subscription createSubscription(
            final int subscriptionId, final GrpcDecoder decoder, final SubscriptionConfig config) {
        final var sub = new Subscription(subscriptionId, decoder, config);
        synchronized (subscriptions) {
            subscriptions.put(subscriptionId, sub);
        }
        return sub;
    }

    void resetSubscriptionStatus() {
        synchronized (subscriptions) {
            subscriptions.forEachValue(Subscription::markUnsubscribed);
        }
    }

    void accept(final SubscriptionResponse response) {
        final int subscriptionId = response.getSubscriptionId();
        synchronized (subscriptions) {
            final Subscription sub = subscriptions.getOrDefault(subscriptionId, null);
            if (sub != null) {
                sub.decoder().accept(response);
            } else {
                log.debug(
                        "ClientOf[{}] Did not find subscription for subscriptionId {}",
                        connectionInfo,
                        response.getSubscriptionId());
            }
        }
    }

    void remove(final int subscriptionId) {
        synchronized (subscriptions) {
            subscriptions.remove(subscriptionId);
        }
    }

    @VisibleForTesting
    int numSubscriptions() {
        return subscriptions.size();
    }

    @VisibleForTesting
    Subscription get(final int subscriptionId) {
        return subscriptions.getOrDefault(subscriptionId, null);
    }
}
