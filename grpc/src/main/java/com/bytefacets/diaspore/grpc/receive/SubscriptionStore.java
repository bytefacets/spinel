package com.bytefacets.diaspore.grpc.receive;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.IntGenericIndexedMap;
import com.bytefacets.diaspore.comms.ConnectionInfo;
import com.bytefacets.diaspore.comms.SubscriptionConfig;
import com.bytefacets.diaspore.grpc.proto.SubscriptionRequest;
import com.bytefacets.diaspore.grpc.proto.SubscriptionResponse;
import com.google.common.annotations.VisibleForTesting;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class SubscriptionStore {
    private static final Logger log = LoggerFactory.getLogger(SubscriptionStore.class);
    private final IntGenericIndexedMap<Subscription> subscriptions = new IntGenericIndexedMap<>(16);
    private final ConnectionInfo connectionInfo;

    SubscriptionStore(final ConnectionInfo connectionInfo) {
        this.connectionInfo = requireNonNull(connectionInfo, "connectionInfo");
    }

    void resubscribe(final Consumer<SubscriptionRequest> consumer) {
        synchronized (subscriptions) {
            subscriptions.forEachValue(sub -> sub.requestSubscriptionIfNecessary(consumer));
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

    void resetSubscriptionStatus() {
        synchronized (subscriptions) {
            subscriptions.forEachValue(Subscription::markUnsubscribed);
        }
    }

    void accept(final SubscriptionResponse response) {
        final int token = response.getRefToken();
        synchronized (subscriptions) {
            final Subscription sub = subscriptions.getOrDefault(token, null);
            if (sub != null) {
                sub.decoder().accept(response);
            } else {
                log.debug(
                        "ClientOf[{}] Did not find subscription for token {}",
                        connectionInfo,
                        token);
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
