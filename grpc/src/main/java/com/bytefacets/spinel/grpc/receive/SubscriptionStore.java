// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.receive;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.IntGenericIndexedMap;
import com.bytefacets.spinel.comms.ConnectionInfo;
import com.bytefacets.spinel.comms.SubscriptionConfig;
import com.bytefacets.spinel.grpc.proto.SubscriptionRequest;
import com.bytefacets.spinel.grpc.proto.SubscriptionResponse;
import com.google.common.annotations.VisibleForTesting;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class SubscriptionStore {
    private static final Logger log = LoggerFactory.getLogger(SubscriptionStore.class);
    private final IntGenericIndexedMap<Subscription> subscriptions = new IntGenericIndexedMap<>(16);
    private final ConnectionInfo connectionInfo;
    private Consumer<SubscriptionRequest> messageSink;
    private MsgHelp msgHelp;

    SubscriptionStore(final ConnectionInfo connectionInfo) {
        this.connectionInfo = requireNonNull(connectionInfo, "connectionInfo");
    }

    void connect(final MsgHelp msgHelp, final Consumer<SubscriptionRequest> messageSink) {
        this.msgHelp = requireNonNull(msgHelp, "msgHelp");
        this.messageSink = requireNonNull(messageSink, "messageSink");
    }

    void resubscribe(final Consumer<SubscriptionRequest> consumer) {
        synchronized (subscriptions) {
            subscriptions.forEachValue(Subscription::requestSubscriptionIfNecessary);
        }
    }

    Subscription createSubscription(
            final int subscriptionId, final GrpcDecoder decoder, final SubscriptionConfig config) {
        final var sub = new Subscription(subscriptionId, decoder, config, msgHelp, messageSink);
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
                log.warn(
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
