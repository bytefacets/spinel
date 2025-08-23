// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.receive;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

import com.bytefacets.collections.hash.GenericIntIndexedMap;
import com.bytefacets.collections.hash.IntGenericIndexedMap;
import com.bytefacets.collections.queue.GenericDeque;
import com.bytefacets.spinel.comms.SubscriptionConfig;
import com.bytefacets.spinel.comms.receive.SubscriptionListener;
import com.bytefacets.spinel.comms.send.ModificationResponse;
import com.bytefacets.spinel.comms.subscription.ModificationRequest;
import com.bytefacets.spinel.grpc.proto.ModificationAddRemove;
import com.bytefacets.spinel.grpc.proto.Response;
import com.bytefacets.spinel.grpc.proto.ResponseType;
import com.bytefacets.spinel.grpc.proto.SubscriptionRequest;
import com.bytefacets.spinel.grpc.proto.SubscriptionResponse;
import com.google.common.annotations.VisibleForTesting;
import java.util.function.Consumer;

final class Subscription {
    private static final SubscriptionListener NO_OP = new SubscriptionListener() {};
    private final Object lock = new Object();
    private final int subscriptionId;
    private final SubscriptionConfig config;
    private final GrpcDecoder decoder;
    private final Consumer<SubscriptionRequest> messageSink;
    private final MsgHelp msgHelp;
    private final GenericIntIndexedMap<ModificationRequest> activeRequests =
            new GenericIntIndexedMap<>(4);
    private final IntGenericIndexedMap<InFlightHolder> inFlightListeners =
            new IntGenericIndexedMap<>(4);
    private final SubscriptionListener listener;
    private final GenericDeque<InFlightHolder> pool = new GenericDeque<>(2);
    private boolean isSubscribed;

    Subscription(
            final int subscriptionId,
            final GrpcDecoder decoder,
            final SubscriptionConfig config,
            final MsgHelp msgHelp,
            final Consumer<SubscriptionRequest> messageSink,
            final SubscriptionListener subscriptionListener) {
        this.subscriptionId = subscriptionId;
        this.decoder = requireNonNull(decoder, "decoder");
        this.config = requireNonNull(config, "config");
        this.msgHelp = requireNonNull(msgHelp, "msgHelp");
        this.messageSink = requireNonNull(messageSink, "messageSink");
        this.listener = requireNonNullElse(subscriptionListener, NO_OP);
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

    void add(final ModificationRequest request) {
        final int before = activeRequests.size();
        final int entry = activeRequests.add(request);
        final int newRefCount = activeRequests.getValueAt(entry) + 1;
        activeRequests.putValueAt(entry, newRefCount);
        if (before != activeRequests.size()) {
            if (newRefCount == 1) {
                final SubscriptionRequest msg = msgHelp.addModification(subscriptionId, request);
                final InFlightHolder holder = allocateHolder(request, ModificationAddRemove.ADD);
                inFlightListeners.put(msg.getMsgToken(), holder);
                messageSink.accept(msg);
            }
        }
    }

    void remove(final ModificationRequest request) {
        final int entry = activeRequests.lookupEntry(request);
        if (entry != -1) {
            final int newRefCount = activeRequests.getValueAt(entry) - 1;
            if (newRefCount == 0) {
                activeRequests.removeAt(entry);
                final SubscriptionRequest msg = msgHelp.removeModification(subscriptionId, request);
                final InFlightHolder holder = allocateHolder(request, ModificationAddRemove.REMOVE);
                inFlightListeners.put(msg.getMsgToken(), holder);
                messageSink.accept(msg);
            } else {
                activeRequests.putValueAt(entry, newRefCount);
            }
        }
    }

    void accept(final SubscriptionResponse response) {
        if (response.getResponseType().equals(ResponseType.RESPONSE_TYPE_MESSAGE)) {
            handleMessage(response);
        } else { // RESPONSE_TYPE_SCHEMA, RESPONSE_TYPE_REM, RESPONSE_TYPE_ADD, RESPONSE_TYPE_CHG
            decoder.accept(response);
        }
    }

    private void handleMessage(final SubscriptionResponse response) {
        final int inFlightEntry = inFlightListeners.lookupEntry(response.getRefMsgToken());
        if (inFlightEntry != -1) {
            final InFlightHolder holder = inFlightListeners.getValueAt(inFlightEntry);
            inFlightListeners.removeAt(inFlightEntry);
            holder.apply(response);
            returnHolder(holder);
        }
    }

    private InFlightHolder allocateHolder(
            final ModificationRequest request, final ModificationAddRemove addRemove) {
        final InFlightHolder holder = pool.isEmpty() ? new InFlightHolder() : pool.removeFirst();
        holder.set(request, addRemove);
        return holder;
    }

    private void returnHolder(final InFlightHolder holder) {
        holder.clear();
        pool.addLast(holder);
    }

    private final class InFlightHolder {
        private ModificationAddRemove addRemove;
        private ModificationRequest request;

        private void clear() {
            this.request = null;
            this.addRemove = null;
        }

        private void set(final ModificationRequest request, final ModificationAddRemove addRemove) {
            this.request = requireNonNull(request, "request");
            this.addRemove = requireNonNull(addRemove, "addRemove");
        }

        void apply(final SubscriptionResponse response) {
            if (addRemove == null || request == null) {
                throw new NullPointerException(
                        "Invalid state: missing addRemove or request referenced for response to a request");
            }
            final ModificationResponse listenerResponse = toModResponse(response.getResponse());
            if (addRemove.equals(ModificationAddRemove.REMOVE)) {
                listener.onModificationRemoveResponse(request, listenerResponse);
            } else {
                listener.onModificationAddResponse(request, listenerResponse);
            }
        }
    }

    private static ModificationResponse toModResponse(final Response response) {
        return new ModificationResponse(!response.getError(), response.getMessage(), null);
    }
}
