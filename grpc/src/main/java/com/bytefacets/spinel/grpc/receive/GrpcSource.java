// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.receive;

import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.comms.ConnectionHandle;
import com.bytefacets.spinel.comms.ConnectionInfo;
import com.bytefacets.spinel.comms.receive.ChangeDecoder;
import com.bytefacets.spinel.comms.receive.SubscriptionHandle;
import com.bytefacets.spinel.comms.subscription.ModificationRequest;
import com.bytefacets.spinel.grpc.proto.SubscriptionResponse;
import com.bytefacets.spinel.transform.OutputProvider;

public final class GrpcSource implements OutputProvider {
    private final Subscription subscription;
    private final ConnectionInfo connectionInfo;
    private final ConnectionHandle connectionHandle;
    private final Control control;

    GrpcSource(
            final ConnectionInfo connectionInfo,
            final ConnectionHandle connectionHandle,
            final Subscription subscription) {
        this.subscription = requireNonNull(subscription, "subscription");
        this.connectionInfo = requireNonNull(connectionInfo, "connectionInfo");
        this.connectionHandle = requireNonNull(connectionHandle, "connectionHandle");
        this.control = new Control();
    }

    public ConnectionInfo connectionInfo() {
        return connectionInfo;
    }

    public ConnectionHandle connectionHandle() {
        return connectionHandle;
    }

    public SubscriptionHandle subscriptionHandle() {
        return control;
    }

    ChangeDecoder<SubscriptionResponse> decoder() {
        return subscription.decoder();
    }

    @Override
    public TransformOutput output() {
        return subscription.decoder().output();
    }

    // REVISIT: get onto event-loop
    private final class Control implements SubscriptionHandle {
        @Override
        public void add(final ModificationRequest request) {
            subscription.add(request);
        }

        @Override
        public void remove(final ModificationRequest request) {
            subscription.remove(request);
        }
    }
}
