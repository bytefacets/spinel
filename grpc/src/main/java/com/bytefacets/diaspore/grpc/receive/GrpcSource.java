package com.bytefacets.diaspore.grpc.receive;

import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.TransformOutput;
import com.bytefacets.diaspore.comms.ConnectionHandle;
import com.bytefacets.diaspore.comms.ConnectionInfo;
import com.bytefacets.diaspore.comms.receive.ChangeDecoder;
import com.bytefacets.diaspore.grpc.proto.SubscriptionResponse;

public final class GrpcSource {
    private final Subscription subscription;
    private final ConnectionInfo connectionInfo;
    private final ConnectionHandle connectionHandle;

    GrpcSource(
            final ConnectionInfo connectionInfo,
            final ConnectionHandle connectionHandle,
            final Subscription subscription) {
        this.subscription = requireNonNull(subscription, "subscription");
        this.connectionInfo = requireNonNull(connectionInfo, "connectionInfo");
        this.connectionHandle = requireNonNull(connectionHandle, "connectionHandle");
    }

    public ConnectionInfo connectionInfo() {
        return connectionInfo;
    }

    public ConnectionHandle connectionHandle() {
        return connectionHandle;
    }

    ChangeDecoder<SubscriptionResponse> decoder() {
        return subscription.decoder();
    }

    public TransformOutput output() {
        return subscription.decoder().output();
    }
}
