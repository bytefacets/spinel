// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.send;

import com.bytefacets.spinel.grpc.proto.SubscriptionResponse;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.util.function.IntSupplier;

public final class SendPackageAccess {
    private final ObjectEncoderImpl encoder = new ObjectEncoderImpl();

    public ByteString encode(final Object value) {
        return encoder.encode(value);
    }

    public GrpcSink sink(
            final int subscriptionId,
            final IntSupplier tokenSupplier,
            final StreamObserver<SubscriptionResponse> stream) {
        return GrpcSink.grpcSink(subscriptionId, stream);
    }

    public GrpcEncoder encoder(final int subscriptionId) {
        return GrpcEncoder.grpcEncoder(subscriptionId);
    }
}
