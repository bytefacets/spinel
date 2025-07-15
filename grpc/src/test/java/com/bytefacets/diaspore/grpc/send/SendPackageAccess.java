package com.bytefacets.diaspore.grpc.send;

import com.bytefacets.diaspore.grpc.proto.SubscriptionResponse;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;

public final class SendPackageAccess {
    private final ObjectEncoderImpl encoder = new ObjectEncoderImpl();

    public ByteString encode(final Object value) {
        return encoder.encode(value);
    }

    public GrpcSink sink(final int token, final StreamObserver<SubscriptionResponse> stream) {
        return GrpcSink.grpcSink(token, stream);
    }
}
