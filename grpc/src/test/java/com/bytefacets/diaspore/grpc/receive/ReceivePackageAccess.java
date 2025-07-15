package com.bytefacets.diaspore.grpc.receive;

import static com.bytefacets.diaspore.schema.MatrixStoreFieldFactory.matrixStoreFieldFactory;

import com.bytefacets.diaspore.TransformOutput;
import com.bytefacets.diaspore.comms.receive.ChangeDecoder;
import com.bytefacets.diaspore.grpc.proto.SubscriptionResponse;
import com.bytefacets.diaspore.transform.OutputProvider;
import com.google.protobuf.ByteString;

public final class ReceivePackageAccess {
    private ReceivePackageAccess() {}

    public static DecoderAccess decoder() {
        return new DecoderAccess(
                GrpcDecoder.grpcDecoder(
                        new SchemaBuilder(matrixStoreFieldFactory(32, 32, i -> {}))));
    }

    public static Object decode(final ByteString value) {
        return ObjectDecoderRegistry.decode(value);
    }

    public static class DecoderAccess
            implements ChangeDecoder<SubscriptionResponse>, OutputProvider {
        private final GrpcDecoder decoder;

        public DecoderAccess(final GrpcDecoder decoder) {
            this.decoder = decoder;
        }

        @Override
        public void accept(final SubscriptionResponse message) {
            decoder.accept(message);
        }

        @Override
        public TransformOutput output() {
            return decoder.output();
        }
    }
}
