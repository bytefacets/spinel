// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.receive;

import static com.bytefacets.spinel.schema.MatrixStoreFieldFactory.matrixStoreFieldFactory;

import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.comms.receive.ChangeDecoder;
import com.bytefacets.spinel.grpc.proto.SubscriptionResponse;
import com.bytefacets.spinel.transform.OutputProvider;
import com.google.protobuf.ByteString;

public final class ReceivePackageAccess {
    private ReceivePackageAccess() {}

    public static DecoderAccess decoder() {
        return new DecoderAccess(grpcDecoder());
    }

    static GrpcDecoder grpcDecoder() {
        return GrpcDecoder.grpcDecoder(new SchemaBuilder(matrixStoreFieldFactory(32, 32, i -> {})));
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
