// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.grpc.send;

import static com.bytefacets.diaspore.grpc.send.GrpcEncoder.grpcEncoder;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.diaspore.TransformInput;
import com.bytefacets.diaspore.TransformOutput;
import com.bytefacets.diaspore.comms.send.ChangeEncoder;
import com.bytefacets.diaspore.grpc.proto.SubscriptionResponse;
import com.bytefacets.diaspore.schema.ChangedFieldSet;
import com.bytefacets.diaspore.schema.Schema;
import com.bytefacets.diaspore.transform.InputProvider;
import io.grpc.stub.StreamObserver;
import javax.annotation.Nullable;

public final class GrpcSink implements InputProvider {
    private final Input input = new Input();
    private final ChangeEncoder<SubscriptionResponse> encoder;
    private final StreamObserver<SubscriptionResponse> streamOutput;
    private TransformOutput source;

    static GrpcSink grpcSink(
            final int subscriptionId, final StreamObserver<SubscriptionResponse> streamOutput) {
        return new GrpcSink(grpcEncoder(subscriptionId), streamOutput);
    }

    private GrpcSink(
            final ChangeEncoder<SubscriptionResponse> encoder,
            final StreamObserver<SubscriptionResponse> streamOutput) {
        this.encoder = requireNonNull(encoder, "encoder");
        this.streamOutput = requireNonNull(streamOutput, "streamOutput");
    }

    @Override
    public TransformInput input() {
        return input;
    }

    public void close() {
        if (source != null) {
            source.detachInput(input);
        }
    }

    private class Input implements TransformInput {
        @Override
        public void setSource(@Nullable final TransformOutput output) {
            source = output;
        }

        @Override
        public void schemaUpdated(@Nullable final Schema schema) {
            streamOutput.onNext(encoder.encodeSchema(schema));
        }

        @Override
        public void rowsAdded(final IntIterable rows) {
            streamOutput.onNext(encoder.encodeAdd(rows));
        }

        @Override
        public void rowsChanged(final IntIterable rows, final ChangedFieldSet changedFields) {
            streamOutput.onNext(encoder.encodeChange(rows, changedFields));
        }

        @Override
        public void rowsRemoved(final IntIterable rows) {
            streamOutput.onNext(encoder.encodeRemove(rows));
        }
    }
}
