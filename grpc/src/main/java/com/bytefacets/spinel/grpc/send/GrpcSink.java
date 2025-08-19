// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.send;

import static com.bytefacets.spinel.grpc.send.GrpcEncoder.grpcEncoder;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.spinel.TransformInput;
import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.comms.send.ChangeEncoder;
import com.bytefacets.spinel.grpc.proto.SubscriptionResponse;
import com.bytefacets.spinel.schema.ChangedFieldSet;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.transform.InputProvider;
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
