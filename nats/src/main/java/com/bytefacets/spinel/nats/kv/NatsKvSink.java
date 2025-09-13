// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats.kv;

import static com.bytefacets.spinel.nats.kv.BucketUtil.DATA_PREFIX;
import static com.bytefacets.spinel.schema.SchemaFieldResolver.schemaFieldResolver;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.collections.store.StringChunkStore;
import com.bytefacets.spinel.TransformInput;
import com.bytefacets.spinel.grpc.proto.SchemaUpdate;
import com.bytefacets.spinel.grpc.send.GrpcEncoder;
import com.bytefacets.spinel.nats.NatsSubjectBuilder;
import com.bytefacets.spinel.schema.ChangedFieldSet;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.SchemaFieldResolver;
import com.bytefacets.spinel.transform.InputProvider;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.BitSet;
import java.util.Objects;

public final class NatsKvSink implements InputProvider {
    private static final GrpcEncoder SCHEMA_ENCODER = GrpcEncoder.grpcEncoder(-1);
    private final Input input;

    NatsKvSink(
            final KeyValue kv,
            final NatsSubjectBuilder subjBuilder,
            final StringChunkStore keyStore,
            final SchemaRegistry schemaRegistry) {
        this.input = new Input(kv, subjBuilder, keyStore, schemaRegistry);
    }

    @Override
    public TransformInput input() {
        return input;
    }

    private static final class Input implements TransformInput {
        private final KeyValue kv;
        private final NatsSubjectBuilder subjBuilder;
        private final StringChunkStore keyStore;
        private final BitSet subjectDependencies = new BitSet();
        private final SchemaFieldResolver fieldResolver =
                schemaFieldResolver(subjectDependencies::set);
        private final SchemaRegistry schemaRegistry;
        private final BucketEncoder valueEncoder = new BucketEncoder();

        Input(
                final KeyValue kv,
                final NatsSubjectBuilder subjBuilder,
                final StringChunkStore keyStore,
                final SchemaRegistry schemaRegistry) {
            this.subjBuilder = requireNonNull(subjBuilder, "subjBuilder");
            this.kv = requireNonNull(kv, "kv");
            this.keyStore = requireNonNull(keyStore, "keyStore");
            this.schemaRegistry = requireNonNull(schemaRegistry, "schemaRegistry");
        }

        @Override
        public void schemaUpdated(@Nullable final Schema schema) {
            if (schema != null) {
                manageSubjectBuilder(schema);
                publishSchema(schema);
            }
        }

        private void manageSubjectBuilder(final Schema schema) {
            subjectDependencies.clear();
            fieldResolver.setSchema(schema);
            subjBuilder.bindToSchema(fieldResolver);
        }

        private void publishSchema(final Schema schema) {
            final SchemaUpdate encodedSchema = SCHEMA_ENCODER.encodeSchema(schema).getSchema();
            final int schemaId;
            try {
                schemaId = schemaRegistry.register(encodedSchema);
            } catch (IOException e) {
                // REVISIT - will need to resend everything on a reconnect
                throw new RuntimeException(e);
            }
            valueEncoder.setSchema(schemaId, schema);
        }

        @Override
        public void rowsAdded(final IntIterable rows) {
            rows.forEach(this::add);
        }

        @Override
        public void rowsChanged(final IntIterable rows, final ChangedFieldSet changedFields) {
            if (changedFields.intersects(subjectDependencies)) {
                rows.forEach(this::changeWithSubjectRebuild);
            } else {
                rows.forEach(this::changeWithNoKeyChange);
            }
        }

        @Override
        public void rowsRemoved(final IntIterable rows) {
            rows.forEach(this::remove);
        }

        private void add(final int row) {
            final String key = subjBuilder.buildSubject(row);
            keyStore.setString(row, key);
            set(key, row);
        }

        private void changeWithSubjectRebuild(final int row) {
            final String newKey = subjBuilder.buildSubject(row);
            final String oldKey = keyStore.getString(row);
            if (Objects.equals(newKey, oldKey)) {
                set(oldKey, row);
            } else {
                remove(row);
                add(row);
            }
        }

        private void changeWithNoKeyChange(final int row) {
            set(keyStore.getString(row), row);
        }

        private void remove(final int row) {
            final String key = keyStore.getString(row);
            keyStore.setString(row, null);
            try {
                kv.delete(DATA_PREFIX + key);
            } catch (IOException | JetStreamApiException e) {
                // DISCUSSION - are there acceptable JetStream exceptions?
                // REVISIT - need to buffer if disconnected
                throw new RuntimeException(e);
            }
        }

        private void set(final String key, final int row) {
            try {
                kv.put(DATA_PREFIX + key, valueEncoder.encode(row));
            } catch (IOException | JetStreamApiException e) {
                // DISCUSSION - are there acceptable JetStream exceptions?
                // REVISIT - need to buffer if disconnected
                throw new RuntimeException(e);
            }
        }
    }
}
