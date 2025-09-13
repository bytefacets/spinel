// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats.kv;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.store.StringChunkStore;
import com.bytefacets.spinel.nats.NatsSubjectBuilder;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueConfiguration;
import java.io.IOException;

public final class NatsKvSinkBuilder {
    private KeyValue keyValueBucket;
    private int initialSize = 128;
    private int chunkSize = 128;
    private NatsSubjectBuilder subjectBuilder;

    private NatsKvSinkBuilder() {}

    public static NatsKvSinkBuilder natsKvSink() {
        return new NatsKvSinkBuilder();
    }

    public NatsKvSinkBuilder keyValueBucket(final KeyValue keyValue) {
        this.keyValueBucket = requireNonNull(keyValue, "keyValue");
        return this;
    }

    public NatsKvSinkBuilder keyValueBucket(
            final Connection connection, final KeyValueConfiguration config) {
        return keyValueBucket(BucketUtil.getOrCreateBucket(connection, config));
    }

    public NatsKvSinkBuilder keyValueBucket(final Connection connection, final String name) {
        return keyValueBucket(connection, KeyValueConfiguration.builder().name(name).build());
    }

    public NatsKvSinkBuilder initialSize(final int initialSize) {
        this.initialSize = initialSize;
        return this;
    }

    public NatsKvSinkBuilder chunkSize(final int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    public NatsKvSinkBuilder subjectBuilder(final NatsSubjectBuilder subjectBuilder) {
        this.subjectBuilder = requireNonNull(subjectBuilder, "subjectBuilder");
        return this;
    }

    public NatsKvSink build() throws JetStreamApiException, IOException, InterruptedException {
        return new NatsKvSink(
                keyValueBucket,
                subjectBuilder,
                new StringChunkStore(initialSize, chunkSize),
                new SchemaRegistry(keyValueBucket));
    }
}
