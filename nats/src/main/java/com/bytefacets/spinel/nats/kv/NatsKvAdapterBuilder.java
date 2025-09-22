// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats.kv;

import static com.bytefacets.spinel.common.DefaultNameSupplier.resolveName;
import static com.bytefacets.spinel.schema.MatrixStoreFieldFactory.matrixStoreFieldFactory;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.StringGenericIndexedMap;
import com.bytefacets.spinel.common.StateChangeSet;
import com.bytefacets.spinel.schema.FieldDescriptor;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.SchemaField;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueConfiguration;
import io.netty.channel.EventLoop;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @see NatsKvAdapter
 */
public final class NatsKvAdapterBuilder {
    private final Map<Byte, List<FieldDescriptor>> typeMap = new HashMap<>();
    private final StringGenericIndexedMap<SchemaField> fieldMap = new StringGenericIndexedMap<>(16);
    private final String name;
    private KeyValue keyValueBucket;
    private int initialSize = 128;
    private int chunkSize = 128;
    private KvUpdateHandler updateHandler;
    private EventLoop eventLoop;

    private NatsKvAdapterBuilder(final String name) {
        this.name = requireNonNull(name, "name");
    }

    public static NatsKvAdapterBuilder natsKvAdapter() {
        return natsKvAdapter(null);
    }

    public static NatsKvAdapterBuilder natsKvAdapter(final String name) {
        return new NatsKvAdapterBuilder(resolveName("NatsKvAdapter", name));
    }

    public NatsKvAdapterBuilder keyValueBucket(final KeyValue keyValue) {
        this.keyValueBucket = requireNonNull(keyValue, "keyValue");
        return this;
    }

    public NatsKvAdapterBuilder keyValueBucket(
            final Connection connection, final KeyValueConfiguration config) {
        return keyValueBucket(BucketUtil.getOrCreateBucket(connection, config));
    }

    public NatsKvAdapterBuilder updateHandler(final KvUpdateHandler updateHandler) {
        this.updateHandler = requireNonNull(updateHandler, "updateHandler");
        return this;
    }

    public NatsKvAdapterBuilder addField(final FieldDescriptor field) {
        if (fieldMap.containsKey(field.name())) {
            throw new IllegalArgumentException("Duplicate field name: " + field.name());
        }
        fieldMap.add(field.name());
        typeMap.computeIfAbsent(field.fieldType(), t -> new ArrayList<>(4)).add(field);
        return this;
    }

    public NatsKvAdapterBuilder addFields(final FieldDescriptor... field) {
        for (FieldDescriptor f : field) {
            addField(f);
        }
        return this;
    }

    public NatsKvAdapterBuilder keyValueBucket(final Connection connection, final String name) {
        return keyValueBucket(connection, KeyValueConfiguration.builder().name(name).build());
    }

    public NatsKvAdapterBuilder initialSize(final int initialSize) {
        this.initialSize = initialSize;
        return this;
    }

    public NatsKvAdapterBuilder chunkSize(final int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    public NatsKvAdapterBuilder eventLoop(final EventLoop eventLoop) {
        this.eventLoop = eventLoop;
        return this;
    }

    public NatsKvAdapter build() throws JetStreamApiException, IOException, InterruptedException {
        final StringGenericIndexedMap<SchemaField> copy =
                new StringGenericIndexedMap<>(fieldMap.size());
        copy.copyFrom(fieldMap);
        final StateChangeSet stateChangeSet = StateChangeSet.stateChangeSet();
        final Schema schema =
                Schema.schema(
                        name,
                        matrixStoreFieldFactory(initialSize, chunkSize, stateChangeSet::changeField)
                                .createFieldList(copy, typeMap));
        return new NatsKvAdapter(
                keyValueBucket,
                eventLoop,
                schema,
                stateChangeSet,
                updateHandler,
                Duration.ofMillis(10),
                initialSize);
    }
}
