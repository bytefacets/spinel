// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats.kv;

import static com.bytefacets.spinel.nats.kv.BucketUtil.SCHEMA_ID_KEY;
import static com.bytefacets.spinel.nats.kv.BucketUtil.SCHEMA_PREFIX;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.GenericIntIndexedMap;
import com.bytefacets.collections.hash.IntGenericIndexedMap;
import com.bytefacets.spinel.grpc.proto.SchemaUpdate;
import com.google.protobuf.InvalidProtocolBufferException;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import jakarta.annotation.Nullable;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class SchemaRegistry {
    private static final Logger log = LoggerFactory.getLogger(SchemaRegistry.class);
    private final GenericIntIndexedMap<SchemaUpdate> schemas = new GenericIntIndexedMap<>(16);
    private final IntGenericIndexedMap<SchemaUpdate> schemasIdMap = new IntGenericIndexedMap<>(16);
    private final DistributedCounter schemaIdCounter;
    private final Latest latest = new Latest();
    private final KeyValue kv;

    SchemaRegistry(final KeyValue kv)
            throws JetStreamApiException, IOException, InterruptedException {
        this(kv, new DistributedCounter(kv, SCHEMA_ID_KEY));
    }

    SchemaRegistry(final KeyValue kv, final DistributedCounter counter) {
        this.kv = requireNonNull(kv, "kv");
        this.schemaIdCounter = requireNonNull(counter, "counter");
    }

    Latest latest() {
        return latest;
    }

    void apply(final KeyValueEntry entry) {
        try {
            collect(entry.getKey(), SchemaUpdate.parseFrom(entry.getValue()), entry.getRevision());
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    @Nullable
    SchemaUpdate lookup(final int schemaId) {
        return schemasIdMap.getOrDefault(schemaId, null);
    }

    void collect(final String key, final SchemaUpdate encodedSchema, final long revision) {
        final int schemaId = readSchemaId(key);
        if (schemaId != -1) {
            latest.updateIfNewer(schemaId, revision, encodedSchema);
            log.info("Found schema {} at {}", schemaId, key);
            schemas.put(encodedSchema, schemaId);
            schemasIdMap.put(schemaId, encodedSchema);
        }
    }

    int register(final SchemaUpdate encodedSchema) throws IOException {
        final int before = schemas.size();
        final int entry = schemas.add(encodedSchema);
        if (schemas.size() != before) {
            final int schemaId = schemaIdCounter.increment();
            schemas.putValueAt(entry, schemaId);
            schemasIdMap.put(schemaId, encodedSchema);
            try {
                final String key = SCHEMA_PREFIX + schemaId;
                kv.put(SCHEMA_PREFIX + schemaId, encodedSchema.toByteArray());
                log.info("Registered Schema {} at key: {}", schemaId, key);
                return schemaId;
            } catch (JetStreamApiException e) {
                throw new RuntimeException(e);
            }
        }
        return schemas.getValueAt(entry);
    }

    private static int readSchemaId(final String key) {
        if (key.length() > SCHEMA_PREFIX.length()) {
            final String idPart = key.substring(SCHEMA_PREFIX.length());
            try {
                return Integer.parseInt(idPart);
            } catch (NumberFormatException ex) {
                log.warn("Could not parse schema id from key: {}, id-part={}", key, idPart);
                return -1;
            }
        } else {
            log.warn("Could not parse schema id at key (not long enough): {}", key);
            return -1;
        }
    }

    static final class Latest {
        private int schemaId = -1;
        private long revision = Long.MIN_VALUE;
        private SchemaUpdate encodedSchema;

        SchemaUpdate encodedSchema() {
            return encodedSchema;
        }

        boolean isPresent() {
            return schemaId != -1;
        }

        int schemaId() {
            return schemaId;
        }

        private void updateIfNewer(
                final int schemaId, final long revision, final SchemaUpdate encodedSchema) {
            if (revision > this.revision) {
                this.schemaId = schemaId;
                this.revision = revision;
                this.encodedSchema = encodedSchema;
            }
        }
    }
}
