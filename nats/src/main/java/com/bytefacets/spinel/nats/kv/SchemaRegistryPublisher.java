// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats.kv;

import static com.bytefacets.spinel.nats.kv.BucketUtil.SCHEMA_ID_KEY;
import static com.bytefacets.spinel.nats.kv.BucketUtil.SCHEMA_PREFIX;
import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.grpc.proto.SchemaUpdate;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class SchemaRegistryPublisher extends SchemaRegistry {
    private static final Logger log = LoggerFactory.getLogger(SchemaRegistryPublisher.class);
    private final DistributedCounter schemaIdCounter;
    private final KeyValue kv;

    SchemaRegistryPublisher(final KeyValue kv)
            throws JetStreamApiException, IOException, InterruptedException {
        this(kv, new DistributedCounter(kv, SCHEMA_ID_KEY));
    }

    SchemaRegistryPublisher(final KeyValue kv, final DistributedCounter counter) {
        this.kv = requireNonNull(kv, "kv");
        this.schemaIdCounter = requireNonNull(counter, "counter");
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
}
