// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats.kv;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueConfiguration;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BucketUtil {
    private static final Logger log = LoggerFactory.getLogger(BucketUtil.class);
    static final String SCHEMA_ID_KEY = "__schema_id";
    static final String SCHEMA_PREFIX = "__s.";
    static final String DATA_PREFIX = "__d.";

    private BucketUtil() {}

    /**
     * Tries to create a bucket with the provided configuration, but swallows any
     * JetStreamApiException assuming that it means the bucket is already created. Throws
     * RuntimeExceptions if there is any IOException.
     */
    public static KeyValue getOrCreateBucket(
            final Connection connection, final KeyValueConfiguration kvConfig) {
        try {
            final var status = connection.keyValueManagement().create(kvConfig);
            log.info("Create Bucket {} result: {}", kvConfig.getBucketName(), status);
        } catch (JetStreamApiException ex) {
            log.info(
                    "Error trying to create bucket {}: {}",
                    kvConfig.getBucketName(),
                    ex.getMessage());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            return connection.keyValue(kvConfig.getBucketName());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
