// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats.kv;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.types.IntType;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueOperation;
import io.nats.client.api.KeyValueWatcher;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages a distributed counter in a NATS KeyValue Bucket. */
public final class DistributedCounter {
    private static final Logger log = LoggerFactory.getLogger(DistributedCounter.class);
    private final KeyValue kv;
    private final String key;
    private int latestValue;
    private long latestRevision;

    public static DistributedCounter counter(final KeyValue kv, final String key)
            throws JetStreamApiException, IOException, InterruptedException {
        return new DistributedCounter(kv, key);
    }

    @SuppressWarnings("resource")
    DistributedCounter(final KeyValue kv, final String key)
            throws JetStreamApiException, IOException, InterruptedException {
        this.kv = requireNonNull(kv, "kv");
        this.key = requireNonNull(key, "key");
        kv.watch(key, new Watcher());
    }

    public int currentValue() {
        return latestValue;
    }

    /** Increments the value at the key. */
    public int increment() throws IOException {
        final byte[] data = new byte[4];
        while (true) {
            final int next = latestValue + 1;
            IntType.writeLE(data, 0, next);
            try {
                synchronized (key) {
                    latestRevision = kv.update(key, data, latestRevision);
                    latestValue = next;
                }
                return next;
            } catch (JetStreamApiException e) {
                if (e.getApiErrorCode() != 10071) {
                    throw new RuntimeException(e);
                }
                // DISCUSS - should we just be in a tight loop waiting for the watcher?
                // perhaps we need to check if we're on the same thread as NATS bc if we are
                // we may not get out
            }
        }
    }

    private final class Watcher implements KeyValueWatcher {
        @Override
        public void watch(final KeyValueEntry entry) {
            synchronized (key) {
                if (entry.getRevision() > latestRevision) {
                    latestRevision = entry.getRevision();
                    if (entry.getOperation().equals(KeyValueOperation.PUT)
                            && entry.getValue() != null) {
                        latestValue = IntType.readLE(entry.getValue(), 0);
                    } else {
                        latestValue = 0;
                    }
                }
            }
        }

        @Override
        public void endOfData() {
            synchronized (key) {
                if (latestRevision == 0) {
                    try {
                        latestRevision = kv.put(key, 0);
                        log.info(
                                "Initialized {}/{} to 0 at {}",
                                kv.getBucketName(),
                                key,
                                latestRevision);
                        // DISCUSS what is the best approach here?
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } catch (JetStreamApiException e) {
                        log.warn("Exception initializing DistributedCounter[{}]", key, e);
                    }
                }
            }
            log.info("Initialized {}/{} to 0 at {}", kv.getBucketName(), key, latestRevision);
        }
    }

    // VisibleForTesting
    long revision() {
        return latestRevision;
    }
}
