// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats.kv;

import static com.bytefacets.spinel.common.BitSetRowProvider.bitSetRowProvider;
import static com.bytefacets.spinel.common.OutputManager.outputManager;
import static com.bytefacets.spinel.common.StateChangeSet.stateChangeSet;
import static com.bytefacets.spinel.nats.kv.BucketUtil.DATA_PREFIX;
import static com.bytefacets.spinel.nats.kv.BucketUtil.SCHEMA_PREFIX;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.StringGenericIndexedMap;
import com.bytefacets.collections.hash.StringIndexedSet;
import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.common.OutputManager;
import com.bytefacets.spinel.common.StateChangeSet;
import com.bytefacets.spinel.grpc.receive.SchemaBuilder;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.transform.OutputProvider;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import io.netty.channel.EventLoop;
import java.io.IOException;
import java.time.Duration;
import java.util.BitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Works in conjunction with {@link NatsKvSink} to unpack and represent structured data stored and
 * published in a NATS KeyValue bucket.
 *
 * <p>Key-Value pairs are published by the NatsKvSink and are picked up in the NatsKvSource.
 *
 * <p>There is a measure of schema evolution possible, according to what can be reasonably expected
 * to still produce accurate results. For example, if there is a NatsKvSource that is running and a
 * field is defined as a long, and a publisher comes in and starts publishing it as an int, the
 * NatsKvSource can withstand that because the int can be cast to a long. These type conversions are
 * handled by the {@link FieldReader}.
 *
 * <p>Data is buffered internally until the KeyValueWatcher is signaled with endOfData(). This is so
 * that the full set of possible schemas is received.
 *
 * <p>Also note that data is delivered on the provided EventLoop and not directly on the NATS
 * connection thread.
 */
public final class NatsKvSource implements OutputProvider {
    private static final Logger log = LoggerFactory.getLogger(NatsKvSource.class);
    private final KeyValue keyValueBucket;
    private final BitSet activeRows = new BitSet();
    private final BitSet changedFields = new BitSet();
    private final OutputManager outputManager = outputManager(bitSetRowProvider(activeRows));
    private final SchemaRegistry schemaRegistry;
    private final KvDataQueue dataQueue;
    private final BucketDecoder decoder;
    private final StringIndexedSet keys;
    private final StateChangeSet changeSet = stateChangeSet(changedFields);

    NatsKvSource(
            final KeyValue keyValueBucket,
            final EventLoop eventLoop,
            final SchemaBuilder schemaBuilder,
            final SchemaRegistry schemaRegistry,
            final Duration changeBudget,
            final int initialKeyCapacity) {
        this.keyValueBucket = requireNonNull(keyValueBucket, "keyValueBucket");
        this.schemaRegistry = requireNonNull(schemaRegistry, "schemaRegistry");
        this.decoder = new BucketDecoder(changedFields, schemaBuilder, schemaRegistry);
        this.dataQueue = new KvDataQueue(eventLoop, new Listener(), changeBudget, System::nanoTime);
        this.keys = new StringIndexedSet(initialKeyCapacity);
    }

    public void open() throws JetStreamApiException, IOException, InterruptedException {
        requireNonNull(keyValueBucket, "keyValueBucket").watchAll(dataQueue.kvWatcher());
    }

    @Override
    public TransformOutput output() {
        return outputManager.output();
    }

    private void updated(final KeyValueEntry entry) {
        final int before = keys.size();
        final int row = keys.add(entry.getKey());
        final boolean isAdd = before != keys.size();
        decoder.write(isAdd, row, entry.getValue());
        if (isAdd) {
            activeRows.set(row);
            changeSet.addRow(row);
        } else {
            changeSet.changeRowIfNotAdded(row);
        }
    }

    private void deleted(final String key) {
        final int row = keys.lookupEntry(key);
        if (row != -1) {
            activeRows.clear(row);
            keys.removeAtAndReserve(row);
            changeSet.removeRow(row);
        }
    }

    private final class Listener implements KvDataQueue.Listener {
        private final StringGenericIndexedMap<KeyValueEntry> buffer =
                new StringGenericIndexedMap<>(128);
        private boolean caughtUp = false;

        @Override
        public void update(final KeyValueEntry entry) {
            if (entry.getKey().startsWith(SCHEMA_PREFIX)) {
                schemaRegistry.apply(entry);
            } else if (entry.getKey().startsWith(DATA_PREFIX)) {
                if (caughtUp) {
                    updated(entry);
                } else {
                    buffer.put(entry.getKey(), entry);
                }
            }
        }

        @Override
        public void delete(final KeyValueEntry entry) {
            if (caughtUp) {
                deleted(entry.getKey());
            } else {
                buffer.remove(entry.getKey());
            }
        }

        @Override
        public void fireChanges() {
            changeSet.fire(outputManager, keys::freeReservedEntry);
        }

        @Override
        public void caughtUp() {
            log.info("Caught up");
            final var latest = schemaRegistry.latest();
            final Schema schema = decoder.setSchema(latest.schemaId(), latest.encodedSchema());
            caughtUp = true;
            outputManager.updateSchema(schema);
            drainBuffer();
        }

        private void drainBuffer() {
            buffer.forEachEntry(e -> updated(buffer.getValueAt(e)));
            buffer.clear();
            fireChanges();
        }
    }
}
