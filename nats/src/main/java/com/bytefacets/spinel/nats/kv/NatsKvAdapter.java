// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats.kv;

import static com.bytefacets.spinel.common.BitSetRowProvider.bitSetRowProvider;
import static com.bytefacets.spinel.common.OutputManager.outputManager;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.StringIndexedSet;
import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.common.OutputManager;
import com.bytefacets.spinel.common.StateChangeSet;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.transform.OutputProvider;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueWatcher;
import io.netty.channel.EventLoop;
import java.time.Duration;
import java.util.BitSet;

/**
 * An adapter to an arbitrary NATS KeyValue bucket that may or many not have Spinel-encoded data. By
 * providing a handler to manage the value decoding into fields, you have more flexibility around
 * what the data is within the buckets. One of the trade-offs is you have to manage the schema more
 * specifically.
 *
 * <p>Another difference from the NatsKvSource is that the NatsKvAdapter does not buffer anything
 * before the `endOfData` indication, because that buffering in the NatsKvSource is meant to
 * accommodate schema management.
 *
 * <p>Note that data is delivered on the provided EventLoop and not directly on the NATS connection
 * thread.
 */
public final class NatsKvAdapter implements OutputProvider {
    private final BitSet activeRows = new BitSet();
    private final OutputManager outputManager = outputManager(bitSetRowProvider(activeRows));
    private final KvDataQueue dataQueue;
    private final StringIndexedSet keys;
    private final StateChangeSet changeSet;
    private final KvUpdateHandler updateHandler;

    NatsKvAdapter(
            final EventLoop eventLoop,
            final Schema schema,
            final StateChangeSet changeSet,
            final KvUpdateHandler updateHandler,
            final Duration changeBudget,
            final int initialKeyCapacity) {
        this.dataQueue = new KvDataQueue(eventLoop, new Listener(), changeBudget, System::nanoTime);
        this.keys = new StringIndexedSet(initialKeyCapacity);
        this.changeSet = requireNonNull(changeSet, "changeSet");
        this.updateHandler = requireNonNull(updateHandler, "updateHandler");
        updateHandler.bindToSchema(schema);
        outputManager.updateSchema(schema);
    }

    public KeyValueWatcher keyValueWatcher() {
        return dataQueue.kvWatcher();
    }

    @Override
    public TransformOutput output() {
        return outputManager.output();
    }

    private void updated(final KeyValueEntry entry) {
        final int before = keys.size();
        final int row = keys.add(entry.getKey());
        final boolean isAdd = before != keys.size();
        changeSet.recordFieldChanges(!isAdd);
        updateHandler.updated(row, entry);
        if (isAdd) {
            activeRows.set(row);
            changeSet.addRow(row);
        } else {
            changeSet.changeRowIfNotAdded(row);
        }
    }

    private void deleted(final KeyValueEntry entry) {
        final String key = entry.getKey();
        final int row = keys.lookupEntry(key);
        if (row != -1) {
            updateHandler.deleted(row, entry);
            activeRows.clear(row);
            keys.removeAtAndReserve(row);
            changeSet.removeRow(row);
        } else {
            updateHandler.unknownDeletedEntry(entry);
        }
    }

    private final class Listener implements KvDataQueue.Listener {
        @Override
        public void update(final KeyValueEntry entry) {
            updated(entry);
        }

        @Override
        public void delete(final KeyValueEntry entry) {
            deleted(entry);
        }

        @Override
        public void fireChanges() {
            changeSet.fire(outputManager, keys::freeReservedEntry);
        }

        @Override
        public void caughtUp() {
            updateHandler.caughtUp();
        }
    }
}
