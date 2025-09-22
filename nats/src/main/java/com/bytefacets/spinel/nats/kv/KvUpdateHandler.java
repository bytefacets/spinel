// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats.kv;

import com.bytefacets.spinel.schema.Schema;
import io.nats.client.api.KeyValueEntry;

public interface KvUpdateHandler {
    /**
     * Allow the implementation to access the schema fields directly, so it can access the
     * Writable*Field instances
     */
    void bindToSchema(Schema schema);

    /**
     * A callback for updated entries. The row will be determined by the adapter.
     *
     * @param row the row into which data should be written to the fields
     * @param entry the entry that was updated
     */
    void updated(int row, KeyValueEntry entry);

    /** Callback for endOfData on the KeyValue */
    default void caughtUp() {}

    /** Callback for deleted or purged entries */
    default void deleted(int row, KeyValueEntry entry) {}

    /** Callback in the event the key update that was received was not known to the adapter */
    default void unknownDeletedEntry(KeyValueEntry entry) {}
}
