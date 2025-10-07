// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.vaadin.data;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntIntConsumer;
import com.bytefacets.collections.hash.IntIndexedSet;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.ui.Pager;
import java.util.function.Consumer;

/**
 * A light wrapper around {@link Pager} that manages the updating of added or removed rows in the
 * UI.
 */
final class UIThreadConsumer {
    private final Consumer<Schema> schemaConsumer;
    private final Pager pager;

    UIThreadConsumer(final Consumer<Schema> schemaConsumer, final Pager pager) {
        this.schemaConsumer = requireNonNull(schemaConsumer, "schemaConsumer");
        this.pager = requireNonNull(pager, "pager");
    }

    int rowCount() {
        return pager.size();
    }

    void updateSchema(final Schema schema) {
        schemaConsumer.accept(schema);
    }

    void rowsInRange(final int offset, final int limit, final IntIntConsumer consumer) {
        pager.rowsInRange(offset, limit, consumer);
    }

    void updateActiveRows(
            final IntIndexedSet addedRows, final IntIndexedSet removedRows, final boolean reset) {
        if (reset) {
            pager.clear();
        }
        pager.remove(removedRows);
        pager.add(addedRows);
    }
}
