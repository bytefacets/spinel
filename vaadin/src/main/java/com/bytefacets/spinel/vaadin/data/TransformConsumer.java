// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.vaadin.data;

import static com.bytefacets.spinel.cache.CacheBuilder.cache;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.collections.hash.IntIndexedSet;
import com.bytefacets.spinel.TransformInput;
import com.bytefacets.spinel.cache.Cache;
import com.bytefacets.spinel.schema.ChangedFieldSet;
import com.bytefacets.spinel.schema.Schema;
import jakarta.annotation.Nullable;

/**
 * Consumes events from a TransformOutput and copies the data into a Cache, and collects added and
 * removed rows until it is asked to report that row additions and deletions to the {@link
 * UIThreadConsumer}. Currently, the data is copied into a private cache on the event loop thread
 * which is read potentially at the same time while the UI thread is accessing it. This is a lot
 * more safe than trying to traverse back up the transform graph from another thread. What is
 * guarded carefully, however, is the active row set, since that will definitely cause problems if
 * the event loop changes that while the UI is doing something.
 */
final class TransformConsumer implements TransformInput {
    private final Object lock = new Object();
    private final IntIndexedSet pendingRemove = new IntIndexedSet(16);
    private final IntIndexedSet pendingAdd = new IntIndexedSet(16);
    private final UIThreadConsumer uiThreadConsumer;
    private Schema pendingSchema;
    private Cache cache;
    private boolean reset;

    TransformConsumer(final UIThreadConsumer uiThreadConsumer) {
        this.uiThreadConsumer = requireNonNull(uiThreadConsumer, "uiThreadConsumer");
    }

    @Override
    public void schemaUpdated(@Nullable final Schema schema) {
        synchronized (lock) {
            reset = true;
            pendingAdd.clear();
            pendingRemove.clear();
            if (schema != null) {
                cache =
                        cache().cacheFields(allFields(schema))
                                .chunkSize(64)
                                .initialSize(64)
                                .build();
                cache.bind(schema);
                pendingSchema = cache.schema();
            } else if (cache != null) {
                cache.unbind();
                pendingSchema = null;
            }
        }
    }

    void applyOnUiThread() {
        synchronized (lock) {
            if (pendingSchema != null) {
                uiThreadConsumer.updateSchema(pendingSchema);
                pendingSchema = null;
            }
            uiThreadConsumer.updateActiveRows(pendingAdd, pendingRemove, reset);
            pendingAdd.clear();
            pendingRemove.clear();
            reset = false;
        }
    }

    private void internalAdd(final int row) {
        // only add to pendingAdds if it was NOT in the pendingRemoves
        final boolean wasGoingToBeRemoved = pendingRemove.remove(row) != -1;
        if (!wasGoingToBeRemoved) {
            pendingAdd.add(row);
        }
        // else we will leave mapped in the UI - it's a no-op for the row
    }

    private void internalRemove(final int row) {
        // only add to pendingRemoves if it was NOT in the pendingAdds
        final boolean wasGoingToBeAdded = pendingAdd.remove(row) != -1;
        if (!wasGoingToBeAdded) {
            pendingRemove.add(row);
        } // else we will leave mapped in the UI - it's a no-op for the row
    }

    @Override
    public void rowsAdded(final IntIterable rows) {
        synchronized (lock) {
            cache.updateAll(rows);
            rows.forEach(this::internalAdd);
        }
    }

    @Override
    public void rowsChanged(final IntIterable rows, final ChangedFieldSet changedFields) {
        synchronized (lock) {
            cache.updateSelected(rows, changedFields); // fieldIds should be aligned
        }
    }

    @Override
    public void rowsRemoved(final IntIterable rows) {
        synchronized (lock) {
            rows.forEach(this::internalRemove);
        }
    }

    private static String[] allFields(final Schema schema) {
        final String[] names = new String[schema.size()];
        for (int i = 0, size = names.length; i < size; i++) {
            names[i] = schema.fieldAt(i).name();
        }
        return names;
    }
}
