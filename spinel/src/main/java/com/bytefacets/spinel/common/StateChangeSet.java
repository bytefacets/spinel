// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.common;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntConsumer;
import com.bytefacets.collections.hash.IntIndexedSet;
import com.bytefacets.spinel.schema.FieldBitSet;
import java.util.BitSet;
import javax.annotation.Nullable;

/**
 * Manages row changes for an operator, but is a little more sophisticated than {@link StateChange}.
 * This class has functionality which handles cases where a row change may occur multiple times
 * during processing, such as an aggregation where two inbound rows could result in an add, then a
 * change.
 */
public final class StateChangeSet {
    private final FieldBitSet changedFields;
    private final IntIndexedSet addedRows = new IntIndexedSet(16);
    private final IntIndexedSet changedRows = new IntIndexedSet(16);
    private final IntIndexedSet removedRows = new IntIndexedSet(16);

    public static StateChangeSet stateChangeSet(final BitSet changeSet) {
        return new StateChangeSet(changeSet);
    }

    public static StateChangeSet stateChangeSet(final FieldBitSet changeSet) {
        return new StateChangeSet(changeSet);
    }

    public static StateChangeSet stateChangeSet() {
        return new StateChangeSet(new BitSet());
    }

    private StateChangeSet(final BitSet changeSet) {
        changedFields = FieldBitSet.fieldBitSet(changeSet);
    }

    private StateChangeSet(final FieldBitSet changeSet) {
        changedFields = requireNonNull(changeSet, "changeSet");
    }

    /** Adds the row to the remove set, and removes from changed and added. */
    public void removeRow(final int row) {
        changedRows.remove(row);
        addedRows.remove(row);
        removedRows.add(row);
    }

    /** Adds the row to the added set, and removes from the removed set. */
    public void addRow(final int row) {
        addedRows.add(row);
        removedRows.remove(row);
    }

    /** Adds the row to the changed set. */
    public void changeRow(final int row) {
        changedRows.add(row);
    }

    /** Adds the row to the changed set if it is not already added. */
    public void changeRowIfNotAdded(final int row) {
        if (!addedRows.containsKey(row)) {
            changedRows.add(row);
        }
    }

    public void changeField(final int fieldId) {
        changedFields.fieldChanged(fieldId);
    }

    /**
     * Notifies of removed, then added, then changed, and if there were any removes, will call back
     * the removedRowConsumer before resetting.
     */
    public void fire(final InputNotifier manager, @Nullable final IntConsumer removedRowConsumer) {
        if (!removedRows.isEmpty()) {
            manager.notifyRemoves(removedRows);
        }
        if (!addedRows.isEmpty()) {
            manager.notifyAdds(addedRows);
        }
        if (!changedRows.isEmpty()) {
            manager.notifyChanges(changedRows, changedFields);
        }
        if (removedRowConsumer != null) {
            removedRows.forEach(removedRowConsumer);
        }
        reset();
    }

    private void reset() {
        removedRows.clear();
        addedRows.clear();
        changedRows.clear();
        changedFields.clear();
    }
}
