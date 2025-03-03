// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.common;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntConsumer;
import com.bytefacets.collections.hash.IntIndexedSet;
import com.bytefacets.diaspore.schema.FieldBitSet;
import java.util.BitSet;
import javax.annotation.Nullable;

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

    public void removeRow(final int row) {
        changedRows.remove(row);
        addedRows.remove(row);
        removedRows.add(row);
    }

    public void addRow(final int row) {
        addedRows.add(row);
        removedRows.remove(row);
    }

    public void changeRow(final int row) {
        changedRows.add(row);
    }

    public void changeRowIfNotAdded(final int row) {
        if (!addedRows.containsKey(row)) {
            changedRows.add(row);
        }
    }

    public void changeField(final int fieldId) {
        changedFields.fieldChanged(fieldId);
    }

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
