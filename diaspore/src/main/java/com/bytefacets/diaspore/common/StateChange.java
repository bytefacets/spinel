// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.common;

import com.bytefacets.collections.functional.IntConsumer;
import com.bytefacets.collections.vector.IntVector;
import com.bytefacets.diaspore.schema.FieldBitSet;
import java.util.BitSet;
import javax.annotation.Nullable;

public final class StateChange {
    private final FieldBitSet changedFields;
    private final IntVector addedRows = new IntVector(16);
    private final IntVector changedRows = new IntVector(16);
    private final IntVector removedRows = new IntVector(16);

    public static StateChange stateChange(final BitSet changeSet) {
        return new StateChange(changeSet);
    }

    public static StateChange stateChange() {
        return new StateChange(new BitSet());
    }

    private StateChange(final BitSet changeSet) {
        changedFields = FieldBitSet.fieldBitSet(changeSet);
    }

    public void removeRow(final int row) {
        removedRows.append(row);
    }

    public void addRow(final int row) {
        addedRows.append(row);
    }

    public void changeRow(final int row) {
        changedRows.append(row);
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
