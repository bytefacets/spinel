// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.table;

import static com.bytefacets.diaspore.table.TableRow.NO_ROW;

import com.bytefacets.collections.functional.IntConsumer;
import com.bytefacets.collections.vector.IntVector;
import com.bytefacets.diaspore.common.InputNotifier;
import com.bytefacets.diaspore.schema.FieldBitSet;
import com.bytefacets.diaspore.schema.FieldChangeListener;

class TableStateChange {
    private final FieldBitSet changedFields = FieldBitSet.fieldBitSet();
    private final IntVector addedRows = new IntVector(16);
    private final IntVector changedRows = new IntVector(16);
    private final IntVector removedRows = new IntVector(16);
    private boolean isChange;
    private int currentRow = NO_ROW;

    FieldChangeListener fieldChangeListener() {
        return this::changeField;
    }

    void addRow(final int row) {
        isChange = false;
        currentRow = row;
    }

    int currentRow() {
        return currentRow;
    }

    void changeRow(final int row) {
        isChange = true;
        currentRow = row;
    }

    void removeRow(final int row) {
        currentRow = NO_ROW;
        isChange = false;
        removedRows.append(row);
    }

    void endAdd() {
        if (currentRow == NO_ROW) {
            throw new RuntimeException();
        }
        addedRows.append(currentRow);
    }

    void endChange() {
        if (currentRow == NO_ROW) {
            throw new RuntimeException();
        }
        changedRows.append(currentRow);
        isChange = false;
    }

    void changeField(final int fieldId) {
        if (isChange) {
            changedFields.fieldChanged(fieldId);
        }
    }

    void fire(final InputNotifier manager, final IntConsumer removedRowConsumer) {
        if (!removedRows.isEmpty()) {
            manager.notifyRemoves(removedRows);
        }
        if (!addedRows.isEmpty()) {
            manager.notifyAdds(addedRows);
        }
        if (!changedRows.isEmpty()) {
            manager.notifyChanges(changedRows, changedFields);
        }
        removedRows.forEach(removedRowConsumer);
        reset();
    }

    private void reset() {
        currentRow = NO_ROW;
        removedRows.clear();
        addedRows.clear();
        changedRows.clear();
        changedFields.clear();
    }
}
