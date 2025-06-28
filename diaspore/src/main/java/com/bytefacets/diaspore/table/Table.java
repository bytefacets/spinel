// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.table;

import static com.bytefacets.diaspore.common.OutputManager.outputManager;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.IntIndexedSet;
import com.bytefacets.collections.queue.IntDeque;
import com.bytefacets.diaspore.TransformOutput;
import com.bytefacets.diaspore.common.OutputManager;
import com.bytefacets.diaspore.schema.Schema;
import com.bytefacets.diaspore.schema.WritableField;

public final class Table {
    private final OutputManager outputManager;
    private final TableStateChange stateChange;
    private final TableRow tableRow;
    private final IntDeque freeList = new IntDeque(4);
    private final IntIndexedSet activeRows = new IntIndexedSet(64);
    private int nextRow = 0;

    Table(final Schema schema, final TableStateChange stateChange) {
        this.stateChange = requireNonNull(stateChange, "stateChange");
        this.outputManager = outputManager(activeRows::forEach);
        this.outputManager.updateSchema(requireNonNull(schema, "schema"));
        tableRow = new TableRow(schema.fields());
    }

    public TableRow tableRow() {
        tableRow.setRow(stateChange.currentRow());
        return tableRow;
    }

    public Schema schema() {
        return outputManager.schema();
    }

    public int fieldId(final String fieldName) {
        return outputManager.schema().field(fieldName).fieldId();
    }

    @SuppressWarnings("unchecked")
    public <T extends WritableField> T writableField(final String fieldName) {
        return (T) schema().field(fieldName).field();
    }

    public int beginAdd() {
        final int row = allocateRow();
        stateChange.addRow(row);
        tableRow.setRow(row);
        return row;
    }

    public void beginChange(final int row) {
        tableRow.setRow(row);
        stateChange.changeRow(row);
    }

    public void endAdd() {
        activeRows.add(stateChange.currentRow());
        tableRow.setNoRow();
        stateChange.endAdd();
    }

    public void endChange() {
        tableRow.setNoRow();
        stateChange.endChange();
    }

    public void remove(final int row) {
        stateChange.removeRow(row);
        activeRows.remove(row);
    }

    public void fireChanges() {
        stateChange.fire(outputManager, freeList::addLast);
    }

    public TransformOutput output() {
        return outputManager.output();
    }

    private int allocateRow() {
        if (freeList.isEmpty()) {
            return nextRow++;
        }
        return freeList.removeLast();
    }
}
