<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.table;

import com.bytefacets.collections.hash.${type.name}IndexedSet;
import com.bytefacets.diaspore.TransformOutput;
import com.bytefacets.diaspore.common.OutputManager;
import com.bytefacets.diaspore.schema.Schema;

import static com.bytefacets.diaspore.exception.DuplicateKeyException.duplicateKeyException;
import static com.bytefacets.diaspore.exception.KeyException.unknownKeyException;
import static java.util.Objects.requireNonNull;

public final class ${type.name}IndexedTable${generics} {
    private final String name;
    private final OutputManager outputManager;
    private final TableStateChange stateChange;
    private final TableRow tableRow;
    private final ${type.name}IndexedSet${generics} index;

    ${type.name}IndexedTable(final ${type.name}IndexedSet${generics} index,
                    final Schema schema,
                    final TableStateChange stateChange) {
        this.index = requireNonNull(index, "index");
        this.stateChange = requireNonNull(stateChange, "stateChange");
        this.outputManager = OutputManager.outputManager(index::forEachEntry);
        this.outputManager.updateSchema(requireNonNull(schema, "schema"));
        tableRow = new TableRow(schema.fields());
        this.name = schema.name();
    }

    public Schema schema() {
        return outputManager.schema();
    }

    public int fieldId(final String name) {
        return schema().field(name).fieldId();
    }

    public TableRow tableRow() {
        tableRow.setRow(stateChange.currentRow());
        return tableRow;
    }

    public int beginAdd(final ${type.javaType} key) {
        final int before = index.size();
        final int row = index.add(key);
        if(before == index.size()) {
            throw duplicateKeyException(getClass(), name, key);
        }
        stateChange.addRow(row);
        tableRow.setRow(row);
        return row;
    }

    public int beginChange(final ${type.javaType} key) {
        int row = index.lookupEntry(key);
        if(row == -1) {
            throw unknownKeyException(getClass(), name, key);
        }
        tableRow.setRow(row);
        stateChange.changeRow(row);
        return row;
    }

    public int lookupKeyRow(final ${type.javaType} key) {
        return index.lookupEntry(key);
    }

    public void endAdd() {
        tableRow.setNoRow();
        stateChange.endAdd();
    }

    public void endChange() {
        tableRow.setNoRow();
        stateChange.endChange();
    }

    public int remove(final ${type.javaType} key) {
        int row = index.lookupEntry(key);
        if(row == -1) {
            throw unknownKeyException(getClass(), name, key);
        }
        index.removeAtAndReserve(row);
        stateChange.removeRow(row);
        return row;
    }

    public void fireChanges() {
        stateChange.fire(outputManager, index::freeReservedEntry);
    }

    public TransformOutput output() {
        return outputManager.output();
    }
}