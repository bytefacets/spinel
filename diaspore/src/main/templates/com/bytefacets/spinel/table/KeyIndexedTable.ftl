<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.table;

import com.bytefacets.collections.hash.${type.name}IndexedSet;
import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.common.OutputManager;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.WritableField;
import com.bytefacets.spinel.transform.OutputProvider;

import static com.bytefacets.spinel.exception.DuplicateKeyException.duplicateKeyException;
import static com.bytefacets.spinel.exception.KeyException.unknownKeyException;
import static java.util.Objects.requireNonNull;

/**
 * A table keyed by ${type.javaType}.
 */
public final class ${type.name}IndexedTable${generics} implements OutputProvider {
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

    /** The schema of this table with access to the table's Fields. */
    public Schema schema() {
        return outputManager.schema();
    }

    /**
     * Looks up the fieldId associated with the fieldName. This can be used in
     * combination with the TableRow to set values in specific fields. You could also
     * directly use {@link #writableField} instead of TableRow.
     */
    public int fieldId(final String fieldName) {
        return schema().field(fieldName).fieldId();
    }

    /**
     * Returns the WritableField associated with fieldName. Note that the key field will
     * not be a WritableField; it will be managed by the modification methods.
     *
     * @throws ClassCastException if the underlying field is not an instance of the
     *                            interface to which it's being cast
     */
    @SuppressWarnings("unchecked")
    public <T extends WritableField> T writableField(final String fieldName) {
        return (T) schema().field(fieldName).field();
    }

    /**
     * A convenience method to access table fields associated with a particular row.
     * Use in conjunction with {@link #fieldId} to lookup the fieldIds to use in the
     * TableRow methods.
     */
    public TableRow tableRow() {
        tableRow.setRow(stateChange.currentRow());
        return tableRow;
    }

    /**
     * Begins an add operation for the given key and returns the row assigned
     * to the key. The operation should be closed by {@link #endAdd}.
     */
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

    /**
     * Begins an change operation for the given key and returns the row assigned
     * to the key. The operation should be closed by {@link #endChange}.
     */
    public int beginChange(final ${type.javaType} key) {
        int row = index.lookupEntry(key);
        if(row == -1) {
            throw unknownKeyException(getClass(), name, key);
        }
        tableRow.setRow(row);
        stateChange.changeRow(row);
        return row;
    }

    /**
     * Begins an add or change operation for the given key and returns the row assigned
     * to the key. The operation should be closed by {@link #endUpsert}.
     */
    public int beginUpsert(final ${type.javaType} key) {
        final int before = index.size();
        final int row = index.add(key);
        if(before != index.size()) {
            stateChange.addRow(row);
        } else {
            stateChange.changeRow(row);
        }
        tableRow.setRow(row);
        return row;
    }

    /** Returns the row associated with the given key */
    public int lookupKeyRow(final ${type.javaType} key) {
        return index.lookupEntry(key);
    }

    /** Called to close off a {@link #beginAdd} and register the row for firing. */
    public void endAdd() {
        tableRow.setNoRow();
        stateChange.endAdd();
    }

    /** Called to close off a {@link #beginChange} and register the row for firing. */
    public void endChange() {
        tableRow.setNoRow();
        stateChange.endChange();
    }

    /** Called to close off a {@link #beginUpsert} and register the row for firing. */
    public void endUpsert() {
        tableRow.setNoRow();
        stateChange.endUpsert();
    }

    /** Removes the row associated with the key and register the row for firing. */
    public int remove(final ${type.javaType} key) {
        int row = index.lookupEntry(key);
        if(row == -1) {
            throw unknownKeyException(getClass(), name, key);
        }
        index.removeAtAndReserve(row);
        stateChange.removeRow(row);
        return row;
    }

    /** Fires the accumulated changes. */
    public void fireChanges() {
        stateChange.fire(outputManager, index::freeReservedEntry);
    }

    /** The output of this table to which you can attach various inputs to receive updates. */
    public TransformOutput output() {
        return outputManager.output();
    }
}