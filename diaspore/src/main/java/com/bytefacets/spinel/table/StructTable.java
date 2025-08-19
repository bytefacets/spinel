// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.table;

import static com.bytefacets.spinel.common.OutputManager.outputManager;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.IntIndexedSet;
import com.bytefacets.collections.queue.IntDeque;
import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.common.OutputManager;
import com.bytefacets.spinel.facade.StructFacade;
import com.bytefacets.spinel.facade.StructFacadeFactory;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.SchemaBindable;
import com.bytefacets.spinel.transform.OutputProvider;

public final class StructTable<T> implements OutputProvider {
    private final OutputManager outputManager;
    private final TableStateChange stateChange;
    private final IntDeque freeList = new IntDeque(4);
    private final IntIndexedSet activeRows = new IntIndexedSet(64);
    private final StructFacadeFactory facadeFactory;
    private final Class<T> structType;
    private int nextRow = 0;

    StructTable(
            final Schema schema,
            final Class<T> structType,
            final TableStateChange stateChange,
            final StructFacadeFactory facadeFactory) {
        this.stateChange = requireNonNull(stateChange, "stateChange");
        this.outputManager = outputManager(activeRows::forEach);
        this.facadeFactory = requireNonNull(facadeFactory, "facadeFactory");
        this.outputManager.updateSchema(requireNonNull(schema, "schema"));
        this.structType = requireNonNull(structType, "structType");
    }

    /**
     * Creates an implementation of your interface to use in the table modification methods. The
     * implementation will also implement {@link StructFacade}, which you can use to position the
     * facade over a row yourself.
     */
    public T createFacade() {
        final T facade = facadeFactory.createFacade(structType);
        ((SchemaBindable) facade).bindToSchema(schema().asFieldResolver());
        return facade;
    }

    /** Detaches the facade implementation from the underlying table components. */
    public void unbindFacade(final T facade) {
        ((SchemaBindable) facade).unbindSchema();
    }

    public Schema schema() {
        return outputManager.schema();
    }

    /**
     * Begins an add operation and positions the facade over the row. The operation should be closed
     * by {@link #endAdd}.
     */
    public T beginAdd(final T facade) {
        final int row = allocateRow();
        stateChange.addRow(row);
        ((StructFacade) facade).moveToRow(row);
        return facade;
    }

    /**
     * Begins a change operation and positions the facade over the row. The operation should be
     * closed by {@link #endChange}.
     */
    public void beginChange(final int row, final T facade) {
        ((StructFacade) facade).moveToRow(row);
        stateChange.changeRow(row);
    }

    /** Called to close off a {@link #beginAdd} and register the row for firing. */
    public void endAdd() {
        activeRows.add(stateChange.currentRow());
        stateChange.endAdd();
    }

    /** Called to close off a {@link #beginChange} and register the row for firing. */
    public void endChange() {
        stateChange.endChange();
    }

    /** Removes the row and registers the row for firing. */
    public void remove(final int row) {
        stateChange.removeRow(row);
        activeRows.remove(row);
    }

    /** Fires the accumulated changes. */
    public void fireChanges() {
        stateChange.fire(outputManager, freeList::addLast);
    }

    /** The output of this table to which you can attach various inputs to receive updates. */
    @Override
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
