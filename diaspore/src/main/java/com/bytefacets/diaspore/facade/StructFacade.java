package com.bytefacets.diaspore.facade;

/**
 * A limited interface used by Struct features in tables and operators to enable row-based access to
 * a schema.
 *
 * @see StructFacadeBuilder
 * @see com.bytefacets.diaspore.table.StructTable
 * @see com.bytefacets.diaspore.table.IntIndexedStructTable
 */
public interface StructFacade {
    void moveToRow(int row);

    int currentRow();
}
