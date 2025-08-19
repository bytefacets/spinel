// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.facade;

/**
 * A limited interface used by Struct features in tables and operators to enable row-based access to
 * a schema.
 *
 * @see StructFacadeBuilder
 * @see com.bytefacets.spinel.table.StructTable
 * @see com.bytefacets.spinel.table.IntIndexedStructTable
 */
public interface StructFacade {
    void moveToRow(int row);

    int currentRow();
}
