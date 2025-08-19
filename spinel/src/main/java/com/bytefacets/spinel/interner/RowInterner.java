// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.interner;

import com.bytefacets.spinel.schema.SchemaBindable;

public interface RowInterner extends SchemaBindable {
    int intern(int row);

    void freeEntry(int entry);
}
