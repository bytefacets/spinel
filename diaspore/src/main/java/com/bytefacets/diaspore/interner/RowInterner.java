// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.interner;

import com.bytefacets.diaspore.schema.SchemaBindable;

public interface RowInterner extends SchemaBindable {
    int intern(int row);

    void freeEntry(int entry);
}
