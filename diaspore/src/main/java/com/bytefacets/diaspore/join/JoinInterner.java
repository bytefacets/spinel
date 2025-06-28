// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.join;

import com.bytefacets.diaspore.interner.RowInterner;
import com.bytefacets.diaspore.schema.FieldResolver;

public interface JoinInterner {
    void bindToSchemas(FieldResolver leftResolver, FieldResolver rightResolver);

    void unbindSchemas();

    RowInterner left();

    RowInterner right();
}
