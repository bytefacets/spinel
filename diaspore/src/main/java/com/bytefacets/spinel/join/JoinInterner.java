// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.join;

import com.bytefacets.spinel.interner.RowInterner;
import com.bytefacets.spinel.schema.FieldResolver;

public interface JoinInterner {
    void bindToSchemas(FieldResolver leftResolver, FieldResolver rightResolver);

    void unbindSchemas();

    RowInterner left();

    RowInterner right();
}
