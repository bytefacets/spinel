// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.interner;

import com.bytefacets.diaspore.schema.FieldResolver;

public final class ConstantRowInterner implements RowInterner {
    public static final ConstantRowInterner Instance = new ConstantRowInterner();

    @Override
    public int intern(final int row) {
        return 0;
    }

    @Override
    public void bindToSchema(final FieldResolver fieldResolver) {}

    @Override
    public void unbindSchema() {}

    @Override
    public void freeEntry(final int entry) {}
}
