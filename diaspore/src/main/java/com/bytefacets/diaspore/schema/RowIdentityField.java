// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.schema;

public final class RowIdentityField implements IntField {
    private static final RowIdentityField Instance = new RowIdentityField();

    private RowIdentityField() {}

    public static IntField rowIdentityField() {
        return Instance;
    }

    @Override
    public int valueAt(final int row) {
        return row;
    }
}
