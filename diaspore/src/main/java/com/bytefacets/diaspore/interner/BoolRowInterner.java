// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.interner;

import com.bytefacets.diaspore.schema.BoolField;
import com.bytefacets.diaspore.schema.FieldResolver;
import java.util.Objects;

public final class BoolRowInterner implements RowInterner {
    private final String sourceFieldName;
    private BoolField field;

    public BoolRowInterner(final String sourceFieldName) {
        this.sourceFieldName = Objects.requireNonNull(sourceFieldName, "sourceFieldName");
    }

    public static RowInterner boolInterner(final String fieldName) {
        return new BoolRowInterner(fieldName);
    }

    @Override
    public void bindToSchema(final FieldResolver fieldResolver) {
        field = fieldResolver.findBoolField(sourceFieldName);
    }

    @Override
    public void unbindSchema() {
        field = null;
    }

    @Override
    public int intern(final int row) {
        return field.valueAt(row) ? 1 : 0;
    }

    @Override
    public void freeEntry(final int entry) {}
}
