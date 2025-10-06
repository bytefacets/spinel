// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.vaadin.data;

import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.schema.Field;
import com.vaadin.flow.function.ValueProvider;

/** Extracts the value from a Field using the row from TransformRow. */
public final class FieldValueProvider implements ValueProvider<TransformRow, Object> {
    private final Field field;

    public static FieldValueProvider valueProvider(final Field field) {
        return new FieldValueProvider(field);
    }

    private FieldValueProvider(final Field field) {
        this.field = requireNonNull(field, "field");
    }

    @Override
    public Object apply(final TransformRow transformRow) {
        return field.objectValueAt(transformRow.getRow());
    }
}
