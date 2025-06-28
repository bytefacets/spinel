// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.schema;

import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.common.FieldDependencySet;

public final class SchemaFieldResolver implements FieldResolver {
    private final FieldDependencySet fieldDependencySet;
    private Schema schema;

    public static SchemaFieldResolver schemaFieldResolver(
            final FieldDependencySet fieldDependencySet) {
        return new SchemaFieldResolver(fieldDependencySet);
    }

    private SchemaFieldResolver(final FieldDependencySet fieldDependencySet) {
        this.fieldDependencySet = requireNonNull(fieldDependencySet, "fieldDependencySet");
    }

    public FieldDependencySet fieldDependencySet() {
        return fieldDependencySet;
    }

    public void setSchema(final Schema schema) {
        this.schema = schema;
    }

    @Override
    public Field findField(final String name) {
        final SchemaField schemaField = schema.field(name);
        if (schemaField != null) {
            fieldDependencySet.dependsOnFieldId(schemaField.fieldId());
            return schemaField.field();
        }
        return null;
    }
}
