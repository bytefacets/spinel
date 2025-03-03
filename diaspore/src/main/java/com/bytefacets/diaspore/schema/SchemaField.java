// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.schema;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

public final class SchemaField {
    private final int fieldId;
    private final Field field;
    private final String name;
    private final Metadata metadata;

    public static SchemaField schemaField(final int fieldId, final String name, final Field field) {
        return new SchemaField(fieldId, name, field, null);
    }

    public static SchemaField schemaField(
            final int fieldId, final String name, final Field field, final Metadata metadata) {
        return new SchemaField(fieldId, name, field, metadata);
    }

    private SchemaField(
            final int fieldId, final String name, final Field field, final Metadata metadata) {
        this.fieldId = fieldId;
        this.field = requireNonNull(field, "field");
        this.name = requireNonNull(name, "name");
        this.metadata = requireNonNullElse(metadata, Metadata.EMPTY);
    }

    public SchemaField withNewFieldId(final int newFieldId) {
        return new SchemaField(newFieldId, name, field, metadata);
    }

    public Metadata metadata() {
        return metadata;
    }

    public String name() {
        return name;
    }

    public int fieldId() {
        return fieldId;
    }

    public byte typeId() {
        return field.typeId();
    }

    public Object objectValueAt(final int row) {
        return field.objectValueAt(row);
    }

    public Field field() {
        return field;
    }
}
