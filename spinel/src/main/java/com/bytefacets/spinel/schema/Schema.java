// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.schema;

import static com.bytefacets.spinel.exception.FieldNotFoundException.fieldNotFound;
import static java.util.Objects.requireNonNull;

import jakarta.annotation.Nullable;
import java.util.function.Consumer;

public final class Schema {
    private final String name;
    private final FieldList fields;

    private Schema(final String name, final FieldList fields) {
        this.name = requireNonNull(name, "name");
        this.fields = requireNonNull(fields, "fields");
    }

    public static Schema schema(final String name, final FieldList fields) {
        return new Schema(name, fields);
    }

    public SchemaField fieldAt(final int fieldId) {
        return fields.fieldAt(fieldId);
    }

    public SchemaField field(final String name) {
        return fields.field(name);
    }

    public @Nullable SchemaField maybeField(final String name) {
        return fields.maybeField(name);
    }

    public int size() {
        return fields.size();
    }

    public void forEachField(final Consumer<SchemaField> consumer) {
        for (int i = 0, len = fields.size(); i < len; i++) {
            consumer.accept(fields.fieldAt(i));
        }
    }

    public FieldResolver asFieldResolver() {
        return new FieldResolver() {
            @Nullable
            @Override
            public Field findField(final String name) {
                final SchemaField schemaField = maybeField(name);
                return schemaField != null ? schemaField.field() : null;
            }

            @Override
            public Field getField(final String name) {
                final Field field = findField(name);
                if (field != null) {
                    return field;
                }
                throw fieldNotFound(name, Schema.this.name);
            }
        };
    }

    public String name() {
        return name;
    }

    public FieldList fields() {
        return fields;
    }
}
