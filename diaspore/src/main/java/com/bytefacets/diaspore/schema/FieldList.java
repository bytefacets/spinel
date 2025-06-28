// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.schema;

import static com.bytefacets.diaspore.exception.FieldNotFoundException.fieldNotFound;
import static com.bytefacets.diaspore.schema.SchemaField.schemaField;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.StringGenericIndexedMap;
import java.util.Map;
import javax.annotation.Nullable;

public final class FieldList {
    private final StringGenericIndexedMap<SchemaField> nameToField;

    private FieldList(final StringGenericIndexedMap<SchemaField> nameToField) {
        this.nameToField = requireNonNull(nameToField, "nameToField");
    }

    public int size() {
        return nameToField.size();
    }

    public SchemaField field(final String name) {
        final int id = nameToField.lookupEntry(name);
        if (id == -1) {
            throw fieldNotFound(String.format("Field '%s' not found in schema", name));
        } else {
            return nameToField.getValueAt(id);
        }
    }

    public @Nullable SchemaField maybeField(final String name) {
        return nameToField.getOrDefault(name, null);
    }

    public SchemaField fieldAt(final int fieldId) {
        return nameToField.getValueAt(fieldId);
    }

    public static FieldList fieldList(final Map<String, Field> fieldNameMap) {
        final var fieldIndex = new StringGenericIndexedMap<SchemaField>(fieldNameMap.size(), 1f);
        for (var entry : fieldNameMap.entrySet()) {
            final int fieldId = fieldIndex.add(entry.getKey());
            fieldIndex.putValueAt(fieldId, schemaField(fieldId, entry.getKey(), entry.getValue()));
        }
        return new FieldList(fieldIndex);
    }

    public static FieldList fieldList(final StringGenericIndexedMap<SchemaField> fieldIndex) {
        return new FieldList(fieldIndex);
    }
}
