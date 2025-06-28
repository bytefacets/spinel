// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.prototype;

import static com.bytefacets.diaspore.prototype.PrototypeFieldFactory.createPrototypeField;
import static com.bytefacets.diaspore.schema.FieldList.fieldList;
import static com.bytefacets.diaspore.schema.Schema.schema;
import static com.bytefacets.diaspore.schema.SchemaField.schemaField;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.StringGenericIndexedMap;
import com.bytefacets.diaspore.schema.Field;
import com.bytefacets.diaspore.schema.FieldDescriptor;
import com.bytefacets.diaspore.schema.Schema;
import com.bytefacets.diaspore.schema.SchemaField;
import java.util.List;

class PrototypeSchemaBuilder {
    private final List<FieldDescriptor> fieldDescriptors;
    private final String name;

    PrototypeSchemaBuilder(final String name, final List<FieldDescriptor> fieldDescriptors) {
        this.name = requireNonNull(name, "name");
        this.fieldDescriptors = requireNonNull(fieldDescriptors, "fieldDescriptors");
    }

    int size() {
        return fieldDescriptors.size();
    }

    Schema buildSchema(final PrototypeFieldFactory.SchemaProvider provider) {
        final StringGenericIndexedMap<SchemaField> fieldMap =
                new StringGenericIndexedMap<>(fieldDescriptors.size(), 1f);
        fieldDescriptors.forEach(
                f -> {
                    final int id = fieldMap.add(f.name());
                    final Field field = createPrototypeField(f.fieldType(), f.name(), provider);
                    fieldMap.putValueAt(id, schemaField(id, f.name(), field, f.metadata()));
                });
        return schema(name, fieldList(fieldMap));
    }
}
