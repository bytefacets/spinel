// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.conflation;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.StringGenericIndexedMap;
import com.bytefacets.diaspore.schema.FieldList;
import com.bytefacets.diaspore.schema.FieldMapping;
import com.bytefacets.diaspore.schema.Schema;
import com.bytefacets.diaspore.schema.SchemaField;

class ConflationSchemaBuilder {
    private final String name;

    ConflationSchemaBuilder(final String name) {
        this.name = requireNonNull(name, "name");
    }

    Schema buildSchema(final Schema inSchema, final FieldMapping.Builder fieldMappingBuilder) {
        final StringGenericIndexedMap<SchemaField> fieldMap =
                new StringGenericIndexedMap<>(inSchema.size());
        inSchema.forEachField(
                schemaField -> {
                    final int id = fieldMap.add(schemaField.name());
                    fieldMap.putValueAt(id, schemaField.withNewFieldId(id));
                    fieldMappingBuilder.mapInboundToOutbound(schemaField.fieldId(), id);
                });
        return Schema.schema(name, FieldList.fieldList(fieldMap));
    }
}
