// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.conflation;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.StringGenericIndexedMap;
import com.bytefacets.spinel.schema.FieldList;
import com.bytefacets.spinel.schema.FieldMapping;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.SchemaField;

/**
 * Builds a schemas for a ChangeConflator.
 *
 * @see ChangeConflator
 */
final class ChangeConflatorSchemaBuilder {
    private final String name;

    ChangeConflatorSchemaBuilder(final String name) {
        this.name = requireNonNull(name, "name");
    }

    Schema buildSchema(final Schema inSchema, final FieldMapping.Builder fieldMappingBuilder) {
        final StringGenericIndexedMap<SchemaField> fieldMap =
                new StringGenericIndexedMap<>(inSchema.size());
        inSchema.forEachField(
                schemaField -> {
                    final int id = fieldMap.add(schemaField.name());
                    // the field is essentially pass thru as we're just conflating row updates
                    fieldMap.putValueAt(id, schemaField.withNewFieldId(id));
                    fieldMappingBuilder.mapInboundToOutbound(schemaField.fieldId(), id);
                });
        return Schema.schema(name, FieldList.fieldList(fieldMap));
    }
}
