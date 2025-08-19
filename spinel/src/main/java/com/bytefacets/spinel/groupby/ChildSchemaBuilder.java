// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.groupby;

import static com.bytefacets.spinel.schema.FieldList.fieldList;
import static com.bytefacets.spinel.schema.Schema.schema;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.StringGenericIndexedMap;
import com.bytefacets.spinel.schema.FieldMapping;
import com.bytefacets.spinel.schema.IntField;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.SchemaField;
import javax.annotation.Nullable;

final class ChildSchemaBuilder {
    private final String name;
    private final String groupFieldName;

    ChildSchemaBuilder(final String name, @Nullable final String groupFieldName) {
        this.name = requireNonNull(name, "name");
        this.groupFieldName = groupFieldName;
    }

    Result buildChildSchema(final Schema inSchema, final GroupMapping mapping) {
        final StringGenericIndexedMap<SchemaField> fieldMap =
                new StringGenericIndexedMap<>(inSchema.size(), 1f);
        final FieldMapping.Builder fieldIdMapping = FieldMapping.fieldMapping(inSchema.size());
        final int groupFieldId = addGroupFieldIfNecessary(fieldMap, mapping);
        for (int i = 0, len = inSchema.size(); i < len; i++) {
            addInField(inSchema.fieldAt(i), fieldMap, fieldIdMapping);
        }
        final Schema outSchema = schema(name, fieldList(fieldMap));
        return new Result(outSchema, fieldIdMapping.build(), groupFieldId);
    }

    private int addGroupFieldIfNecessary(
            final StringGenericIndexedMap<SchemaField> fieldMap, final GroupMapping mapping) {
        if (groupFieldName != null) {
            final int id = fieldMap.add(groupFieldName);
            final IntField field = mapping.childGroupIdField();
            fieldMap.putValueAt(id, SchemaField.schemaField(id, groupFieldName, field));
            return id;
        }
        return -1;
    }

    private void addInField(
            final SchemaField schemaField,
            final StringGenericIndexedMap<SchemaField> fieldMap,
            final FieldMapping.Builder fieldIdMapping) {
        final int fieldId = fieldMap.add(schemaField.name());
        fieldMap.putValueAt(fieldId, schemaField.withNewFieldId(fieldId));
        fieldIdMapping.mapInboundToOutbound(schemaField.fieldId(), fieldId);
    }

    record Result(Schema schema, FieldMapping fieldMapping, int groupFieldId) {}
}
