// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.union;

import static com.bytefacets.spinel.schema.SchemaField.schemaField;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.StringGenericIndexedMap;
import com.bytefacets.spinel.schema.FieldList;
import com.bytefacets.spinel.schema.FieldMapping;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.SchemaField;
import jakarta.annotation.Nullable;

class UnionSchemaBuilder {
    private final String name;
    private final String inputNameFieldName;
    private final String inputIdFieldName;
    private final DependencyMap dependencyMap = new DependencyMap();

    UnionSchemaBuilder(
            final String name,
            final @Nullable String inputNameFieldName,
            final @Nullable String inputIdFieldName) {
        this.name = requireNonNull(name, "name");
        this.inputNameFieldName = inputNameFieldName;
        this.inputIdFieldName = inputIdFieldName;
    }

    DependencyMap dependencyMap() {
        return dependencyMap;
    }

    Schema buildSchema(
            final int inputIndex, final Schema firstSchema, final UnionRowMapper mapper) {
        final var fieldMapping = FieldMapping.fieldMapping(firstSchema.size());
        final StringGenericIndexedMap<SchemaField> fieldMap =
                new StringGenericIndexedMap<>(firstSchema.size());
        addInputIdFieldNameIfNecessary(fieldMap, mapper);
        addInputNameFieldNameIfNecessary(fieldMap, mapper);
        firstSchema.forEachField(
                inField -> {
                    final int outFieldId = fieldMap.add(inField.name());
                    fieldMapping.mapInboundToOutbound(inField.fieldId(), outFieldId);
                    final UnionField outField =
                            UnionFieldFactory.createUnionField(inField.typeId(), mapper);
                    outField.setField(inputIndex, inField.field());
                    fieldMap.putValueAt(
                            outFieldId,
                            schemaField(outFieldId, inField.name(), outField, inField.metadata()));
                });
        dependencyMap.register(inputIndex, fieldMapping.build());
        return Schema.schema(name, FieldList.fieldList(fieldMap));
    }

    private void addInputIdFieldNameIfNecessary(
            final StringGenericIndexedMap<SchemaField> fieldMap, final UnionRowMapper mapper) {
        if (inputIdFieldName != null) {
            final int id = fieldMap.add(inputIdFieldName);
            fieldMap.putValueAt(id, schemaField(id, inputIdFieldName, mapper.inputIndexField()));
        }
    }

    private void addInputNameFieldNameIfNecessary(
            final StringGenericIndexedMap<SchemaField> fieldMap, final UnionRowMapper mapper) {
        if (inputNameFieldName != null) {
            final int id = fieldMap.add(inputNameFieldName);
            fieldMap.putValueAt(id, schemaField(id, inputNameFieldName, mapper.inputNameField()));
        }
    }

    void unmapInput(final int inputIndex, final Schema outSchema) {
        outSchema.forEachField(
                schemaField -> {
                    if (schemaField.field() instanceof UnionField unionField) {
                        unionField.setField(inputIndex, null);
                    }
                });
        dependencyMap.register(inputIndex, null);
    }

    void mapNewSource(final Schema outSchema, final int inputIndex, final Schema schema) {
        final var fieldMapping = FieldMapping.fieldMapping(schema.size());
        schema.forEachField(
                inField -> {
                    final SchemaField outSchemaField = outSchema.field(inField.name());
                    final UnionField outField = (UnionField) outSchemaField.field();
                    fieldMapping.mapInboundToOutbound(inField.fieldId(), outSchemaField.fieldId());
                    outField.setField(inputIndex, inField.field());
                });
        dependencyMap.register(inputIndex, fieldMapping.build());
    }
}
