// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.projection;

import static com.bytefacets.diaspore.schema.FieldList.fieldList;
import static com.bytefacets.diaspore.schema.SchemaField.schemaField;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.StringGenericIndexedMap;
import com.bytefacets.diaspore.schema.Field;
import com.bytefacets.diaspore.schema.Schema;
import com.bytefacets.diaspore.schema.SchemaField;
import java.util.List;
import java.util.Map;

class ProjectionSchemaBuilder {
    private final String name;
    private final InboundFieldSelector inboundFieldSelector;
    private final Map<String, String> aliases;
    private final Map<String, CalculatedFieldDescriptor> newFields;
    private final StringGenericIndexedMap<SchemaField> outboundFieldIndex =
            new StringGenericIndexedMap<>(16);
    private final FieldSorter fieldSorter;

    ProjectionSchemaBuilder(
            final String name,
            final InboundFieldSelector inboundFieldSelector,
            final Map<String, String> aliases,
            final Map<String, CalculatedFieldDescriptor> newFields,
            final FieldSorter fieldSorter) {
        this.name = requireNonNull(name, "name");
        this.inboundFieldSelector = requireNonNull(inboundFieldSelector, "inboundFieldSelector");
        this.aliases = requireNonNull(aliases, "aliases");
        this.newFields = requireNonNull(newFields, "newFields");
        this.fieldSorter = requireNonNull(fieldSorter, "fieldSorter");
        this.fieldSorter.validateUniqueNames(name);
    }

    public Schema buildOutboundSchema(
            final Schema inSchema, final ProjectionDependencyMap dependencyMap) {
        final Map<String, SchemaField> selectedFields =
                inboundFieldSelector.selectFields(name, inSchema.fields());
        applyAliases(selectedFields);
        final Schema outSchema =
                Schema.schema(name, fieldList(buildIndex(selectedFields, dependencyMap)));
        dependencyMap.bindCalculatedFields(inSchema, outSchema, newFields);
        return outSchema;
    }

    private StringGenericIndexedMap<SchemaField> buildIndex(
            final Map<String, SchemaField> selectedFields,
            final ProjectionDependencyMap dependencyMap) {
        outboundFieldIndex.clear();
        dependencyMap.reset();
        fieldSorter.rebuild(
                name -> {
                    final int id = outboundFieldIndex.add(name);
                    final var inField = selectedFields.get(name);
                    if (inField != null) {
                        dependencyMap.mapInboundFieldIdToOutboundFieldId(inField.fieldId(), id);
                        outboundFieldIndex.putValueAt(
                                id, schemaField(id, name, inField.field(), inField.metadata()));
                    } else {
                        outboundFieldIndex.putValueAt(id, createCalculatedField(id, name));
                    }
                },
                List.of(selectedFields.keySet(), newFields.keySet()));
        return outboundFieldIndex;
    }

    private SchemaField createCalculatedField(final int id, final String name) {
        final var descriptor = newFields.get(name);
        final Field field = CalculatedField.asCalculatedField(descriptor.calculation());
        return schemaField(id, name, field, descriptor.metadata());
    }

    private void applyAliases(final Map<String, SchemaField> selectedFields) {
        for (Map.Entry<String, String> entry : aliases.entrySet()) {
            final SchemaField inField = selectedFields.remove(entry.getKey());
            if (inField != null) {
                selectedFields.put(entry.getValue(), inField);
            }
        }
    }
}
