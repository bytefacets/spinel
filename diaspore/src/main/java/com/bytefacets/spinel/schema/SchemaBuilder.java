// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.schema;

import static com.bytefacets.spinel.schema.FieldMapping.fieldMapping;
import static com.bytefacets.spinel.schema.SchemaField.schemaField;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.StringGenericIndexedMap;

public final class SchemaBuilder {
    private final String name;
    private final FieldMapping.Builder fieldMappingBuilder;
    private final StringGenericIndexedMap<SchemaField> fieldIndex;

    private SchemaBuilder(final String name, final int initialSize) {
        this.name = requireNonNull(name, "name");
        this.fieldMappingBuilder = fieldMapping(initialSize);
        this.fieldIndex = new StringGenericIndexedMap<>(initialSize, 1f);
    }

    public static SchemaBuilder schemaBuilder(final String name, final int initialSize) {
        return new SchemaBuilder(name, initialSize);
    }

    public void addInboundSchema(final Schema inboundSchema, final FieldProvider fieldProvider) {
        inboundSchema.forEachField(
                inboundField -> {
                    final String outboundName = inboundField.name();
                    final int outboundId = fieldIndex.add(inboundField.name());
                    fieldMappingBuilder.mapInboundToOutbound(inboundField.fieldId(), outboundId);
                    final Field outboundField = fieldProvider.mapField(inboundField, outboundId);
                    fieldIndex.putValueAt(
                            outboundId,
                            schemaField(
                                    outboundId,
                                    outboundName,
                                    outboundField,
                                    inboundField.metadata()));
                });
    }

    public void addField(final String name, final Field outboundField, final Metadata metadata) {
        final int outboundId = fieldIndex.add(name);
        fieldIndex.putValueAt(outboundId, schemaField(outboundId, name, outboundField, metadata));
    }

    public Schema buildSchema() {
        return Schema.schema(name, FieldList.fieldList(fieldIndex));
    }

    public FieldMapping buildFieldMapping() {
        return fieldMappingBuilder.build();
    }

    public interface FieldProvider {
        Field mapField(SchemaField inboundField, int outboundFieldId);
    }
}
