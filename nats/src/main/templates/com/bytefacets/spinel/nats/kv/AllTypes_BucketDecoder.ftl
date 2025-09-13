<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats.kv;

import com.bytefacets.collections.hash.IntGenericIndexedMap;
<#list types as type>
import com.bytefacets.collections.types.${type.name}Type;
import com.bytefacets.spinel.schema.${type.name}WritableField;
</#list>
import com.bytefacets.spinel.grpc.proto.SchemaUpdate;
import com.bytefacets.spinel.grpc.receive.SchemaBuilder;
import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.SchemaField;
import com.bytefacets.spinel.schema.TypeId;

import java.util.BitSet;
import java.util.Objects;

final class BucketDecoder {
    private static final SkipAttr SKIP_ATTR = new SkipAttr();
    private final SchemaBuilder schemaBuilder;
    private final SchemaRegistry schemaRegistry;
    private final FieldReader fieldReader = new FieldReader();
    private final IntGenericIndexedMap<Attr[]> schemaMappings = new IntGenericIndexedMap<>(16);
    private final BitSet changedFields;
    private Schema schema;
    private Attr[] attrs;

    BucketDecoder(final BitSet changedFields, final SchemaBuilder schemaBuilder, final SchemaRegistry schemaRegistry) {
        this.schemaBuilder = Objects.requireNonNull(schemaBuilder, "schemaBuilder");
        this.schemaRegistry = Objects.requireNonNull(schemaRegistry, "schemaRegistry");
        this.changedFields = Objects.requireNonNull(changedFields, "changedFields");
    }

    Schema setSchema(final int schemaId, final SchemaUpdate update) {
        schema = schemaBuilder.createSchema(update);
        attrs = new Attr[schema.size()];
        for(int i = 0, len = schema.size(); i < len; i++) {
            final var attr = createAttr(i, schema.fieldAt(i).field());
            if(attr != null) {
                attrs[i] = attr;
            } else {
                attrs[i] = SKIP_ATTR;
            }
        }
        schemaMappings.put(schemaId, attrs);
        return schema;
    }

    void write(final boolean isAdd, final int row, final byte[] data) {
        final int schemaId = IntType.readLE(data, 0);
        fieldReader.set(4, data); // skip schemaId
        final Attr[] entryAttrs = resolveSchema(schemaId);
        if(entryAttrs != null) {
            for(Attr attr : entryAttrs){
                attr.readIntoRow(isAdd, row, fieldReader);
            }
        } else {
            // warn
        }
    }

    private Attr[] resolveSchema(final int schemaId) {
        Attr[] attrs = schemaMappings.getOrDefault(schemaId, null);
        if(attrs != null) {
            return attrs;
        }
        final SchemaUpdate encoded = schemaRegistry.lookup(schemaId);
        if(encoded == null) {
            return null;
        }
        final int len = encoded.getFieldsCount();
        attrs = new Attr[len];
        for(int i = 0; i < len; i++) {
            final String name = encoded.getFields(i).getName();
            final SchemaField targetField = schema.maybeField(name);
            if(targetField != null) {
                attrs[i] = this.attrs[targetField.fieldId()];
            } else {
                attrs[i] = SKIP_ATTR;
            }
        }
        schemaMappings.put(schemaId, attrs);
        return attrs;
    }

    interface Attr {
        void readIntoRow(boolean isAdd, int row, FieldReader reader);
    }

    Attr createAttr(final int fieldId, final Field field) {
        return switch(field.typeId()) {
<#list types as type>
            case TypeId.${type.name} -> new ${type.name}Attr(fieldId, changedFields, (${type.name}WritableField)field);
</#list>
            default -> null;
        };
    }

<#list types as type>
    private static class ${type.name}Attr implements Attr {
        private final int fieldId;
        private final ${type.name}WritableField field;
        private final BitSet changedFields;

        private ${type.name}Attr(final int fieldId, final BitSet changedFields, final ${type.name}WritableField field) {
            this.fieldId = fieldId;
            this.changedFields = Objects.requireNonNull(changedFields, "changedFields");
            this.field = Objects.requireNonNull(field, "field");
        }

        @Override
        public void readIntoRow(final boolean isAdd, final int row, final FieldReader reader) {
            final var newVal = reader.readTo${type.name}();
            final var curVal = field.valueAt(row);
            if(isAdd) {
                field.setValueAt(row, newVal);
            } else if(!${type.name}Type.EqImpl.areEqual(newVal, curVal)) {
                field.setValueAt(row, newVal);
                changedFields.set(fieldId);
            }
        }
    }

 </#list>
    private static class SkipAttr implements Attr {
        @Override
        public void readIntoRow(final boolean isAdd, final int row, final FieldReader reader) {
            reader.skip();
        }
    }
}
