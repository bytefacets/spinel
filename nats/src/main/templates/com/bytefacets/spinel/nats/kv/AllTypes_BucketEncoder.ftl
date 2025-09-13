<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats.kv;

<#list types as type>
import com.bytefacets.collections.types.${type.name}Type;
import com.bytefacets.spinel.schema.${type.name}Field;
</#list>
import com.bytefacets.spinel.grpc.codec.ObjectEncoderImpl;
import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.TypeId;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

final class BucketEncoder {
    private static final SkipAttr SKIP_ATTR = new SkipAttr();
    private final ObjectEncoderImpl encoder = ObjectEncoderImpl.encoder();
    private int schemaId = -1;
    private Attr[] attrs;

    BucketEncoder() {
    }

    void setSchema(final int schemaId, final Schema schema) {
        this.schemaId = schemaId;
        attrs = new Attr[schema.size()];
        for(int i = 0, len = schema.size(); i < len; i++) {
            final var attr = createAttr(schema.fieldAt(i).field());
            if(attr != null) {
                attrs[i] = attr;
            } else {
                attrs[i] = SKIP_ATTR;
            }
        }
    }

    byte[] encode(final int row) {
        int size = 4; // for the schemaId
        for(Attr attr : attrs){
            size += (1 + attr.read(row));
        }
        final byte[] target = new byte[size];
        int pos = IntType.writeLE(target, 0, schemaId);
        for(Attr attr : attrs){
            pos = attr.write(pos, target);
        }
        return target;
    }

    private interface Attr {
        int read(int row);
        int write(int pos, byte[] target);
    }

    Attr createAttr(final Field field) {
        return switch(field.typeId()) {
<#list types as type>
<#if type.name == "Generic">
            case TypeId.${type.name} -> new ${type.name}Attr((${type.name}Field)field, encoder);
<#else>
            case TypeId.${type.name} -> new ${type.name}Attr((${type.name}Field)field);
</#if>
</#list>
            default -> null;
        };
    }

<#list types as type>
    private static class ${type.name}Attr implements Attr {
        private final ${type.name}Field field;
        private ${type.arrayType} value;
        <#if type.name == "String">
        private byte[] bytes;
        private ${type.name}Attr(final ${type.name}Field field) {
            this.field = Objects.requireNonNull(field, "field");
        }
        <#elseif type.name == "Generic">
        private final ObjectEncoderImpl encoder;
        private byte[] bytes;
        private ${type.name}Attr(final ${type.name}Field field, final ObjectEncoderImpl encoder) {
            this.field = Objects.requireNonNull(field, "field");
            this.encoder = Objects.requireNonNull(encoder, "encoder");
        }
        <#else>
        private ${type.name}Attr(final ${type.name}Field field) {
            this.field = Objects.requireNonNull(field, "field");
        }
        </#if>

        @Override
        public int read(final int row) {
            value = field.valueAt(row);
            <#if type.name == "String">
            if(value != null && value.length() > 0) {
                bytes = value.getBytes(StandardCharsets.UTF_8);
            } else {
                bytes = null;
            }
            <#elseif type.name == "Generic">
            bytes = encoder.encodeToArray(value);
            </#if>
            return 4 + size();
        }

        private int size() {
            <#if type.name == "Bool" || type.name == "Byte">
            return 1;
            <#elseif type.name == "Short" || type.name == "Char">
            return 2;
            <#elseif type.name == "Int" || type.name == "Float">
            return 4;
            <#elseif type.name == "Long" || type.name == "Double">
            return 8;
            <#elseif type.name == "String" || type.name == "Generic">
            return (bytes != null ? bytes.length : 0);
            </#if>
        }

        @Override
        public int write(int pos, final byte[] target) {
            target[pos++] = TypeId.${type.name};
            <#if type.name == "String" || type.name == "Generic">
            final int size = value != null ? size() : -1;
            pos = IntType.writeLE(target, pos, size);
            if(bytes != null) {
                System.arraycopy(bytes, 0, target, pos, bytes.length);
                pos += bytes.length;
            }
            return pos;
            <#else>
            return ${type.name}Type.writeLE(target, pos, value);
            </#if>
        }
    }

 </#list>
    private static class SkipAttr implements Attr {
        @Override
        public int read(final int row) {
            return 0;
        }

        @Override
        public int write(final int pos, final byte[] target) {
            return pos;
        }
    }
}
