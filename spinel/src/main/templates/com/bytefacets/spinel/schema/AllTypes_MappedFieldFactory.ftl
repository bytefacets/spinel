<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.schema;

<#list types as type>
import com.bytefacets.collections.types.${type.name}Type;
</#list>

public final class MappedFieldFactory {
    private MappedFieldFactory() {

    }

    public static Field asMappedField(final Field field, final RowMapper mapper) {
        return switch(field.typeId()) {
<#list types as type>
            case TypeId.${type.name} -> asMapped${type.name}Field((${type.name}Field)field, mapper);
</#list>
            default -> throw new IllegalArgumentException("Unknown typeId: " + field.typeId());
        };
    }
<#list types as type>

    public static ${type.name}Field asMapped${type.name}Field(final ${type.name}Field field, final RowMapper mapper) {
        return row -> {
            final int sourceRow = mapper.sourceRowOf(row);
            return sourceRow != -1 ? field.valueAt(sourceRow) : ${type.name}Type.DEFAULT;
        };
    }
</#list>
}
