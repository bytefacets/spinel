<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.interner;

<#list types as type>
import com.bytefacets.spinel.schema.TypeId;
</#list>

public final class InternerFactory {
    private InternerFactory() {}

    // improve later in java21
    public static RowInterner interner(final String fieldName, final byte typeId, final int initialCapacity) {
        return switch(typeId) {
<#list types as type>
<#if type.name == "Bool">
            case TypeId.${type.name} -> ${type.name}RowInterner.${type.name?lower_case}Interner(fieldName);
<#else>
            case TypeId.${type.name} -> ${type.name}RowInterner.${type.name?lower_case}Interner(fieldName, initialCapacity);
</#if>
</#list>
            default -> throw new IllegalArgumentException("Unknown type: " + typeId);
        };
    }

    public static RowInterner interner(final String fieldName, final byte typeId, final int initialCapacity, final InternSetProvider setProvider) {
        return switch(typeId) {
<#list types as type>
<#if type.name == "Bool">
            case TypeId.${type.name} -> ${type.name}RowInterner.${type.name?lower_case}Interner(fieldName);
<#else>
            case TypeId.${type.name} -> ${type.name}RowInterner.${type.name?lower_case}Interner(fieldName, setProvider.getOrCreate${type.name}Set(initialCapacity));
</#if>
</#list>
            default -> throw new IllegalArgumentException("Unknown type: " + typeId);
        };
    }
}

