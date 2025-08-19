<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.schema;

<#list types as type>
<#if type.name != "Bool">
import com.bytefacets.collections.hash.${type.name}IndexedSet;
</#if>
</#list>

public final class IndexedSetFieldFactory {
    private IndexedSetFieldFactory() {

    }

<#list types as type>
<#if type.name == "Generic">
    public static ${type.name}Field asKeyField(final ${type.name}IndexedSet<?> set) {
        return set::getKeyAt;
    }

<#elseif type.name != "Bool">
    public static ${type.name}Field asKeyField(final ${type.name}IndexedSet set) {
        return set::getKeyAt;
    }

</#if>
</#list>
}

