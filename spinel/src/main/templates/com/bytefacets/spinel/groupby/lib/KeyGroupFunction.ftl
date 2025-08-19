<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.groupby.lib;

<#if type.name != "Bool">
import com.bytefacets.collections.hash.${type.name}IndexedSet;
</#if>
import com.bytefacets.spinel.groupby.GroupFunction;
import com.bytefacets.spinel.schema.FieldResolver;
import com.bytefacets.spinel.schema.${type.name}Field;

import java.util.Objects;

public final class ${type.name}GroupFunction implements GroupFunction {
    private final String fieldName;
    private ${type.name}Field field;
<#if type.name != "Bool">
    private final ${type.name}IndexedSet${instanceGenerics} set;

    public static ${type.name}GroupFunction ${type.name?lower_case}GroupFunction(final String fieldName, final int initialSize) {
        return new ${type.name}GroupFunction(fieldName, initialSize);
    }

    private ${type.name}GroupFunction(final String fieldName, final int initialSize) {
        this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
        this.set = new ${type.name}IndexedSet${instanceGenerics}(initialSize);
    }

    public int group(final int row) {
        return set.add(field.valueAt(row));
    }

    public void onEmptyGroup(final int group) {
        set.removeAt(group);
    }
<#else>

    public static ${type.name}GroupFunction ${type.name?lower_case}GroupFunction(final String fieldName) {
        return new ${type.name}GroupFunction(fieldName);
    }

    private ${type.name}GroupFunction(final String fieldName) {
        this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
    }

    public int group(final int row) {
        return field.valueAt(row) ? 1 : 0;
    }

    public void onEmptyGroup(final int group) {
        // do nothing
    }
</#if>

    @Override
    public void bindToSchema(final FieldResolver fieldResolver) {
        field = Objects.requireNonNull(fieldResolver.find${type.name}Field(fieldName), fieldName);
    }

    @Override
    public void unbindSchema() {
        field = null;
    }
}
