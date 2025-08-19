<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.interner;

import com.bytefacets.collections.hash.${type.name}IndexedSet;
import com.bytefacets.spinel.schema.${type.name}Field;
import com.bytefacets.spinel.schema.FieldResolver;
import java.util.Objects;

public final class ${type.name}RowInterner implements RowInterner {
    private final String sourceFieldName;
    private final ${type.name}IndexedSet${instanceGenerics} set;
    private ${type.name}Field field;

    public static ${type.name}RowInterner ${type.name?lower_case}Interner(final String fieldName, final int initialSize) {
        return new ${type.name}RowInterner(fieldName, new ${type.name}IndexedSet${type.instanceGenerics}(initialSize));
    }

    public static ${type.name}RowInterner ${type.name?lower_case}Interner(final String fieldName, final ${type.name}IndexedSet${type.instanceGenerics} set) {
        return new ${type.name}RowInterner(fieldName, set);
    }

    private ${type.name}RowInterner(final String sourceFieldName, final ${type.name}IndexedSet${type.instanceGenerics} set) {
        this.sourceFieldName = Objects.requireNonNull(sourceFieldName, "sourceFieldName");
        this.set = Objects.requireNonNull(set, "set");
    }

    @Override
    public void bindToSchema(final FieldResolver fieldResolver) {
        field = fieldResolver.find${type.name}Field(sourceFieldName);
    }

    @Override
    public void unbindSchema() {
        field = null;
        set.clear();
    }

    @Override
    public int intern(final int row) {
        return set.add(field.valueAt(row));
    }

    @Override
    public void freeEntry(final int entry) {
        set.removeAt(entry);
    }
}
