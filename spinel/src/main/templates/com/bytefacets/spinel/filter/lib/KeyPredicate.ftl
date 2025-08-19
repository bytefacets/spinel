<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.filter.lib;

import com.bytefacets.spinel.filter.RowPredicate;
import com.bytefacets.spinel.schema.${type.name}Field;
import com.bytefacets.spinel.schema.FieldResolver;
import java.util.Objects;

public abstract class ${type.name}Predicate implements RowPredicate {
    private final String name;
    private ${type.name}Field field;

    public static ${type.name}Predicate ${type.name?lower_case}Predicate(final String name, final ${type.name}PredicateTest test) {
        return new ${type.name}Predicate(name) {
            @Override
            protected boolean testValue(final ${type.arrayType} value) {
                return test.testValue(value);
            }
        };
    }

    public ${type.name}Predicate(final String name) {
        this.name = Objects.requireNonNull(name, "name");
    }

    @Override
    public final void bindToSchema(final FieldResolver fieldResolver) {
        field = fieldResolver.find${type.name}Field(name);
    }

    @Override
    public final void unbindSchema() {
        field = null;
    }

    @Override
    public final boolean testRow(final int row) {
        return testValue(field.valueAt(row));
    }

    protected abstract boolean testValue(final ${type.arrayType} value);

    public interface ${type.name}PredicateTest {
        boolean testValue(${type.arrayType} value);
    }
}
