<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.projection.lib;

import com.bytefacets.spinel.schema.${type.name}Field;
import com.bytefacets.spinel.schema.FieldResolver;
import java.util.Objects;

public abstract class ${type.name}Calculation implements ${type.name}FieldCalculation {
    private final String name;
    private ${type.name}Field field;

    public ${type.name}Calculation(final String name) {
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
    public final ${type.arrayType} calculate(final int row) {
        return calculateValue(field.valueAt(row));
    }

    protected abstract ${type.arrayType} calculateValue(final ${type.arrayType} value);
}
