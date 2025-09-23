<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.projection.lib;

import com.bytefacets.spinel.schema.${type.name}Field;
import com.bytefacets.spinel.schema.FieldResolver;
import java.util.Objects;

/**
 * A convenience class for a calculation that uses a two source fields that are, or can be
 * cast to a single type. Note that the type here only indicates the ${type.name}Calculation should
 * produce a ${type.arrayType}. The source fields are accessed using the FieldResolver which will
 * attempt to upcast the field if necessary.
 *
 * @see com.bytefacets.spinel.schema.FieldResolver
 * @see com.bytefacets.spinel.schema.Cast
 */
public final class ${type.name}BiCalculation implements ${type.name}FieldCalculation {
    private final String field1Name;
    private final String field2Name;
    private final Calculation calculation;
    private ${type.name}Field field1;
    private ${type.name}Field field2;

    public static ${type.name}BiCalculation ${type.name?lower_case}BiCalculation(final String field1, final String field2, final Calculation calculation) {
        return new ${type.name}BiCalculation(field1, field2, calculation);
    }

    ${type.name}BiCalculation(final String field1, final String field2, final Calculation calculation) {
        this.field1Name = Objects.requireNonNull(field1, "field1");
        this.field2Name = Objects.requireNonNull(field2, "field2");
        this.calculation = Objects.requireNonNull(calculation, "calculation");
    }

    @Override
    public final void bindToSchema(final FieldResolver fieldResolver) {
        field1 = fieldResolver.find${type.name}Field(field1Name);
        field2 = fieldResolver.find${type.name}Field(field2Name);
    }

    @Override
    public final void unbindSchema() {
        field1 = null;
        field2 = null;
    }

    @Override
    public final ${type.arrayType} calculate(final int row) {
        return calculation.calculateValue(field1.valueAt(row), field2.valueAt(row));
    }

    public interface Calculation {
        ${type.arrayType} calculateValue(${type.arrayType} value1, ${type.arrayType} value2);
    }
}
