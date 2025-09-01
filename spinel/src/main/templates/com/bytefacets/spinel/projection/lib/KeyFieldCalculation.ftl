<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.projection.lib;

import com.bytefacets.spinel.projection.FieldCalculation;

/**
 * A calculation that produces a ${type.arrayType}. Implementations should follow these guidelines:
 * <p/>
 * 1) In bindToSchema, should use the provided FieldResolver collect field references uses by the
 * calculation. Use the fields to access the values you need to calculate the result for the give
 * row. The field references made during bindToSchema are used by the project to decide when to
 * call you back if there might have been a change to any of your referenced fields.
 * <p/>
 * 2) In unbindSchema, null out the field references. You won't be called back until another
 * bindToSchema call.
 * <p/>
 * 3) Because you're storing references to fields in a schema, you should not share a
 * FieldCalculation with another Projection.
 */
public interface ${type.name}FieldCalculation extends FieldCalculation {

    /**
     * Calculates the value for the given row, using the fields collected from the FieldResolver
     * during bindToSchema.
     */
    ${type.arrayType} calculate(int row);
}
