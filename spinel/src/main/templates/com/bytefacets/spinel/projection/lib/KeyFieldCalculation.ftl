<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.projection.lib;

import com.bytefacets.spinel.projection.FieldCalculation;

public interface ${type.name}FieldCalculation extends FieldCalculation {
    ${type.arrayType} calculate(int row);
}
