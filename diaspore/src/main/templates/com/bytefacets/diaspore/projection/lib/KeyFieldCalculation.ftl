<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.projection.lib;

import com.bytefacets.diaspore.projection.FieldCalculation;

public interface ${type.name}FieldCalculation extends FieldCalculation {
    ${type.arrayType} calculate(int row);
}
