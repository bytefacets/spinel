<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.groupby.lib;

import com.bytefacets.diaspore.groupby.AggregationFunction;
<#list types as type>
import com.bytefacets.diaspore.schema.${type.name}Field;
</#list>

@SuppressWarnings("cast")
public final class SumFactory {
    private SumFactory() {}

<#list types as type>
<#if type.name != "String" && type.name != "Generic" && type.name != "Bool" && type.name != "Char">
    public static AggregationFunction sumTo${type.name}(final String inputFieldName, final String sumFieldName) {
        return ${type.name}Aggregation.${type.name?lower_case}Aggregation(
            inputFieldName, sumFieldName, (total, oldValue, newValue) -> (${type.arrayType})(total - oldValue + newValue));
    }
</#if>
</#list>
}