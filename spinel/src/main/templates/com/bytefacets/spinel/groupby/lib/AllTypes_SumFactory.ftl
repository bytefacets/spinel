<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.groupby.lib;

import com.bytefacets.spinel.groupby.AggregationFunction;
import com.bytefacets.spinel.schema.Metadata;
<#list types as type>
import com.bytefacets.spinel.schema.${type.name}Field;
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

    public static AggregationFunction sumTo${type.name}(final String inputFieldName, final String sumFieldName, final Metadata metadata) {
        return ${type.name}Aggregation.${type.name?lower_case}Aggregation(
            inputFieldName, sumFieldName, (total, oldValue, newValue) -> (${type.arrayType})(total - oldValue + newValue), metadata);
    }
</#if>
</#list>
}