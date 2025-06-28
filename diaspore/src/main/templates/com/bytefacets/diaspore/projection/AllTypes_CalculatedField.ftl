<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.projection;

<#list types as type>
import com.bytefacets.diaspore.projection.lib.${type.name}FieldCalculation;
</#list>
import com.bytefacets.diaspore.schema.Field;
<#list types as type>
import com.bytefacets.diaspore.schema.${type.name}Field;
</#list>

final class CalculatedField {
    private CalculatedField() {}

    // improve later in java21
    static Field asCalculatedField(final FieldCalculation calculation) {
<#list types as type>
        if(calculation instanceof ${type.name}FieldCalculation _calc${type.name}) {
            return ${type.name?lower_case}CalculatedField(_calc${type.name});
        }
</#list>
        throw new IllegalArgumentException("Unknown type: " + calculation.getClass());
    }

<#list types as type>

    static ${type.name}Field ${type.name?lower_case}CalculatedField(final ${type.name}FieldCalculation calculation) {
        return calculation::calculate;
    }
</#list>
}

