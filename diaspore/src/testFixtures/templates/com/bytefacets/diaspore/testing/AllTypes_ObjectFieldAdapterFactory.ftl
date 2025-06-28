<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.testing;

<#list types as type>
import com.bytefacets.collections.types.${type.name}Type;
</#list>
import com.bytefacets.diaspore.schema.SchemaField;
import com.bytefacets.diaspore.schema.TypeId;
import com.bytefacets.diaspore.table.TableRow;

import static java.util.Objects.requireNonNull;

final class ObjectFieldAdapterFactory {
    private ObjectFieldAdapterFactory() {
    }

    static ObjectFieldAdapter createAdapter(final SchemaField field) {
        return switch(field.typeId()) {
<#list types as type>
            case TypeId.${type.name} -> new ${type.name}Adapter(field.fieldId());
</#list>
            default -> throw new IllegalArgumentException("Unknown type: " + field.typeId());
        };
    }

<#list types as type>
    private static class ${type.name}Adapter implements ObjectFieldAdapter {
        private final int fieldId;
        private ${type.name}Adapter(final int fieldId) {
            this.fieldId = fieldId;
        }
        @Override
        public void apply(final TableRow row, final Object value) {
            row.set${type.name}(fieldId, ${type.name}Type.convert(value));
        }
    }
</#list>
}
