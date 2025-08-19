<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.schema;

public final class FieldCopierFactory {
    private FieldCopierFactory() {
    }

    public static FieldCopier fieldCopier(final Field sourceField, final WritableField destField) {
        return switch(destField.typeId()) {
<#list types as type>
            case TypeId.${type.name} -> to${type.name}Copier(Cast.to${type.name}Field(sourceField), (${type.name}WritableField) destField);
</#list>
            default -> throw new IllegalArgumentException("Unknown typeId: " + destField.typeId());
        };
    }

<#list types as type>
    public static FieldCopier to${type.name}Copier(final ${type.name}Field source, final ${type.name}WritableField dest) {
        return row -> dest.setValueAt(row, source.valueAt(row));
    }
</#list>
}
