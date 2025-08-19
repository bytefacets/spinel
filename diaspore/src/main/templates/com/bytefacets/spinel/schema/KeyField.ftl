<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.schema;

public interface ${type.name}Field extends Field {
    default byte typeId() {
        return TypeId.${type.name};
    }

    default Object objectValueAt(int row) {
        return valueAt(row);
    }

    ${type.arrayType} valueAt(int row);
}
