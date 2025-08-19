<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.schema;

public interface ${type.name}WritableField extends ${type.name}Field, WritableField {
    void setValueAt(int row, ${type.arrayType} value);
}
