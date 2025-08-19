<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.schema;

public record FieldDescriptor(byte fieldType, String name, Metadata metadata) {
<#list types as type>

    public static FieldDescriptor ${type.name?lower_case}Field(final String name, final Metadata metadata) {
        return new FieldDescriptor(TypeId.${type.name}, name, metadata);
    }
    public static FieldDescriptor ${type.name?lower_case}Field(final String name) {
        return new FieldDescriptor(TypeId.${type.name}, name, Metadata.EMPTY);
    }

</#list>
}

