<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.jdbc.source;

import com.bytefacets.spinel.schema.FieldDescriptor;
import com.bytefacets.spinel.schema.FieldResolver;
import com.bytefacets.spinel.schema.${type.name}WritableField;

import java.sql.ResultSet;
import java.sql.SQLException;

public final class ${type.name}JdbcFieldBinding implements JdbcFieldBinding {
    private ${type.name}WritableField field;

    ${type.name}JdbcFieldBinding() {
    }

    @Override
    public FieldDescriptor createDescriptor(final String fieldName) {
        return FieldDescriptor.${type.name?lower_case}Field(fieldName);
    }

    @Override
    public void bindToSchema(final FieldResolver fieldResolver, final String fieldName) {
        field = (${type.name}WritableField) fieldResolver.find${type.name}Field(fieldName);
    }

    @Override
    public void readIntoField(final int rowId, final ResultSet resultSet, final int columnIndex) throws SQLException {
<#if type.name == "Generic">
        field.setValueAt(rowId, resultSet.getObject(columnIndex));
<#elseif type.name == "Bool">
        field.setValueAt(rowId, resultSet.getBoolean(columnIndex));
<#elseif type.name == "Char">
        final String value = resultSet.getString(columnIndex);
        field.setValueAt(rowId, value != null && value.length() > 0 ? value.charAt(0) : '\0');
<#else>
        field.setValueAt(rowId, resultSet.get${type.name}(columnIndex));
</#if>
    }
}
