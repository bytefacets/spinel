<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.jdbc.source;

import com.bytefacets.collections.types.${type.name}Type;
import com.bytefacets.spinel.schema.FieldDescriptor;
import com.bytefacets.spinel.schema.FieldResolver;
import com.bytefacets.spinel.schema.${type.name}WritableField;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.ResultSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class Test${type.name}JdbcFieldBinding {
    private final ${type.name}JdbcFieldBinding binding = new ${type.name}JdbcFieldBinding();
    private @Mock FieldResolver resolver;
    private @Mock ${type.name}WritableField field;
    private @Mock ResultSet resultSet;

    @Test
    void shouldCreateFieldDescriptor() {
        assertThat(binding.createDescriptor("some_field"),
            equalTo(FieldDescriptor.${type.name?lower_case}Field("some_field")));
    }

    @Test
    void shouldBindToSchema() {
        when(resolver.find${type.name}Field("foo")).thenReturn(field);
        binding.bindToSchema(resolver, "foo");
        verify(resolver, times(1)).find${type.name}Field("foo");
    }

    @Test
    void shouldReadIntoField() throws Exception {
        when(resolver.find${type.name}Field("foo")).thenReturn(field);
<#if type.name == "Generic">
        when(resultSet.getObject(4)).thenReturn(v(55));
<#elseif type.name == "Bool">
        when(resultSet.getBoolean(4)).thenReturn(v(55));
<#elseif type.name == "Char">
        when(resultSet.getString(4)).thenReturn("7"); // ascii 55
<#else>
        when(resultSet.get${type.name}(4)).thenReturn(v(55));
</#if>

        binding.bindToSchema(resolver, "foo");
        binding.readIntoField(36, resultSet, 4);
        verify(field, times(1)).setValueAt(36, v(55));
    }

    private ${type.arrayType} v(final int x) {
        return ${type.name}Type.castTo${type.name}(x);
    }
}