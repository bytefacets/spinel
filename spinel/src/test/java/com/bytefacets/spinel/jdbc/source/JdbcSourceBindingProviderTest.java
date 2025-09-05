// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.jdbc.source;

import static com.bytefacets.spinel.jdbc.source.JdbcSourceBindingProvider.jdbcSourceBindingProvider;
import static com.bytefacets.spinel.jdbc.source.JdbcUtil.typeMapping;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

import com.bytefacets.spinel.schema.TypeId;
import java.sql.Types;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JdbcSourceBindingProviderTest {
    private final JdbcSourceBindingProvider provider = jdbcSourceBindingProvider();
    private @Mock Supplier<JdbcFieldBinding> mockSupplier;

    private static Stream<Arguments> sqlTypes() {
        return Stream.of(
                        typeMapping(Types.BIT, TypeId.Bool),
                        typeMapping(Types.BOOLEAN, TypeId.Bool),
                        typeMapping(Types.TINYINT, TypeId.Byte),
                        typeMapping(Types.SMALLINT, TypeId.Short),
                        typeMapping(Types.CHAR, TypeId.Char),
                        typeMapping(Types.INTEGER, TypeId.Int),
                        typeMapping(Types.BIGINT, TypeId.Long),
                        typeMapping(Types.FLOAT, TypeId.Float),
                        typeMapping(Types.REAL, TypeId.Double),
                        typeMapping(Types.DECIMAL, TypeId.Double),
                        typeMapping(Types.NUMERIC, TypeId.Double),
                        typeMapping(Types.DATE, TypeId.Int),
                        typeMapping(Types.TIME, TypeId.Int),
                        typeMapping(Types.TIME_WITH_TIMEZONE, TypeId.Int),
                        typeMapping(Types.TIMESTAMP, TypeId.Long),
                        typeMapping(Types.TIMESTAMP_WITH_TIMEZONE, TypeId.Long),
                        typeMapping(Types.VARCHAR, TypeId.String),
                        typeMapping(Types.NVARCHAR, TypeId.String),
                        typeMapping(Types.LONGNVARCHAR, TypeId.String),
                        typeMapping(Types.LONGVARCHAR, TypeId.String),
                        typeMapping(Types.JAVA_OBJECT, TypeId.Generic))
                .map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("sqlTypes")
    void shouldProduceBindingWithExpectedDescriptor(final JdbcUtil.TypeMapping mapping) {
        final JdbcFieldBinding binding = provider.create("foo", mapping.sqlType());
        assertThat(binding.createDescriptor("foo").fieldType(), equalTo(mapping.type()));
        assertThat(binding.createDescriptor("foo").name(), equalTo("foo"));
    }

    @Test
    void shouldOverrideForName() {
        final JdbcFieldBinding binding = new StringJdbcFieldBinding();
        when(mockSupplier.get()).thenReturn(binding);
        // when
        provider.register("bar", mockSupplier);

        // this Types.INTEGER is the default
        assertThat(
                provider.create("foo", Types.INTEGER).createDescriptor("foo").fieldType(),
                equalTo(TypeId.Int));
        // this one is overridden
        assertThat(
                provider.create("bar", Types.INTEGER).createDescriptor("bar").fieldType(),
                equalTo(TypeId.String));
    }

    @Test
    void shouldOverrideDefault() {
        final JdbcFieldBinding binding = new StringJdbcFieldBinding();
        when(mockSupplier.get()).thenReturn(binding);
        // when
        provider.setSqlTypeDefault(Types.CLOB, mockSupplier);
        assertThat(
                provider.create("foo", Types.CLOB).createDescriptor("foo").fieldType(),
                equalTo(TypeId.String));
    }
}
