// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.jdbc.source;

import static com.bytefacets.spinel.jdbc.source.JdbcSourceBindingProvider.jdbcSourceBindingProvider;
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
                        mapping(Types.BIT, TypeId.Bool),
                        mapping(Types.BOOLEAN, TypeId.Bool),
                        mapping(Types.TINYINT, TypeId.Byte),
                        mapping(Types.SMALLINT, TypeId.Short),
                        mapping(Types.CHAR, TypeId.Char),
                        mapping(Types.INTEGER, TypeId.Int),
                        mapping(Types.BIGINT, TypeId.Long),
                        mapping(Types.FLOAT, TypeId.Float),
                        mapping(Types.REAL, TypeId.Double),
                        mapping(Types.DECIMAL, TypeId.Double),
                        mapping(Types.NUMERIC, TypeId.Double),
                        mapping(Types.DATE, TypeId.Int),
                        mapping(Types.TIME, TypeId.Int),
                        mapping(Types.TIME_WITH_TIMEZONE, TypeId.Int),
                        mapping(Types.TIMESTAMP, TypeId.Long),
                        mapping(Types.TIMESTAMP_WITH_TIMEZONE, TypeId.Long),
                        mapping(Types.VARCHAR, TypeId.String),
                        mapping(Types.NVARCHAR, TypeId.String),
                        mapping(Types.LONGNVARCHAR, TypeId.String),
                        mapping(Types.LONGVARCHAR, TypeId.String),
                        mapping(Types.JAVA_OBJECT, TypeId.Generic))
                .map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("sqlTypes")
    void shouldProduceBindingWithExpectedDescriptor(final TypeMapping mapping) {
        final JdbcFieldBinding binding = provider.create("foo", mapping.sqlType);
        assertThat(binding.createDescriptor("foo").fieldType(), equalTo(mapping.type));
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

    private static TypeMapping mapping(final int sqlType, final byte type) {
        return new TypeMapping(sqlType, type);
    }

    private record TypeMapping(int sqlType, byte type) {}
}
