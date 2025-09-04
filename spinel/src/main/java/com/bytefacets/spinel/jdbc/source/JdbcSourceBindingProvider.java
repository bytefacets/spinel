// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.jdbc.source;

import com.bytefacets.collections.hash.IntGenericIndexedMap;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Provider of JdbcFieldBinding objects which bind ResultSet fields to Schema fields and hide the
 * type-dependent details of that transfer. This provider allows overriding by field name and
 * overriding sqlType defaults.
 *
 * <p>Most defaults are obvious. The ones that are not are Date->Int, Time->Int, and
 * Timestamp->Long.
 *
 * @see JdbcFieldBinding
 * @see TimestampToLongBinding
 * @see DateToIntBinding
 * @see TimeToIntBinding
 */
public final class JdbcSourceBindingProvider {
    private static final IntGenericIndexedMap<Supplier<JdbcFieldBinding>> DEFAULT_SUPPLIER_MAP =
            new IntGenericIndexedMap<>(16);

    static {
        Stream.of(Types.BIT, Types.BOOLEAN)
                .forEach(type -> DEFAULT_SUPPLIER_MAP.put(type, BoolJdbcFieldBinding::new));

        DEFAULT_SUPPLIER_MAP.put(Types.TINYINT, ByteJdbcFieldBinding::new);
        DEFAULT_SUPPLIER_MAP.put(Types.SMALLINT, ShortJdbcFieldBinding::new);
        DEFAULT_SUPPLIER_MAP.put(Types.CHAR, CharJdbcFieldBinding::new);
        DEFAULT_SUPPLIER_MAP.put(Types.INTEGER, IntJdbcFieldBinding::new);
        DEFAULT_SUPPLIER_MAP.put(Types.BIGINT, LongJdbcFieldBinding::new);
        DEFAULT_SUPPLIER_MAP.put(Types.FLOAT, FloatJdbcFieldBinding::new);

        Stream.of(Types.DECIMAL, Types.NUMERIC, Types.REAL)
                .forEach(type -> DEFAULT_SUPPLIER_MAP.put(type, DoubleJdbcFieldBinding::new));

        DEFAULT_SUPPLIER_MAP.put(Types.DATE, DateToIntBinding::new);

        Stream.of(Types.TIME, Types.TIME_WITH_TIMEZONE)
                .forEach(type -> DEFAULT_SUPPLIER_MAP.put(type, TimeToIntBinding::new));

        Stream.of(Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE)
                .forEach(type -> DEFAULT_SUPPLIER_MAP.put(type, TimestampToLongBinding::new));

        Stream.of(Types.VARCHAR, Types.NVARCHAR, Types.LONGVARCHAR, Types.LONGNVARCHAR)
                .forEach(type -> DEFAULT_SUPPLIER_MAP.put(type, StringJdbcFieldBinding::new));
    }

    private final Map<String, Supplier<JdbcFieldBinding>> nameToSupplierMap = new HashMap<>();
    private final IntGenericIndexedMap<Supplier<JdbcFieldBinding>> defaultSupplierMap =
            new IntGenericIndexedMap<>(16);

    private JdbcSourceBindingProvider() {
        defaultSupplierMap.copyFrom(DEFAULT_SUPPLIER_MAP);
    }

    public static JdbcSourceBindingProvider jdbcSourceBindingProvider() {
        return new JdbcSourceBindingProvider();
    }

    /** Sets a default binding supplier for the sql type. */
    public JdbcSourceBindingProvider setSqlTypeDefault(
            final int sqlType, final Supplier<JdbcFieldBinding> supplier) {
        this.defaultSupplierMap.put(sqlType, supplier);
        return this;
    }

    /** Registers specific overrides for the given names and suppliers. */
    public JdbcSourceBindingProvider register(
            final Map<String, Supplier<JdbcFieldBinding>> nameToSupplierMap) {
        this.nameToSupplierMap.putAll(nameToSupplierMap);
        return this;
    }

    /** Registers a specific override for the given names. */
    public JdbcSourceBindingProvider register(
            final String columnName, final Supplier<JdbcFieldBinding> bindingSupplier) {
        this.nameToSupplierMap.put(columnName, bindingSupplier);
        return this;
    }

    /** Creates a new binding for the given name and sqlType by calling the registered supplier. */
    public JdbcFieldBinding create(final String name, final int sqlType) {
        final Supplier<JdbcFieldBinding> supplier = nameToSupplierMap.get(name);
        if (supplier != null) {
            return supplier.get();
        } else {
            return pickDefaultBinding(sqlType);
        }
    }

    private JdbcFieldBinding pickDefaultBinding(final int sqlType) {
        return defaultSupplierMap.getOrDefault(sqlType, GenericJdbcFieldBinding::new).get();
    }
}
