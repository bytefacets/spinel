// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.jdbc.source;

final class JdbcUtil {
    private JdbcUtil() {}

    static Mapping mapping(final int type, final String name) {
        return new Mapping(type, name);
    }

    record Mapping(int type, String name) {}

    static TypeMapping typeMapping(final int sqlType, final byte type) {
        return new TypeMapping(sqlType, type);
    }

    record TypeMapping(int sqlType, byte type) {}
}
