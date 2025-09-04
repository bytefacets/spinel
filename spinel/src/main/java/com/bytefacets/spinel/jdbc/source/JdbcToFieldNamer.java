// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.jdbc.source;

public interface JdbcToFieldNamer {
    String jdbcToFieldName(String jdbcName);
}
