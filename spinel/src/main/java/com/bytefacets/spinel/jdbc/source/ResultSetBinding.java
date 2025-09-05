// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.jdbc.source;

import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.schema.FieldResolver;
import java.sql.ResultSet;
import java.sql.SQLException;

final class ResultSetBinding {
    private final int columnIndex;
    private final JdbcFieldBinding fieldBinding;
    private final String targetFieldName;

    ResultSetBinding(
            final int columnIndex,
            final JdbcFieldBinding fieldBinding,
            final String targetFieldName) {
        if (columnIndex < 1) {
            throw new IllegalArgumentException(
                    "Invalid columnIndex " + columnIndex + ": must be >= 1");
        }
        this.columnIndex = columnIndex;
        this.fieldBinding = requireNonNull(fieldBinding, "fieldBinding");
        this.targetFieldName = requireNonNull(targetFieldName, "targetFieldName");
    }

    void bindToSchema(final FieldResolver fieldResolver) {
        fieldBinding.bindToSchema(fieldResolver, targetFieldName);
    }

    void process(final int row, final ResultSet resultSet) throws SQLException {
        fieldBinding.readIntoField(row, resultSet, columnIndex);
    }
}
