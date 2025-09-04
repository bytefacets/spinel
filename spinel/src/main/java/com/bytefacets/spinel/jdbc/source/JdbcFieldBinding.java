// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.jdbc.source;

import com.bytefacets.spinel.schema.FieldDescriptor;
import com.bytefacets.spinel.schema.FieldResolver;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Abstracts type-specific details in reading from a ResultSet and applying to a typed field. */
public interface JdbcFieldBinding {

    /**
     * Create a typed FieldDescriptor given the field name. This will be the possibly transformed
     * ResultSet field after passing through the {@link JdbcToFieldNamer}.
     */
    FieldDescriptor createDescriptor(String fieldName);

    /**
     * Tells this binding to get the instance of the target field in the schema into which the
     * binding will write. This fieldName will be the same fieldName that is passed in the {@link
     * #createDescriptor(String)} method.
     */
    void bindToSchema(FieldResolver fieldResolver, String fieldName);

    /**
     * Called during data processing to read the value from the ResultSet at the given column and
     * write it to the bound field at the given row.
     */
    void readIntoField(int rowId, ResultSet resultSet, int columnIndex) throws SQLException;
}
