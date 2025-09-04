// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.jdbc.source;

import com.bytefacets.spinel.schema.FieldDescriptor;
import com.bytefacets.spinel.schema.FieldResolver;
import com.bytefacets.spinel.schema.LongWritableField;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

/** Translates a JDBC Timestamp field to a long field. */
final class TimestampToLongBinding implements JdbcFieldBinding {
    private static final Calendar UTC = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    private LongWritableField field;

    TimestampToLongBinding() {}

    @Override
    public FieldDescriptor createDescriptor(final String fieldName) {
        return FieldDescriptor.longField(fieldName);
    }

    @Override
    public void bindToSchema(final FieldResolver fieldResolver, final String fieldName) {
        field = (LongWritableField) fieldResolver.findLongField(fieldName);
    }

    /**
     * Reads the given column using {@link ResultSet#getTimestamp(int, Calendar)} and if not null,
     * takes the epoch milliseconds from the timestamp and writes it to the Schema's field in the
     * given row. Note that Time handling accesses the default timezone when "normalizing."
     */
    @Override
    public void readIntoField(final int rowId, final ResultSet resultSet, final int columnIndex)
            throws SQLException {
        final Timestamp timestamp = resultSet.getTimestamp(columnIndex, UTC);
        if (timestamp != null) {
            field.setValueAt(rowId, timestamp.getTime());
        }
    }
}
