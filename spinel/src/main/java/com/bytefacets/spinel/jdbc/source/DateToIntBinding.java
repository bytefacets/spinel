// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.jdbc.source;

import com.bytefacets.spinel.schema.FieldDescriptor;
import com.bytefacets.spinel.schema.FieldResolver;
import com.bytefacets.spinel.schema.IntWritableField;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * Translates a JDBC Date field to an int field. Note that Date handling accesses the default
 * timezone when "normalizing."
 */
final class DateToIntBinding implements JdbcFieldBinding {
    private static final Calendar UTC = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    private IntWritableField field;

    DateToIntBinding() {}

    @Override
    public FieldDescriptor createDescriptor(final String fieldName) {
        return FieldDescriptor.intField(fieldName);
    }

    @Override
    public void bindToSchema(final FieldResolver fieldResolver, final String fieldName) {
        field = (IntWritableField) fieldResolver.findIntField(fieldName);
    }

    /**
     * Reads the given column using {@link ResultSet#getDate(int, Calendar)} and if not null,
     * encodes the date into an int and writes it to the Schema's field in the given row. Note that
     * Date handling accesses the default timezone when "normalizing."
     */
    @Override
    public void readIntoField(final int rowId, final ResultSet resultSet, final int columnIndex)
            throws SQLException {
        final Date date = resultSet.getDate(columnIndex, UTC);
        if (date != null) {
            field.setValueAt(rowId, fromSqlDate(date));
        }
    }

    /** Uses the same logic as {@link Date#toLocalDate()}, but does not create the object. */
    @SuppressWarnings("deprecation")
    static int fromSqlDate(final Date date) {
        // e.g. date.toLocalDate()
        return fromDate(date.getYear() + 1900, date.getMonth() + 1, date.getDate());
    }

    /** Encodes date into int as yyyyMMdd. */
    static int fromDate(final int year, final int month, final int day) {
        return year * 10000 + month * 100 + day;
    }
}
