// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.jdbc.source;

import com.bytefacets.spinel.schema.FieldDescriptor;
import com.bytefacets.spinel.schema.FieldResolver;
import com.bytefacets.spinel.schema.IntWritableField;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * Translates a JDBC Time field to an int field. Note that Time handling accesses the default
 * timezone when "normalizing."
 */
final class TimeToIntBinding implements JdbcFieldBinding {
    private static final Calendar UTC = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    private IntWritableField field;

    TimeToIntBinding() {}

    @Override
    public FieldDescriptor createDescriptor(final String fieldName) {
        return FieldDescriptor.intField(fieldName);
    }

    @Override
    public void bindToSchema(final FieldResolver fieldResolver, final String fieldName) {
        field = (IntWritableField) fieldResolver.findIntField(fieldName);
    }

    /**
     * Reads the given column using {@link ResultSet#getTime(int, Calendar)} and if not null,
     * encodes the time into an int and writes it to the Schema's field in the given row. Note that
     * Time handling accesses the default timezone when "normalizing."
     */
    @Override
    public void readIntoField(final int rowId, final ResultSet resultSet, final int columnIndex)
            throws SQLException {
        final Time time = resultSet.getTime(columnIndex, UTC);
        if (time != null) {
            field.setValueAt(rowId, fromSqlTime(time));
        }
    }

    /** Uses the same logic as {@link Time#toLocalTime()}, but does not create the object. */
    @SuppressWarnings("deprecation")
    static int fromSqlTime(final Time time) {
        // e.g. date.toLocalTime()
        return fromTime(time.getHours(), time.getMinutes(), time.getSeconds());
    }

    /** Encodes time into int as HHmmss. */
    static int fromTime(final int hour, final int minute, final int second) {
        return (hour * 10000) + (minute * 100) + second;
    }
}
