// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats;

import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.FieldResolver;
import java.util.ArrayList;
import java.util.List;

/** Creates a NATS subject from a row in a schema. */
public final class FieldSequenceNatsSubjectBuilder implements NatsSubjectBuilder {
    private final List<String> fieldNames;
    private final List<ValueAppender> appenders = new ArrayList<>(4);
    private final StringBuilder sb = new StringBuilder(16);
    private final int resetLength;

    public static FieldSequenceNatsSubjectBuilder fieldSequenceNatsSubjectBuilder(
            final List<String> fieldNames) {
        return new FieldSequenceNatsSubjectBuilder("", fieldNames);
    }

    public static FieldSequenceNatsSubjectBuilder fieldSequenceNatsSubjectBuilder(
            final String prefix, final List<String> fieldNames) {
        return new FieldSequenceNatsSubjectBuilder(prefix, fieldNames);
    }

    private FieldSequenceNatsSubjectBuilder(final String prefix, final List<String> fieldNames) {
        this.fieldNames = requireNonNull(fieldNames, "fieldNames");
        if (prefix != null) {
            sb.append(prefix);
            resetLength = prefix.length();
        } else {
            resetLength = 0;
        }
    }

    @Override
    public String buildSubject(final int row) {
        sb.setLength(resetLength);
        appenders.forEach(app -> app.append(row, sb));
        if (sb.charAt(sb.length() - 1) == '.') {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    @Override
    public void bindToSchema(final FieldResolver fieldResolver) {
        for (String name : fieldNames) {
            appenders.add(appender(fieldResolver.findField(name)));
        }
    }

    @Override
    public void unbindSchema() {
        appenders.clear();
    }

    private interface ValueAppender {
        void append(int row, StringBuilder sb);
    }

    private static ValueAppender appender(final Field field) {
        return (row, sb) -> sb.append(field.objectValueAt(row)).append('.');
    }
}
