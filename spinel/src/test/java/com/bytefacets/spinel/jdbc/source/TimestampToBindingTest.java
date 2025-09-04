// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.jdbc.source;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bytefacets.spinel.schema.FieldDescriptor;
import com.bytefacets.spinel.schema.FieldResolver;
import com.bytefacets.spinel.schema.LongWritableField;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Calendar;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TimestampToBindingTest {
    private final TimestampToLongBinding binding = new TimestampToLongBinding();
    private @Mock FieldResolver resolver;
    private @Mock LongWritableField field;
    private @Mock ResultSet resultSet;

    @Test
    void shouldCreateFieldDescriptor() {
        assertThat(
                binding.createDescriptor("some_field"),
                equalTo(FieldDescriptor.longField("some_field")));
    }

    @Test
    void shouldBindToSchema() {
        when(resolver.findLongField("foo")).thenReturn(field);
        binding.bindToSchema(resolver, "foo");
        verify(resolver, times(1)).findLongField("foo");
    }

    @Test
    void shouldReadIntoField() throws Exception {
        when(resolver.findLongField("foo")).thenReturn(field);
        final long epochMs = Instant.parse("2025-09-03T03:23:45Z").toEpochMilli();
        when(resultSet.getTimestamp(eq(4), any(Calendar.class))).thenReturn(new Timestamp(epochMs));

        binding.bindToSchema(resolver, "foo");
        binding.readIntoField(36, resultSet, 4);
        verify(field, times(1)).setValueAt(36, epochMs);
    }
}
