// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.jdbc.source;

import static com.bytefacets.spinel.jdbc.source.TimeToIntBinding.fromTime;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bytefacets.spinel.schema.FieldDescriptor;
import com.bytefacets.spinel.schema.FieldResolver;
import com.bytefacets.spinel.schema.IntWritableField;
import java.sql.ResultSet;
import java.sql.Time;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Calendar;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TimeToIntBindingTest {
    private final TimeToIntBinding binding = new TimeToIntBinding();
    private @Mock FieldResolver resolver;
    private @Mock IntWritableField field;
    private @Mock ResultSet resultSet;

    @Test
    void shouldCreateFieldDescriptor() {
        assertThat(
                binding.createDescriptor("some_field"),
                equalTo(FieldDescriptor.intField("some_field")));
    }

    @Test
    void shouldBindToSchema() {
        when(resolver.findIntField("foo")).thenReturn(field);
        binding.bindToSchema(resolver, "foo");
        verify(resolver, times(1)).findIntField("foo");
    }

    @Test
    void shouldReadIntoField() throws Exception {
        when(resolver.findIntField("foo")).thenReturn(field);
        final Instant instant = Instant.parse("2025-09-03T03:23:45Z");
        final long epochMs = instant.toEpochMilli();
        when(resultSet.getTime(eq(4), any(Calendar.class))).thenReturn(new Time(epochMs));

        binding.bindToSchema(resolver, "foo");
        binding.readIntoField(36, resultSet, 4);
        final LocalTime expectedTime = instant.atZone(ZoneId.systemDefault()).toLocalTime();
        verify(field, times(1))
                .setValueAt(
                        36,
                        fromTime(
                                expectedTime.getHour(),
                                expectedTime.getMinute(),
                                expectedTime.getSecond()));
    }
}
