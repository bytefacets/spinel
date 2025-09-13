// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats;

import static com.bytefacets.spinel.nats.FieldSequenceNatsSubjectBuilder.fieldSequenceNatsSubjectBuilder;
import static com.bytefacets.spinel.schema.ArrayFieldFactory.writableIntArrayField;
import static com.bytefacets.spinel.schema.ArrayFieldFactory.writableStringArrayField;
import static com.bytefacets.spinel.schema.FieldList.fieldList;
import static com.bytefacets.spinel.schema.Schema.schema;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.bytefacets.spinel.schema.IntWritableField;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.StringWritableField;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FieldSequenceNatsSubjectBuilderTest {
    final FieldSequenceNatsSubjectBuilder builder =
            fieldSequenceNatsSubjectBuilder("pre.", List.of("f1", "f2"));
    private IntWritableField f1;
    private StringWritableField f2;
    private Schema schema;

    @BeforeEach
    void setUp() {
        final IntWritableField f0 = writableIntArrayField(10, 0, i -> {});
        f1 = writableIntArrayField(10, 1, i -> {});
        f2 = writableStringArrayField(10, 2, i -> {});
        schema = schema("test", fieldList(Map.of("f0", f0, "f1", f1, "f2", f2)));
    }

    @Test
    void shouldCreateSubject() {
        builder.bindToSchema(schema.asFieldResolver());
        set(4, 2, "eiuy38");
        assertThat(builder.buildSubject(4), equalTo("pre.2.eiuy38"));
    }

    @Test
    void shouldHandleNullValue() {
        builder.bindToSchema(schema.asFieldResolver());
        set(4, 2, null);
        assertThat(builder.buildSubject(4), equalTo("pre.2.null"));
    }

    @Test
    void shouldResetBuilderBetweenRows() {
        builder.bindToSchema(schema.asFieldResolver());
        set(4, 3, "a");
        set(5, 6, "b");
        assertThat(builder.buildSubject(4), equalTo("pre.3.a"));
        assertThat(builder.buildSubject(5), equalTo("pre.6.b"));
    }

    private void set(final int row, final int f1Val, final String f2Val) {
        f1.setValueAt(row, f1Val);
        f2.setValueAt(row, f2Val);
    }
}
