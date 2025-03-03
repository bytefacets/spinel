// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.cache;

import static com.bytefacets.diaspore.schema.FieldDescriptor.intField;
import static com.bytefacets.diaspore.table.IntIndexedTableBuilder.intIndexedTable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThrows;

import com.bytefacets.diaspore.exception.SchemaNotBoundException;
import com.bytefacets.diaspore.schema.FieldBitSet;
import com.bytefacets.diaspore.schema.IntField;
import com.bytefacets.diaspore.schema.IntWritableField;
import com.bytefacets.diaspore.table.IntIndexedTable;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CacheTest {
    private final CacheBuilder builder = CacheBuilder.cache();
    private IntIndexedTable table;
    private int value1FieldId;
    private int value2FieldId;
    private Cache cache;
    private IntField value1CacheField;
    private IntField value2CacheField;

    @BeforeEach
    void setUp() {
        table =
                intIndexedTable("table")
                        .addFields(intField("Value1"), intField("Value2"))
                        .keyFieldName("Id")
                        .build();
        value1FieldId = table.fieldId("Value1");
        value2FieldId = table.fieldId("Value2");
    }

    void initialize() {
        cache = builder.build();
        cache.bind(table.schema());
        value1CacheField = cache.resolver().findIntField("Value1");
        value2CacheField = cache.resolver().findIntField("Value2");
    }

    @Nested
    class BindingTests {
        @BeforeEach
        void setUp() {
            builder.cacheFields("Value1", "Value2");
            initialize();
        }

        @Test
        void shouldSetUpCacheFields() {
            for (String name : List.of("Value1", "Value2")) {
                assertThat(cache.resolver().getField(name), instanceOf(IntWritableField.class));
            }
        }

        @Test
        void shouldReleaseFieldsWhenUnbound() {
            cache.unbind();
            assertThrows(SchemaNotBoundException.class, () -> cache.resolver().findField("Value1"));
        }

        /**
         * We don't have a good way of resetting all internal data of the writable field/store, so
         * the simplest thing is to rebuild the fields.
         */
        @Test
        void shouldCreateNewFieldsWhenBoundAgain() {
            cache.unbind();
            cache.bind(table.schema());
            assertThat(cache.resolver().findField("Value1"), not(sameInstance(value1CacheField)));
            assertThat(cache.resolver().findField("Value2"), not(sameInstance(value2CacheField)));
        }
    }

    @Nested
    class CaptureTests {
        @BeforeEach
        void setUp() {
            builder.cacheFields("Value1", "Value2");
            initialize();
            addSourceRow(1, 10, 100);
            addSourceRow(2, 20, 200);
            addSourceRow(3, 30, 300);
        }

        @Test
        void shouldCaptureAllFields() {
            // when
            cache.updateAll(table.output().rowProvider());
            IntStream.rangeClosed(1, 3)
                    .forEach(
                            id -> {
                                final int row = table.lookupKeyRow(id);
                                assertThat(value1CacheField.valueAt(row), equalTo(id * 10));
                                assertThat(value2CacheField.valueAt(row), equalTo(id * 100));
                            });
        }

        @Test
        void shouldCaptureSomeFields() {
            final FieldBitSet changes = FieldBitSet.fieldBitSet();
            changes.fieldChanged(table.fieldId("Value2"));
            // when
            cache.updateSelected(table.output().rowProvider(), changes);
            IntStream.rangeClosed(1, 3)
                    .forEach(
                            id -> {
                                final int row = table.lookupKeyRow(id);
                                assertThat(
                                        value1CacheField.valueAt(row), equalTo(0)); // not updated
                                assertThat(value2CacheField.valueAt(row), equalTo(id * 100));
                            });
        }
    }

    private void addSourceRow(final int id, final int value1, final int value2) {
        final var row = table.tableRow();
        table.beginAdd(id);
        row.setInt(value1FieldId, value1);
        row.setInt(value2FieldId, value2);
        table.endAdd();
    }

    private void changeValue1(final int id, final int value1) {
        final var row = table.tableRow();
        table.beginChange(id);
        row.setInt(value1FieldId, value1);
        table.endChange();
    }
}
