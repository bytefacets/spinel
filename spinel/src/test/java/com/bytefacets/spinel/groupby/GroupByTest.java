// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.groupby;

import static com.bytefacets.spinel.interner.IntRowInterner.intInterner;
import static com.bytefacets.spinel.printer.OutputLoggerBuilder.logger;
import static com.bytefacets.spinel.schema.FieldDescriptor.intField;
import static com.bytefacets.spinel.table.IntIndexedTableBuilder.intIndexedTable;

import com.bytefacets.spinel.groupby.lib.SumFactory;
import com.bytefacets.spinel.table.IntIndexedTable;
import com.bytefacets.spinel.validation.Key;
import com.bytefacets.spinel.validation.RowData;
import com.bytefacets.spinel.validation.ValidationOperator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class GroupByTest {
    private final GroupByBuilder builder = GroupByBuilder.groupBy();
    private final ValidationOperator validation =
            new ValidationOperator(new String[] {"GroupId"}, "Count", "Value1", "Sum2");
    private final ValidationOperator childValidation =
            new ValidationOperator(new String[] {"Id"}, "GroupId", "Value1", "Value2");
    private IntIndexedTable table;
    private int value1FieldId;
    private int value2FieldId;
    private GroupBy groupBy;

    @BeforeEach
    void setUp() {
        table =
                intIndexedTable("table")
                        .addField(intField("Value1"))
                        .addField(intField("Value2"))
                        .addField(intField("Value3"))
                        .keyFieldName("Id")
                        .build();
        value1FieldId = table.fieldId("Value1");
        value2FieldId = table.fieldId("Value2");
        builder.groupByFunction(intInterner("Value1", 16))
                .addForwardedFields("Value1")
                .addAggregation(SumFactory.sumToInt("Value2", "Sum2"))
                .includeCountField("Count")
                .includeGroupIdField("GroupId");
    }

    @AfterEach
    void tearDown() {
        validation.assertNoActiveValidation();
    }

    void initialize() {
        groupBy = builder.build();
        groupBy.parentOutput().attachInput(logger("parent").build());
        groupBy.childOutput().attachInput(logger("child").build());
        groupBy.parentOutput().attachInput(validation.input());
        groupBy.childOutput().attachInput(childValidation.input());
        table.output().attachInput(groupBy.input());
    }

    @Nested
    class SchemaTests {
        @Test
        void shouldCreateParentOutput() {
            initialize();
            validation.expect().schema(parentSchema()).validate();
        }

        @Test
        void shouldCreateChildOutput() {
            initialize();
            childValidation.expect().schema(childSchema()).validate();
        }

        @Test
        void shouldForwardNullSchemaToParentOutput() {
            initialize();
            validation.clearChanges();
            table.output().detachInput(groupBy.input());
            validation.expect().nullSchema().validate();
        }

        @Test
        void shouldForwardNullSchemaToChildOutput() {
            initialize();
            validation.clearChanges();
            table.output().detachInput(groupBy.input());
            childValidation.expect().nullSchema().validate();
        }

        @Test
        void shouldBindAndUnbindGroupFunction() {}

        @Test
        void shouldBindAndUnbindAggregationFunctions() {}
    }

    @Nested
    class AddTests {
        @BeforeEach
        void setUp() {
            initialize();
            validation.clearChanges();
            childValidation.clearChanges();
        }

        @Test
        void shouldCreateGroup() {
            addSourceRow(10, 4, 33);
            table.fireChanges();
            validation.expect().added(key(0), parentRowData(4, 1, 33)).validate();
        }

        @Test
        void shouldAddToGroup() {
            addSourceRow(10, 4, 33);
            table.fireChanges();
            validation.clearChanges();

            addSourceRow(11, 4, 25);
            table.fireChanges();
            validation.expect().changed(key(0), parentRowData(null, 2, 58)).validate();
        }

        @Test
        void shouldDecorateChildRowWithGroup() {
            addSourceRow(10, 4, 33);
            addSourceRow(11, 4, 25);
            table.fireChanges();
            childValidation
                    .expect()
                    .added(key(10), childRowData(0, 4, 33))
                    .added(key(11), childRowData(0, 4, 25))
                    .validate();
        }
    }

    @Nested
    class ChangeTests {
        @BeforeEach
        void setUp() {
            initialize();
            addSourceRow(1, 4, 10);
            addSourceRow(2, 4, 17);
            addSourceRow(3, 5, 11);
            table.fireChanges();
            validation.clearChanges();
            childValidation.clearChanges();
        }

        @Test
        void shouldCreateNewGroupWhenGroupChange() {
            changeValue1(2, 6);
            table.fireChanges();
            validation
                    .expect()
                    // 4 is picked up bc of the change on Value1 in the source table
                    .changed(key(0), parentRowData(4, 1, 10))
                    .added(key(2), parentRowData(6, 1, 17))
                    .validate();
        }

        @Test
        void shouldRemoveEmptyOldGroupWhenGroupChange() {
            changeValue1(3, 4);
            table.fireChanges();
            validation.expect().removed(key(1)).changed(key(0), parentRowData(4, 3, 38)).validate();
        }

        @Test
        void shouldUpdateChangedGroup() {
            changeValue2(2, 31);
            table.fireChanges();
            validation.expect().changed(key(0), parentRowData(null, null, 41)).validate();
        }

        @Test
        void shouldForwardChangesToChildOutput() {
            changeValue2(2, 31);
            table.fireChanges();
            childValidation.expect().changed(key(2), childRowData(null, null, 31)).validate();
        }

        @Test
        void shouldMarkChildGroupIdFieldChanged() {
            changeValue1(2, 6);
            table.fireChanges();
            childValidation.expect().changed(key(2), childRowData(2, 6, null)).validate();
        }

        @Test
        void shouldNotForwardChangesWhenNotInOutput() {
            final var row = table.tableRow();
            table.beginChange(2);
            row.setInt(table.fieldId("Value3"), 5);
            table.endChange();
            table.fireChanges();
            validation.expect().validate();
        }
    }

    @Nested
    class RemoveTests {
        @BeforeEach
        void setUp() {
            initialize();
            addSourceRow(1, 4, 10);
            addSourceRow(2, 4, 17);
            addSourceRow(3, 5, 11);
            table.fireChanges();
            validation.clearChanges();
            childValidation.clearChanges();
        }

        @Test
        void shouldRemoveEmptyGroup() {
            table.remove(3);
            table.fireChanges();
            validation.expect().removed(key(1)).validate();
        }

        @Test
        void shouldRemoveEmptyGroupWhenRemainingRowsExit() {
            IntStream.of(1, 2).forEach(table::remove);
            table.fireChanges();
            validation.expect().removed(key(0)).validate();
        }

        @Test
        void shouldRemoveFromOldGroup() {
            table.remove(1);
            table.fireChanges();
            validation.expect().changed(key(0), parentRowData(null, 1, 17)).validate();
        }

        @Test
        void shouldRemoveFromChildOutput() {
            table.remove(1);
            table.fireChanges();
            childValidation.expect().removed(key(1)).validate();
        }
    }

    private Map<String, Class<?>> parentSchema() {
        return Map.of("GroupId", Integer.class, "Value1", Integer.class, "Sum2", Integer.class);
    }

    private Map<String, Class<?>> childSchema() {
        return Map.of("GroupId", Integer.class, "Value1", Integer.class, "Value2", Integer.class);
    }

    private Key key(final int key) {
        return new Key(List.of(key));
    }

    private RowData childRowData(
            final Integer groupId, final Integer value1, final Integer value2) {
        final Map<String, Object> data = new HashMap<>();
        putIfNotNull(data, "GroupId", groupId);
        putIfNotNull(data, "Value1", value1);
        putIfNotNull(data, "Value2", value2);
        return new RowData(data);
    }

    private RowData parentRowData(final Integer value1, final Integer count, final Integer sum2) {
        final Map<String, Object> data = new HashMap<>();
        putIfNotNull(data, "Value1", value1);
        putIfNotNull(data, "Count", count);
        putIfNotNull(data, "Sum2", sum2);
        return new RowData(data);
    }

    private void putIfNotNull(
            final Map<String, Object> map, final String name, final Object value) {
        if (value != null) {
            map.put(name, value);
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

    private void changeValue2(final int id, final int value2) {
        final var row = table.tableRow();
        table.beginChange(id);
        row.setInt(value2FieldId, value2);
        table.endChange();
    }
}
