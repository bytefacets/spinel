// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.union;

import static com.bytefacets.spinel.schema.FieldDescriptor.intField;
import static com.bytefacets.spinel.table.IntIndexedTableBuilder.intIndexedTable;

import com.bytefacets.spinel.printer.OutputPrinter;
import com.bytefacets.spinel.table.IntIndexedTable;
import com.bytefacets.spinel.validation.Key;
import com.bytefacets.spinel.validation.RowData;
import com.bytefacets.spinel.validation.ValidationOperator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class UnionTest {
    private static final boolean print = true;
    private final UnionBuilder builder = UnionBuilder.union("TestUnion");
    private final ValidationOperator validation =
            new ValidationOperator(
                    new String[] {"SourceId", "Id"}, "SourceName", "Value1", "Value2");
    private IntIndexedTable table1;
    private IntIndexedTable table2;
    private Union union;
    private TableChanger table1Handle;
    private TableChanger table2Handle;

    @BeforeEach
    void setUp() {
        table1 = createTable("table-1");
        table2 = createTable("table-2");
        table1Handle = new TableChanger(table1);
        table2Handle = new TableChanger(table2);
        builder.initialSize(4).inputIdFieldName("SourceId").inputNameFieldName("SourceName");
    }

    @AfterEach
    void tearDown() {
        validation.assertNoActiveValidation();
    }

    void initialize() {
        union = builder.build();
        if (print) {
            union.output().attachInput(OutputPrinter.printer().input());
        }
        union.output().attachInput(validation.input());
        table1.output().attachInput(union.newInput("t1"));
        table2.output().attachInput(union.newInput("t2"));
    }

    private void populateTables() {
        table1Handle.addSourceRow(1, 10, 100);
        table1Handle.addSourceRow(2, 20, 200);
        table1.fireChanges();
        table2Handle.addSourceRow(3, 30, 300);
        table2Handle.addSourceRow(4, 40, 400);
        table2.fireChanges();
    }

    @Nested
    class SchemaTests {
        @Test
        void shouldForwardRowsAsInputIsAdded() {
            union = builder.build();
            if (print) {
                union.output().attachInput(OutputPrinter.printer().input());
            }
            table1.output().attachInput(union.newInput("t1"));
            union.output().attachInput(validation.input());
            populateTables();
            validation.clearChanges();

            // when
            table2.output().attachInput(union.newInput("t2"));
            validation
                    .expect()
                    .added(key(1, 3), rowData("t2", 30, 300))
                    .added(key(1, 4), rowData("t2", 40, 400))
                    .validate();
        }

        @Test
        void shouldRemoveInitialInput() {
            initialize();
            populateTables();
            validation.clearChanges();
            // when
            table2.output().detachInput(union.input("t2"));
            validation.expect().removed(key(1, 3)).removed(key(1, 4)).validate();
        }
    }

    @Nested
    class AddTests {
        @Test
        void shouldAddSource0() {
            validation.clearChanges();
            initialize();
            table1Handle.addSourceRow(1, 10, 100);
            table1Handle.addSourceRow(2, 20, 200);
            table1.fireChanges();
            validation
                    .expect()
                    .added(key(0, 1), rowData("t1", 10, 100))
                    .added(key(0, 2), rowData("t1", 20, 200))
                    .validate();
        }

        @Test
        void shouldAddSource1() {
            initialize();
            table1Handle.addSourceRow(1, 10, 100);
            table1Handle.addSourceRow(2, 20, 200);
            table1.fireChanges();
            validation.clearChanges();
            // when
            table2Handle.addSourceRow(3, 30, 300);
            table2Handle.addSourceRow(4, 40, 400);
            table2.fireChanges();
            // then
            validation
                    .expect()
                    .added(key(1, 3), rowData("t2", 30, 300))
                    .added(key(1, 4), rowData("t2", 40, 400))
                    .validate();
        }
    }

    @Nested
    class ChangeTests {
        @BeforeEach
        void setUp() {
            initialize();
            populateTables();
            validation.clearChanges();
        }

        @Test
        void shouldForwardFieldChanges() {
            table2Handle.changeValue1(3, 60);
            table2.fireChanges();
            //
            validation.expect().changed(key(1, 3), rowData(null, 60, null)).validate();
        }
    }

    @Nested
    class RemoveTests {
        @BeforeEach
        void setUp() {
            initialize();
            populateTables();
            validation.clearChanges();
        }

        @Test
        void shouldForwardFieldRemoves() {
            table2.remove(3);
            table2.remove(4);
            table2.fireChanges();
            //
            validation.expect().removed(key(1, 3)).removed(key(1, 4)).validate();
        }
    }

    private IntIndexedTable createTable(final String name) {
        return intIndexedTable(name)
                .addFields(intField("Value1"), intField("Value2"))
                .keyFieldName("Id")
                .build();
    }

    private static final class TableChanger {
        private final IntIndexedTable table;
        private final int value1FieldId;
        private final int value2FieldId;

        private TableChanger(final IntIndexedTable table) {
            this.table = table;
            value1FieldId = table.fieldId("Value1");
            value2FieldId = table.fieldId("Value2");
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

    private Key key(final int source, final int key) {
        return new Key(List.of(source, key));
    }

    private RowData rowData(final String name, final Integer value1, final Integer value2) {
        final Map<String, Object> data = new HashMap<>();
        putIfNotNull(data, "SourceName", name);
        putIfNotNull(data, "Value1", value1);
        putIfNotNull(data, "Value2", value2);
        return new RowData(data);
    }

    private void putIfNotNull(
            final Map<String, Object> map, final String name, final Object value) {
        if (value != null) {
            map.put(name, value);
        }
    }
}
