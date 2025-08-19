// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.prototype;

import static com.bytefacets.spinel.prototype.PrototypeBuilder.prototype;
import static com.bytefacets.spinel.schema.FieldDescriptor.doubleField;
import static com.bytefacets.spinel.schema.FieldDescriptor.intField;
import static com.bytefacets.spinel.schema.FieldDescriptor.longField;
import static com.bytefacets.spinel.schema.FieldDescriptor.stringField;
import static com.bytefacets.spinel.table.IntIndexedTableBuilder.intIndexedTable;
import static com.bytefacets.spinel.testing.IntTableHandle.intTableHandle;
import static com.bytefacets.spinel.validation.Key.key;

import com.bytefacets.spinel.printer.OutputPrinter;
import com.bytefacets.spinel.table.IntIndexedTable;
import com.bytefacets.spinel.testing.IntTableHandle;
import com.bytefacets.spinel.validation.RowData;
import com.bytefacets.spinel.validation.ValidationOperator;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PrototypeTest {
    private static final boolean print = true;
    private final ValidationOperator validation =
            new ValidationOperator(new String[] {"Id"}, "Value1", "Value2");
    private final RowData.RowDataTemplate template = RowData.template("Value1", "Value2");
    private IntIndexedTable table;
    private Prototype prototype;
    private final PrototypeBuilder builder = prototype();
    private IntTableHandle tableHandle;

    @BeforeEach
    void setUp() {
        table =
                intIndexedTable("table")
                        .addField(intField("Value1"))
                        .addField(intField("Value2"))
                        .keyFieldName("Id")
                        .build();
        tableHandle = intTableHandle("Id", table);
    }

    @AfterEach
    void tearDown() {
        validation.assertNoActiveValidation();
    }

    void initialize() {
        prototype = builder.build();
        if (print) {
            prototype.output().attachInput(OutputPrinter.printer().input());
        }
        prototype.output().attachInput(validation.input());
        table.output().attachInput(prototype.input());
    }

    @Nested
    class SchemaTests {
        @Test
        void shouldMakeSchemaAvailableRightAway() {
            builder.addFields(intField("Id"), intField("Value1"), intField("Value2"));
            prototype = builder.build();
            prototype.output().attachInput(validation.input());
            validation.expect().schema(expectedSchema()).validate();
        }

        @Test
        void shouldCastInputToOutputField() {
            builder.addFields(stringField("Id"), longField("Value1"), doubleField("Value2"));
            initialize();
            validation
                    .expect()
                    .schema(
                            Map.of(
                                    "Id",
                                    String.class,
                                    "Value1",
                                    Long.class,
                                    "Value2",
                                    Double.class))
                    .validate();
            validation.clearChanges();
            tableHandle.add(1, 10, 200).fire();
            validation.expect().added(key("1"), template.rowData(10L, 200d)).validate();
        }

        @Test
        void shouldSendRemovesWhenInputSchemaIsNull() {
            builder.addFields(intField("Id"), intField("Value1"), intField("Value2"));
            initialize();
            tableHandle.add(1, 10, 200).add(2, 20, 300).fire();
            validation.clearChanges();
            //
            table.output().detachInput(prototype.input());
            validation.expect().removed(key(1)).removed(key(2)).validate();
        }
    }

    @Nested
    class AddTests {
        @Test
        void shouldAddRows() {
            builder.addFields(intField("Value1"), intField("Value2"), intField("Id"));
            initialize();
            validation.clearChanges();
            tableHandle.add(1, 30, 300).add(2, 40, 400).fire();
            validation
                    .expect()
                    .added(key(1), rowData(30, 300))
                    .added(key(2), rowData(40, 400))
                    .validate();
        }
    }

    @Nested
    class ChangeTests {
        @BeforeEach
        void setUp() {
            builder.addFields(intField("Value1"), intField("Value2"), intField("Id"));
            initialize();
            tableHandle.add(1, 30, 300).add(2, 40, 400).fire();
            validation.clearChanges();
        }

        @Test
        void shouldTranslateFieldIds() {
            tableHandle.change(1, null, 301).change(2, null, 401).fire();
            validation
                    .expect()
                    .changed(key(1), rowData(null, 301))
                    .changed(key(2), rowData(null, 401))
                    .validate();
        }

        @Test
        void shouldNotForwardChangesWhenFieldsDropped() {
            // special set up
            prototype = prototype().addFields(intField("Value1"), intField("Id")).build();
            final ValidationOperator validation =
                    new ValidationOperator(new String[] {"Id"}, "Value1");
            prototype.output().attachInput(validation.input());
            table.output().attachInput(prototype.input());
            validation.clearChanges();
            // given
            // when
            tableHandle.change(1, null, 301).change(2, null, 401).fire();
            // then
            validation.validateNoChanges();
        }
    }

    @Nested
    class RemoveTests {
        @BeforeEach
        void setUp() {
            builder.addFields(intField("Value1"), intField("Value2"), intField("Id"));
            initialize();
            tableHandle.add(1, 30, 300).add(2, 40, 400).fire();
            validation.clearChanges();
        }

        @Test
        void shouldForwardRemoves() {
            tableHandle.remove(2).fire();
            validation.expect().removed(key(2)).validate();
        }
    }

    private Map<String, Class<?>> expectedSchema() {
        return Map.of("Id", Integer.class, "Value1", Integer.class, "Value2", Integer.class);
    }

    private RowData rowData(final Integer value1, final Integer value2) {
        return template.rowData(value1, value2);
    }
}
