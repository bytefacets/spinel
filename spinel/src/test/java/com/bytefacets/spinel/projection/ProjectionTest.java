// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.projection;

import static com.bytefacets.spinel.printer.OutputLoggerBuilder.logger;
import static com.bytefacets.spinel.schema.FieldDescriptor.intField;
import static com.bytefacets.spinel.table.IntIndexedTableBuilder.intIndexedTable;

import com.bytefacets.spinel.projection.lib.IntCalculation;
import com.bytefacets.spinel.projection.lib.IntFieldCalculation;
import com.bytefacets.spinel.schema.FieldResolver;
import com.bytefacets.spinel.schema.IntField;
import com.bytefacets.spinel.table.IntIndexedTable;
import com.bytefacets.spinel.validation.Key;
import com.bytefacets.spinel.validation.RowData;
import com.bytefacets.spinel.validation.ValidationOperator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ProjectionTest {
    private final ProjectionBuilder builder = ProjectionBuilder.projection();
    private ValidationOperator validation =
            new ValidationOperator(new String[] {"Id"}, "Value1", "Value2", "Sum", "Value2^2");
    private Projection projection;
    private IntIndexedTable table;
    private int value1FieldId;
    private int value2FieldId;

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

    @AfterEach
    void tearDown() {
        validation.assertNoActiveValidation();
    }

    void initialize() {
        projection = builder.build();
        projection.output().attachInput(logger().build());
        projection.output().attachInput(validation.input());
        table.output().attachInput(projection.input());
    }

    @Nested
    class SchemaTests {
        @Test
        void shouldProduceSchema() {
            builder.lazyCalculation("Sum", sum()).lazyCalculation("Value2^2", v2Squared());
            initialize();
            validation.expect().schema(expectedSchema()).validate();
        }

        private Map<String, Class<?>> expectedSchema() {
            final Map<String, Class<?>> map = new HashMap<>();
            Stream.of("Id", "Value1", "Value2", "Sum", "Value2^2")
                    .forEach(n -> map.put(n, Integer.class));
            return map;
        }
    }

    @Nested
    class AddTests {
        @Test
        void shouldForwardAddedRows() {
            builder.lazyCalculation("Sum", sum()).lazyCalculation("Value2^2", v2Squared());
            initialize();
            validation.clearChanges();
            addSourceRow(1, 10, 100);
            addSourceRow(2, 20, 200);
            table.fireChanges();

            validation
                    .expect()
                    .added(key(1), rowData(10, 100))
                    .added(key(2), rowData(20, 200))
                    .validate();
        }
    }

    @Nested
    class ChangeTests {
        @BeforeEach
        void setUp() {
            validation = new ValidationOperator(new String[] {"Id"}, "Value2", "Value2^2");
            builder.omit("Value1").lazyCalculation("Value2^2", v2Squared());
            initialize();
            addSourceRow(1, 10, 100);
            addSourceRow(2, 20, 200);
            table.fireChanges();
            validation.clearChanges();
        }

        @Test
        void shouldForwardDependentChanges() {
            changeValue2(1, 5);
            changeValue2(2, 6);
            table.fireChanges();
            validation
                    .expect()
                    .changed(key(1), new RowData(Map.of("Value2", 5, "Value2^2", 25)))
                    .changed(key(2), new RowData(Map.of("Value2", 6, "Value2^2", 36)))
                    .validate();
        }

        @Test
        void shouldNotForwardChangesWhenInboundFieldsHaveNoEffect() {
            changeValue1(1, 30);
            changeValue1(2, 40);
            table.fireChanges();
            validation.validateNoChanges();
        }
    }

    @Nested
    class RemoveTests {
        @BeforeEach
        void setUp() {
            builder.lazyCalculation("Sum", sum()).lazyCalculation("Value2^2", v2Squared());
            initialize();
            addSourceRow(1, 10, 100);
            addSourceRow(2, 20, 200);
            table.fireChanges();
            validation.clearChanges();
        }

        @Test
        void shouldForwardRemovedRows() {
            table.remove(2);
            table.fireChanges();
            validation.expect().removed(key(2)).validate();
        }
    }

    private IntFieldCalculation sum() {
        return new IntFieldCalculation() {
            IntField v1;
            IntField v2;

            @Override
            public int calculate(final int row) {
                return v1.valueAt(row) + v2.valueAt(row);
            }

            @Override
            public void bindToSchema(final FieldResolver fieldResolver) {
                v1 = (IntField) fieldResolver.findField("Value1");
                v2 = (IntField) fieldResolver.findField("Value2");
            }

            @Override
            public void unbindSchema() {
                v1 = null;
                v2 = null;
            }
        };
    }

    private IntFieldCalculation v2Squared() {
        return new IntCalculation("Value2") {
            @Override
            protected int calculateValue(final int value) {
                return value * value;
            }
        };
    }

    private Key key(final int key) {
        return new Key(List.of(key));
    }

    private RowData rowData(final int value, final int value2) {
        return new RowData(
                Map.of(
                        "Value1",
                        value,
                        "Value2",
                        value2,
                        "Sum",
                        value + value2,
                        "Value2^2",
                        value2 * value2));
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
