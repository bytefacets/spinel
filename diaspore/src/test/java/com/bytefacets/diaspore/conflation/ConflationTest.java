// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.conflation;

import static com.bytefacets.diaspore.schema.FieldDescriptor.intField;
import static com.bytefacets.diaspore.table.IntIndexedTableBuilder.intIndexedTable;
import static com.bytefacets.diaspore.validation.Key.key;

import com.bytefacets.diaspore.printer.OutputPrinter;
import com.bytefacets.diaspore.table.IntIndexedTable;
import com.bytefacets.diaspore.testing.IntTableHandle;
import com.bytefacets.diaspore.validation.RowData;
import com.bytefacets.diaspore.validation.ValidationOperator;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ConflationTest {
    private static final boolean print = true;
    private final ConflationBuilder builder = ConflationBuilder.conflation();
    private ValidationOperator validation =
            new ValidationOperator(new String[] {"Id"}, "Value1", "Value2");
    private Conflation conflation;
    private IntIndexedTable table;
    private IntTableHandle tableHandle;
    private final RowData.RowDataTemplate template = RowData.template("Value1", "Value2");

    @BeforeEach
    void setUp() {
        builder.initialCapacity(2).maxPendingRows(3);
        table =
                intIndexedTable("table")
                        .addFields(intField("Value1"), intField("Value2"))
                        .keyFieldName("Id")
                        .build();
        tableHandle = IntTableHandle.intTableHandle("Id", table);
    }

    @AfterEach
    void tearDown() {
        validation.assertNoActiveValidation();
    }

    void initialize() {
        conflation = builder.build();
        if (print) {
            conflation.output().attachInput(OutputPrinter.printer().input());
        }
        conflation.output().attachInput(validation.input());
        table.output().attachInput(conflation.input());
    }

    @Nested
    class SchemaTests {} // REVISIT

    @Nested
    class AddTests {
        @Test
        void shouldFireAddsImmediately() {
            initialize();
            validation.clearChanges();
            tableHandle.add(1, 10, 100).add(2, 20, 200).fire();
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
            initialize();
            IntStream.rangeClosed(1, 10).forEach(i -> tableHandle.add(i, i * 10, i * 100));
            tableHandle.fire();
            validation.clearChanges();
        }

        @Test
        void shouldHoldAddsWithinTheMaxPendingRows() {
            for (int i = 0; i < 10; i++) {
                tableHandle.change(2, 2000 + i, null).change(3, 3000 + i, null).fire();
                validation.validateNoChanges();
                validation.clearChanges();
            }
        }

        @Test
        void shouldReleaseOldestRowsWhenMaxPendingBreached() {
            tableHandle.change(2, 2000, null).change(3, 3000, null).fire();
            validation.clearChanges();
            // when
            tableHandle.change(2, 2001, null).change(4, 4001, null).fire();
            validation
                    .expect()
                    .changed(key(2), rowData(2001, null))
                    .changed(key(3), rowData(3000, null))
                    .validate();
        }

        @Test
        void shouldReleasePendingChangesWhenExplicitlyTriggered() {
            tableHandle.change(2, 2000, null).change(3, 3000, null).fire();
            validation.clearChanges();
            // when
            conflation.firePendingChanges();
            validation
                    .expect()
                    .changed(key(2), rowData(2000, null))
                    .changed(key(3), rowData(3000, null))
                    .validate();
        }

        @Test
        void shouldAccumulateFieldChanges() {
            tableHandle.change(2, 2000, null).fire();
            tableHandle.change(3, null, 3000).fire();
            validation.clearChanges();
            // when
            conflation.firePendingChanges();
            validation
                    .expect()
                    .changed(key(2), rowData(2000, 200))
                    .changed(key(3), rowData(30, 3000))
                    .validate();
        }

        @Test
        void shouldResetFieldChanges() {
            tableHandle.change(2, 2000, null).change(3, 3000, null).fire();

            // when
            tableHandle.change(4, null, 401).change(5, null, 501).fire();
            // accumulated
            validation
                    .expect()
                    .changed(key(2), rowData(2000, null))
                    .changed(key(3), rowData(3000, null))
                    .validate();
            validation.clearChanges();

            conflation.firePendingChanges();
            // reset
            validation
                    .expect()
                    .changed(key(4), rowData(null, 401))
                    .changed(key(5), rowData(null, 501))
                    .validate();
        }
    }

    @Nested
    class RemoveTests {
        @BeforeEach
        void setUp() {
            initialize();
            IntStream.rangeClosed(1, 10).forEach(i -> tableHandle.add(i, i * 10, i * 100));
            tableHandle.fire();
            validation.clearChanges();
        }

        @Test
        void shouldPassRemovesThruDirectly() {
            tableHandle.remove(2).remove(3).fire();
            validation.expect().removed(key(2)).removed(key(3)).validate();
        }

        @Test
        void shouldReleasePendingChangesSkippingRemovedRows() {
            tableHandle.change(2, 2000, null).change(3, 3000, null).fire();
            tableHandle.remove(3).fire();
            validation.expect().changed(key(2), rowData(2000, null)).removed(key(3)).validate();
        }
    }

    private RowData rowData(final Object val1, final Object val2) {
        return template.rowData(val1, val2);
    }
}
