// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.conflation;

import static com.bytefacets.spinel.printer.OutputLoggerBuilder.logger;
import static com.bytefacets.spinel.schema.FieldDescriptor.intField;
import static com.bytefacets.spinel.table.IntIndexedTableBuilder.intIndexedTable;
import static com.bytefacets.spinel.validation.Key.key;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.bytefacets.spinel.table.IntIndexedTable;
import com.bytefacets.spinel.testing.IntTableHandle;
import com.bytefacets.spinel.transform.TransformBuilder;
import com.bytefacets.spinel.validation.RowData;
import com.bytefacets.spinel.validation.ValidationOperator;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ChangeConflatorTest {
    private final ChangeConflatorBuilder builder = ChangeConflatorBuilder.changeConflator();
    private final ValidationOperator validation =
            new ValidationOperator(new String[] {"Id"}, "Value1", "Value2");
    private ChangeConflator conflation;
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
        conflation.output().attachInput(logger().build());
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

    @Nested
    class TransformTests {
        private final TransformBuilder transform = TransformBuilder.transform();

        @BeforeEach
        void setUp() {
            transform
                    .intIndexedTable("table")
                    .addFields(intField("Value1"), intField("Value2"))
                    .keyFieldName("Id")
                    .then()
                    .changeConflator("conflator")
                    .initialCapacity(2)
                    .maxPendingRows(3);
            transform.build();
            table = transform.lookupNode("table");
            tableHandle = IntTableHandle.intTableHandle("Id", table);
            conflation = transform.lookupNode("conflator");
            conflation.output().attachInput(validation.input());
            IntStream.rangeClosed(1, 10).forEach(i -> tableHandle.add(i, i * 10, i * 100));
            tableHandle.fire();
            validation.clearChanges();
        }

        @Test
        void shouldPassThruConflator() {
            for (int i = 0; i < 10; i++) {
                tableHandle.change(2, 2000 + i, null).change(3, 3000 + i, null).fire();
                validation.validateNoChanges();
                validation.clearChanges();
            }
            assertThat(conflation.changesPending(), equalTo(2));
            conflation.firePendingChanges();
            validation
                    .expect()
                    .changed(key(2), rowData(2009, null))
                    .changed(key(3), rowData(3009, null))
                    .validate();
        }
    }

    private RowData rowData(final Object val1, final Object val2) {
        return template.rowData(val1, val2);
    }
}
