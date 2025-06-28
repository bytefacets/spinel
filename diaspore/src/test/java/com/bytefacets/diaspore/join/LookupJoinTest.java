// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.join;

import static com.bytefacets.diaspore.schema.FieldDescriptor.intField;
import static com.bytefacets.diaspore.table.IntIndexedTableBuilder.intIndexedTable;
import static com.bytefacets.diaspore.validation.Key.key;
import static com.bytefacets.diaspore.validation.RowData.template;

import com.bytefacets.diaspore.printer.OutputPrinter;
import com.bytefacets.diaspore.table.IntIndexedTable;
import com.bytefacets.diaspore.testing.IntTableHandle;
import com.bytefacets.diaspore.validation.RowData;
import com.bytefacets.diaspore.validation.ValidationOperator;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LookupJoinTest {
    private ValidationOperator validation =
            new ValidationOperator(new String[] {"LId", "RId"}, "LKey", "LValue", "RValue");
    private IntIndexedTable left;
    private IntIndexedTable right;
    private IntTableHandle leftHandle;
    private IntTableHandle rightHandle;
    private RowData.RowDataTemplate template;

    @BeforeEach
    void setUp() {
        left =
                intIndexedTable("left")
                        .addFields(intField("LKey"), intField("LValue"))
                        .keyFieldName("LId")
                        .build();
        right =
                intIndexedTable("right")
                        .addFields(intField("RKey"), intField("RValue"))
                        .keyFieldName("RId")
                        .build();
        leftHandle = IntTableHandle.intTableHandle("LId", left);
        rightHandle = IntTableHandle.intTableHandle("RId", right);
        template = template("LKey", "LValue", "RValue");
    }

    @AfterEach
    void tearDown() {
        validation.assertNoActiveValidation();
    }

    private void initialize(final JoinBuilder builder) {
        final Join join = builder.joinOn(List.of("LKey"), List.of("RKey"), 2).build();
        left.output().attachInput(join.leftInput());
        right.output().attachInput(join.rightInput());
        join.output().attachInput(OutputPrinter.printer().input());
        join.output().attachInput(validation.input());
    }

    private void addLeft() {
        leftHandle.add(1, 100, 1000).add(2, 200, 2000).fire();
    }

    private void addRight() {
        rightHandle.add(-1, 100, 1111).add(-2, 200, 2222).fire();
    }

    private void clearExpectations() {
        validation.clearChanges();
    }

    @Nested
    class InnerLookupTests {
        private final JoinBuilder builder = JoinBuilder.lookupJoin().inner();

        @BeforeEach
        void setUp() {}

        @Nested
        class SchemaTests {}

        @Nested
        class AddTests {
            @BeforeEach
            void setUp() {
                initialize(builder);
                clearExpectations();
            }

            @Test
            void shouldNotAddWhenLeftOnly() {
                addLeft();
                validation.validateNoChanges();
            }

            @Test
            void shouldNotAddWhenRightOnly() {
                addRight();
                validation.validateNoChanges();
            }

            @Test
            void shouldAddWhenLeftThenRight() {
                addLeft();
                addRight();
                validation
                        .expect()
                        .added(key(1, -1), template.rowData(100, 1000, 1111))
                        .added(key(2, -2), template.rowData(200, 2000, 2222))
                        .validate();
            }
        }

        @Nested
        class ChangeTests {
            @BeforeEach
            void setUp() {
                initialize(builder);
                clearExpectations();
            }

            @Test
            void shouldPassLeftChangesThruWhenMapped() {
                addLeft();
                addRight();
                clearExpectations();
                // when
                leftHandle.change(1, null, 1001).change(2, null, 2002).fire();
                validation
                        .expect()
                        .changed(key(1, -1), rowData(null, 1001, null))
                        .changed(key(2, -2), rowData(null, 2002, null))
                        .validate();
            }

            @Test
            void shouldPassRightChangesThruWhenMapped() {
                addLeft();
                addRight();
                clearExpectations();
                // when
                rightHandle.change(-1, null, 1110).change(-2, null, 2220).fire();
                validation
                        .expect()
                        .changed(key(1, -1), rowData(null, null, 1110))
                        .changed(key(2, -2), rowData(null, null, 2220))
                        .validate();
            }

            @Test
            void shouldNotPassLeftChangesThruWhenUnmapped() {
                addLeft();
                clearExpectations();
                // when
                leftHandle.change(1, null, 1001).change(2, null, 2002).fire();
                validation.validateNoChanges();
            }

            @Test
            void shouldNotPassRightChangesThruWhenUnmapped() {
                addRight();
                clearExpectations();
                rightHandle.change(-1, null, 1110).change(-2, null, 2220).fire();
                validation.validateNoChanges();
            }

            @Test
            void shouldRemoveRowWhenLeftKeyChangeToUnmapped() {
                addLeft();
                addRight();
                clearExpectations();
                leftHandle.change(1, 500, null).change(2, 600, null).fire();
                validation.expect().removed(key(1, -1)).removed(key(2, -2)).validate();
            }

            @Test
            void shouldReplaceRightRowWhenLeftKeyChangeToAnotherRight() {
                addLeft();
                addRight();
                clearExpectations();
                leftHandle.change(1, 200, null).fire();
                validation.expect().changed(key(1, -2), rowData(200, null, 2222)).validate();
            }

            @Test
            void shouldRemoveRowWhenRightKeyChangeToUnmapped() {
                addLeft();
                addRight();
                clearExpectations();
                rightHandle.change(-1, 500, null).change(-2, 600, null).fire();
                validation.expect().removed(key(1, -1)).removed(key(2, -2)).validate();
            }

            @Test
            void shouldAddRowWhenRightKeyChangeToAnotherLeft() {
                addLeft();
                leftHandle.add(3, 300, 3000).add(4, 400, 4000).fire();
                addRight();
                clearExpectations();
                rightHandle.change(-1, 300, null).change(-2, 400, null).fire();
                validation
                        .expect()
                        .removed(key(1, -1))
                        .removed(key(2, -2))
                        .added(key(3, -1), rowData(300, 3000, 1111))
                        .added(key(4, -2), rowData(400, 4000, 2222))
                        .validate();
            }
        }

        @Nested
        class RemoveTests {
            @BeforeEach
            void setUp() {
                initialize(builder);
                addLeft();
                addRight();
                leftHandle.add(3, 300, 3000).add(4, 400, 4000).fire();
                rightHandle.add(-5, 500, 5555).add(-6, 600, 6666).fire();
                clearExpectations();
            }

            @Test
            void shouldNotEmitWhenLeftRemoveIsNotMapped() {
                leftHandle.remove(3).remove(4).fire();
                validation.validateNoChanges();
            }

            @Test
            void shouldNotEmitWhenRightRemoveIsNotMapped() {
                rightHandle.remove(-5).remove(-6).fire();
                validation.validateNoChanges();
            }

            @Test
            void shouldEmitRemoveWhenLeftRemoveIsMapped() {
                leftHandle.remove(1).remove(2).fire();
                validation.expect().removed(key(1, -1)).removed(key(2, -2)).validate();
            }

            @Test
            void shouldEmitRemoveWhenRightRemoveIsMapped() {
                rightHandle.remove(-1).remove(-2).fire();
                validation.expect().removed(key(1, -1)).removed(key(2, -2)).validate();
            }
        }
    }

    @Nested
    class OuterLookupTests {
        private final JoinBuilder builder = JoinBuilder.lookupJoin().outer();

        @Nested
        class SchemaTests {}

        @Nested
        class AddTests {
            @BeforeEach
            void setUp() {
                initialize(builder);
                clearExpectations();
            }

            @Test
            void shouldAddWhenLeftOnly() {
                addLeft();
                validation
                        .expect()
                        .added(key(1, 0), rowData(100, 1000, 0))
                        .added(key(2, 0), rowData(200, 2000, 0))
                        .validate();
            }

            @Test
            void shouldNotAddWhenRightOnly() {
                addRight();
                validation.validateNoChanges();
            }

            @Test
            void shouldAddWhenRightThenLeft() {
                addRight();
                addLeft();
                validation
                        .expect()
                        .added(key(1, -1), rowData(100, 1000, 1111))
                        .added(key(2, -2), rowData(200, 2000, 2222))
                        .validate();
            }
        }

        @Nested
        class ChangeTests {
            @BeforeEach
            void setUp() {
                initialize(builder);
                clearExpectations();
            }

            @Test
            void shouldPassLeftChangesThruWhenNoRight() {
                addLeft();
                clearExpectations();
                // when
                leftHandle.change(1, null, 1001).change(2, null, 2002).fire();
                // then
                validation
                        .expect()
                        .changed(key(1, 0), rowData(null, 1001, null))
                        .changed(key(2, 0), rowData(null, 2002, null))
                        .validate();
            }

            @Test
            void shouldPassLeftChangesThruWhenRight() {
                addLeft();
                addRight();
                clearExpectations();
                // when
                leftHandle.change(1, null, 1001).change(2, null, 2002).fire();
                // then
                validation
                        .expect()
                        .changed(key(1, -1), rowData(null, 1001, null))
                        .changed(key(2, -2), rowData(null, 2002, null))
                        .validate();
            }

            @Test
            void shouldNotPassRightChangesThruWhenNoLeft() {
                addRight();
                clearExpectations();
                // when
                rightHandle.change(-1, null, 1110).change(-2, null, 2220).fire();
                // then
                validation.validateNoChanges();
            }

            @Test
            void shouldPassRightChangesThruWhenLeft() {
                addLeft();
                addRight();
                clearExpectations();
                // when
                rightHandle.change(-1, null, 1110).change(-2, null, 2220).fire();
                // then
                validation
                        .expect()
                        .changed(key(1, -1), rowData(null, null, 1110))
                        .changed(key(2, -2), rowData(null, null, 2220))
                        .validate();
            }

            @Test
            void leftKeyChangeShouldReplaceRightSide() {
                addLeft();
                addRight();
                clearExpectations();
                // when
                leftHandle.change(1, 500, null).change(2, 600, null).fire();
                // then
                validation
                        .expect()
                        .changed(key(1, 0), rowData(500, null, 0))
                        .changed(key(2, 0), rowData(600, null, 0))
                        .validate();
            }

            @Test
            void rightKeyChangeToUnmappedRowShouldReplaceRightSide() {
                addLeft();
                addRight();
                clearExpectations();
                System.out.println();
                // when
                rightHandle.change(-1, 500, null).change(-2, 600, null).fire();
                // then
                validation
                        .expect()
                        .changed(key(1, 0), rowData(null, null, 0))
                        .changed(key(2, 0), rowData(null, null, 0))
                        .validate();
            }

            @Test
            void shouldUpdateWhenLeftThenRight() {
                addLeft();
                clearExpectations();
                addRight();
                validation
                        .expect()
                        .changed(key(1, -1), rowData(null, null, 1111))
                        .changed(key(2, -2), rowData(null, null, 2222))
                        .validate();
            }
        }

        @Nested
        class RemoveTests {
            @BeforeEach
            void setUp() {
                initialize(builder);
                addLeft();
                addRight();
                leftHandle.add(3, 300, 3000).add(4, 400, 4000).fire();
                rightHandle.add(-5, 500, 5555).add(-6, 600, 6666).fire();
                clearExpectations();
            }

            @Test
            void shouldEmitRemoveWhenLeftRemoveIsNotMapped() {
                leftHandle.remove(3).remove(4).fire();
                validation.expect().removed(key(3, 0)).removed(key(4, 0)).validate();
            }

            @Test
            void shouldNotEmitWhenRightRemoveIsNotMapped() {
                rightHandle.remove(-5).remove(-6).fire();
                validation.validateNoChanges();
            }

            @Test
            void shouldEmitRemoveWhenLeftRemoveIsMapped() {
                leftHandle.remove(1).remove(2).fire();
                validation.expect().removed(key(1, -1)).removed(key(2, -2)).validate();
            }

            @Test
            void shouldEmitChangeWhenRightRemoveIsMapped() {
                rightHandle.remove(-1).remove(-2).fire();
                validation
                        .expect()
                        .changed(key(1, 0), rowData(null, null, 0))
                        .changed(key(2, 0), rowData(null, null, 0))
                        .validate();
            }
        }
    }

    private RowData rowData(
            final Integer leftKey, final Integer leftValue, final Integer rightValue) {
        return template.rowData(leftKey, leftValue, rightValue);
    }
}
