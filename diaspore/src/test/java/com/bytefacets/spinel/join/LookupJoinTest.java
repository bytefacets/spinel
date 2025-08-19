// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.join;

import static com.bytefacets.spinel.schema.FieldDescriptor.intField;
import static com.bytefacets.spinel.table.IntIndexedTableBuilder.intIndexedTable;
import static com.bytefacets.spinel.validation.Key.key;
import static com.bytefacets.spinel.validation.RowData.template;

import com.bytefacets.spinel.printer.OutputPrinter;
import com.bytefacets.spinel.table.IntIndexedTable;
import com.bytefacets.spinel.testing.IntTableHandle;
import com.bytefacets.spinel.validation.RowData;
import com.bytefacets.spinel.validation.ValidationOperator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
        initialize(builder, true);
    }

    private void initialize(final JoinBuilder builder, final boolean leftFirst) {
        final Join join = builder.joinOn(List.of("LKey"), List.of("RKey"), 2).build();
        if (leftFirst) {
            left.output().attachInput(join.leftInput());
            right.output().attachInput(join.rightInput());
        } else {
            right.output().attachInput(join.rightInput());
            left.output().attachInput(join.leftInput());
        }
        join.output().attachInput(validation.input());
        join.output().attachInput(OutputPrinter.printer().input());
    }

    private void addLeft() {
        // keyed by 1, 2 in the left table
        // joined on 100,200 in the join
        leftHandle.add(1, 100, 1000).add(2, 200, 2000).fire();
    }

    private void addRight() {
        // keyed by -1, -2 in the right table
        // joined on 100,200 in the join
        rightHandle.add(-1, 100, 1111).add(-2, 200, 2222).fire();
    }

    private void clearExpectations() {
        validation.clearChanges();
    }

    @Nested
    class SchemaTests {
        private final JoinBuilder builder = JoinBuilder.lookupJoin().inner();
        private final Map<String, Class<?>> expectedSchema =
                Stream.of("RId", "LId", "LKey", "LValue", "RValue")
                        .collect(Collectors.toMap(String::toString, key -> Integer.class));
        private Join join;

        @BeforeEach
        void setUp() {
            join = builder.joinOn(List.of("LKey"), List.of("RKey"), 2).build();
            join.output().attachInput(OutputPrinter.printer().input());
            join.output().attachInput(validation.input());
        }

        private void attachOneSide(final boolean attachLeft) {
            if (attachLeft) {
                left.output().attachInput(join.leftInput());
            } else {
                right.output().attachInput(join.rightInput());
            }
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void shouldProduceExpectedSchemaWhenEitherAttachedFirst(final boolean leftFirst) {
            attachOneSide(leftFirst);
            attachOneSide(!leftFirst);
            validation.expect().schema(expectedSchema).validate();
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void shouldProduceRowsWhenSchemaReady(final boolean leftFirst) {
            addLeft();
            addRight();
            attachOneSide(leftFirst);
            attachOneSide(!leftFirst);
            validation
                    .expect()
                    .schema(expectedSchema)
                    .added(key(1, -1), template.rowData(100, 1000, 1111))
                    .added(key(2, -2), template.rowData(200, 2000, 2222))
                    .validate();
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void shouldProducedLatestWhenReceivingChangesWhileNotReady(final boolean leftFirst) {
            attachOneSide(leftFirst);

            addLeft();
            addRight();
            leftHandle.change(1, null, 1001).change(2, null, 2002).fire();
            rightHandle.change(-1, null, 1112).change(-2, null, 2223).fire();

            attachOneSide(!leftFirst);

            validation
                    .expect()
                    .schema(expectedSchema)
                    .added(key(1, -1), template.rowData(100, 1001, 1112))
                    .added(key(2, -2), template.rowData(200, 2002, 2223))
                    .validate();
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void shouldProducedLatestWhenReceivingRemovesWhileNotReady(final boolean leftFirst) {
            attachOneSide(leftFirst);

            addLeft();
            addRight();
            leftHandle.remove(2).fire();
            rightHandle.change(-2).fire();

            attachOneSide(!leftFirst);

            validation
                    .expect()
                    .schema(expectedSchema)
                    .added(key(1, -1), template.rowData(100, 1000, 1111))
                    .validate();
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void shouldNotReAddRowsWhenReAttachingSide(final boolean reattachLeft) {
            left.output().attachInput(join.leftInput());
            right.output().attachInput(join.rightInput());
            addLeft();
            addRight();

            if (reattachLeft) {
                left.output().detachInput(join.leftInput());
                validation.clearChanges();
                left.output().attachInput(join.leftInput());
            } else {
                right.output().detachInput(join.rightInput());
                validation.clearChanges();
                right.output().attachInput(join.rightInput());
            }
            validation
                    .expect()
                    .schema(expectedSchema)
                    .added(key(1, -1), template.rowData(100, 1000, 1111))
                    .added(key(2, -2), template.rowData(200, 2000, 2222))
                    .validate();
        }
    }

    @Nested
    class InnerLookupTests {
        private final JoinBuilder builder = JoinBuilder.lookupJoin().inner();

        @BeforeEach
        void setUp() {}

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
