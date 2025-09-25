// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.groupby;

import static com.bytefacets.spinel.groupby.RecordAggregationFunction.recordAggregationFunction;
import static com.bytefacets.spinel.schema.MetadataBuilder.metadataBuilder;
import static com.bytefacets.spinel.table.IntIndexedStructTableBuilder.intIndexedStructTable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.bytefacets.spinel.common.Connector;
import com.bytefacets.spinel.exception.OperatorSetupException;
import com.bytefacets.spinel.schema.DisplayMetadata;
import com.bytefacets.spinel.table.IntIndexedStructTable;
import com.bytefacets.spinel.validation.Key;
import com.bytefacets.spinel.validation.RowData;
import com.bytefacets.spinel.validation.ValidationOperator;
import java.util.Objects;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RecordAggregationFunctionTest {
    private final IntIndexedStructTable<TableRec> table =
            intIndexedStructTable(TableRec.class).build();
    private final TableRec facade = table.createFacade();
    private final ValidationOperator validator =
            new ValidationOperator(new String[] {"Category"}, "Result1", "Result2");
    private RecordAggregationFunction<Input, Output> function;
    private final RowData.RowDataTemplate template = RowData.template("Result1", "Result2");
    private GroupBy groupBy;

    @BeforeEach
    void setUp() {
        function =
                recordAggregationFunction(
                        Input.class,
                        Output.class,
                        (event, currentGroupValue, oldValue, newValue) -> {
                            final int old1 = oldValue.getValue1();
                            final int old2 = oldValue.getValue2();
                            final int new1 = newValue.getValue1();
                            final int new2 = newValue.getValue2();
                            if (old1 != new1) { // for testing specific field changes
                                currentGroupValue.setResult1(
                                        currentGroupValue.getResult1() - old1 + new1);
                            }
                            if (old2 != new2) {
                                currentGroupValue.setResult2(
                                        currentGroupValue.getResult2() - old2 + new2);
                            }
                        });
        groupBy =
                GroupByBuilder.groupBy("test")
                        .groupByFields("Category")
                        .includeCountField("Count")
                        .addAggregation(function)
                        .build();
        Connector.connectInputToOutput(groupBy, table);
        Connector.connectInputToOutput(validator, groupBy);
    }

    @AfterEach
    void tearDown() {
        validator.assertNoActiveValidation();
    }

    @Nested
    class SchemaTests {
        @Test
        void shouldCaptureOutputFieldMetadata() {
            assertThat(
                    groupBy.output().schema().field("Result1").metadata(),
                    equalTo(metadataBuilder().displayFormat("#,###").build()));
        }

        @Test
        void shouldAccessGroupField() {
            groupBy.output().detachInput(validator.input());
            final RecordAggregationFunction<Input, OutputWithGroupField> grpFunction =
                    recordAggregationFunction(
                            Input.class,
                            OutputWithGroupField.class,
                            (event, currentGroupValue, oldValue, newValue) -> {
                                final int hc =
                                        Objects.hashCode(
                                                currentGroupValue.getCategory().hashCode());
                                currentGroupValue.setResult1(newValue.getValue1() + hc);
                                currentGroupValue.setResult2(newValue.getValue2() + hc);
                            });
            groupBy =
                    GroupByBuilder.groupBy("test")
                            .groupByFields("Category")
                            .includeCountField("MyCount")
                            .addAggregation(grpFunction)
                            .build();
            Connector.connectInputToOutput(groupBy, table);
            Connector.connectInputToOutput(validator, groupBy);
            validator.clearChanges();
            final int hc = "Books".hashCode();
            add(1, "Books", 5, 6);
            table.fireChanges();
            validator.expect().added(Key.key("Books"), template.rowData(hc + 5, hc + 6)).validate();
        }

        @Test
        void shouldAccessCountField() {
            groupBy.output().detachInput(validator.input());
            final RecordAggregationFunction<Input, OutputWithCount> ctFunction =
                    recordAggregationFunction(
                            Input.class,
                            OutputWithCount.class,
                            (event, currentGroupValue, oldValue, newValue) -> {
                                final int r1 = currentGroupValue.getResult1();
                                final int r2 = currentGroupValue.getResult2();
                                final int delta1 =
                                        newValue.getValue1() * currentGroupValue.getMyCount();
                                final int delta2 =
                                        newValue.getValue2() * currentGroupValue.getMyCount();
                                currentGroupValue.setResult1(r1 + delta1);
                                currentGroupValue.setResult2(r2 + delta2);
                            });
            groupBy =
                    GroupByBuilder.groupBy("test")
                            .groupByFields("Category")
                            .includeCountField("MyCount")
                            .addAggregation(ctFunction)
                            .build();
            Connector.connectInputToOutput(groupBy, table);
            Connector.connectInputToOutput(validator, groupBy);
            validator.clearChanges();
            add(1, "Books", 5, 6);
            add(2, "Books", 7, 8);
            table.fireChanges();
            validator.expect().added(Key.key("Books"), template.rowData(24, 28)).validate();
        }

        @Test
        void shouldNotAllowSetterForCountField() {
            final RecordAggregationFunction<Input, InvalidCountSetter> func =
                    recordAggregationFunction(
                            Input.class,
                            InvalidCountSetter.class,
                            (event, currentGroupValue, oldValue, newValue) -> {
                                currentGroupValue.setMyCount(10);
                            });
            final OperatorSetupException ex =
                    assertThrows(OperatorSetupException.class, () -> invalidInterfaceSetup(func));
            assertThat(
                    ex.getMessage(),
                    equalTo(
                            "Name collision setting up test: attempted to use 'MyCount' "
                                    + "for CalculatedField, but was already used for CountFieldName"));
        }

        @Test
        void shouldNotAllowSetterForForwardedField() {
            final RecordAggregationFunction<Input, InvalidForwardedSetter> func =
                    recordAggregationFunction(
                            Input.class,
                            InvalidForwardedSetter.class,
                            (event, currentGroupValue, oldValue, newValue) -> {
                                currentGroupValue.setCategoryId(10);
                            });
            final OperatorSetupException ex =
                    assertThrows(OperatorSetupException.class, () -> invalidInterfaceSetup(func));
            assertThat(
                    ex.getMessage(),
                    equalTo(
                            "Name collision setting up test: attempted to use 'CategoryId' "
                                    + "for CalculatedField, but was already used for ForwardedField"));
        }

        @Test
        void shouldNotAllowSetterForGroupField() {
            final RecordAggregationFunction<Input, InvalidGroupFieldSetter> func =
                    recordAggregationFunction(
                            Input.class,
                            InvalidGroupFieldSetter.class,
                            (event, currentGroupValue, oldValue, newValue) -> {
                                currentGroupValue.setCategory("foo");
                            });
            final OperatorSetupException ex =
                    assertThrows(OperatorSetupException.class, () -> invalidInterfaceSetup(func));
            assertThat(
                    ex.getMessage(),
                    equalTo(
                            "Name collision setting up test: attempted to use 'Category' "
                                    + "for CalculatedField, but was already used for GroupByField"));
        }

        private void invalidInterfaceSetup(final RecordAggregationFunction<?, ?> function) {
            groupBy =
                    GroupByBuilder.groupBy("test")
                            .groupByFields("Category")
                            .addForwardedFields("CategoryId")
                            .includeCountField("MyCount")
                            .addAggregation(function)
                            .build();
            Connector.connectInputToOutput(groupBy, table);
        }
    }

    @Nested
    class AddTests {
        @BeforeEach
        void setUp() {
            validator.clearChanges();
        }

        @Test
        void shouldAdd() {
            add(1, "Books", 5, 6);
            add(2, "Books", 8, 9);
            table.fireChanges();
            validator.expect().added(Key.key("Books"), template.rowData(13, 15)).validate();
        }
    }

    @Nested
    class ChangeTests {
        @BeforeEach
        void setUp() {
            add(1, "Books", 5, 6);
            add(2, "Books", 8, 9);
            add(3, "CDs", 10, 12);
            add(4, "DVDs", 30, 47);
            table.fireChanges();
            validator.clearChanges();
        }

        @Test
        void shouldUpdateFields() {
            table.beginChange(2, facade).setCategory("Books").setValue1(70);
            table.endChange();
            table.fireChanges();
            validator.expect().changed(Key.key("Books"), template.rowData(75, null)).validate();
        }

        @Test
        void shouldChangeGroups() {
            table.beginChange(2, facade).setCategory("CDs").setValue1(7);
            table.endChange();
            table.fireChanges();
            validator
                    .expect()
                    .changed(Key.key("Books"), template.rowData(5, 6))
                    .changed(Key.key("CDs"), template.rowData(17, 21))
                    .validate();
        }

        @Test
        void shouldChangeToNewGroup() {
            table.beginChange(2, facade).setCategory("Sports").setValue1(7);
            table.endChange();
            table.fireChanges();
            validator
                    .expect()
                    .changed(Key.key("Books"), template.rowData(5, 6))
                    .added(Key.key("Sports"), template.rowData(7, 9))
                    .validate();
        }

        @Test
        void shouldDrainGroup() {
            table.beginChange(3, facade).setCategory("Books");
            table.endChange();
            table.fireChanges();
            validator
                    .expect()
                    .changed(Key.key("Books"), template.rowData(23, 27))
                    .removed(Key.key("CDs"))
                    .validate();
        }
    }

    @Nested
    class RemoveTests {
        @BeforeEach
        void setUp() {
            add(1, "Books", 5, 6);
            add(2, "Books", 8, 9);
            add(3, "CDs", 10, 12);
            add(4, "DVDs", 30, 47);
            table.fireChanges();
            validator.clearChanges();
        }

        @Test
        void shouldUpdateGroupFromRemove() {
            table.remove(2);
            table.fireChanges();
            validator.expect().changed(Key.key("Books"), template.rowData(5, 6)).validate();
        }

        @Test
        void shouldDrainGroup() {
            table.remove(1);
            table.remove(2);
            table.fireChanges();
            validator.expect().removed(Key.key("Books")).validate();
        }
    }

    private void add(final int key, final String category, final int v1, final int v2) {
        table.beginAdd(key, facade)
                .setCategory(category)
                .setValue1(v1)
                .setValue2(v2)
                .setCategoryId(category.charAt(0));
        table.endAdd();
    }

    // formatting:off
    interface TableRec {
        int getKey();
        String getCategory(); TableRec setCategory(String value);
        int getCategoryId();  TableRec setCategoryId(int value);
        int getValue1();      TableRec setValue1(int value);
        int getValue2();      TableRec setValue2(int value);
    }

    interface Input {
        int getValue1();
        int getValue2();
    }

    interface Output {
        @DisplayMetadata(format = "#,###")
        int getResult1(); void setResult1(int value);
        int getResult2(); void setResult2(int value);
    }

    interface OutputWithCount extends Output {
        int getMyCount();
    }

    interface OutputWithGroupField extends Output {
        String getCategory();
    }

    interface InvalidCountSetter extends Output {
        int getMyCount(); void setMyCount(int value);
    }
    interface InvalidForwardedSetter extends Output {
        int getCategoryId(); void setCategoryId(int value);
    }
    interface InvalidGroupFieldSetter extends Output {
        String getCategory(); void setCategory(String value);
    }
    // formatting:on
}
