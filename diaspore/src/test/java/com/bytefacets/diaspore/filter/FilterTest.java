// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.filter;

import static com.bytefacets.diaspore.schema.FieldDescriptor.intField;
import static com.bytefacets.diaspore.table.IntIndexedTableBuilder.intIndexedTable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bytefacets.diaspore.filter.lib.IntPredicate;
import com.bytefacets.diaspore.printer.OutputPrinter;
import com.bytefacets.diaspore.schema.FieldResolver;
import com.bytefacets.diaspore.table.IntIndexedTable;
import com.bytefacets.diaspore.validation.Key;
import com.bytefacets.diaspore.validation.RowData;
import com.bytefacets.diaspore.validation.ValidationOperator;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class FilterTest {
    private static final boolean print = true;
    private final ValidationOperator validation =
            new ValidationOperator(new String[] {"Id"}, "Value1", "Value2");
    private IntIndexedTable table;
    private Filter filter;
    private int value1FieldId;
    private int value2FieldId;
    private RowPredicate predicate =
            new IntPredicate("Value1") {
                @Override
                protected boolean testValue(final int value) {
                    return value % 100 == 0;
                }
            };

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

    void initialize(final FilterBuilder filterBuilder) {
        filter = filterBuilder.build();
        if (print) {
            filter.output().attachInput(OutputPrinter.printer().input());
        }
        filter.output().attachInput(validation.input());
        table.output().attachInput(filter.input());
    }

    @Nested
    class SchemaTests {
        @Test
        void shouldEmitSchema() {
            initialize(FilterBuilder.filter());
            validation.expect().schema(expectedSchema()).validate();
        }

        @Test
        void shouldBindSchemaToPredicate() {
            final RowPredicate mockPredicate = mock(RowPredicate.class);
            initialize(FilterBuilder.filter().initialPredicate(mockPredicate));
            verify(mockPredicate, times(1)).bindToSchema(any());
        }

        @Test
        void shouldUnbindSchemaFromPredicateWhenNullSchema() {
            final RowPredicate mockPredicate = mock(RowPredicate.class);
            initialize(FilterBuilder.filter().initialPredicate(mockPredicate));

            table.output().detachInput(filter.input());
            verify(mockPredicate, times(1)).unbindSchema();
        }

        @Test
        void shouldEmitNullSchemaWhenDetached() {
            initialize(FilterBuilder.filter());
            table.output().detachInput(filter.input());
            validation.expect().nullSchema().validate();
        }
    }

    @Nested
    class DefaultPredicateTests {
        @Test
        void shouldPassWhenNoPredicate() {
            initialize(FilterBuilder.filter().passesWhenNoPredicate(true));
            validation.clearChanges();

            addSourceRow(1, 100, 177);
            addSourceRow(2, 200, 277);
            table.fireChanges();
            validation
                    .expect()
                    .added(key(1), rowData(100, 177))
                    .added(key(2), rowData(200, 277))
                    .validate();
        }

        @Test
        void shouldFailWhenNoPredicate() {
            initialize(FilterBuilder.filter().passesWhenNoPredicate(false));
            validation.clearChanges();

            addSourceRow(1, 100, 177);
            addSourceRow(2, 200, 277);
            table.fireChanges();
            validation.validateNoChanges();
        }
    }

    @Nested
    class PredicateTests {
        final RowPredicate mockPredicate = mock(RowPredicate.class);

        @BeforeEach
        void setUp() {
            when(mockPredicate.testRow(anyInt())).thenReturn(true);
            doAnswer(
                            invocation -> {
                                final var resolver = invocation.getArgument(0, FieldResolver.class);
                                resolver.findField("Value1");
                                return null;
                            })
                    .when(mockPredicate)
                    .bindToSchema(any());
            initialize(FilterBuilder.filter().initialPredicate(mockPredicate));
            addSourceRow(1, 98, 77);
            addSourceRow(2, 100, 177);
            addSourceRow(3, 102, 177);
            table.fireChanges();
        }

        @Test
        void shouldNotEvaluatePredicateWhenDependenciesAreNotChanged() {
            reset(mockPredicate);
            changeValue2(2, 555);
            table.fireChanges();
            verify(mockPredicate, never()).testRow(anyInt());
        }

        @Test
        void shouldEvaluatePredicateWhenDependenciesAreChanged() {
            reset(mockPredicate);
            changeValue1(2, 555);
            table.fireChanges();
            verify(mockPredicate, times(1)).testRow(anyInt());
        }

        @Test
        void shouldReEvaluateAllRowsWhenPredicateChanged() {
            when(mockPredicate.testRow(anyInt())).thenReturn(false);
            addSourceRow(4, 200, 277); // from not passing to passing
            table.fireChanges();
            validation.clearChanges();

            filter.updatePredicate(predicate);

            validation
                    .expect()
                    .removed(key(1))
                    .removed(key(3))
                    .added(key(4), rowData(200, 277))
                    .validate();
        }
    }

    @Nested
    class AddTests {
        @BeforeEach
        void setUp() {
            initialize(FilterBuilder.filter().initialPredicate(predicate));
            validation.clearChanges();
        }

        @Test
        void shouldForwardRowWhenPassingPredicate() {
            addSourceRow(1, 98, 77);
            addSourceRow(2, 100, 177);
            addSourceRow(3, 102, 177);
            table.fireChanges();
            validation.expect().added(key(2), rowData(100, 177)).validate();
        }
    }

    @Nested
    class ChangeTests {
        @BeforeEach
        void setUp() {
            initialize(FilterBuilder.filter().initialPredicate(predicate));
            addSourceRow(1, 98, 77);
            addSourceRow(2, 100, 177);
            addSourceRow(3, 102, 177);
            table.fireChanges();
            validation.clearChanges();
        }

        @Test
        void shouldForwardRowWhenInOutputWithNoDependencyChange() {
            changeValue2(2, 555);
            table.fireChanges();
            validation.expect().changed(key(2), new RowData(Map.of("Value2", 555))).validate();
        }

        @Test
        void shouldForwardRowWhenInOutputWithDependencyChange() {
            changeValue1(2, 500);
            table.fireChanges();
            validation.expect().changed(key(2), new RowData(Map.of("Value1", 500))).validate();
        }

        @Test
        void shouldNotForwardRowWhenNotInOutputWithNoDependencyChange() {
            changeValue2(1, 555);
            table.fireChanges();
            validation.validateNoChanges();
        }

        @Test
        void shouldNotForwardRowWhenNotInOutputWithDependencyChange() {
            changeValue1(1, 555);
            table.fireChanges();
            validation.validateNoChanges();
        }

        @Test
        void shouldSendRowAsAddWhenRowChangesFromNotPassingToPassing() {
            changeValue1(1, 200);
            table.fireChanges();
            validation.expect().added(key(1), rowData(200, 77)).validate();
        }

        @Test
        void shouldSendRowAsRemoveWhenRowChangesFromPassingToNotPassing() {
            changeValue1(2, 111);
            table.fireChanges();
            validation.expect().removed(key(2)).validate();
        }
    }

    @Nested
    class RemoveTests {
        @BeforeEach
        void setUp() {
            initialize(FilterBuilder.filter().initialPredicate(predicate));
            addSourceRow(1, 98, 77);
            addSourceRow(2, 100, 177);
            addSourceRow(3, 102, 177);
            table.fireChanges();
            validation.clearChanges();
        }

        @Test
        void shouldNotForwardRowWhenNotInOutput() {
            table.remove(1);
            table.fireChanges();
            validation.validateNoChanges();
        }

        @Test
        void shouldForwardRowWhenInOutput() {
            table.remove(2);
            table.fireChanges();
            validation.expect().removed(key(2)).validate();
        }
    }

    private Map<String, Class<?>> expectedSchema() {
        return Map.of("Id", Integer.class, "Value1", Integer.class, "Value2", Integer.class);
    }

    private Key key(final int key) {
        return new Key(List.of(key));
    }

    private RowData rowData(final int value, final int value2) {
        return new RowData(Map.of("Value1", value, "Value2", value2));
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
