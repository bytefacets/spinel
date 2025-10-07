// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.vaadin.data;

import static com.bytefacets.spinel.table.IntIndexedStructTableBuilder.intIndexedStructTable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.spinel.common.Connector;
import com.bytefacets.spinel.facade.StructFacade;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.table.IntIndexedStructTable;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TransformConsumerTest {
    private final IntIndexedStructTable<Rec> table = intIndexedStructTable(Rec.class).build();
    private final Rec facade = table.createFacade();
    private @Mock(strictness = Mock.Strictness.LENIENT) UIThreadConsumer uiThreadConsumer;
    private @Captor ArgumentCaptor<Schema> schemaCaptor;
    private final Set<Integer> capturedAdds = new HashSet<>();
    private final Set<Integer> capturedRemoved = new HashSet<>();
    private TransformConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new TransformConsumer(uiThreadConsumer);
    }

    @Nested
    class SchemaTests {
        @Test
        void shouldCallbackUIConsumerWithSchema() {
            Connector.connectInputToOutput(consumer, table);
            verify(uiThreadConsumer, never()).updateSchema(any());
            consumer.applyOnUiThread();
            verify(uiThreadConsumer, times(1)).updateSchema(any());
        }

        @Test
        void shouldTriggerRowResetWhenSchemaUpdated() {
            Connector.connectInputToOutput(consumer, table);
            consumer.applyOnUiThread();
            verify(uiThreadConsumer, times(1)).updateActiveRows(any(), any(), eq(true));
            reset(uiThreadConsumer);

            table.output().detachInput(consumer);
            consumer.applyOnUiThread();
            verify(uiThreadConsumer, times(1)).updateActiveRows(any(), any(), eq(true));
        }
    }

    @Nested
    class AddTests {
        private Schema schema;

        @BeforeEach
        void setUp() {
            Connector.connectInputToOutput(consumer, table);
            consumer.applyOnUiThread();
            verify(uiThreadConsumer, times(1)).updateSchema(schemaCaptor.capture());
            schema = schemaCaptor.getValue();
            reset(uiThreadConsumer);
            captureReportedRows();
        }

        @Test
        void shouldCaptureAddedRowData() {
            final int r1 = add(1, 10, 100);
            final int r2 = add(2, 20, 200);
            table.fireChanges();
            validate(schema, r1, 1, 10, 100);
            validate(schema, r2, 2, 20, 200);
        }

        @Test
        void shouldReportAddedRows() {
            final int r1 = add(1, 10, 100);
            final int r2 = add(2, 20, 200);
            table.fireChanges();

            consumer.applyOnUiThread();
            assertThat(capturedAdds, containsInAnyOrder(r1, r2));
            assertThat(capturedRemoved, empty());
        }
    }

    @Nested
    class ChangeTests {
        private Schema schema;
        private int r1;
        private int r2;

        @BeforeEach
        void setUp() {
            Connector.connectInputToOutput(consumer, table);
            r1 = add(1, 10, 100);
            r2 = add(2, 20, 200);
            table.fireChanges();
            consumer.applyOnUiThread();
            verify(uiThreadConsumer, times(1)).updateSchema(schemaCaptor.capture());
            schema = schemaCaptor.getValue();
            reset(uiThreadConsumer);
            captureReportedRows();
        }

        @Test
        void shouldCaptureUpdatedRowData() {
            update(1, 11, 100);
            update(2, 21, 201);
            table.fireChanges();
            validate(schema, r1, 1, 11, 100);
            validate(schema, r2, 2, 21, 201);
        }
    }

    @Nested
    class RemoveTests {
        private int r2;

        @BeforeEach
        void setUp() {
            Connector.connectInputToOutput(consumer, table);
            add(1, 10, 100);
            r2 = add(2, 20, 200);
            table.fireChanges();
            consumer.applyOnUiThread();
            verify(uiThreadConsumer, times(1)).updateSchema(any());
            reset(uiThreadConsumer);
            captureReportedRows();
        }

        @Test
        void shouldReportRemovedRows() {
            table.remove(2);
            table.fireChanges();
            consumer.applyOnUiThread();
            assertThat(capturedAdds, empty());
            assertThat(capturedRemoved, contains(r2));
        }

        @Test
        void shouldConflateAddThenRemove() {
            add(3, 30, 300);
            table.fireChanges();
            table.remove(3);
            table.fireChanges();
            consumer.applyOnUiThread();
            assertThat(capturedAdds, empty());
            assertThat(capturedRemoved, empty());
        }

        @Test
        void shouldConflateRemoveThenAdd() {
            table.remove(1);
            table.fireChanges();
            add(3, 30, 300); // rely on free-list to re-assign the row we just removed
            table.fireChanges();
            consumer.applyOnUiThread();
            assertThat(capturedAdds, empty());
            assertThat(capturedRemoved, empty());
        }
    }

    private int add(final int key, final int val1, final int val2) {
        table.beginAdd(key, facade).setValue1(val1).setValue2(val2);
        table.endAdd();
        return ((StructFacade) facade).currentRow();
    }

    private int update(final int key, final int val1, final int val2) {
        table.beginChange(key, facade).setValue1(val1).setValue2(val2);
        table.endChange();
        return ((StructFacade) facade).currentRow();
    }

    private void validate(
            final Schema schema, final int row, final int key, final int val1, final int val2) {
        assertThat(schema.field("Key").objectValueAt(row), equalTo(key));
        assertThat(schema.field("Value1").objectValueAt(row), equalTo(val1));
        assertThat(schema.field("Value2").objectValueAt(row), equalTo(val2));
    }

    private void captureReportedRows() {
        doAnswer(
                        inv -> {
                            inv.getArgument(0, IntIterable.class).forEach(capturedAdds::add);
                            inv.getArgument(1, IntIterable.class).forEach(capturedRemoved::add);
                            return null;
                        })
                .when(uiThreadConsumer)
                .updateActiveRows(any(), any(), anyBoolean());
    }

    // formatting:off
    interface Rec {
        int getKey();
        int getValue1(); Rec setValue1(int value);
        int getValue2(); Rec setValue2(int value);
    }
    // formatting:on
}
