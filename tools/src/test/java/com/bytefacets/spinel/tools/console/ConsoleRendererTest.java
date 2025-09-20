// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.tools.console;

import static com.bytefacets.spinel.table.IntIndexedStructTableBuilder.intIndexedStructTable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bytefacets.spinel.common.Connector;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.TypeId;
import com.bytefacets.spinel.table.IntIndexedStructTable;
import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConsoleRendererTest {
    private @Mock Control control;
    private @Mock Presenter presenter;
    private @Mock RowMapping rowMapping;
    private @Captor ArgumentCaptor<Integer> rowCaptor;
    final IntIndexedStructTable<Data> table = intIndexedStructTable(Data.class).build();
    final Data facade = table.createFacade();
    private ConsoleRenderer renderer;

    @BeforeEach
    void setUp() {
        renderer = new ConsoleRenderer("title", presenter, rowMapping, control);
        verify(control, times(1)).setTitle("title");
        Connector.connectInputToOutput(renderer, table);
    }

    @Nested
    class SchemaTests {
        @Test
        void shouldInitializePresenterWithSchema() {
            verify(presenter, times(1)).initialize(3);
            verify(presenter, times(3)).initializeField(any(), anyInt(), anyByte(), any());
            final Schema inputSchema = table.schema();
            verify(presenter, times(1))
                    .initializeField(
                            eq("Key"),
                            eq(inputSchema.field("Key").fieldId()),
                            eq(TypeId.Int),
                            any());
            verify(presenter, times(1))
                    .initializeField(
                            eq("Value1"),
                            eq(inputSchema.field("Value1").fieldId()),
                            eq(TypeId.Long),
                            any());
            verify(presenter, times(1))
                    .initializeField(
                            eq("Value2"),
                            eq(inputSchema.field("Value2").fieldId()),
                            eq(TypeId.String),
                            any());
        }

        @Test
        void shouldResetPresenterWhenSchemaReset() {
            reset(presenter, rowMapping, control);
            renderer.input().schemaUpdated(null);
            verify(presenter, times(1)).reset();
        }

        @Test
        void shouldResetRowMappingWhenSchemaReset() {
            reset(presenter, rowMapping, control);
            renderer.input().schemaUpdated(null);
            verify(rowMapping, times(1)).reset();
        }

        @Test
        void shouldInitializeHeaderWithSchema() {
            verify(control, times(1)).emitClear();
            verify(presenter, times(1)).renderHeader();
            verify(presenter, times(2)).update(); // one for schema, one for rowsAdded
        }

        @Test
        void shouldClearScreenWithSchemaReset() {
            reset(presenter, rowMapping, control);
            renderer.input().schemaUpdated(null);
            verify(control, times(1)).emitClear();
        }
    }

    @Nested
    class AddTests {
        @BeforeEach
        void setUp() {
            resetMocks();
        }

        @Test
        void shouldMapAddedRows() {
            final int r1 = upsert(10, 20, "foo");
            final int r2 = upsert(12, 30, "bar");
            table.fireChanges();
            verify(rowMapping, times(2)).mapRow(rowCaptor.capture());
            assertThat(rowCaptor.getAllValues(), containsInAnyOrder(r1, r2));
        }

        @Test
        void shouldUpdatePresenter() {
            upsert(10, 20, "foo");
            upsert(12, 30, "bar");
            table.fireChanges();
            verify(presenter, times(1)).update();
        }

        @Test
        void shouldRepaintWhenWidthUpdated() {
            // do false second so we confirm we don't lose the true response
            final LinkedList<Boolean> responses = new LinkedList<>(List.of(true, false));
            doAnswer(inv -> responses.removeFirst())
                    .when(presenter)
                    .calculateColumnWidths(anyInt());
            upsert(10, 20, "foo");
            upsert(12, 30, "bar");
            table.fireChanges();
            verify(control, times(1)).repaint();
        }
    }

    @Nested
    class ChangeTests {
        int r1;
        int r2;

        @BeforeEach
        void setUp() {
            r1 = upsert(10, 20, "foo");
            r2 = upsert(12, 30, "bar");
            table.fireChanges();
            resetMocks();
        }

        @Test
        void shouldRepaintRows() {
            upsert(10, 21, "foo1");
            upsert(12, 31, "bar1");
            table.fireChanges();
            verify(control, times(2)).repaintRow(rowCaptor.capture());
            assertThat(rowCaptor.getAllValues(), containsInAnyOrder(r1, r2));
        }

        @Test
        void shouldRepaintWhenWidthUpdated() {
            // do false second so we confirm we don't lose the true response
            final LinkedList<Boolean> responses =
                    new LinkedList<>(List.of(true, false, false, false));
            doAnswer(inv -> responses.removeFirst())
                    .when(presenter)
                    .calculateColumnWidth(anyInt(), anyInt());
            upsert(10, 21, "foo1");
            upsert(12, 31, "bar1");
            table.fireChanges();
            verify(control, times(1)).repaint();
        }

        @Test
        void shouldUpdatePresenter() {
            upsert(12, 31, "bar1");
            table.fireChanges();
            verify(presenter, times(1)).update();
        }
    }

    @Nested
    class RemoveTests {
        int r1;
        int r2;

        @BeforeEach
        void setUp() {
            r1 = upsert(10, 20, "foo");
            r2 = upsert(12, 30, "bar");
            table.fireChanges();
            resetMocks();
        }

        private void fireRemoves() {
            table.remove(10);
            table.remove(12);
            table.fireChanges();
        }

        @Test
        void shouldUnmapRows() {
            fireRemoves();
            verify(rowMapping, times(2)).freeRow(rowCaptor.capture());
            assertThat(rowCaptor.getAllValues(), containsInAnyOrder(r1, r2));
        }

        @Test
        void shouldClearRows() {
            when(rowMapping.freeRow(r1)).thenReturn(5);
            when(rowMapping.freeRow(r2)).thenReturn(6);
            fireRemoves();
            verify(control, times(2)).clearRow(rowCaptor.capture());
            assertThat(rowCaptor.getAllValues(), containsInAnyOrder(5, 6));
        }

        @Test
        void shouldUpdate() {
            fireRemoves();
            verify(presenter, times(1)).update();
        }
    }

    private void resetMocks() {
        reset(presenter, rowMapping, control);
    }

    private int upsert(final int key, final int value1, final String value2) {
        table.beginUpsert(key, facade).setValue1(value1).setValue2(value2);
        table.endUpsert();
        return table.lookupKeyRow(key);
    }

    // formatting:off
    interface Data {
        int getKey();
        long getValue1(); Data setValue1(long value);
        String getValue2(); Data setValue2(String value);
    }
    // formatting:on
}
