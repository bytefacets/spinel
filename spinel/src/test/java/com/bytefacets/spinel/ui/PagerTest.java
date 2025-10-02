// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.ui;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.bytefacets.collections.functional.IntIntConsumer;
import com.bytefacets.collections.functional.IntIterable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PagerTest {
    private final Pager pager = Pager.pager(3);
    private final Random rnd = new Random(38768768);

    @Test
    void shouldGrow() {
        final var rows = rowsInRandomOrder(0, 500);
        pager.add(iterable(rows));
        assertThat(pager.size(), equalTo(rows.length));
        pager.remove(iterable(IntStream.range(100, 200).toArray()));
        assertThat(pager.size(), equalTo(rows.length - 100));
    }

    @Nested
    class AddTests {
        @Test
        void shouldAddRows() {
            pager.add(iterable(5, 6, 7));
            assertRowOrder(5, 6, 7);
        }

        @Test
        void shouldAddAfterRemove() {
            pager.add(iterable(5, 6, 7, 15, 20, 10));
            pager.remove(iterable(10, 7, 5));
            pager.add(iterable(30, 60, 40, 50));
            assertRowOrder(6, 15, 20, 30, 60, 40, 50);
            assertThat(pager.size(), equalTo(7));
        }
    }

    @Nested
    class RemoveTests {
        @BeforeEach
        void setUp() {
            pager.add(iterable(5, 6, 7, 15, 20, 10));
        }

        @Test
        void shouldRemoveFirstRow() {
            pager.remove(iterable(5));
            assertRowOrder(6, 7, 15, 20, 10);
            assertThat(pager.size(), equalTo(5));
        }

        @Test
        void shouldRemoveLastRow() {
            pager.remove(iterable(10));
            assertRowOrder(5, 6, 7, 15, 20);
            assertThat(pager.size(), equalTo(5));
        }

        @Test
        void shouldRemoveMiddle() {
            pager.remove(iterable(7));
            assertRowOrder(5, 6, 15, 20, 10);
            assertThat(pager.size(), equalTo(5));
        }

        @Test
        void shouldRemoveMultipleMiddle() {
            pager.remove(iterable(20, 7));
            assertRowOrder(5, 6, 15, 10);
            assertThat(pager.size(), equalTo(4));
        }

        @Test
        void shouldRemoveMultipleUnordered() {
            pager.remove(iterable(20, 6, 7, 15));
            assertRowOrder(5, 10);
            assertThat(pager.size(), equalTo(2));
        }

        @Test
        void shouldClear() {
            pager.add(iterable(rowsInRandomOrder(100, 400)));
            pager.clear();
            assertThat(pager.size(), equalTo(0));
            final int[] newRows = rowsInRandomOrder(10, 20);
            pager.add(iterable(newRows));
            assertRowOrder(newRows);
        }
    }

    @Nested
    class RangeTests {
        @Test
        void shouldReturnRowsInRange() {
            final int[] rows = rowsInRandomOrder(0, 100);
            pager.add(iterable(rows));
            final int[] view = new int[35];
            // formatting:off
            pager.rowsInRange(40, 35, (relativePosition, row) -> view[relativePosition] = row);
            // formatting:on
            for (int i = 0; i < view.length; i++) {
                assertThat(pager.positionForRow(view[i]), equalTo(i + 40));
            }
        }

        @Test
        void shouldReturnNothingWhenStartIsPastLimit() {
            pager.add(iterable(IntStream.range(30, 45).toArray()));
            final IntIntConsumer consumer = mock(IntIntConsumer.class);
            pager.rowsInRange(15, 1, consumer);
            verify(consumer, never()).accept(anyInt(), anyInt());
        }

        @Test
        void shouldReturnFewerItemsWhenLimitIsPastEnd() {
            pager.add(iterable(IntStream.range(30, 45).toArray()));
            final IntIntConsumer consumer = mock(IntIntConsumer.class);
            pager.rowsInRange(0, 20, consumer);
            verify(consumer, times(15)).accept(anyInt(), anyInt());
        }
    }

    private int[] rowsInRandomOrder(final int start, final int end) {
        final var rowList = new ArrayList<>(IntStream.range(start, end).boxed().toList());
        Collections.shuffle(rowList, rnd);
        return rowList.stream().mapToInt(Integer::intValue).toArray();
    }

    private void assertRowOrder(final int... rows) {
        for (int pos = 0; pos < rows.length; pos++) {
            final int row = rows[pos];
            assertThat(pager.positionForRow(row), equalTo(pos));
            assertThat(pager.rowPosition(pos), equalTo(row));
        }
    }

    private static IntIterable iterable(final int... values) {
        return intConsumer -> {
            for (final int value : values) {
                intConsumer.accept(value);
            }
        };
    }
}
