// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.tools.console;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.bytefacets.collections.functional.IntConsumer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class RowMappingTest {
    private final RowMapping mapping = new RowMapping();

    @Test
    void shouldGrowArraysWhenMappingRow() {
        mapping.mapRow(2);
        mapping.mapRow(20);
        mapping.mapRow(200);
    }

    @Test
    void shouldResetMappings() {
        mapping.mapRow(10);
        final int screenRow1 = mapping.screenRow(10);
        assertThat(screenRow1, not(equalTo(-1)));
        mapping.reset();
        assertThat(mapping.screenRow(10), equalTo(-1));
        // back the first screen row
        mapping.mapRow(5);
        assertThat(mapping.screenRow(5), equalTo(screenRow1));
    }

    @Test
    void shouldFreeRow() {
        mapping.mapRow(10);
        final int screenRow1 = mapping.screenRow(10);
        mapping.freeRow(10);
        assertThat(mapping.screenRow(10), equalTo(-1));
        mapping.mapRow(5);
        assertThat(mapping.screenRow(5), equalTo(screenRow1));
    }

    @Test
    void shouldAllocateSequentialRows() {
        mapping.mapRow(10);
        mapping.mapRow(5);
        mapping.mapRow(15);
        final int screenRow1 = mapping.screenRow(10);
        assertThat(mapping.screenRow(5), equalTo(screenRow1 + 1));
        assertThat(mapping.screenRow(15), equalTo(screenRow1 + 2));
    }

    @Test
    void shouldIterateDataRows() {
        mapping.mapRow(10);
        mapping.mapRow(5);
        mapping.mapRow(15);

        final IntConsumer consumer = mock(IntConsumer.class);
        mapping.forEach(consumer);

        final ArgumentCaptor<Integer> rowCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(consumer, times(3)).accept(rowCaptor.capture());
        assertThat(rowCaptor.getAllValues(), contains(10, 5, 15));
    }

    @Test
    void shouldSkipUnmappedDataRowsDuringIteration() {
        mapping.mapRow(10);
        mapping.mapRow(5);
        mapping.mapRow(15);
        mapping.freeRow(5); // free it in the middle

        final IntConsumer consumer = mock(IntConsumer.class);
        mapping.forEach(consumer);

        final ArgumentCaptor<Integer> rowCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(consumer, times(2)).accept(rowCaptor.capture());
        assertThat(rowCaptor.getAllValues(), contains(10, 15));
    }
}
