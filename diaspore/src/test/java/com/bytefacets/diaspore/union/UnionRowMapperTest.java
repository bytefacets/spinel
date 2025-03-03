// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.union;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class UnionRowMapperTest {
    private final UnionRowMapper map = new UnionRowMapper(2);

    @Test
    void shouldGrowNameStorage() {
        IntStream.range(0, 10).forEach(i -> map.mapInputName(i, "foo"));
    }

    @Test
    void shouldAccessNameViaNameField() {
        map.mapInputName(5, "xyz");
        final int outRow = map.mapInputRow(5, 10);
        assertThat(map.inputNameField().valueAt(outRow), equalTo("xyz"));
    }

    @Test
    void shouldAccessIndexViaIndexField() {
        map.mapInputName(5, "xyz");
        final int outRow = map.mapInputRow(5, 10);
        assertThat(map.inputIndexField().valueAt(outRow), equalTo(5));
    }

    @Nested
    class MappingTests {
        @Test
        void shouldIterateAllRowsFromSource() {
            final Set<Integer> source3 = new HashSet<>();
            IntStream.range(0, 3).forEach(in -> map.mapInputRow(2, in));
            IntStream.range(0, 3).forEach(in -> source3.add(map.mapInputRow(3, in)));
            IntStream.range(0, 3).forEach(in -> map.mapInputRow(4, in));
            final Set<Integer> observed = new HashSet<>();
            map.iterateInputRowsInOutput(3, observed::add);
            assertThat(observed, equalTo(source3));
        }
    }

    @Nested
    class RemoveTests {
        final int[] outRows = new int[3];

        @BeforeEach
        void setUp() {
            IntStream.range(10, 20).forEach(in -> map.mapInputRow(4, in));
            outRows[0] = map.mapInputRow(3, 7);
            outRows[1] = map.mapInputRow(3, 8);
            outRows[2] = map.mapInputRow(3, 9);
            IntStream.range(10, 20).forEach(in -> map.mapInputRow(5, in));
        }

        @Test
        void shouldNotReAllocateRemovedRowUntilFreed() {
            map.removeInputRow(3, 8);
            assertThat(map.mapInputRow(3, 40), not(equalTo(outRows[1])));
            map.freeOutRow(outRows[1]);
            assertThat(map.mapInputRow(3, 41), equalTo(outRows[1]));
        }

        @Test
        void shouldReturnOutboundRowWhenRemoved() {
            assertThat(map.removeInputRow(3, 8), equalTo(outRows[1]));
        }

        @Test
        void shouldRemoveRowDirectlyOnInputRemoval() {
            map.removeRowOnInputRemoval(outRows[1]);
            assertThat(map.mapInputRow(3, 41), equalTo(outRows[1]));
        }
    }
}
