// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.groupby;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class GroupRowModsTest {
    final GroupRowMods mods = new GroupRowMods(2);
    private final Map<Integer, List<Integer>> receiveRows = new HashMap<>();

    @Test
    void shouldIterateRowsForGroup() {
        Stream.of(4, 10, 42).forEach(row -> mods.addGroupRow(3, row));
        consume();
        validate(3, 4, 10, 42);
        assertThat(receiveRows.isEmpty(), equalTo(true));
    }

    @Test
    void shouldGrow() {
        IntStream.range(0, 100).forEach(group -> mods.addGroupRow(group, group * 100));
        consume();
        IntStream.range(0, 100).forEach(group -> validate(group, group * 100));
        assertThat(receiveRows.isEmpty(), equalTo(true));
    }

    private void consume() {
        mods.fire(
                (modGroup, iter) -> {
                    final var capture =
                            receiveRows.computeIfAbsent(modGroup, k -> new ArrayList<>());
                    iter.forEach(capture::add);
                });
    }

    private void validate(final int group, final Integer... rows) {
        assertThat(receiveRows.remove(group), containsInAnyOrder(rows));
    }
}
