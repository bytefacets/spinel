// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.union;

import static com.bytefacets.spinel.schema.FieldBitSet.fieldBitSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.bytefacets.spinel.schema.FieldBitSet;
import com.bytefacets.spinel.schema.FieldMapping;
import java.util.BitSet;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class DependencyMapTest {
    private final DependencyMap map = new DependencyMap();

    @Test
    void shouldGrow() {
        IntStream.range(0, 32)
                .forEach(
                        i -> {
                            final var builder = FieldMapping.fieldMapping(2);
                            map.register(i, builder.build());
                        });
    }

    @Test
    void shouldMapChangeSetsForEachIndex() {
        final FieldBitSet inChanges = fieldBitSet();
        final BitSet observedChanges = new BitSet();
        final BitSet expectedChanges = new BitSet();
        IntStream.range(0, 32)
                .forEach(
                        i -> {
                            final var builder = FieldMapping.fieldMapping(i + 8);
                            builder.mapInboundToOutbound(i + 1, i + 7);
                            map.register(i, builder.build());
                            // test mapping of the index
                            inChanges.fieldChanged(i + 1);
                            inChanges.fieldChanged(i + 2);
                            map.translateChanges(i, inChanges, observedChanges::set);

                            expectedChanges.set(i + 7);
                            assertThat(observedChanges, equalTo(expectedChanges));
                            // reset
                            inChanges.clear();
                            expectedChanges.clear();
                            observedChanges.clear();
                        });
    }
}
