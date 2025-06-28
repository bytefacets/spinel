// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.common;

import com.bytefacets.collections.functional.IntConsumer;
import com.bytefacets.diaspore.RowProvider;
import java.util.BitSet;

/** A RowProvider backed by a BitSet */
public final class BitSetRowProvider implements RowProvider {
    private final BitSet activeRows;

    private BitSetRowProvider(final BitSet activeRows) {
        this.activeRows = activeRows;
    }

    public static BitSetRowProvider bitSetRowProvider(final BitSet activeRowSet) {
        return new BitSetRowProvider(activeRowSet);
    }

    public static BitSetRowProvider bitSetRowProvider() {
        return new BitSetRowProvider(new BitSet());
    }

    @Override
    public void forEach(final IntConsumer action) {
        for (int i = activeRows.nextSetBit(0); i >= 0; i = activeRows.nextSetBit(i + 1)) {
            // operate on index i here
            action.accept(i);
            if (i == Integer.MAX_VALUE) {
                break; // or (i+1) would overflow
            }
        }
    }
}
