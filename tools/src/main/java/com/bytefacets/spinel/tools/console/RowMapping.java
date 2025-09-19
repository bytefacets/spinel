// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.tools.console;

import com.bytefacets.collections.arrays.IntArray;
import com.bytefacets.collections.functional.IntConsumer;
import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.collections.queue.IntDeque;

/** Maps rows from the transformation to rows on screen */
final class RowMapping implements IntIterable {
    private static final int FIRST_SCREEN_ROW_FOR_DATA = 2;
    private int[] rowToScreenRow = IntArray.create(8, -1);
    private int[] screenRowToRow = IntArray.create(8, -1);
    private int maxScreenRowEstimate = 0;
    private final IntDeque freeScreenRows = new IntDeque(8);
    private int nextScreenRow = FIRST_SCREEN_ROW_FOR_DATA;

    RowMapping() {}

    void mapRow(final int row) {
        final int screenRow = allocateScreenRow();
        rowToScreenRow = IntArray.ensureEntry(rowToScreenRow, row, -1);
        screenRowToRow = IntArray.ensureEntry(screenRowToRow, screenRow, -1);
        screenRowToRow[screenRow] = row;
        rowToScreenRow[row] = screenRow;
        maxScreenRowEstimate = Math.max(screenRow, maxScreenRowEstimate);
    }

    void reset() {
        maxScreenRowEstimate = 0;
        nextScreenRow = FIRST_SCREEN_ROW_FOR_DATA;
        IntArray.fill(rowToScreenRow, -1);
        IntArray.fill(screenRowToRow, -1);
    }

    /** Iterate screen */
    @Override
    public void forEach(final IntConsumer intConsumer) {
        for (int screenRow = FIRST_SCREEN_ROW_FOR_DATA;
                screenRow <= maxScreenRowEstimate;
                screenRow++) {
            final int dataRow = screenRowToRow[screenRow];
            if (dataRow != -1) {
                intConsumer.accept(dataRow);
            }
        }
    }

    /** Look up the screen row when updating just a row */
    int screenRow(final int dataRow) {
        return dataRow < rowToScreenRow.length ? rowToScreenRow[dataRow] : -1;
    }

    int freeRow(final int row) {
        final int screenRow = rowToScreenRow[row];
        freeScreenRows.addLast(screenRow);
        screenRowToRow[screenRow] = -1;
        rowToScreenRow[row] = -1;
        return screenRow;
    }

    private int allocateScreenRow() {
        if (!freeScreenRows.isEmpty()) {
            return freeScreenRows.removeLast();
        } else {
            return nextScreenRow++;
        }
    }
}
