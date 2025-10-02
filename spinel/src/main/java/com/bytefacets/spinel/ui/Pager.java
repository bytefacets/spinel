// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.ui;

import com.bytefacets.collections.arrays.IntArray;
import com.bytefacets.collections.functional.IntConsumer;
import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.collections.store.IntChunkStore;
import com.bytefacets.collections.store.IntStore;
import java.util.Arrays;

/** A Paging utility that keeps a compact, bidirectional mapping of row to view port position. */
public final class Pager {
    private final IntStore activeRows;
    private final IntStore rowToPosition;
    private int[] removedPositions = new int[16];
    private int numRemoved;
    private int limit;

    public static Pager pager(final int initialSize) {
        return pager(initialSize, 128);
    }

    public static Pager pager(final int initialSize, final int chunkSize) {
        return new Pager(initialSize, chunkSize);
    }

    private Pager(final int initialSize, final int chunkSize) {
        activeRows = new IntChunkStore(initialSize, chunkSize);
        rowToPosition = new IntChunkStore(initialSize, chunkSize);
    }

    public void add(final IntIterable rows) {
        rows.forEach(this::internalAdd);
    }

    public void remove(final IntIterable rows) {
        rows.forEach(this::internalRemove);
        compact();
    }

    public void clear() {
        limit = 0;
        // don't need to do anything else here
    }

    public void rowsInRange(final int offset, final int limit, final IntConsumer consumer) {
        final int end = Math.min(this.limit, offset + limit);
        final int start = Math.min(offset, this.limit);
        for (int position = start; position < end; position++) {
            consumer.accept(activeRows.getInt(position));
        }
    }

    public int size() {
        return limit;
    }

    private void internalAdd(final int row) {
        rowToPosition.setInt(row, limit);
        activeRows.setInt(limit, row);
        limit++;
    }

    private void internalRemove(final int row) {
        removedPositions = IntArray.ensureEntry(removedPositions, numRemoved);
        final int position = rowToPosition.getInt(row);
        removedPositions[numRemoved++] = position;
    }

    private void compact() {
        if (numRemoved == 0) {
            return;
        }

        Arrays.sort(removedPositions, 0, numRemoved);

        int writePos = removedPositions[0]; // first hole
        int readPos = removedPositions[0] + 1;

        // index of current removed slot we’re skipping
        int removeIdx = 1;

        while (readPos < limit) {
            if (removeIdx < numRemoved && readPos == removedPositions[removeIdx]) {
                // skip over this removed position
                readPos++;
                removeIdx++;
            } else {
                // move row down
                final int row = activeRows.getInt(readPos++);
                activeRows.setInt(writePos, row);
                rowToPosition.setInt(row, writePos);
                writePos++;
            }
        }

        // adjust limit
        limit = writePos;
        numRemoved = 0;
    }

    // VisibleForTesting
    int position(final int row) {
        return rowToPosition.getInt(row);
    }

    // VisibleForTesting
    int row(final int position) {
        return activeRows.getInt(position);
    }
}
