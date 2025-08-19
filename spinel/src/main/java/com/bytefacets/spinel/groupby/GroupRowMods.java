// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.groupby;

import com.bytefacets.collections.arrays.IntArray;
import com.bytefacets.collections.functional.IntConsumer;
import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.collections.hash.IntIndexedSet;

final class GroupRowMods {
    private final IntIndexedSet set;
    private final Iterator iterator = new Iterator();
    private int nextPosition = 0;
    private int[] heads;
    private int[] nexts;
    private int[] values;

    GroupRowMods(final int initialSize) {
        set = new IntIndexedSet(initialSize);
        heads = IntArray.create(16, -1);
        nexts = IntArray.create(16, -1);
        values = IntArray.create(16, -1);
    }

    void addGroupRow(final int group, final int row) {
        final int position = nextPosition++;
        values = IntArray.ensureEntry(values, position, -1);
        nexts = IntArray.ensureEntry(nexts, position, -1);

        final int groupEntry = set.add(group);
        heads = IntArray.ensureEntry(heads, groupEntry, -1);

        final int head = heads[groupEntry];
        heads[groupEntry] = position;
        values[position] = row;
        if (head != -1) {
            nexts = IntArray.ensureEntry(nexts, head, -1);
            nexts[position] = head;
        }
    }

    void fire(final GroupUpdateMethod consumer) {
        if (set.isEmpty()) {
            return;
        }
        set.forEachEntry(
                groupEntry -> {
                    iterator.init(groupEntry);
                    consumer.notifyGroupRows(iterator.group, iterator);
                });
    }

    interface GroupUpdateMethod {
        void notifyGroupRows(int group, IntIterable rows);
    }

    private final class Iterator implements IntIterable {
        private int group;
        private int position;

        void init(final int groupEntry) {
            this.group = set.getKeyAt(groupEntry);
            this.position = heads[groupEntry];
        }

        @Override
        public void forEach(final IntConsumer action) {
            while (position != -1) {
                final int row = values[position];
                action.accept(row);
                position = nexts[position];
            }
        }
    }

    void reset() {
        if (nextPosition != 0) {
            IntArray.fill(values, -1, 0, nextPosition);
            IntArray.fill(nexts, -1, 0, nextPosition);
            IntArray.fill(heads, -1, 0, set.size());
            set.clear();
        }
    }

    boolean isEmpty() {
        return nextPosition == 0;
    }
}
