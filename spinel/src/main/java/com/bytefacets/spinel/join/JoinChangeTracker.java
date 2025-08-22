// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.join;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntConsumer;
import com.bytefacets.collections.hash.IntIndexedSet;
import com.bytefacets.spinel.common.InputNotifier;
import com.bytefacets.spinel.schema.FieldBitSet;
import jakarta.annotation.Nullable;
import java.util.BitSet;

final class JoinChangeTracker implements JoinListener {
    private final BitSet changedFields;
    private final FieldBitSet outboundFieldSet;
    private final IntIndexedSet addedRows = new IntIndexedSet(16);
    private final IntIndexedSet changedRows = new IntIndexedSet(16);
    private final IntIndexedSet removedRows = new IntIndexedSet(16);
    private BitSet leftFields;
    private BitSet rightFields;

    public static JoinChangeTracker stateChangeSet(final BitSet changeSet) {
        return new JoinChangeTracker(changeSet);
    }

    public static JoinChangeTracker stateChangeSet() {
        return new JoinChangeTracker(new BitSet());
    }

    JoinChangeTracker(final BitSet changeSet) {
        changedFields = requireNonNull(changeSet, "changeSet");
        outboundFieldSet = FieldBitSet.fieldBitSet(changeSet);
    }

    void outboundFieldIds(final BitSet leftFields, final BitSet rightFields) {
        this.leftFields = requireNonNull(leftFields, "leftFields");
        this.rightFields = requireNonNull(rightFields, "rightFields");
    }

    @Override
    public void joinAdded(final int outRow) {
        addedRows.add(outRow);
        removedRows.remove(outRow);
    }

    @Override
    public void joinUpdated(
            final int outRow, final boolean leftReplaced, final boolean rightReplaced) {
        if (!addedRows.containsKey(outRow)) {
            changedRows.add(outRow);
            if (leftReplaced) {
                changedFields.or(leftFields);
            }
            if (rightReplaced) {
                changedFields.or(rightFields);
            }
        }
    }

    @Override
    public void joinRemoved(final int outRow) {
        addedRows.remove(outRow);
        changedRows.remove(outRow);
        removedRows.add(outRow);
    }

    public void changeField(final int fieldId) {
        changedFields.set(fieldId);
    }

    void fire(final InputNotifier manager, @Nullable final IntConsumer removedRowConsumer) {
        if (!removedRows.isEmpty()) {
            manager.notifyRemoves(removedRows);
        }
        if (!addedRows.isEmpty()) {
            manager.notifyAdds(addedRows);
        }
        if (!changedRows.isEmpty()) {
            manager.notifyChanges(changedRows, outboundFieldSet);
        }
        if (removedRowConsumer != null && !removedRows.isEmpty()) {
            removedRows.forEach(removedRowConsumer);
        }
        reset();
    }

    private void reset() {
        removedRows.clear();
        addedRows.clear();
        changedRows.clear();
        changedFields.clear();
    }
}
