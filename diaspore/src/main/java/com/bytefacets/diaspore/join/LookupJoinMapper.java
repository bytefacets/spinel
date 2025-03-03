// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.join;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.arrays.IntArray;
import com.bytefacets.collections.arrays.LongArray;
import com.bytefacets.collections.types.Pack;
import com.bytefacets.diaspore.RowProvider;
import com.bytefacets.diaspore.common.BitSetRowProvider;
import com.bytefacets.diaspore.schema.RowMapper;
import java.util.BitSet;

final class LookupJoinMapper implements JoinMapper {
    private static final int UNSET = -1;
    private static final long EMPTY_MAPPING = Pack.packToLong(UNSET, UNSET);
    private final JoinListener listener;
    private final BitSet activeRows;
    private final BitSetRowProvider rowProvider;
    private final boolean outer;
    private final JoinInterner interner;
    private int[] leftRowToKey;
    private int[] rightRowToKey;
    private long[] keyToLeftRight;

    LookupJoinMapper(
            final JoinInterner interner,
            final JoinListener listener,
            final int leftInitialCapacity,
            final int initialRightCapacity,
            final boolean outer) {
        this.interner = requireNonNull(interner, "interner");
        this.activeRows = new BitSet(leftInitialCapacity);
        this.rowProvider = BitSetRowProvider.bitSetRowProvider(activeRows);
        this.listener = requireNonNull(listener, "listener");
        this.leftRowToKey = IntArray.create(leftInitialCapacity, UNSET);
        this.rightRowToKey = IntArray.create(initialRightCapacity, UNSET);
        this.keyToLeftRight = LongArray.create(leftInitialCapacity, EMPTY_MAPPING);
        this.outer = outer;
    }

    @Override
    public RowProvider rowProvider() {
        return rowProvider;
    }

    @Override
    public JoinInterner interner() {
        return interner;
    }

    @Override
    public RowMapper leftMapper() {
        return row -> row;
    }

    @Override
    public RowMapper rightMapper() {
        return row -> {
            if (row >= 0 && row < leftRowToKey.length) {
                final int key = leftRowToKey[row];
                if (key != UNSET) {
                    final long mapping = keyToLeftRight[key];
                    return Pack.unpackLoInt(mapping);
                }
            }
            return -1;
        };
    }

    @Override
    public void leftRowAdd(final int leftRow) {
        final int joinKey = interner.left().intern(leftRow);
        mapLeftRowKey(leftRow, joinKey);
        mapLeftRow(leftRow, UNSET, joinKey);
    }

    @Override
    public void rightRowAdd(final int rightRow) {
        final int joinKey = interner.right().intern(rightRow);
        mapNewRight(rightRow, UNSET, joinKey);
    }

    @Override
    public void leftRowChange(final int leftRow, final boolean reEvalKey) {
        final int oldJoinKey = leftRowToKey[leftRow];
        if (reEvalKey) {
            final int newJoinKey = interner.left().intern(leftRow);
            mapLeftRowKey(leftRow, newJoinKey);
            if (newJoinKey != oldJoinKey) {
                // leftRowToKey[leftRow] = newJoinKey;
                mapLeftRow(leftRow, oldJoinKey, newJoinKey);
                return;
            }
        }
        if (activeRows.get(leftRow)) {
            listener.joinUpdated(leftRow, false, false);
        }
    }

    @Override
    public void rightRowChange(final int rightRow, final boolean reEvalKey) {
        final int oldJoinKey = rightRowToKey[rightRow];
        if (reEvalKey) {
            final int newJoinKey = interner.right().intern(rightRow);
            if (newJoinKey != oldJoinKey) {
                mapNewRight(rightRow, oldJoinKey, newJoinKey);
                return;
            }
        }
        final long curMapping = keyToLeftRight[oldJoinKey];
        final int leftRow = Pack.unpackHiInt(curMapping);
        if (leftRow != UNSET) {
            listener.joinUpdated(leftRow, false, false);
        }
    }

    @Override
    public void leftRowRemove(final int leftRow) {
        final int joinKey = leftRowToKey[leftRow];
        leftRowToKey[leftRow] = UNSET;
        final long mapping = keyToLeftRight[joinKey];
        final int rightRow = Pack.unpackLoInt(mapping);
        keyToLeftRight[joinKey] = Pack.packToLong(UNSET, rightRow);
        if (outer || rightRow != UNSET) {
            activeRows.clear(leftRow);
            listener.joinRemoved(leftRow);
        }
    }

    @Override
    public void rightRowRemove(final int rightRow) {
        final int joinKey = rightRowToKey[rightRow];
        rightRowToKey[rightRow] = UNSET;
        unmapRight(joinKey);
    }

    @Override
    public void cleanUpRemovedRow(final int outRow) {}

    private void mapLeftRow(final int leftRow, final int oldJoinKey, final int newJoinKey) {
        if (oldJoinKey != UNSET) {
            final long oldMapping = keyToLeftRight[oldJoinKey];
            final int oldRight = Pack.unpackLoInt(oldMapping);
            keyToLeftRight[oldJoinKey] = Pack.packToLong(UNSET, oldRight);
        }

        final long newMapping = keyToLeftRight[newJoinKey];
        final int newRight = Pack.unpackLoInt(newMapping);
        keyToLeftRight[newJoinKey] = Pack.packToLong(leftRow, newRight);

        final boolean oldActive = activeRows.get(leftRow);
        final boolean newActive = outer || newRight != UNSET;
        if (newActive && !oldActive) {
            activeRows.set(leftRow);
            listener.joinAdded(leftRow);
        } else if (!newActive && oldActive) {
            activeRows.clear(leftRow);
            listener.joinRemoved(leftRow);
        } else if (newActive) {
            listener.joinUpdated(leftRow, false, oldJoinKey != newJoinKey);
        }
    }

    private void mapNewRight(final int rightRow, final int oldJoinKey, final int newJoinKey) {
        final boolean keyChange = oldJoinKey != newJoinKey;
        if (oldJoinKey != UNSET && keyChange) {
            final long oldMapping = keyToLeftRight[oldJoinKey];
            final int oldLeft = Pack.unpackHiInt(oldMapping);
            keyToLeftRight[oldJoinKey] = Pack.packToLong(oldLeft, UNSET);
            if (oldLeft != UNSET) {
                if (outer) {
                    listener.joinUpdated(oldLeft, false, true);
                } else {
                    activeRows.clear(oldLeft);
                    listener.joinRemoved(oldLeft);
                }
            }
        }
        mapRightRowKey(rightRow, newJoinKey);
        final long newMapping = keyToLeftRight[newJoinKey];
        final int newLeft = Pack.unpackHiInt(newMapping);
        keyToLeftRight[newJoinKey] = Pack.packToLong(newLeft, rightRow);
        if (newLeft != UNSET) {
            if (outer) {
                listener.joinUpdated(newLeft, false, keyChange);
            } else {
                activeRows.set(newLeft);
                listener.joinAdded(newLeft);
            }
        }
    }

    private void unmapRight(final int oldJoinKey) {
        final long curMapping = keyToLeftRight[oldJoinKey];
        final int oldLeft = Pack.unpackHiInt(curMapping);
        keyToLeftRight[oldJoinKey] = Pack.packToLong(oldLeft, UNSET);
        if (oldLeft != UNSET) {
            if (!outer) {
                activeRows.clear(oldLeft);
                listener.joinRemoved(oldLeft);
            } else {
                listener.joinUpdated(oldLeft, false, true);
            }
        }
    }

    private void mapRightRowKey(final int rightRow, final int joinKey) {
        rightRowToKey = IntArray.ensureEntry(rightRowToKey, rightRow, UNSET);
        rightRowToKey[rightRow] = joinKey;

        keyToLeftRight = LongArray.ensureEntry(keyToLeftRight, joinKey, EMPTY_MAPPING);
    }

    private void mapLeftRowKey(final int leftRow, final int joinKey) {
        leftRowToKey = IntArray.ensureEntry(leftRowToKey, leftRow, UNSET);
        leftRowToKey[leftRow] = joinKey;

        keyToLeftRight = LongArray.ensureEntry(keyToLeftRight, joinKey, EMPTY_MAPPING);
    }
}
