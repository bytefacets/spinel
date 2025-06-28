// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.schema;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntConsumer;
import java.util.BitSet;

public final class FieldBitSet implements ChangedFieldSet {
    private final BitSet fieldIds;

    private FieldBitSet(final BitSet fieldIds) {
        this.fieldIds = requireNonNull(fieldIds, "fieldIds");
    }

    public static FieldBitSet fieldBitSet() {
        return new FieldBitSet(new BitSet());
    }

    public static FieldBitSet fieldBitSet(final BitSet fieldIds) {
        return new FieldBitSet(fieldIds);
    }

    public void fieldChanged(final int fieldId) {
        fieldIds.set(fieldId);
    }

    public boolean isEmpty() {
        return fieldIds.isEmpty();
    }

    @Override
    public boolean isChanged(final int fieldId) {
        return fieldIds.get(fieldId);
    }

    @Override
    public int size() {
        return fieldIds.cardinality();
    }

    @Override
    public void forEach(final IntConsumer consumer) {
        for (int i = fieldIds.nextSetBit(0); i >= 0; i = fieldIds.nextSetBit(i + 1)) {
            // operate on index i here
            consumer.accept(i);
            if (i == Integer.MAX_VALUE) {
                break; // or (i+1) would overflow
            }
        }
    }

    @Override
    public boolean intersects(final BitSet dependencies) {
        return fieldIds.intersects(dependencies);
    }

    public void clear() {
        fieldIds.clear();
    }
}
