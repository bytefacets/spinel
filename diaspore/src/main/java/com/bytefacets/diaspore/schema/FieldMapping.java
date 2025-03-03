// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.schema;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.arrays.IntArray;
import com.bytefacets.collections.functional.IntConsumer;

public final class FieldMapping {
    private final int[] inboundToOutbound;

    private FieldMapping(final int[] inboundToOutbound) {
        this.inboundToOutbound = requireNonNull(inboundToOutbound, "inboundToOutbound");
    }

    public void translateInboundChangeSet(
            final ChangedFieldSet inboundSet, final IntConsumer outboundFieldIdConsumer) {
        inboundSet.forEach(
                inboundField -> {
                    final int outboundField = outboundFieldId(inboundField);
                    if (outboundField != -1) {
                        outboundFieldIdConsumer.accept(outboundField);
                    }
                });
    }

    public int outboundFieldId(final int inboundFieldId) {
        if (inboundFieldId >= 0 && inboundFieldId < inboundToOutbound.length) {
            return inboundToOutbound[inboundFieldId];
        } else {
            return -1;
        }
    }

    public static Builder fieldMapping(final int initialSize) {
        return new Builder(initialSize);
    }

    public static final class Builder {
        private int[] inboundToOutbound;

        private Builder(final int initialSize) {
            inboundToOutbound = IntArray.create(initialSize, -1);
        }

        public void mapInboundToOutbound(final int inboundFieldId, final int outboundFieldId) {
            inboundToOutbound = IntArray.ensureEntry(inboundToOutbound, inboundFieldId, -1);
            inboundToOutbound[inboundFieldId] = outboundFieldId;
        }

        public FieldMapping build() {
            return new FieldMapping(inboundToOutbound);
        }
    }
}
