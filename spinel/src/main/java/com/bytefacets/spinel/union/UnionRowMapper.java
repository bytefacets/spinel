// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.union;

import com.bytefacets.collections.arrays.StringArray;
import com.bytefacets.collections.bi.CompactOneToMany;
import com.bytefacets.collections.functional.IntConsumer;
import com.bytefacets.spinel.RowProvider;
import com.bytefacets.spinel.schema.IntField;
import com.bytefacets.spinel.schema.StringField;

final class UnionRowMapper {
    private final CompactOneToMany map;
    private String[] inputNames = StringArray.create(2);

    UnionRowMapper(final int initialSize) {
        this.map = new CompactOneToMany(16, initialSize, false);
    }

    RowProvider asRowProvider() {
        return map::forEachEntry;
    }

    void iterateInputRowsInOutput(final int inputIndex, final IntConsumer outputRowConsumer) {
        map.withLeft(inputIndex).forEachEntry(outputRowConsumer);
    }

    void mapInputName(final int inputIndex, final String name) {
        inputNames = StringArray.ensureEntry(inputNames, inputIndex);
        inputNames[inputIndex] = name;
    }

    int mapInputRow(final int inputIndex, final int sourceRow) {
        return map.put(inputIndex, sourceRow);
    }

    int lookupOutboundRow(final int inputIndex, final int sourceRow) {
        return map.lookupEntry(inputIndex, sourceRow);
    }

    void removeRowOnInputRemoval(final int outputRow) {
        map.removeAt(outputRow);
    }

    int removeInputRow(final int inputIndex, final int sourceRow) {
        final int outboundRow = map.lookupEntry(inputIndex, sourceRow);
        map.removeAtAndReserve(outboundRow);
        return outboundRow;
    }

    void freeOutRow(final int outboundRow) {
        map.freeReservedEntry(outboundRow);
    }

    int inputIndexOf(final int outboundRow) {
        return map.getLeftAt(outboundRow);
    }

    int inputRowOf(final int outboundRow) {
        return map.getRightAt(outboundRow);
    }

    IntField inputIndexField() {
        return map::getLeftAt;
    }

    StringField inputNameField() {
        return outboundRow -> {
            final int inputEntry = map.getLeftAt(outboundRow);
            return inputNames[inputEntry];
        };
    }
}
