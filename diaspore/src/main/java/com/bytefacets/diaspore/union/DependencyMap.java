// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.union;

import com.bytefacets.collections.arrays.GenericArray;
import com.bytefacets.collections.functional.IntConsumer;
import com.bytefacets.diaspore.schema.ChangedFieldSet;
import com.bytefacets.diaspore.schema.FieldMapping;
import javax.annotation.Nullable;

class DependencyMap {
    private FieldMapping[] sourceFieldMappings = new FieldMapping[2];

    void register(final int inputIndex, final @Nullable FieldMapping mapping) {
        sourceFieldMappings = GenericArray.ensureEntry(sourceFieldMappings, inputIndex);
        sourceFieldMappings[inputIndex] = mapping;
    }

    void translateChanges(
            final int inputIndex, final ChangedFieldSet inbound, final IntConsumer fieldChanged) {
        final var mapping = sourceFieldMappings[inputIndex];
        mapping.translateInboundChangeSet(inbound, fieldChanged);
    }
}
