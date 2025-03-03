// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.groupby;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.diaspore.schema.FieldResolver;

public interface AggregationFunction {
    void collectFieldReferences(AggregationSetupVisitor visitor);

    void bindToSchema(
            FieldResolver previousResolver,
            FieldResolver currentResolver,
            FieldResolver outboundResolver);

    void unbindSchema();

    void groupRowsAdded(int group, IntIterable rows);

    void groupRowsChanged(int group, IntIterable rows);

    void groupRowsRemoved(int group, IntIterable rows);
}
