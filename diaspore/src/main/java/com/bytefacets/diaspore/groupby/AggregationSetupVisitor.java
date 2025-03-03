// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.groupby;

import com.bytefacets.diaspore.schema.FieldDescriptor;

public interface AggregationSetupVisitor {
    default void addPreviousValueField(String fieldName) {}

    default void addInboundField(String fieldName) {}

    default void addOutboundField(FieldDescriptor outboundFieldDescriptor) {}
}
