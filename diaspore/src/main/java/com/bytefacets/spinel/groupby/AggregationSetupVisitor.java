// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.groupby;

import com.bytefacets.spinel.schema.FieldDescriptor;

public interface AggregationSetupVisitor {
    default void addPreviousValueField(String fieldName) {}

    default void addInboundField(String fieldName) {}

    default void addOutboundField(FieldDescriptor outboundFieldDescriptor) {}
}
