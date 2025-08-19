// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.projection;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

import com.bytefacets.spinel.schema.Metadata;

record CalculatedFieldDescriptor(String name, FieldCalculation calculation, Metadata metadata) {
    CalculatedFieldDescriptor(
            final String name, final FieldCalculation calculation, final Metadata metadata) {
        this.name = requireNonNull(name, "name");
        this.calculation = requireNonNull(calculation, "calculation");
        this.metadata = requireNonNullElse(metadata, Metadata.EMPTY);
    }
}
