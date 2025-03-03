// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.conflation;

import static com.bytefacets.diaspore.common.DefaultNameSupplier.defaultPrefixedName;
import static java.util.Objects.requireNonNullElseGet;

public final class ConflationBuilder {
    private int initialCapacity = 128;
    private int maxPendingRows = 128;
    private String name;

    private ConflationBuilder() {}

    public static ConflationBuilder conflation() {
        return new ConflationBuilder();
    }

    public ConflationBuilder named(final String name) {
        this.name = name;
        return this;
    }

    public ConflationBuilder initialCapacity(final int initialCapacity) {
        this.initialCapacity = initialCapacity;
        return this;
    }

    public ConflationBuilder maxPendingRows(final int maxPendingRows) {
        this.maxPendingRows = maxPendingRows;
        return this;
    }

    private String conflationName() {
        return requireNonNullElseGet(name, () -> defaultPrefixedName("Conflation"));
    }

    public Conflation build() {
        return new Conflation(
                new ConflationSchemaBuilder(conflationName()), initialCapacity, maxPendingRows);
    }
}
