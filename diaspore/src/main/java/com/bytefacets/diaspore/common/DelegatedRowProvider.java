// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.common;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntConsumer;
import com.bytefacets.diaspore.TransformOutput;
import com.bytefacets.diaspore.RowProvider;
import java.util.function.Supplier;

public final class DelegatedRowProvider implements RowProvider {
    private final Supplier<TransformOutput> outputSupplier;

    private DelegatedRowProvider(final Supplier<TransformOutput> outputSupplier) {
        this.outputSupplier = requireNonNull(outputSupplier, "outputSupplier");
    }

    public static DelegatedRowProvider delegatedRowProvider(
            final Supplier<TransformOutput> outputSupplier) {
        return new DelegatedRowProvider(outputSupplier);
    }

    @Override
    public void forEach(final IntConsumer rowConsumer) {
        final TransformOutput output = outputSupplier.get();
        if (output != null) {
            output.rowProvider().forEach(rowConsumer);
        }
    }
}
