// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.common;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntConsumer;
import com.bytefacets.diaspore.RowProvider;
import com.bytefacets.diaspore.TransformOutput;
import java.util.function.Supplier;

/**
 * A RowProvider that is used when the operator is going to report the same active rows as its
 * input. The Supplier it uses can either return null, if the operator hasn't yet been plugged in,
 * or the input. If the Supplier returns null, there are no active rows.
 */
public final class DelegatedRowProvider implements RowProvider {
    private final Supplier<TransformOutput> outputSupplier;

    private DelegatedRowProvider(final Supplier<TransformOutput> outputSupplier) {
        this.outputSupplier = requireNonNull(outputSupplier, "outputSupplier");
    }

    /**
     * Creates the DelegatedRowProvider with a Supplier. It can return null, or the output to which
     * this is delegating. When returning null, this provider will consider no rows active.
     */
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
