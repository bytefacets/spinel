// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.transform;

import static java.util.Objects.requireNonNull;

import java.util.function.Supplier;

public final class DeferredTransformNode<T> implements TransformNode<T> {
    private final Supplier<String> nameSupplier;
    private final Supplier<T> operatorSupplier;

    private DeferredTransformNode(
            final Supplier<String> nameSupplier, final Supplier<T> operatorSupplier) {
        this.nameSupplier = requireNonNull(nameSupplier, "nameSupplier");
        this.operatorSupplier = requireNonNull(operatorSupplier, "operatorSupplier");
    }

    public static <T> TransformNode<T> deferredTransformNode(
            final Supplier<String> nameSupplier, final Supplier<T> operatorSupplier) {
        return new DeferredTransformNode<>(nameSupplier, operatorSupplier);
    }

    @Override
    public T operator() {
        return operatorSupplier.get();
    }

    @Override
    public String name() {
        return nameSupplier.get();
    }
}
