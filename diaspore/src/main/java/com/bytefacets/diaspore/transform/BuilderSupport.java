// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.transform;

import static com.bytefacets.diaspore.exception.OperatorSetupException.setupException;
import static java.util.Objects.requireNonNull;

import java.util.function.Supplier;

public final class BuilderSupport<T> {
    private final String name;
    private final Supplier<T> builder;
    private final TransformNode<T> node;
    private T operator;

    private BuilderSupport(final String name, final Supplier<T> builder) {
        this.name = requireNonNull(name, "name");
        this.builder = requireNonNull(builder, "builder");
        this.node = createNode();
    }

    public static <T> BuilderSupport<T> builderSupport(
            final String name, final Supplier<T> builder) {
        return new BuilderSupport<>(name, builder);
    }

    public boolean isOperatorBuilt() {
        return operator != null;
    }

    public T getOrCreate() {
        if (operator == null) {
            operator = builder.get();
        }
        return operator;
    }

    public T createOperator() {
        throwIfBuilt();
        operator = builder.get();
        return operator;
    }

    public void throwIfBuilt() {
        if (operator != null) {
            throw setupException(name + " is already built");
        }
    }

    public TransformNode<T> transformNode() {
        return node;
    }

    private TransformNode<T> createNode() {
        return DeferredTransformNode.deferredTransformNode(() -> name, this::getOrCreate);
    }
}
