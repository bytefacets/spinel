// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.transform;

import static java.util.Objects.requireNonNull;

public final class ExplicitTransformNode<T> implements TransformNode<T> {
    private final String name;
    private final T operator;

    private ExplicitTransformNode(final String name, final T operator) {
        this.name = requireNonNull(name, "name");
        this.operator = requireNonNull(operator, "operator");
    }

    public static <T> TransformNode<T> transformNode(final String name, final T operator) {
        return new ExplicitTransformNode<>(name, operator);
    }

    @Override
    public T operator() {
        return operator;
    }

    @Override
    public String name() {
        return name;
    }
}
