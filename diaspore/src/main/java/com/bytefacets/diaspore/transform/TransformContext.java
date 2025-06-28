// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.transform;

import static com.bytefacets.diaspore.transform.TransformContinuation.throwIfMissingTransform;
import static com.bytefacets.diaspore.transform.TransformNodeImpl.transformNode;
import static java.util.Objects.requireNonNull;

import java.util.function.Supplier;

public final class TransformContext {
    private final TransformBuilder transform;
    private final OutputProvider source;
    private final String name;

    TransformContext(final String name, final TransformBuilder transform) {
        this(name, transform, null);
    }

    TransformContext(
            final String name,
            final TransformBuilder transform,
            final OutputProvider outputProvider) {
        this.name = requireNonNull(name, "name");
        this.transform = transform;
        this.source = outputProvider;
    }

    public static TransformContinuation continuation(
            final TransformContext context,
            final TransformNode<?> node,
            final OutputProvider outputProvider) {
        throwIfMissingTransform(context);
        return context.createContinuation(node, outputProvider);
    }

    public <T> BuilderSupport<T> createBuilderSupport(
            final Supplier<T> creator, final InputProvider inputProvider) {
        final var support = BuilderSupport.builderSupport(name, creator);
        transform.registerTransformNode(transformNode(() -> name, support::getOrCreate));
        if (source != null && inputProvider != null) {
            transform.registerEdge(source, inputProvider);
        }
        return support;
    }

    public TransformContinuation createContinuation(
            final TransformNode<?> node, final OutputProvider outputProvider) {
        return transform.createContinuation(node, outputProvider);
    }

    public String name() {
        return name;
    }
}
