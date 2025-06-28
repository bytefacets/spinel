// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.transform;

import static com.bytefacets.diaspore.common.DefaultNameSupplier.resolveName;
import static com.bytefacets.diaspore.exception.OperatorSetupException.setupException;
import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.conflation.ChangeConflatorBuilder;
import com.bytefacets.diaspore.filter.FilterBuilder;
import com.bytefacets.diaspore.groupby.GroupByBuilder;
import com.bytefacets.diaspore.projection.ProjectionBuilder;
import com.bytefacets.diaspore.prototype.PrototypeBuilder;
import com.bytefacets.diaspore.union.UnionBuilder;
import javax.annotation.Nullable;

public class TransformContinuation {
    private final TransformBuilder owner;
    private final TransformNode<?> node;
    private final OutputProvider sourceOutput;

    TransformContinuation(
            final TransformBuilder owner,
            final TransformNode<?> node,
            final OutputProvider sourceOutput) {
        this.owner = requireNonNull(owner, "owner");
        this.node = requireNonNull(node, "node");
        this.sourceOutput = requireNonNull(sourceOutput, "sourceNode");
    }

    public static TransformContinuation continuation(
            final TransformBuilder transform,
            final TransformNode<?> node,
            final OutputProvider outputProvider) {
        throwIfMissingTransform(transform);
        return transform.createContinuation(node, outputProvider);
    }

    public static void throwIfMissingTransform(final TransformBuilder transform) {
        if (transform == null) {
            throw setupException(
                    "continuation method must be called in the context of building a Transform using TransformBuilder");
        }
    }

    public static void throwIfMissingTransform(final TransformContext transform) {
        if (transform == null) {
            throw setupException(
                    "continuation method must be called in the context of building a Transform using TransformBuilder");
        }
    }

    public FilterBuilder filter() {
        return filter(null);
    }

    public FilterBuilder filter(final @Nullable String name) {
        return FilterBuilder.filter(newContext(resolveName("Filter", name)));
    }

    public GroupByBuilder groupBy() {
        return groupBy(null);
    }

    public GroupByBuilder groupBy(final @Nullable String name) {
        return GroupByBuilder.groupBy(newContext(resolveName("GroupBy", name)));
    }

    public ProjectionBuilder project() {
        return project(null);
    }

    public ProjectionBuilder project(final @Nullable String name) {
        return ProjectionBuilder.projection(newContext(resolveName("Projection", name)));
    }

    public PrototypeBuilder prototyped(final String name) {
        return PrototypeBuilder.prototype(newContext(resolveName("Prototype", name)));
    }

    public PrototypeBuilder prototyped() {
        return prototyped(null);
    }

    public UnionBuilder union() {
        return union(null);
    }

    public UnionBuilder union(final @Nullable String name) {
        return UnionBuilder.union(newContext(resolveName("Union", name)));
    }

    /**
     * @see com.bytefacets.diaspore.conflation.ChangeConflator
     */
    public ChangeConflatorBuilder changeConflator() {
        return changeConflator(null);
    }

    /**
     * @see com.bytefacets.diaspore.conflation.ChangeConflator
     */
    public ChangeConflatorBuilder changeConflator(final @Nullable String name) {
        return ChangeConflatorBuilder.changeConflator(
                newContext(resolveName("ChangeConflator", name)));
    }

    private TransformContext newContext(final String name) {
        return new TransformContext(name, owner, sourceOutput);
    }
}
