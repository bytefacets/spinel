// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.conflation;

import static com.bytefacets.spinel.common.DefaultNameSupplier.resolveName;
import static com.bytefacets.spinel.transform.BuilderSupport.builderSupport;
import static com.bytefacets.spinel.transform.TransformContext.continuation;
import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.transform.BuilderSupport;
import com.bytefacets.spinel.transform.TransformContext;
import com.bytefacets.spinel.transform.TransformContinuation;

/**
 * Builder for a {@link ChangeConflator}. To be called directly from {@link #changeConflator()} or
 * {@link #changeConflator(String)}, or from a {@link
 * com.bytefacets.spinel.transform.TransformBuilder} or {@link TransformContinuation}.
 */
public final class ChangeConflatorBuilder {
    private final BuilderSupport<ChangeConflator> builderSupport;
    private final TransformContext transformContext;
    private final String name;
    private int initialCapacity = 128;
    private int maxPendingRows = 128;

    private ChangeConflatorBuilder(final String name) {
        this.name = requireNonNull(name, "name");
        this.builderSupport = builderSupport(name, this::internalBuild);
        this.transformContext = null;
    }

    private ChangeConflatorBuilder(final TransformContext context) {
        this.transformContext = requireNonNull(context, "transform context");
        this.name = context.name();
        this.builderSupport =
                context.createBuilderSupport(this::internalBuild, () -> getOrCreate().input());
    }

    public static ChangeConflatorBuilder changeConflator() {
        return changeConflator((String) null);
    }

    public static ChangeConflatorBuilder changeConflator(final String name) {
        return new ChangeConflatorBuilder(resolveName("ChangeConflator", name));
    }

    public static ChangeConflatorBuilder changeConflator(final TransformContext transformContext) {
        return new ChangeConflatorBuilder(transformContext);
    }

    /**
     * Used in the context of a TransformBuilder to chain the conflator to some next operator. This
     * call is only valid for ChangeConflatorBuilders that were created using {@link
     * #changeConflator(TransformContext)}, which usually comes from {@link
     * TransformBuilder#changeConflator()} or {@link TransformBuilder#changeConflator(String)}.
     *
     * @see TransformBuilder
     */
    public TransformContinuation then() {
        return continuation(
                transformContext, builderSupport.transformNode(), () -> getOrCreate().output());
    }

    /** An initial capacity for the tracking inbound rows. Default is 128. */
    public ChangeConflatorBuilder initialCapacity(final int initialCapacity) {
        this.initialCapacity = initialCapacity;
        return this;
    }

    /**
     * The maximum number of distinct pending rows. When the max is reached, the conflator will
     * start to emit row changes. Default is 128.
     */
    public ChangeConflatorBuilder maxPendingRows(final int maxPendingRows) {
        this.maxPendingRows = maxPendingRows;
        return this;
    }

    private ChangeConflator internalBuild() {
        return new ChangeConflator(
                new ChangeConflatorSchemaBuilder(name), initialCapacity, maxPendingRows);
    }

    public ChangeConflator getOrCreate() {
        return builderSupport.getOrCreate();
    }

    public ChangeConflator build() {
        return builderSupport.createOperator();
    }
}
