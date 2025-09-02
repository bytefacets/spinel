// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.filter;

import static com.bytefacets.spinel.common.DefaultNameSupplier.resolveName;
import static com.bytefacets.spinel.filter.JexlRowPredicate.jexlPredicate;
import static com.bytefacets.spinel.transform.BuilderSupport.builderSupport;
import static com.bytefacets.spinel.transform.TransformContext.continuation;
import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.schema.FieldResolver;
import com.bytefacets.spinel.transform.BuilderSupport;
import com.bytefacets.spinel.transform.TransformContext;
import com.bytefacets.spinel.transform.TransformContinuation;

public final class FilterBuilder {
    private final BuilderSupport<Filter> builderSupport;
    private final TransformContext transformContext;
    private int initialSize = 64;
    private boolean passesWhenNoPredicate = false;
    private RowPredicate initialPredicate = null;
    private final String name;

    // UPCOMING originalRowId as a field?
    private FilterBuilder(final String name) {
        this.name = requireNonNull(name, "name");
        this.builderSupport = builderSupport(name, this::internalBuild);
        this.transformContext = null;
    }

    private FilterBuilder(final TransformContext context) {
        this.transformContext = requireNonNull(context, "transform context");
        this.name = context.name();
        this.builderSupport =
                context.createBuilderSupport(this::internalBuild, () -> getOrCreate().input());
    }

    public static FilterBuilder filter() {
        return filter((String) null);
    }

    public static FilterBuilder filter(final String name) {
        return new FilterBuilder(resolveName("Filter", name));
    }

    public static FilterBuilder filter(final TransformContext transformContext) {
        return new FilterBuilder(transformContext);
    }

    /**
     * The initial predicate for the Filter. This is optional, and you can change the Filter's
     * RowPredicate while it's running. If you do not set this, the default behavior of a Filter
     * with no RowPredicate is governed by {@link #passesWhenNoPredicate}.
     */
    public FilterBuilder where(final RowPredicate predicate) {
        this.initialPredicate = predicate;
        return this;
    }

    /**
     * The initial predicate for the Filter. This is optional, and you can change the Filter's
     * RowPredicate while it's running. If you do not set this, the default behavior of a Filter
     * with no RowPredicate is governed by {@link #passesWhenNoPredicate}.
     */
    public FilterBuilder where(final String predicate) {
        this.initialPredicate = jexlPredicate(predicate);
        return this;
    }

    /** Initial size of the set managing the active rows. Default is 64. */
    public FilterBuilder initialSize(final int initialSize) {
        if (initialSize <= 0) {
            throw new IllegalArgumentException("initialSize must be > 0, but was " + initialSize);
        }
        this.initialSize = initialSize;
        return this;
    }

    /**
     * Whether a row is forwarded by the Filter when there is no RowPredicate set. The default is
     * false, meaning "rows do not pass when there is no predicate."
     */
    public FilterBuilder passesWhenNoPredicate(final boolean passesWhenNoPredicate) {
        this.passesWhenNoPredicate = passesWhenNoPredicate;
        return this;
    }

    public Filter getOrCreate() {
        return builderSupport.getOrCreate();
    }

    public Filter build() {
        return builderSupport.createOperator();
    }

    public TransformContinuation then() {
        return continuation(
                transformContext, builderSupport.transformNode(), () -> getOrCreate().output());
    }

    private Filter internalBuild() {
        builderSupport.throwIfBuilt();
        final var filter = new Filter(name, initialSize, passesWhenNoPredicate ? PASSES : FAILS);
        filter.updatePredicate(initialPredicate);
        return filter;
    }

    private abstract static class ConstantPredicate implements RowPredicate {
        @Override
        public void bindToSchema(final FieldResolver fieldResolver) {}

        @Override
        public void unbindSchema() {}
    }

    private static final ConstantPredicate PASSES =
            new ConstantPredicate() {
                @Override
                public boolean testRow(final int row) {
                    return true;
                }
            };
    private static final ConstantPredicate FAILS =
            new ConstantPredicate() {
                @Override
                public boolean testRow(final int row) {
                    return false;
                }
            };
}
