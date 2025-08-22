// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.union;

import static com.bytefacets.spinel.common.DefaultNameSupplier.resolveName;
import static com.bytefacets.spinel.transform.BuilderSupport.builderSupport;
import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.transform.BuilderSupport;
import com.bytefacets.spinel.transform.TransformContext;
import jakarta.annotation.Nullable;

public final class UnionBuilder {
    private final BuilderSupport<Union> builderSupport;
    private final TransformContext transformContext;
    private int initialSize = 128;
    private String inputIdFieldName;
    private String inputNameFieldName;
    private final String name;

    private UnionBuilder(final String name) {
        this.name = requireNonNull(name);
        this.builderSupport = builderSupport(this.name, this::internalBuild);
        this.transformContext = null;
    }

    private UnionBuilder(final TransformContext transformContext) {
        this.transformContext = requireNonNull(transformContext, "transform context");
        this.name = transformContext.name();
        this.builderSupport = transformContext.createBuilderSupport(this::internalBuild, null);
    }

    public static UnionBuilder union() {
        return union((String) null);
    }

    public static UnionBuilder union(final @Nullable String name) {
        return new UnionBuilder(resolveName("Union", name));
    }

    public static UnionBuilder union(final TransformContext transformContext) {
        return new UnionBuilder(transformContext);
    }

    public UnionBuilder initialSize(final int initialSize) {
        this.initialSize = initialSize;
        return this;
    }

    public UnionBuilder inputIdFieldName(final String inputIdFieldName) {
        this.inputIdFieldName = inputIdFieldName;
        return this;
    }

    public UnionBuilder inputNameFieldName(final String inputNameFieldName) {
        this.inputNameFieldName = inputNameFieldName;
        return this;
    }

    public Union getOrCreate() {
        return builderSupport.getOrCreate();
    }

    public Union build() {
        return builderSupport.createOperator();
    }

    private Union internalBuild() {
        builderSupport.throwIfBuilt();
        return new Union(
                initialSize, new UnionSchemaBuilder(name, inputNameFieldName, inputIdFieldName));
    }
}
