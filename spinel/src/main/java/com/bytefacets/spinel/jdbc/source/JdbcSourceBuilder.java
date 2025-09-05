// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.jdbc.source;

import static com.bytefacets.spinel.common.DefaultNameSupplier.resolveName;
import static com.bytefacets.spinel.jdbc.source.JdbcSourceBindingProvider.jdbcSourceBindingProvider;
import static com.bytefacets.spinel.schema.MatrixStoreFieldFactory.matrixStoreFieldFactory;
import static com.bytefacets.spinel.transform.BuilderSupport.builderSupport;
import static com.bytefacets.spinel.transform.TransformContext.continuation;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

import com.bytefacets.spinel.transform.BuilderSupport;
import com.bytefacets.spinel.transform.TransformContext;
import com.bytefacets.spinel.transform.TransformContinuation;
import java.util.Map;
import java.util.function.Supplier;

public final class JdbcSourceBuilder {
    private static final JdbcToFieldNamer DEFAULT_NAMER = JdbcToFieldNamers.TitleCase;
    private final String name;
    private final BuilderSupport<JdbcSource> builderSupport;
    private final TransformContext transformContext;
    private final JdbcSourceBindingProvider bindingProvider = jdbcSourceBindingProvider();
    private JdbcToFieldNamer jdbcToFieldNamer = DEFAULT_NAMER;
    private int initialSize = 128;
    private int chunkSize = 128;
    private int batchSize = 128;

    private JdbcSourceBuilder(final String name) {
        this.name = requireNonNull(name, "name");
        this.builderSupport = builderSupport(name, this::internalBuild);
        this.transformContext = null;
    }

    private JdbcSourceBuilder(final TransformContext context) {
        this.transformContext = requireNonNull(context, "transform context");
        this.name = context.name();
        this.builderSupport = context.createBuilderSupport(this::internalBuild, null);
    }

    public JdbcSource getOrCreate() {
        return builderSupport.getOrCreate();
    }

    public static JdbcSourceBuilder jdbcSource() {
        return jdbcSource((String) null);
    }

    public static JdbcSourceBuilder jdbcSource(final String name) {
        return new JdbcSourceBuilder(resolveName("JdbcSource", name));
    }

    public static JdbcSourceBuilder jdbcSource(final TransformContext transformContext) {
        return new JdbcSourceBuilder(transformContext);
    }

    public TransformContinuation then() {
        return continuation(
                transformContext, builderSupport.transformNode(), () -> getOrCreate().output());
    }

    public JdbcSourceBuilder initialSize(final int initialSize) {
        this.initialSize = initialSize;
        return this;
    }

    public JdbcSourceBuilder chunkSize(final int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    public JdbcSourceBuilder batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public JdbcSourceBuilder withNamer(final JdbcToFieldNamer namer) {
        this.jdbcToFieldNamer = requireNonNullElse(namer, DEFAULT_NAMER);
        return this;
    }

    public JdbcSourceBuilder register(
            final Map<String, Supplier<JdbcFieldBinding>> nameToSupplierMap) {
        bindingProvider.register(nameToSupplierMap);
        return this;
    }

    public JdbcSourceBuilder register(
            final String columnName, final Supplier<JdbcFieldBinding> bindingSupplier) {
        bindingProvider.register(columnName, bindingSupplier);
        return this;
    }

    private JdbcSource internalBuild() {
        return new JdbcSource(createSchemaBuilder(), batchSize);
    }

    private JdbcSourceSchemaBuilder createSchemaBuilder() {
        return new JdbcSourceSchemaBuilder(
                name,
                bindingProvider,
                jdbcToFieldNamer,
                matrixStoreFieldFactory(initialSize, chunkSize, x -> {}));
    }
}
