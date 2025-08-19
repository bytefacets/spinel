// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.grpc.receive;

import static com.bytefacets.diaspore.common.DefaultNameSupplier.resolveName;
import static com.bytefacets.diaspore.schema.MatrixStoreFieldFactory.matrixStoreFieldFactory;
import static com.bytefacets.diaspore.transform.BuilderSupport.builderSupport;
import static com.bytefacets.diaspore.transform.TransformContext.continuation;
import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.comms.SubscriptionConfig;
import com.bytefacets.diaspore.transform.BuilderSupport;
import com.bytefacets.diaspore.transform.TransformContext;
import com.bytefacets.diaspore.transform.TransformContinuation;
import javax.annotation.Nullable;

/**
 * A builder for a gRPC source, which is an adapter that uses a GrpcClient to subscribe to an output
 * from a gRPC server.
 */
public final class GrpcSourceBuilder {
    private final BuilderSupport<GrpcSource> builderSupport;
    private final TransformContext transformContext;
    private int chunkSize = 256;
    private int initialSize = 256;
    private final GrpcClient client;
    private SubscriptionConfig subscription;

    private GrpcSourceBuilder(final GrpcClient client, final String name) {
        this.client = requireNonNull(client, "client");
        this.builderSupport = builderSupport(name, this::internalBuild);
        this.transformContext = null;
    }

    private GrpcSourceBuilder(final GrpcClient client, final TransformContext context) {
        this.client = requireNonNull(client, "client");
        this.transformContext = requireNonNull(context, "transform context");
        this.builderSupport = context.createBuilderSupport(this::internalBuild, null);
    }

    public static GrpcSourceBuilder grpcSource(final GrpcClient client) {
        return grpcSource(client, (String) null);
    }

    public static GrpcSourceBuilder grpcSource(
            final GrpcClient client, final @Nullable String name) {
        return new GrpcSourceBuilder(client, resolveName("GrpcSource", name));
    }

    public static GrpcSourceBuilder grpcSource(
            final GrpcClient client, final TransformContext transformContext) {
        return new GrpcSourceBuilder(client, transformContext);
    }

    public GrpcSource getOrCreate() {
        return builderSupport.getOrCreate();
    }

    public GrpcSource build() {
        return builderSupport.createOperator();
    }

    public TransformContinuation then() {
        return continuation(
                transformContext, builderSupport.transformNode(), () -> getOrCreate().output());
    }

    private GrpcSource internalBuild() {
        final var factory = matrixStoreFieldFactory(initialSize, chunkSize, i -> {});
        final Subscription subscription =
                client.createSubscription(new SchemaBuilder(factory), this.subscription);
        return new GrpcSource(client.connectionInfo(), client.connection(), subscription);
    }

    public GrpcSourceBuilder subscription(final SubscriptionConfig config) {
        this.subscription = requireNonNull(config, "config");
        return this;
    }

    public GrpcSourceBuilder chunkSize(final int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    public GrpcSourceBuilder initialSize(final int initialSize) {
        this.initialSize = initialSize;
        return this;
    }
}
