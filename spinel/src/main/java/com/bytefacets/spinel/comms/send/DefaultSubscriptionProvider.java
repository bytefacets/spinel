// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms.send;

import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.comms.SubscriptionConfig;
import java.util.function.Supplier;
import jakarta.annotation.Nullable;

/**
 * Creates a DefaultSubscriptionContainer for each subscription.
 *
 * @see DefaultSubscriptionContainer
 */
public final class DefaultSubscriptionProvider implements SubscriptionProvider {
    private final OutputRegistry registry;
    private final Supplier<ModificationHandlerRegistry> modificationHandlerSupplier;
    private final SubscriptionFactory subscriptionFactory;

    public static DefaultSubscriptionProvider defaultSubscriptionProvider(
            final OutputRegistry registry) {
        return defaultSubscriptionProvider(
                registry, ModificationHandlerRegistry::modificationHandlerRegistry);
    }

    public static DefaultSubscriptionProvider defaultSubscriptionProvider(
            final OutputRegistry registry,
            final Supplier<ModificationHandlerRegistry> modificationHandlerSupplier) {
        return new DefaultSubscriptionProvider(
                registry,
                modificationHandlerSupplier,
                DefaultSubscriptionContainer::defaultSubscriptionContainer);
    }

    public static DefaultSubscriptionProvider defaultSubscriptionProvider(
            final OutputRegistry registry,
            final SubscriptionFactory subscriptionFactory,
            final Supplier<ModificationHandlerRegistry> modificationHandlerSupplier) {
        return new DefaultSubscriptionProvider(
                registry, modificationHandlerSupplier, subscriptionFactory);
    }

    public static DefaultSubscriptionProvider defaultSubscriptionProvider(
            final OutputRegistry registry, final SubscriptionFactory subscriptionFactory) {
        return new DefaultSubscriptionProvider(
                registry,
                ModificationHandlerRegistry::modificationHandlerRegistry,
                subscriptionFactory);
    }

    DefaultSubscriptionProvider(
            final OutputRegistry registry,
            final Supplier<ModificationHandlerRegistry> modificationHandler,
            final SubscriptionFactory subscriptionFactory) {
        this.registry = requireNonNull(registry, "registry");
        this.subscriptionFactory = requireNonNull(subscriptionFactory, "subscriptionFactory");
        this.modificationHandlerSupplier =
                requireNonNull(modificationHandler, "modificationHandler");
    }

    @Override
    public @Nullable SubscriptionContainer getSubscription(
            final ConnectedSessionInfo sessionInfo, final SubscriptionConfig config) {
        final TransformOutput output = registry.lookup(config.remoteOutputName());
        if (output == null) {
            return null;
        }
        return subscriptionFactory.create(
                new Context(sessionInfo, config, output, modificationHandlerSupplier.get()));
    }

    private record Context(
            ConnectedSessionInfo sessionInfo,
            SubscriptionConfig subscriptionConfig,
            TransformOutput output,
            ModificationHandlerRegistry modificationHandler)
            implements CommonSubscriptionContext {}
}
