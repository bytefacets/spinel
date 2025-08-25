// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms.send;

import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.comms.SubscriptionConfig;
import com.bytefacets.spinel.comms.subscription.ModificationRequest;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.function.Supplier;

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
            final ConnectedSessionInfo sessionInfo,
            final SubscriptionConfig config,
            final List<ModificationRequest> initialModifications) {
        final TransformOutput output = registry.lookup(config.remoteOutputName());
        if (output == null) {
            return null;
        }
        return subscriptionFactory.create(
                new Context(
                        sessionInfo,
                        config,
                        initialModifications,
                        output,
                        modificationHandlerSupplier.get()));
    }

    private record Context(
            ConnectedSessionInfo sessionInfo,
            SubscriptionConfig subscriptionConfig,
            List<ModificationRequest> initialModifications,
            TransformOutput output,
            ModificationHandlerRegistry modificationHandler)
            implements CommonSubscriptionContext {}
}
