// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.comms.send;

import static com.bytefacets.diaspore.comms.send.DefaultSubscriptionContainer.defaultSubscriptionContainer;
import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.TransformOutput;
import com.bytefacets.diaspore.comms.SubscriptionConfig;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * Creates a DefaultSubscriptionContainer for each subscription.
 *
 * @see DefaultSubscriptionContainer
 */
public final class DefaultSubscriptionProvider implements SubscriptionProvider {
    private final OutputRegistry registry;
    private final Supplier<ModificationHandlerRegistry> modificationHandlerSupplier;

    public static DefaultSubscriptionProvider defaultSubscriptionProvider(
            final OutputRegistry registry) {
        return defaultSubscriptionProvider(
                registry, ModificationHandlerRegistry::modificationHandlerRegistry);
    }

    public static DefaultSubscriptionProvider defaultSubscriptionProvider(
            final OutputRegistry registry,
            final Supplier<ModificationHandlerRegistry> modificationHandlerSupplier) {
        return new DefaultSubscriptionProvider(registry, modificationHandlerSupplier);
    }

    DefaultSubscriptionProvider(
            final OutputRegistry registry,
            final Supplier<ModificationHandlerRegistry> modificationHandler) {
        this.registry = requireNonNull(registry, "registry");
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
        return defaultSubscriptionContainer(
                sessionInfo, config, output, modificationHandlerSupplier.get());
    }
}
