// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms.send;

import com.bytefacets.spinel.comms.subscription.ChangeDescriptor;
import com.bytefacets.spinel.comms.subscription.ModificationRequest;
import java.util.HashMap;
import java.util.Map;

/**
 * A registry of handlers per target for a specific SubscriptionContainer. This enables the server
 * to configure handlers that target different components in a subscription, such as modifications
 * which target a Filter or a Projection.
 */
public final class ModificationHandlerRegistry implements ModificationHandler {
    private final Map<String, ModificationHandler> handlers = new HashMap<>(4);

    public static ModificationHandlerRegistry modificationHandlerRegistry() {
        return new ModificationHandlerRegistry();
    }

    private ModificationHandlerRegistry() {}

    public void register(final String target, final ModificationHandler handler) {
        synchronized (handlers) {
            handlers.put(target, handler);
        }
    }

    public void unregister(final String target) {
        synchronized (handlers) {
            handlers.remove(target);
        }
    }

    public void clear() {
        synchronized (handlers) {
            handlers.clear();
        }
    }

    @Override
    public ModificationResponse apply(final ModificationRequest modificationRequest) {
        if (modificationRequest instanceof ChangeDescriptor descriptor) {
            final ModificationHandler handler = getHandler(descriptor.target());
            if (handler == null) {
                return new ModificationResponse(
                        false, "Target not found: " + descriptor.target(), null);
            }
            return handler.apply(descriptor);
        } else {
            return ModificationResponse.MODIFICATION_NOT_UNDERSTOOD;
        }
    }

    private ModificationHandler getHandler(final String target) {
        synchronized (handlers) {
            return handlers.get(target);
        }
    }
}
