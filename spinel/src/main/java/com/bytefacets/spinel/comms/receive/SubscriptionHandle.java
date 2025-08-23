// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms.receive;

import com.bytefacets.spinel.comms.subscription.ModificationRequest;

/**
 * A handle to the underlying active subscription which allows limited runtime re-configuration.
 * Callbacks to the these methods are available in the {@link
 * com.bytefacets.spinel.comms.receive.SubscriptionListener}
 *
 * @see com.bytefacets.spinel.comms.subscription.ModificationRequestFactory
 */
public interface SubscriptionHandle {
    void add(ModificationRequest request);

    void remove(ModificationRequest request);
}
