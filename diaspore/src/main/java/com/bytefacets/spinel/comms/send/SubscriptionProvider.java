// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms.send;

import com.bytefacets.spinel.comms.SubscriptionConfig;
import javax.annotation.Nullable;

public interface SubscriptionProvider {
    /** Gets the subscription */
    @Nullable
    SubscriptionContainer getSubscription(
            ConnectedSessionInfo sessionInfo, SubscriptionConfig config);
}
