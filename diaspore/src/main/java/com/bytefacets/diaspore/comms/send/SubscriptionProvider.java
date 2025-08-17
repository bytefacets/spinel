// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.comms.send;

import com.bytefacets.diaspore.comms.SubscriptionConfig;
import javax.annotation.Nullable;

public interface SubscriptionProvider {
    /** Gets the subscription */
    @Nullable
    SubscriptionContainer getSubscription(
            ConnectedSessionInfo sessionInfo, SubscriptionConfig config);
}
