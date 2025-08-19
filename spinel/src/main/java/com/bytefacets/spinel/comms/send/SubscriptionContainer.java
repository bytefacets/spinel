// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms.send;

import com.bytefacets.spinel.comms.subscription.ModificationRequest;
import com.bytefacets.spinel.transform.OutputProvider;

public interface SubscriptionContainer extends OutputProvider {
    ModificationResponse apply(ModificationRequest update);

    void terminateSubscription();
}
