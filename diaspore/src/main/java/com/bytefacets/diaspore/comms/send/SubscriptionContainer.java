// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.comms.send;

import com.bytefacets.diaspore.comms.subscription.ModificationRequest;
import com.bytefacets.diaspore.transform.OutputProvider;

public interface SubscriptionContainer extends OutputProvider {
    ModificationResponse apply(ModificationRequest update);

    void terminateSubscription();
}
