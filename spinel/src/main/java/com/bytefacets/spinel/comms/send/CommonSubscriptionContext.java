// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms.send;

import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.comms.SubscriptionConfig;
import com.bytefacets.spinel.comms.subscription.ModificationRequest;
import java.util.List;

/**
 * Can be used by a SubscriptionProvider to encapsulate the context of a new Subscription request.
 *
 * @see SubscriptionFactory
 * @see DefaultSubscriptionProvider
 */
public interface CommonSubscriptionContext {
    ConnectedSessionInfo sessionInfo();

    SubscriptionConfig subscriptionConfig();

    List<ModificationRequest> initialModifications();

    TransformOutput output();

    ModificationHandlerRegistry modificationHandler();
}
