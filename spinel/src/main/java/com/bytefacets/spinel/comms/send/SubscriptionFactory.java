// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms.send;

/**
 * Used to configure a @{link {@link DefaultSubscriptionProvider}}. This can be used to intercept
 * subscriptions. See the PermissionFilter in the examples for an example of an implementation of a
 * custom SubscriptionFactory that applies permissioning to the output.
 */
public interface SubscriptionFactory {
    SubscriptionContainer create(CommonSubscriptionContext subscriptionContext);
}
