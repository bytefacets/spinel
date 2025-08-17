// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.comms.send;

final class SubscriptionException extends RuntimeException {
    SubscriptionException(final String message) {
        super(message);
    }

    static SubscriptionException subscriptionNotFound(
            final ConnectedSessionInfo session, final String name) {
        return new SubscriptionException(
                String.format(
                        "Subscription not found: user=%s, output=%s", session.getUser(), name));
    }
}
