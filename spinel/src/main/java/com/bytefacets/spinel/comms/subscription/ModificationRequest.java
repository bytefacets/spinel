// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms.subscription;

/**
 * Describes a change to a subscription.
 *
 * @see ModificationRequestFactory
 */
public interface ModificationRequest {
    /**
     * The named "target" within the server-side SubscriptionContainer. For example, in the {@link
     * com.bytefacets.spinel.comms.send.DefaultSubscriptionContainer}, you can only target "filter"
     * ({@link ModificationRequestFactory.Target#FILTER}).
     */
    String target();

    /**
     * An optional action if you want to distinguish among different things you can do to a target.
     * The action would typically not be "add" or "remove" because that is something you do on the
     * request as a whole. In other words, if I sent a request that was added to the state
     * server-side, I want to be able to send the exact same request when I go to remove it.
     */
    String action();

    /**
     * Arguments to the request. Objects in here may need to have registered encoders in the grpc
     * ObjectEncoderRegistry and ObjectDecoderRegistry for example, if the Object is not covered by
     * the default types (Number types and String).
     */
    Object[] arguments();
}
