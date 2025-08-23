// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms.receive;

import com.bytefacets.spinel.comms.send.ModificationResponse;
import com.bytefacets.spinel.comms.subscription.ModificationRequest;

public interface SubscriptionListener {
    default void onConnected() {}

    default void onDisconnected() {}

    default void onModificationAddResponse(
            ModificationRequest request, ModificationResponse response) {}

    default void onModificationRemoveResponse(
            ModificationRequest request, ModificationResponse response) {}
}
