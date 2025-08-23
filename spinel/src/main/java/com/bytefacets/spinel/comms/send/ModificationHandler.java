// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms.send;

import com.bytefacets.spinel.comms.subscription.ModificationRequest;

/**
 * A modification handler for an individual subscription container. The ModificationRequest is an
 * instruction to modify the internal components in a subscription container, such as a filter or a
 * projection.
 */
public interface ModificationHandler {
    ModificationResponse add(ModificationRequest descriptor);

    ModificationResponse remove(ModificationRequest descriptor);
}
