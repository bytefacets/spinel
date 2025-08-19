// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms.send;

public record ModificationResponse(boolean success, String message, Exception exception) {
    public static final ModificationResponse SUCCESS = new ModificationResponse(true, "", null);
    public static final ModificationResponse NOT_MODIFIABLE =
            new ModificationResponse(false, "Subscription is not modifiable", null);
    public static final ModificationResponse MODIFICATION_NOT_UNDERSTOOD =
            new ModificationResponse(false, "Modification not understood", null);

    public static ModificationResponse failureResponse(final Exception exception) {
        return new ModificationResponse(false, "Exception", exception);
    }

    public static ModificationResponse failureResponse(
            final String message, final Exception exception) {
        return new ModificationResponse(false, message, exception);
    }
}
