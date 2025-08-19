// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms.subscription;

public interface ModificationRequest {
    String target();

    String action();

    Object[] arguments();
}
