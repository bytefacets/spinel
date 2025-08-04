// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.comms.send;

public interface ConnectedSessionInfo {
    String getTenant();

    String getUser();

    String getRemote();
}
