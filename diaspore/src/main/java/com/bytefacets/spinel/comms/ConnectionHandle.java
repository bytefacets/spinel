// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms;

public interface ConnectionHandle {
    void disconnect();

    void connect();

    boolean isConnected();
}
