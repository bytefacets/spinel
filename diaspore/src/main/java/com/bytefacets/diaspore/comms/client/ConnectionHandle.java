// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.comms.client;

public interface ConnectionHandle {
    void disconnect();

    void connect();

    boolean isConnected();
}
