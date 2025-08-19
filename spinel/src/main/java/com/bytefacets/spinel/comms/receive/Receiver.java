// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms.receive;

import com.bytefacets.spinel.comms.ConnectionHandle;
import com.bytefacets.spinel.comms.ConnectionInfo;

public interface Receiver {

    ConnectionHandle connection();

    ConnectionInfo connectionInfo();
}
