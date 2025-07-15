// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.comms.receive;

import com.bytefacets.diaspore.comms.ConnectionHandle;
import com.bytefacets.diaspore.comms.ConnectionInfo;

public interface Receiver {

    ConnectionHandle connection();

    ConnectionInfo connectionInfo();
}
