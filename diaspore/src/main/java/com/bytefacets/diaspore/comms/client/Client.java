// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.comms.client;

import java.util.List;

public interface Client {

    ConnectionHandle connection();

    ConnectionInfo connectionInfo();

    Subscription createSubscription(String outputName, List<String> fields);
}
