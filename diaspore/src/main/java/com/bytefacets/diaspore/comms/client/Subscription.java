// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.comms.client;

import com.bytefacets.diaspore.transform.OutputProvider;

public interface Subscription extends OutputProvider {
    Client client();
}
