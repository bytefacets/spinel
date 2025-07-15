// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.comms.receive;

import com.bytefacets.diaspore.transform.OutputProvider;

public interface Subscription extends OutputProvider {
    Receiver receiver();
}
