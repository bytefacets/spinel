// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.comms;

import static java.util.Objects.requireNonNull;

public record ConnectionInfo(String name, Object endpoint) {
    public ConnectionInfo(final String name, final Object endpoint) {
        this.name = requireNonNull(name, "name");
        this.endpoint = requireNonNull(endpoint, "endpoint");
    }
}
