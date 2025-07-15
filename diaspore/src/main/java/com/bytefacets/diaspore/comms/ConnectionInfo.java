// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.comms;

import static java.util.Objects.requireNonNull;

import java.net.URI;

public record ConnectionInfo(String name, URI uri) {
    public ConnectionInfo(final String name, final URI uri) {
        this.name = requireNonNull(name, "name");
        this.uri = requireNonNull(uri, "uri");
    }
}
