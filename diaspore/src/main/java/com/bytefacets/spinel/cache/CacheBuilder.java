// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.cache;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public final class CacheBuilder {
    private final Set<String> cacheFieldNames = new LinkedHashSet<>();
    private int initialSize = 128;
    private int chunkSize = 128;

    private CacheBuilder() {}

    public static CacheBuilder cache() {
        return new CacheBuilder();
    }

    public Cache build() {
        return new Cache(cacheFieldNames, initialSize, chunkSize);
    }

    public CacheBuilder cacheFields(final String... fieldNames) {
        Collections.addAll(cacheFieldNames, fieldNames);
        return this;
    }

    public CacheBuilder initialSize(final int initialSize) {
        this.initialSize = initialSize;
        return this;
    }

    public CacheBuilder chunkSize(final int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    // VisibleForTests
    Set<String> cacheFields() {
        return Set.copyOf(cacheFieldNames);
    }
}
