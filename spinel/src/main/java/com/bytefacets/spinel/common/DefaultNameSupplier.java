// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.common;

import static java.util.Objects.requireNonNullElseGet;

import jakarta.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utility class used in Transform building to auto-assign names when operators are not given one by
 * th author.
 */
public final class DefaultNameSupplier {
    private static final Map<String, AtomicInteger> nextIdMap = new HashMap<>();

    private DefaultNameSupplier() {}

    public static String resolveName(final String prefix, final @Nullable String name) {
        return requireNonNullElseGet(name, () -> defaultPrefixedName(prefix));
    }

    public static String defaultPrefixedName(final String prefix) {
        synchronized (nextIdMap) {
            final int id =
                    nextIdMap.computeIfAbsent(prefix, k -> new AtomicInteger()).getAndIncrement();
            return String.format("%s-%08d", prefix, id);
        }
    }
}
