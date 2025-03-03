// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.exception;

public final class KeyException extends RuntimeException {
    public KeyException(final String message) {
        super(message);
    }

    public static KeyException unknownKeyException(
            final Class<?> clazz, final String name, final Object key) {
        return new KeyException(
                String.format("Unknown key in %s \"%s\": %s", clazz.getSimpleName(), name, key));
    }
}
