// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.exception;

public final class DuplicateKeyException extends RuntimeException {
    public DuplicateKeyException(final String message) {
        super(message);
    }

    public static DuplicateKeyException duplicateKeyException(
            final Class<?> clazz, final String name, final Object key) {
        return new DuplicateKeyException(
                String.format("Duplicate key in %s \"%s\": %s", clazz.getSimpleName(), name, key));
    }
}
