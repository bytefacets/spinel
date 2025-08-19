// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.transform;

public final class TransformException extends RuntimeException {
    public TransformException(final String message) {
        super(message);
    }

    static TransformException notFound(final String name) {
        return new TransformException("Operator not found: " + name);
    }

    static TransformException notAnOutputProvider(final String name, final Object operator) {
        return new TransformException(
                String.format(
                        "Requested operator is not an OutputProvider: %s is %s",
                        name, operator.getClass().getName()));
    }

    static TransformException duplicate(final String name) {
        return new TransformException(
                String.format("Different operators registered with the same name: %s", name));
    }
}
