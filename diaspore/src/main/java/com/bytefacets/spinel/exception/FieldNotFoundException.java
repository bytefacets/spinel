// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.exception;

public class FieldNotFoundException extends RuntimeException {
    public static FieldNotFoundException fieldNotFound(final String message) {
        return new FieldNotFoundException(message);
    }

    public static FieldNotFoundException fieldNotFound(
            final String fieldName, final String schemaName) {
        return new FieldNotFoundException(
                String.format("Field '%s' not found in schema '%s'", fieldName, schemaName));
    }

    public static FieldNotFoundException fieldNotFound(
            final String fieldName, final String referencedBy, final String schemaName) {
        return new FieldNotFoundException(
                String.format(
                        "Field '%s' referenced by %s not found in schema '%s'",
                        fieldName, referencedBy, schemaName));
    }

    public FieldNotFoundException(final String message) {
        super(message);
    }
}
