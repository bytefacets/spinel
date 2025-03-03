// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.exception;

public final class SchemaNotBoundException extends RuntimeException {

    private SchemaNotBoundException(final String message) {
        super(message);
    }

    public static SchemaNotBoundException schemaNotBound() {
        return new SchemaNotBoundException("Schema is not bound");
    }

    public static SchemaNotBoundException schemaNotBound(final String attemptedOperation) {
        return new SchemaNotBoundException("Schema is not bound while " + attemptedOperation);
    }
}
