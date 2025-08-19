// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.exception;

public class OperatorSetupException extends RuntimeException {
    public static OperatorSetupException setupException(final String message) {
        return new OperatorSetupException(message);
    }

    public OperatorSetupException(final String message) {
        super(message);
    }
}
