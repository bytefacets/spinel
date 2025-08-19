// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.exception;

public final class TableModificationException extends RuntimeException {
    private TableModificationException(final String message) {
        super(message);
    }

    public static TableModificationException expectedRow(final String operation) {
        final var msg =
                String.format("Expected a row in progress during %s, but none was", operation);
        return new TableModificationException(msg);
    }

    public static TableModificationException expectedNoRow(final int row, final String operation) {
        final var msg =
                String.format(
                        "Expected no row in progress during %s, but there was: %d", operation, row);
        return new TableModificationException(msg);
    }

    public static TableModificationException expectedChangeInProgress(
            final int row, final String operation) {
        final var msg =
                String.format(
                        "Expected change in progress during %s, but was not for row %d",
                        operation, row);
        return new TableModificationException(msg);
    }

    public static TableModificationException expectedNoChangeInProgress(
            final int row, final String operation) {
        final var msg =
                String.format(
                        "Expected no change in progress during %s, but was not for row %d",
                        operation, row);
        return new TableModificationException(msg);
    }
}
