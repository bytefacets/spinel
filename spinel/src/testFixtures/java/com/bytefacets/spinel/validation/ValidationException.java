// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.validation;

import java.util.List;

public final class ValidationException extends RuntimeException {
    private static final String DELIM = "\n\t--> ";
    public ValidationException(final List<String> errors) {
        super("Validation errors occurred (" + errors.size() + "):" + DELIM +
                String.join(DELIM, errors));
    }
}
