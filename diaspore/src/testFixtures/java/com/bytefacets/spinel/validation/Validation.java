// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.validation;

import com.bytefacets.spinel.schema.Metadata;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

public final class Validation {
    private final ValidationOperator operator;
    private final ChangeSet expectation;
    private final AtomicBoolean activeValidation;

    Validation(final ValidationOperator operator, final ChangeSet expectation, final AtomicBoolean activeValidation) {
        this.operator = requireNonNull(operator, "operator");
        this.expectation = requireNonNull(expectation, "expectation");
        this.activeValidation = requireNonNull(activeValidation, "activeValidation");
    }

    public Validation nullSchema() {
        expectation.nullSchema();
        return this;
    }

    public Validation schema(final Map<String, Class<?>> fieldMap) {
        expectation.schema(fieldMap);
        return this;
    }

    public Validation metadata(final Map<String, Metadata> metadata) {
        expectation.metadata(metadata);
        return this;
    }

    public Validation added(final Key key, final RowData row) {
        expectation.added(key, row);
        return this;
    }

    public Validation changed(final Key key, final RowData row) {
        expectation.changed(key, row);
        return this;
    }

    public Validation removed(final Key key) {
        expectation.removed(key);
        return this;
    }

    public void validate() {
        activeValidation.set(false);
        operator.validate(expectation);
    }
}
