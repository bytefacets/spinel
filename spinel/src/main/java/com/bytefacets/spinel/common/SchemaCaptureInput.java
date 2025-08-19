// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.common;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.spinel.TransformInput;
import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.schema.ChangedFieldSet;
import com.bytefacets.spinel.schema.Schema;
import java.util.function.Consumer;
import javax.annotation.Nullable;

/** A utility class for capturing a schema from an output when it is set or unset. */
public final class SchemaCaptureInput implements TransformInput {
    private final Consumer<Schema> schemaConsumer;
    private TransformOutput source;
    private Schema schema;

    private SchemaCaptureInput(final Consumer<Schema> schemaConsumer) {
        this.schemaConsumer = requireNonNull(schemaConsumer, "schemaConsumer");
    }

    public static SchemaCaptureInput schemaCaptureInput(final Consumer<Schema> schemaConsumer) {
        return new SchemaCaptureInput(schemaConsumer);
    }

    public TransformOutput source() {
        return source;
    }

    public Schema schema() {
        return schema;
    }

    @Override
    public void setSource(@Nullable final TransformOutput output) {
        this.source = output;
    }

    @Override
    public void schemaUpdated(@Nullable final Schema schema) {
        this.schema = schema;
        schemaConsumer.accept(schema);
    }

    @Override
    public void rowsAdded(final IntIterable rows) {}

    @Override
    public void rowsChanged(final IntIterable rows, final ChangedFieldSet changedFields) {}

    @Override
    public void rowsRemoved(final IntIterable rows) {}
}
