// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.common;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.spinel.RowProvider;
import com.bytefacets.spinel.TransformInput;
import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.schema.ChangedFieldSet;
import com.bytefacets.spinel.schema.Schema;
import java.util.ArrayList;
import java.util.List;
import jakarta.annotation.Nullable;

/**
 * Operators use the OutputManager to standardize management of notifications to the inputs
 * connected to their outputs.
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
public final class OutputManager implements InputNotifier {
    private final List<TransformInput> subscriptions = new ArrayList<>(2);
    private final TransformOutput output;
    private final ArrayList<TransformInput> iterable = new ArrayList<>(2);
    private Schema schema;

    public static OutputManager outputManager(final RowProvider rowProvider) {
        return new OutputManager(rowProvider);
    }

    private OutputManager(final RowProvider rowProvider) {
        output = new Output(rowProvider);
    }

    public TransformOutput output() {
        return output;
    }

    public Schema schema() {
        return schema;
    }

    private void copyIterable() {
        iterable.clear();
        iterable.ensureCapacity(subscriptions.size());
        subscriptions.forEach(iterable::add); // addAll has an allocation of Object[]
    }

    /**
     * Adds an input to list of subscribed inputs if it was not already registered. If it is a new
     * input, it initializes the input by setting the source, then the schema if available, and the
     * then the rows from the rowProvider.
     *
     * @see #initializeSubscription(TransformOutput, TransformInput)
     */
    void addInput(final TransformInput input) {
        if (!subscriptions.contains(input)) {
            subscriptions.add(input);
            initializeSubscription(output, input);
        }
    }

    /**
     * Removes the input from the registered inputs, and does nothing if the input was not
     * registered.
     */
    void removeInput(final TransformInput input) {
        if (subscriptions.remove(input)) {
            terminateSubscription(input);
        }
    }

    public void updateSchema(final Schema schema) {
        this.schema = schema;
        copyIterable();
        for (TransformInput input : iterable) {
            input.schemaUpdated(schema);
        }
    }

    @Override
    public void notifyAdds(final IntIterable rows) {
        assertSchema();
        copyIterable();
        for (int i = 0, size = iterable.size(); i < size; i++) {
            iterable.get(i).rowsAdded(rows);
        }
    }

    @Override
    public void notifyChanges(final IntIterable rows, final ChangedFieldSet changedFields) {
        assertSchema();
        copyIterable();
        for (int i = 0, size = iterable.size(); i < size; i++) {
            iterable.get(i).rowsChanged(rows, changedFields);
        }
    }

    @Override
    public void notifyRemoves(final IntIterable rows) {
        assertSchema();
        copyIterable();
        for (int i = 0, size = iterable.size(); i < size; i++) {
            iterable.get(i).rowsRemoved(rows);
        }
    }

    private void initializeSubscription(final TransformOutput output, final TransformInput input) {
        input.setSource(output);
        final var schema = output.schema();
        if (schema != null) {
            input.schemaUpdated(schema);
            input.rowsAdded(output.rowProvider());
        }
    }

    private void terminateSubscription(final TransformInput input) {
        input.schemaUpdated(null);
        input.setSource(null);
    }

    private void assertSchema() {
        if (schema == null) {
            throw new IllegalStateException("Attempted notification before setting schema.");
        }
    }

    private final class Output implements TransformOutput {
        private final RowProvider rowProvider;

        private Output(final RowProvider rowProvider) {
            this.rowProvider = requireNonNull(rowProvider, "rowProvider");
        }

        @Override
        public void attachInput(final TransformInput input) {
            addInput(input);
        }

        @Override
        public @Nullable Schema schema() {
            return schema;
        }

        @Override
        public RowProvider rowProvider() {
            return rowProvider;
        }

        @Override
        public void detachInput(final TransformInput input) {
            removeInput(input);
        }
    }
}
