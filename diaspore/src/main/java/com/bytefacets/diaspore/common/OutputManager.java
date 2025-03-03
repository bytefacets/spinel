// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.common;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.diaspore.TransformInput;
import com.bytefacets.diaspore.TransformOutput;
import com.bytefacets.diaspore.RowProvider;
import com.bytefacets.diaspore.schema.ChangedFieldSet;
import com.bytefacets.diaspore.schema.Schema;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

public final class OutputManager implements InputNotifier {
    private final List<TransformInput> subscriptions = new ArrayList<>();
    private final TransformOutput output;
    private final ArrayList<TransformInput> iteratable = new ArrayList<>();
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
        iteratable.clear();
        iteratable.ensureCapacity(subscriptions.size());
        iteratable.addAll(subscriptions);
    }

    public void addInput(final TransformInput input) {
        if (!subscriptions.contains(input)) {
            subscriptions.add(input);
            initializeSubscription(output, input);
        }
    }

    public void removeInput(final TransformInput input) {
        if (subscriptions.remove(input)) {
            terminateSubscription(input);
        }
    }

    public void updateSchema(final Schema schema) {
        this.schema = schema;
        copyIterable();
        for (TransformInput input : iteratable) {
            input.schemaUpdated(schema);
        }
    }

    @Override
    public void notifyAdds(final IntIterable rows) {
        copyIterable();
        for (TransformInput input : iteratable) {
            input.rowsAdded(rows);
        }
    }

    @Override
    public void notifyChanges(final IntIterable rows, final ChangedFieldSet changedFields) {
        copyIterable();
        for (TransformInput input : iteratable) {
            input.rowsChanged(rows, changedFields);
        }
    }

    @Override
    public void notifyRemoves(final IntIterable rows) {
        copyIterable();
        for (TransformInput input : iteratable) {
            input.rowsRemoved(rows);
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
